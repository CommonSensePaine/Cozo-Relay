use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{timeout, Duration};
use thiserror::Error;
use futures_util::{StreamExt, SinkExt};
use tracing::{info};
use axum::{Router, routing::get, extract::State};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};

use cozo_repo::RelayRepo;
use nostr_model::{NostrMsg, NostrEvent, NostrFilter};
use relay_settings::Settings;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("bind error")] Bind,
}

pub async fn run_ws(repo: Arc<dyn RelayRepo>, settings: Settings, mut shutdown: broadcast::Receiver<()>) -> Result<(), WsError> {
    let addr: SocketAddr = format!("{}:{}", settings.ws.address, settings.ws.port)
        .parse()
        .map_err(|_| WsError::Bind)?;
    info!("ws listening on {}", addr);
    let app = Router::new()
        .route("/", get(ws_route))
        .route("/*path", get(ws_route))
        .with_state((repo.clone(), settings.ws.max_event_bytes, settings.limits.messages_per_sec));
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|_| WsError::Bind)?;
    tokio::select! {
        res = axum::serve(listener, app) => { res.map_err(|_| WsError::Bind)?; },
        _ = shutdown.recv() => {}
    }
    Ok(())
}

async fn ws_route(State((repo, max_event_bytes, msgs_per_sec)): State<(Arc<dyn RelayRepo>, Option<usize>, Option<u32>)>, ws: WebSocketUpgrade) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, repo, max_event_bytes, msgs_per_sec))
}

async fn handle_socket(socket: WebSocket, repo: Arc<dyn RelayRepo>, max_event_bytes: Option<usize>, msgs_per_sec: Option<u32>) {
    info!("ws: connection open");
    let mut subs: HashMap<String, Vec<NostrFilter>> = HashMap::new();
    // split socket for concurrent send/recv
    let (sender_raw, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender_raw));
    // global broadcast subscription
    let mut global_rx = repo.subscribe();
    // simple rate limit window
    let window = Duration::from_secs(1);
    let max_msgs = msgs_per_sec.unwrap_or(100) as usize;
    let mut window_start = tokio::time::Instant::now();
    let mut sent_in_window = 0usize;

    // forward task: relay globally broadcast events to this connection if match subs
    let mut subs_for_fwd = subs.clone();
    let sender_for_fwd = sender.clone();
    let forward = tokio::spawn(async move {
        loop {
            match global_rx.recv().await {
                Ok(ev) => {
                    // refresh local snapshot of subs every loop (cheap clone)
                    // Note: we cannot access 'subs' here after move, so we keep a local map that remains empty until first REQ.
                    for (sid, fs) in subs_for_fwd.iter() {
                        if fs.iter().any(|f| matches_filter(&ev, f)) {
                            let payload = serde_json::json!(["EVENT", sid, ev]);
                            let mut s = sender_for_fwd.lock().await; let _ = s.send(Message::Text(payload.to_string())).await;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    let idle = Duration::from_secs(30);
    let mut missed_pongs = 0u8;
    loop {
        let next_msg = timeout(idle, receiver.next()).await;
        let Some(msg_res) = (match next_msg {
            Ok(v) => v,
            Err(_) => {
                // idle: send ping, and if still idle next time, close
                let mut s = sender.lock().await; let _ = s.send(Message::Ping(Vec::new())).await;
                missed_pongs += 1;
                if missed_pongs >= 2 { break; }
                continue;
            }
        }) else { break; };
        let Ok(msg) = msg_res else { break; };
        if let Ok(txt) = msg.to_text() {
            info!("ws: recv text: {} bytes", txt.len());
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                match parse_msg(&val) {
                    Some(NostrMsg::Req { sub_id, filters }) => {
                        // query and stream back results, then EOSE
                        let limit = 100usize; // simple cap for MVP
                        subs.insert(sub_id.clone(), filters.clone());
                        // also update forward task's view
                        subs_for_fwd = subs.clone();
                        if let Ok(events) = repo.query_once(filters, limit).await {
                            info!("ws: query matched {} events", events.len());
                            for ev in events {
                                let payload = serde_json::json!(["EVENT", sub_id, ev]);
                                // rate-limit outgoing per window
                                if window_start.elapsed() >= window { window_start = tokio::time::Instant::now(); sent_in_window = 0; }
                                if sent_in_window < max_msgs { let mut s = sender.lock().await; let _ = s.send(Message::Text(payload.to_string())).await; sent_in_window += 1; }
                            }
                        }
                        let eose = serde_json::json!(["EOSE", sub_id]);
                        { let mut s = sender.lock().await; let _ = s.send(Message::Text(eose.to_string())).await; }
                    }
                    Some(NostrMsg::Event(evt)) => {
                        // minimal validation: size cap
                        let evt_len = serde_json::to_string(&evt).map(|s| s.len()).unwrap_or(0);
                        if let Some(max) = max_event_bytes { if evt_len > max { 
                            let ok = serde_json::json!(["OK", evt.id, false, "too large"]);
                            let mut s = sender.lock().await; let _ = s.send(Message::Text(ok.to_string())).await;
                            continue;
                        }}
                        // store and return OK, or overloaded
                        match repo.write_event(&evt).await {
                            Ok(()) => {
                                let ok = serde_json::json!(["OK", evt.id, true, "stored"]);
                                let mut s = sender.lock().await; let _ = s.send(Message::Text(ok.to_string())).await;
                            }
                            Err(_) => {
                                let ok = serde_json::json!(["OK", evt.id, false, "overloaded"]);
                                let mut s = sender.lock().await; let _ = s.send(Message::Text(ok.to_string())).await;
                            }
                        }
                        // push to matching live subscriptions on this connection
                        let ev = evt.clone();
                        for (sid, fs) in subs.iter() {
                            if fs.iter().any(|f| matches_filter(&ev, f)) {
                                let payload = serde_json::json!(["EVENT", sid, ev]);
                                if window_start.elapsed() >= window { window_start = tokio::time::Instant::now(); sent_in_window = 0; }
                                if sent_in_window < max_msgs { let mut s = sender.lock().await; let _ = s.send(Message::Text(payload.to_string())).await; sent_in_window += 1; }
                            }
                        }
                    }
                    Some(NostrMsg::Close { sub_id }) => {
                        subs.remove(&sub_id);
                        subs_for_fwd = subs.clone();
                    }
                    Some(NostrMsg::Auth(_) ) | None => {}
                }
            }
        } else {
            match msg {
                Message::Ping(p) => { let mut s = sender.lock().await; let _ = s.send(Message::Pong(p)).await; }
                Message::Pong(_) => { missed_pongs = 0; }
                Message::Close(_) => { break; }
                Message::Binary(_) => { /* ignore */ }
                Message::Text(_) => {}
            }
        }
    }
    info!("ws: connection closed");
    let _ = forward.abort();
}

fn parse_msg(v: &serde_json::Value) -> Option<NostrMsg> {
    let arr = v.as_array()?;
    let kind = arr.get(0)?.as_str()?;
    match kind {
        "REQ" => {
            let sub_id = arr.get(1)?.as_str()?.to_string();
            // parse filters from remaining array elements (NIP-01)
            let mut filters: Vec<NostrFilter> = Vec::new();
            for i in 2..arr.len() {
                if let Some(fv) = arr.get(i) {
                    if let Ok(f) = serde_json::from_value::<NostrFilter>(fv.clone()) {
                        filters.push(f);
                    }
                }
            }
            Some(NostrMsg::Req { sub_id, filters })
        }
        "EVENT" => {
            let evt_val = arr.get(1)?;
            let evt: NostrEvent = serde_json::from_value(evt_val.clone()).ok()?;
            Some(NostrMsg::Event(evt))
        }
        "CLOSE" => {
            let sub_id = arr.get(1)?.as_str()?.to_string();
            Some(NostrMsg::Close { sub_id })
        }
        "AUTH" => None,
        _ => None,
    }
}

fn matches_filter(e: &NostrEvent, f: &NostrFilter) -> bool {
    if let Some(authors) = &f.authors {
        if !authors.iter().any(|a| a == &e.pubkey) { return false; }
    }
    if let Some(kinds) = &f.kinds {
        if !kinds.iter().any(|k| *k == e.kind) { return false; }
    }
    if let Some(ids) = &f.ids {
        let id = &e.id;
        if !ids.iter().any(|p| id.starts_with(p)) { return false; }
    }
    if let Some(since) = f.since { if e.created_at < since { return false; } }
    if let Some(until) = f.until { if e.created_at > until { return false; } }
    if let Some(tag_filters) = &f.tags {
        for (name, vals) in tag_filters {
            let key = name.trim_start_matches('#');
            let mut ok = false;
            for t in &e.tags {
                if t.get(0).map(|s| s.as_str()) == Some(key) {
                    if let Some(val) = t.get(1) {
                        if vals.iter().any(|v| v == val) { ok = true; break; }
                    }
                }
            }
            if !ok { return false; }
        }
    }
    true
}
