use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use thiserror::Error;
use axum::{routing::{get, post}, extract::Query, Json, Router, http::StatusCode};
use tower_http::cors::CorsLayer;
use serde::Deserialize;
use tracing::{info, error};

use cozo_repo::RelayRepo;
use nostr_model::{relay_info_document, RelayInfoConfig, NostrEvent, NostrFilter};
use graph_cozo::types::{Worldview, GraphRankSettings, Timestamp, Scorecard};
use relay_settings::Settings;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("bind error")] Bind,
}

pub async fn run_http(repo: Arc<dyn RelayRepo>, settings: Settings, mut shutdown: broadcast::Receiver<()>) -> Result<(), HttpError> {
    let info_cfg = RelayInfoConfig::default();

    async fn nip11_handler(cfg: RelayInfoConfig) -> Json<serde_json::Value> {
        Json(relay_info_document(&cfg))
    }

    #[derive(Deserialize, Default)]
    struct EventQuery {
        authors: Option<String>,
        kinds: Option<String>,
        ids: Option<String>,
        since: Option<u64>,
        until: Option<u64>,
        cursor: Option<String>,
        limit: Option<usize>,
    }

    async fn events_handler(
        repo: Arc<dyn RelayRepo>,
        q: Query<EventQuery>,
    ) -> Json<Vec<NostrEvent>> {
        let mut filters = Vec::new();
        let mut f = NostrFilter::default();
        if let Some(a) = &q.authors { f.authors = Some(a.split(',').map(|s| s.to_string()).collect()); }
        if let Some(k) = &q.kinds { f.kinds = Some(k.split(',').filter_map(|s| s.parse::<u64>().ok()).collect()); }
        if let Some(i) = &q.ids { f.ids = Some(i.split(',').map(|s| s.to_string()).collect()); }
        f.since = q.since;
        // support composite cursor "created_at:id"; if only number given, treat as until
        let mut cursor_pair: Option<(u64, String)> = None;
        if let Some(cur) = &q.cursor {
            if let Some((a,b)) = cur.split_once(':') {
                if let Ok(ts) = a.parse::<u64>() { cursor_pair = Some((ts, b.to_string())); }
            } else if let Ok(ts) = cur.parse::<u64>() {
                // numeric only fallback
                f.until = Some(ts);
            }
        }
        if f.until.is_none() { f.until = q.until; }
        f.limit = q.limit;
        let limit = q.limit.unwrap_or(1000);
        filters.push(f);
        let mut events = repo.query_once(filters, limit.saturating_mul(2)).await.unwrap_or_default();
        // stable post-filter on composite cursor if provided
        if let Some((ts,id)) = cursor_pair {
            events.retain(|e| e.created_at < ts || (e.created_at == ts && e.id.as_str() < id.as_str()));
        }
        // ensure sort by created_at desc then id asc for stability
        events.sort_by(|a,b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
        events.truncate(limit);
        Json(events)
    }

    async fn health_handler() -> (StatusCode, Json<serde_json::Value>) {
        (StatusCode::OK, Json(serde_json::json!({"status":"ok"})))
    }

    async fn ready_handler(repo: Arc<dyn RelayRepo>) -> (StatusCode, Json<serde_json::Value>) {
        // simple readiness: can execute a trivial query
        let ok = repo.query_once(Vec::new(), 1).await.is_ok();
        if ok {
            (StatusCode::OK, Json(serde_json::json!({"ready":true})))
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"ready":false})))
        }
    }

    async fn metrics_handler() -> Json<serde_json::Value> {
        let snap = graph_cozo::metrics_snapshot();
        Json(snap)
    }

    async fn metrics_prom_handler() -> (StatusCode, String) {
        let snap = graph_cozo::metrics_snapshot();
        // minimal Prometheus exposition from JSON snapshot
        let mut out = String::new();
        let pairs = [
            ("cozo_query_count", "counter"),
            ("cozo_query_total_ms", "counter"),
            ("cozo_tx_count", "counter"),
            ("cozo_tx_total_ms", "counter"),
            ("persist_queue_depth", "gauge"),
            ("hot_refresh_count", "counter"),
            ("hot_refresh_total_ms", "counter"),
            ("verify_count", "counter"),
            ("verify_total_ms", "counter"),
            ("hot_refresh_rows", "counter"),
            ("verify_rows", "counter"),
        ];
        for (k, typ) in pairs {
            if let Some(v) = snap.get(k).and_then(|x| x.as_f64()) {
                out.push_str(&format!("# HELP {} \n# TYPE {} {}\n{} {}\n", k, k, typ, k, v));
            }
        }
        (StatusCode::OK, out)
    }

    async fn dbhealth_handler(repo: Arc<dyn RelayRepo>) -> (StatusCode, &'static str) {
        match repo.health_check().await {
            Ok(_) => (StatusCode::OK, "ok"),
            Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "unhealthy"),
        }
    }

    // --- Admin/GraphRank endpoints ---
    #[derive(Deserialize)]
    struct WorldviewQuery { observer: String, context: String }
    async fn worldview_get_handler(repo: Arc<dyn RelayRepo>, Query(q): Query<WorldviewQuery>) -> (StatusCode, Json<serde_json::Value>) {
        match repo.worldview_get(&q.observer, &q.context).await {
            Ok(Some(wv)) => (StatusCode::OK, Json(serde_json::to_value(wv).unwrap_or(serde_json::json!({})))),
            Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"not_found"}))),
            Err(e) => { error!("/worldview GET repo error: {:?}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"repo"}))) },
        }
    }

    async fn worldview_put_handler(repo: Arc<dyn RelayRepo>, Json(wv): Json<Worldview>) -> (StatusCode, Json<serde_json::Value>) {
        match repo.worldview_put(&wv).await {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"ok":true}))),
            Err(e) => { error!("/worldview POST repo error: {:?}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"ok":false}))) },
        }
    }

    #[derive(Deserialize)]
    struct GenerateBody { observer: String, context: String, settings: Option<GraphRankSettings> }
    async fn grapevine_generate_handler(repo: Arc<dyn RelayRepo>, Json(b): Json<GenerateBody>) -> (StatusCode, Json<serde_json::Value>) {
        let settings = b.settings.unwrap_or(GraphRankSettings { max_dos: 0, rigor: 1.0, attenuation: 0.5, precision: 0.01, overwrite: None, expiry: None });
        match repo.grapevine_generate(&b.observer, &b.context, settings).await {
            Ok(ts) => (StatusCode::OK, Json(serde_json::json!({"ts": ts }))),
            Err(e) => { error!("/grapevine/generate POST repo error: {:?}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"repo"}))) },
        }
    }

    #[derive(Deserialize)]
    struct ScorecardsQuery { observer: String, context: String, ts: Option<Timestamp> }
    async fn scorecards_get_handler(repo: Arc<dyn RelayRepo>, Query(q): Query<ScorecardsQuery>) -> (StatusCode, Json<serde_json::Value>) {
        match repo.scorecards_get(&q.observer, &q.context, q.ts).await {
            Ok(Some(list)) => (StatusCode::OK, Json(serde_json::json!(list))),
            Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"not_found"}))),
            Err(e) => { error!("/scorecards GET repo error: {:?}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"repo"}))) },
        }
    }

    #[derive(Deserialize)]
    struct ScorecardsBody { observer: String, context: String, ts: Timestamp, scorecards: Vec<Scorecard> }
    async fn scorecards_put_handler(repo: Arc<dyn RelayRepo>, Json(b): Json<ScorecardsBody>) -> (StatusCode, Json<serde_json::Value>) {
        match repo.scorecards_put(&b.observer, &b.context, b.ts, &b.scorecards).await {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))),
            Err(e) => { error!("/scorecards POST repo error: {:?}", e); (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"ok": false}))) }
        }
    }

    // simple global limiter for HTTP writes
    #[derive(Clone)]
    struct Limiter { inner: Arc<tokio::sync::Mutex<(std::time::Instant, u64)>>, limit: Option<u32> }
    let http_limit = settings.limits.messages_per_sec;
    let limiter = Limiter { inner: Arc::new(tokio::sync::Mutex::new((std::time::Instant::now(), 0))), limit: http_limit };

    async fn write_handler(repo: Arc<dyn RelayRepo>, limiter: Limiter, Json(evt): Json<NostrEvent>) -> (StatusCode, Json<serde_json::Value>) {
        // global cap per-second
        if let Some(lim) = limiter.limit { 
            let mut guard = limiter.inner.lock().await; 
            if guard.0.elapsed() >= std::time::Duration::from_secs(1) { guard.0 = std::time::Instant::now(); guard.1 = 0; }
            if guard.1 as u32 >= lim { return (StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({"ok": false, "reason": "overloaded"}))); }
            guard.1 += 1; 
        }
        match repo.write_event(&evt).await {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))),
            Err(_) => (StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({"ok": false, "reason": "overloaded"}))),
        }
    }

    let repo_clone = repo.clone();
    let mut app = Router::new()
        .route("/", get({
            let cfg = info_cfg.clone();
            move || nip11_handler(cfg.clone())
        }))
        .route("/health", get(health_handler))
        .route("/ready", get({
            let repo = repo_clone.clone();
            move || async move { ready_handler(repo.clone()).await }
        }))
        .route("/events", get({
            let repo = repo_clone.clone();
            move |q| events_handler(repo.clone(), q)
        }))
        .route("/metrics", get(metrics_handler))
        .route("/metrics_prom", get(metrics_prom_handler))
        .route("/dbhealth", get({ let repo = repo_clone.clone(); move || async move { dbhealth_handler(repo.clone()).await } }))
        .route("/worldview", get({ let repo = repo_clone.clone(); move |q| worldview_get_handler(repo.clone(), q) }))
        .route("/worldview", post({ let repo = repo_clone.clone(); move |body| worldview_put_handler(repo.clone(), body) }))
        .route("/grapevine/generate", post({ let repo = repo_clone.clone(); move |body| grapevine_generate_handler(repo.clone(), body) }))
        .route("/scorecards", get({ let repo = repo_clone.clone(); move |q| scorecards_get_handler(repo.clone(), q) }))
        .route("/scorecards", post({ let repo = repo_clone.clone(); move |body| scorecards_put_handler(repo.clone(), body) }))
        .route("/event", post({
            let repo = repo_clone.clone();
            let lim = limiter.clone();
            move |payload| write_handler(repo.clone(), lim.clone(), payload)
        }));

    if settings.http.cors.unwrap_or(false) {
        app = app.layer(CorsLayer::very_permissive());
    }

    let addr: SocketAddr = format!("{}:{}", settings.http.address, settings.http.port)
        .parse()
        .map_err(|_| HttpError::Bind)?;
    info!("http listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|_| HttpError::Bind)?;
    tokio::select! {
        res = axum::serve(listener, app) => { res.map_err(|_| HttpError::Bind)?; },
        _ = shutdown.recv() => { /* graceful shutdown */ }
    }
    Ok(())
}
