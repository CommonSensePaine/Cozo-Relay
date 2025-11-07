use std::{process::Stdio, time::Duration};
use futures_util::{SinkExt, StreamExt};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, process::Command};
use tokio_tungstenite::tungstenite::protocol::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Prepare log dir
    let log_dir = std::path::PathBuf::from("e2e-logs");
    tokio::fs::create_dir_all(&log_dir).await.ok();
    let relay_out = tokio::fs::File::create(log_dir.join("relay.out.log")).await?;
    let relay_err = tokio::fs::File::create(log_dir.join("relay.err.log")).await?;

    // 1) Build and start relay-bin directly (captures real runtime logs)
    // Build relay first
    let mut build_cmd = Command::new("cargo");
    build_cmd.arg("build").arg("-p").arg("relay-bin");
    let status = build_cmd.status().await?;
    if !status.success() { anyhow::bail!("failed to build relay-bin"); }
    // Run from project root via cargo run (reliable even with spaces in paths)
    let project_root = std::path::PathBuf::from("../..");
    let mut child = Command::new("cargo")
        .arg("run").arg("-p").arg("relay-bin").arg("-q")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir(&project_root)
        .spawn()?;

    // Pipe child stdout/stderr to files
    if let Some(out) = child.stdout.take() {
        let mut reader = BufReader::new(out).lines();
        let mut file = relay_out.try_clone().await?;
        tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                let _ = file.write_all(line.as_bytes()).await;
                let _ = file.write_all(b"\n").await;
            }
        });
    }
    if let Some(err) = child.stderr.take() {
        let mut reader = BufReader::new(err).lines();
        let mut file = relay_err.try_clone().await?;
        tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                let _ = file.write_all(line.as_bytes()).await;
                let _ = file.write_all(b"\n").await;
            }
        });
    }

    // wait for /health up to 10s
    let http = reqwest::Client::builder().timeout(Duration::from_secs(2)).build()?;
    let mut healthy = false;
    for _ in 0..10 {
        if let Ok(resp) = http.get("http://127.0.0.1:7500/health").send().await { if resp.status().is_success() { healthy = true; break; } }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 2) HTTP checks
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    // /health
    let t0 = tokio::time::Instant::now();
    let health = http.get("http://127.0.0.1:7500/health").send().await?;
    let health_ok = health.status().is_success();
    let health_ms = t0.elapsed().as_millis();

    // /metrics
    let t1 = tokio::time::Instant::now();
    let metrics_resp = http.get("http://127.0.0.1:7500/metrics").send().await?;
    let metrics_ok = metrics_resp.status().is_success();
    let _metrics: serde_json::Value = metrics_resp.json().await.unwrap_or_else(|_| serde_json::json!({}));
    let metrics_ms = t1.elapsed().as_millis();

    // /events basic
    let t2 = tokio::time::Instant::now();
    let events: serde_json::Value = http.get("http://127.0.0.1:7500/events?limit=1").send().await?.json().await.unwrap_or_else(|_| serde_json::json!([]));
    let events_ok = events.is_array();
    let events_ms = t2.elapsed().as_millis();

    // /dbhealth
    let tdb = tokio::time::Instant::now();
    let dbhealth = http.get("http://127.0.0.1:7500/dbhealth").send().await?;
    let dbhealth_ok = dbhealth.status().is_success();
    let dbhealth_ms = tdb.elapsed().as_millis();

    // GraphRank/admin flows
    // Worldview PUT
    let wv_body = serde_json::json!({
        "observer": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "context": "default",
        "settings_json": {"rigor":1.0},
        "calculating_ts": null,
        "calculated_ts": null
    });
    let twp = tokio::time::Instant::now();
    let wv_put = http.post("http://127.0.0.1:7500/worldview").json(&wv_body).send().await;
    let (worldview_put_ok, worldview_put_status, worldview_put_body) = match wv_put {
        Ok(mut r) => {
            let st = r.status().as_u16();
            let txt = r.text().await.unwrap_or_default();
            (st >= 200 && st < 300, st, txt)
        }
        Err(_) => (false, 0, String::new()),
    };
    let worldview_put_ms = twp.elapsed().as_millis();

    // Worldview GET
    let twg = tokio::time::Instant::now();
    let wv_get = http.get("http://127.0.0.1:7500/worldview?observer=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&context=default").send().await;
    let (worldview_get_ok, worldview_get_status, worldview_get_body) = match wv_get {
        Ok(mut r) => {
            let st = r.status().as_u16();
            let txt = r.text().await.unwrap_or_default();
            (st >= 200 && st < 300, st, txt)
        }
        Err(_) => (false, 0, String::new()),
    };
    let worldview_get_ms = twg.elapsed().as_millis();

    // Grapevine generate
    let tgen = tokio::time::Instant::now();
    let gen_resp = http.post("http://127.0.0.1:7500/grapevine/generate")
        .json(&serde_json::json!({"observer":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","context":"default"}))
        .send().await;
    let (grapevine_ok, grapevine_status, grapevine_body, score_ts) = match gen_resp {
        Ok(mut resp) => {
            let st = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            let ts = serde_json::from_str::<serde_json::Value>(&body).ok().and_then(|v| v.get("ts").and_then(|x| x.as_u64()));
            (st >= 200 && st < 300, st, body, ts)
        }
        Err(_) => (false, 0, String::new(), None),
    };
    let grapevine_ms = tgen.elapsed().as_millis();

    // Scorecards get (will likely 404 if no scorecards for ts)
    let tsc = tokio::time::Instant::now();
    let sc_url = match score_ts { Some(ts) => format!("http://127.0.0.1:7500/scorecards?observer={}&context={}&ts={}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "default", ts), None => "http://127.0.0.1:7500/scorecards?observer=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&context=default&ts=0".to_string() };
    let sc_resp = http.get(&sc_url).send().await;
    let (scorecards_ok, scorecards_status, scorecards_body) = match sc_resp {
        Ok(mut r) => {
            let st = r.status().as_u16();
            let txt = r.text().await.unwrap_or_default();
            (st >= 200 && st < 300 || st == 404, st, txt)
        }
        Err(_) => (false, 0, String::new()),
    };
    let scorecards_ms = tsc.elapsed().as_millis();

    // 3) WS checks
    let (mut ws, _resp) = tokio_tungstenite::connect_async("ws://127.0.0.1:7501").await?;
    let ws_connected = true;

    // Subscribe empty (should eose)
    let sub = serde_json::json!(["REQ","sub1",{"limit":1}]).to_string();
    ws.send(Message::Text(sub)).await?;

    // try send an EVENT (dummy, expect OK false or true depending on validation)
    let evt = serde_json::json!(["EVENT",{
        "id":"0000000000000000000000000000000000000000000000000000000000000000",
        "pubkey":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "created_at": 1,
        "kind":1,
        "tags":[],
        "content":"hi",
        "sig":"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    }]);
    ws.send(Message::Text(evt.to_string())).await?;

    // read a few frames
    let mut got_any = false;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        if let Some(Ok(msg)) = ws.next().await {
            if msg.is_text() { got_any = true; break; }
        } else { break; }
    }
    let ws_any = got_any;

    // Cleanup
    let _ = child.kill().await;

    // 4) Write summary JSON
    let summary = serde_json::json!({
        "health_ok": health_ok,
        "health_ms": health_ms,
        "metrics_ok": metrics_ok,
        "metrics_ms": metrics_ms,
        "events_ok": events_ok,
        "events_ms": events_ms,
        "dbhealth_ok": dbhealth_ok,
        "dbhealth_ms": dbhealth_ms,
        "ws_connected": ws_connected,
        "ws_any_response": ws_any,
        "worldview_put_ok": worldview_put_ok,
        "worldview_put_ms": worldview_put_ms,
        "worldview_get_ok": worldview_get_ok,
        "worldview_get_ms": worldview_get_ms,
        "grapevine_ok": grapevine_ok,
        "grapevine_ms": grapevine_ms,
        "scorecards_ok": scorecards_ok,
        "scorecards_ms": scorecards_ms
        ,"worldview_put_status": worldview_put_status
        ,"worldview_put_body": worldview_put_body
        ,"worldview_get_status": worldview_get_status
        ,"worldview_get_body": worldview_get_body
        ,"grapevine_status": grapevine_status
        ,"grapevine_body": grapevine_body
        ,"scorecards_status": scorecards_status
        ,"scorecards_body": scorecards_body
    });
    let mut sum_file = tokio::fs::File::create(log_dir.join("e2e-summary.json")).await?;
    sum_file.write_all(serde_json::to_string_pretty(&summary)?.as_bytes()).await?;

    println!("e2e summary written to {}/e2e-summary.json", log_dir.display());
    println!("relay logs: {}/relay.out.log and {}/relay.err.log", log_dir.display(), log_dir.display());
    Ok(())
}
