use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use relay_settings::{Settings, network::{Ws as WsCfg, Http as HttpCfg}, database::Cozo as DbCfg, limits::Limits, authorization::Auth};
use graph_cozo::CozoStore;
use cozo_repo::{CozoRepo, RelayRepo};
use http_server::run_http;
use ws_server::run_ws;

#[tokio::main]
async fn main() {
    // simple logger
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    // default settings for MVP
    let settings = Settings {
        ws: WsCfg { address: "0.0.0.0".into(), port: 7501, max_event_bytes: Some(131072), max_ws_message_bytes: Some(131072), max_ws_frame_bytes: Some(131072) },
        http: HttpCfg { address: "0.0.0.0".into(), port: 7500, cors: Some(true) },
        database: DbCfg { path: "./data/cozo".into() },
        limits: Limits { messages_per_sec: Some(50), subscriptions_per_min: Some(60), db_conns_per_client: Some(4), broadcast_buffer: 16384, event_persist_buffer: 4096 },
        authorization: Auth { nip42_auth: false, pubkey_whitelist: None },
        verification: None,
        payments: None,
    };

    // Repo
    let store = Arc::new(CozoStore::open(&settings.database.path).expect("open store"));
    store.migrate().expect("migrate");
    // Run derived-edges backfill in background so startup is not blocked
    let store_for_backfill = store.clone();
    tokio::spawn(async move {
        info!("cozo: deriving edges in background...");
        let _ = store_for_backfill.derive_all_edges().await;
        info!("cozo: edge derivation complete");
    });
    // Periodic dedup: enforce first-write-wins by removing later duplicates
    let store_for_dedup = store.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(600));
        loop {
            ticker.tick().await;
            info!("cozo: running periodic event dedup...");
            let _ = store_for_dedup.dedup_events().await;
        }
    });

    // Periodic hot materialization refresh: every 5 minutes, window = 7 days
    let store_for_hot = store.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
            let cutoff = now.saturating_sub(7 * 24 * 3600);
            info!("cozo: refreshing hot_latest window cutoff={} (now={})", cutoff, now);
            let t0 = std::time::Instant::now();
            let rows = store_for_hot.refresh_hot_latest(cutoff).await.unwrap_or(0);
            let dt = t0.elapsed().as_millis();
            graph_cozo::record_refresh_ms(dt);
            graph_cozo::record_refresh_rows(rows);
        }
    });

    // Periodic latest_by_kind refresh
    let store_for_kind = store.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
            let cutoff = now.saturating_sub(7 * 24 * 3600);
            info!("cozo: refreshing latest_by_kind cutoff={}", cutoff);
            let t0 = std::time::Instant::now();
            let rows = store_for_kind.refresh_latest_by_kind(cutoff).await.unwrap_or(0);
            let dt = t0.elapsed().as_millis();
            graph_cozo::record_refresh_ms(dt);
            graph_cozo::record_refresh_rows(rows);
        }
    });

    // Periodic latest_by_author refresh
    let store_for_author = store.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
            let cutoff = now.saturating_sub(7 * 24 * 3600);
            info!("cozo: refreshing latest_by_author cutoff={}", cutoff);
            let t0 = std::time::Instant::now();
            let rows = store_for_author.refresh_latest_by_author(cutoff).await.unwrap_or(0);
            let dt = t0.elapsed().as_millis();
            graph_cozo::record_refresh_ms(dt);
            graph_cozo::record_refresh_rows(rows);
        }
    });

    // Periodic derivation verification: every 10 minutes for last 24 hours
    let store_for_verify = store.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(600));
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
            let since = now.saturating_sub(24 * 3600);
            info!("cozo: verifying derivations since={} (now={})", since, now);
            let t0 = std::time::Instant::now();
            let rows = store_for_verify.verify_derivations_since(since).await.unwrap_or(0);
            let dt = t0.elapsed().as_millis();
            graph_cozo::record_verify_ms(dt);
            graph_cozo::record_verify_rows(rows);
        }
    });

    // Periodic snapshot export hourly
    let store_for_snap = store.clone();
    let snapshots_dir = {
        let mut p = std::path::PathBuf::from(&settings.database.path);
        p.push("snapshots");
        p
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            ticker.tick().await;
            info!("cozo: exporting hourly snapshot to {}", snapshots_dir.display());
            let _ = store_for_snap.snapshot_export(&snapshots_dir).await;
        }
    });
    let repo: Arc<dyn RelayRepo> = Arc::new(CozoRepo::new(store));

    let (shutdown_tx, _shutdown_rx_unused) = broadcast::channel::<()>(1);
    let ws_settings = settings.clone();
    let http_settings = settings.clone();

    // Clone shared resources per task
    let repo_ws = repo.clone();
    let repo_http = repo.clone();
    let ws_shutdown = shutdown_tx.subscribe();
    let http_shutdown = shutdown_tx.subscribe();

    let ws_handle = tokio::spawn(async move {
        let _ = run_ws(repo_ws, ws_settings, ws_shutdown).await;
    });
    let http_handle = tokio::spawn(async move {
        let _ = run_http(repo_http, http_settings, http_shutdown).await;
    });

    info!("relay running; press Ctrl+C to stop");
    let _ = tokio::signal::ctrl_c().await;
    let _ = shutdown_tx.send(());
    let _ = ws_handle.await;
    let _ = http_handle.await;
}
