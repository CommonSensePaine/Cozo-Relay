#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

static QUERY_COUNT: AtomicU64 = AtomicU64::new(0);
static QUERY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static TX_COUNT: AtomicU64 = AtomicU64::new(0);
static TX_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static REFRESH_COUNT: AtomicU64 = AtomicU64::new(0);
static REFRESH_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static VERIFY_COUNT: AtomicU64 = AtomicU64::new(0);
static VERIFY_TOTAL_MS: AtomicU64 = AtomicU64::new(0);
static HOT_REFRESH_ROWS: AtomicU64 = AtomicU64::new(0);
static VERIFY_ROWS: AtomicU64 = AtomicU64::new(0);
static LATEST_BY_KIND_ROWS: AtomicU64 = AtomicU64::new(0);
static LATEST_BY_AUTHOR_ROWS: AtomicU64 = AtomicU64::new(0);

pub struct Metrics;

impl Metrics {
    pub fn record_query_latency_ms(ms: u128) {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        QUERY_TOTAL_MS.fetch_add(ms as u64, Ordering::Relaxed);
    }
    pub fn record_tx_time_ms(ms: u128) {
        TX_COUNT.fetch_add(1, Ordering::Relaxed);
        TX_TOTAL_MS.fetch_add(ms as u64, Ordering::Relaxed);
    }
    pub fn record_queue_depth(depth: usize) {
        QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
    }
    pub fn record_refresh_ms(ms: u128) {
        REFRESH_COUNT.fetch_add(1, Ordering::Relaxed);
        REFRESH_TOTAL_MS.fetch_add(ms as u64, Ordering::Relaxed);
    }
    pub fn record_verify_ms(ms: u128) {
        VERIFY_COUNT.fetch_add(1, Ordering::Relaxed);
        VERIFY_TOTAL_MS.fetch_add(ms as u64, Ordering::Relaxed);
    }
    pub fn snapshot() -> serde_json::Value {
        let qc = QUERY_COUNT.load(Ordering::Relaxed);
        let qtm = QUERY_TOTAL_MS.load(Ordering::Relaxed);
        let tc = TX_COUNT.load(Ordering::Relaxed);
        let ttm = TX_TOTAL_MS.load(Ordering::Relaxed);
        let qd = QUEUE_DEPTH.load(Ordering::Relaxed);
        let rc = REFRESH_COUNT.load(Ordering::Relaxed);
        let rtm = REFRESH_TOTAL_MS.load(Ordering::Relaxed);
        let vc = VERIFY_COUNT.load(Ordering::Relaxed);
        let vtm = VERIFY_TOTAL_MS.load(Ordering::Relaxed);
        serde_json::json!({
            "cozo_query_count": qc,
            "cozo_query_total_ms": qtm,
            "cozo_query_avg_ms": if qc>0 { qtm as f64 / qc as f64 } else { 0.0 },
            "cozo_tx_count": tc,
            "cozo_tx_total_ms": ttm,
            "cozo_tx_avg_ms": if tc>0 { ttm as f64 / tc as f64 } else { 0.0 },
            "persist_queue_depth": qd,
            "hot_refresh_count": rc,
            "hot_refresh_total_ms": rtm,
            "hot_refresh_avg_ms": if rc>0 { rtm as f64 / rc as f64 } else { 0.0 },
            "verify_count": vc,
            "verify_total_ms": vtm,
            "verify_avg_ms": if vc>0 { vtm as f64 / vc as f64 } else { 0.0 },
            "hot_refresh_rows": HOT_REFRESH_ROWS.load(Ordering::Relaxed),
            "verify_rows": VERIFY_ROWS.load(Ordering::Relaxed),
            "latest_by_kind_rows": LATEST_BY_KIND_ROWS.load(Ordering::Relaxed),
            "latest_by_author_rows": LATEST_BY_AUTHOR_ROWS.load(Ordering::Relaxed),
        })
    }
}

pub fn record_refresh_rows(n: u64) { HOT_REFRESH_ROWS.fetch_add(n, Ordering::Relaxed); }
pub fn record_verify_rows(n: u64) { VERIFY_ROWS.fetch_add(n, Ordering::Relaxed); }
pub fn record_latest_by_kind_rows(n: u64) { LATEST_BY_KIND_ROWS.fetch_add(n, Ordering::Relaxed); }
pub fn record_latest_by_author_rows(n: u64) { LATEST_BY_AUTHOR_ROWS.fetch_add(n, Ordering::Relaxed); }
