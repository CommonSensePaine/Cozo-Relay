#![allow(dead_code)]

#[cfg(feature = "cozo-native")]
pub fn apply_sqlite_tuning(_db: &cozo::DbInstance) {
    // Best-effort: Cozo 0.7.6 does not expose SQLite PRAGMA directly via run_script.
    // We log the intended settings so operators can configure via environment/flags if supported by runtime.
    tracing::info!(target="graph_cozo", "sqlite tuning: WAL, synchronous=NORMAL, cache_size=MB-tuned, mmap_size, temp_store=memory");
    // If future Cozo exposes pragmas, apply here against the underlying sqlite handle.
}
