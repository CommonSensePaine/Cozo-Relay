use thiserror::Error;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use tracing::{info, warn};
#[cfg(feature = "cozo-native")]
use cozo::{DbInstance, ScriptMutability};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

pub mod graphrank;
mod schema;
mod queries;
mod pagination;
mod write_batch;
mod tuning;
mod metrics;

// Public helpers for cross-crate observability
pub fn metrics_snapshot() -> serde_json::Value { metrics::Metrics::snapshot() }
pub fn record_persist_queue_depth(depth: usize) { metrics::Metrics::record_queue_depth(depth) }
pub fn record_refresh_ms(ms: u128) { metrics::Metrics::record_refresh_ms(ms) }
pub fn record_verify_ms(ms: u128) { metrics::Metrics::record_verify_ms(ms) }
pub fn record_refresh_rows(n: u64) { metrics::record_refresh_rows(n) }
pub fn record_verify_rows(n: u64) { metrics::record_verify_rows(n) }

pub mod types {
    use serde::{Deserialize, Serialize};
    use nostr_model::Hex;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Worldview {
        pub observer: Hex,
        pub context: String,
        pub settings_json: serde_json::Value,
        pub calculating_ts: Option<u64>,
        pub calculated_ts: Option<u64>,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GraphRankSettings {
        pub max_dos: u8,
        pub rigor: f64,
        pub attenuation: f64,
        pub precision: f64,
        pub overwrite: Option<bool>,
        pub expiry: Option<u64>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Scorecard {
        pub subject: Hex,
        pub score: f64,
        pub confidence: f64,
        pub interpretersums_json: serde_json::Value,
    }

    pub type Timestamp = u64;
}

use nostr_model::{NostrEvent, NostrFilter, Hex, EventId};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("unimplemented")] Unimplemented,
    #[error("not found")] NotFound,
    #[error("internal error")] Internal,
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct CozoStore {
    _path: String,
    #[cfg(feature = "cozo-native")]
    db: Option<cozo::DbInstance>,
    // MVP fallback: in-memory store until Cozo queries are wired
    mem_events: Arc<Mutex<Vec<nostr_model::NostrEvent>>>,
    events_path: PathBuf,
    // fast dedup index for event ids
    mem_ids: Arc<Mutex<HashSet<String>>>,
}

impl CozoStore {
    pub fn open(path: &str) -> Result<Self> {
        let dir = PathBuf::from(path);
        if !dir.exists() {
            fs::create_dir_all(&dir).map_err(|_| StoreError::Internal)?;
        }
        let events_path = dir.join("events.jsonl");
        #[cfg(feature = "cozo-native")]
        let db = {
            let mut db_file = dir.clone();
            db_file.push("cozo.db");
            match DbInstance::new(
                "sqlite",
                db_file.to_string_lossy().as_ref(),
                Default::default(),
            ) {
                Ok(db) => {
                    // Apply SQLite tuning knobs if available in this runtime
                    crate::tuning::apply_sqlite_tuning(&db);
                    Some(db)
                },
                Err(_) => None,
            }
        };
        // Preload events from Cozo if available; otherwise from JSONL
        let mem_events = Arc::new(Mutex::new(Vec::new()));
        let mem_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut loaded_from_cozo = false;
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &db {
            if let Ok(n) = Self::load_from_cozo_into_memory(db, &mem_events, &mem_ids) {
                if n > 0 { loaded_from_cozo = true; }
            }
        }
        if !loaded_from_cozo && events_path.exists() {
            if let Ok(file) = File::open(&events_path) {
                let reader = BufReader::new(file);
                let mut guard = mem_events.lock().unwrap();
                let mut ids_guard = mem_ids.lock().unwrap();
                for line in reader.lines() {
                    if let Ok(l) = line {
                        if l.trim().is_empty() { continue; }
                        if let Ok(ev) = serde_json::from_str::<nostr_model::NostrEvent>(&l) {
                            if ids_guard.insert(ev.id.clone()) {
                                guard.push(ev);
                            }
                        }
                    }
                }
            }
        }
        let store = Self {
            _path: path.to_string(),
            #[cfg(feature = "cozo-native")]
            db,
            mem_events,
            events_path,
            mem_ids,
        };
        // If Cozo empty but memory has events (from JSONL), seed Cozo once
        #[cfg(feature = "cozo-native")]
        if store.db.is_some() && !loaded_from_cozo {
            let _ = store.seed_cozo_from_memory();
        }
        // Apply migrations at open to ensure schema/indexes present
        let _ = store.migrate();
        Ok(store)
    }

    pub fn migrate(&self) -> Result<()> {
        // Apply migration files from ./migrations in lexical order.
        let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dir.push("migrations");
        if let Ok(entries) = fs::read_dir(&dir) {
            let mut files: Vec<PathBuf> = entries
                .filter_map(|e| e.ok().map(|de| de.path()))
                .filter(|p| p.is_file())
                .collect();
            files.sort();
            for file in files {
                match fs::read_to_string(&file) {
                    Ok(sql) => {
                        info!("applying cozo migration: {} ({} bytes)", file.display(), sql.len());
                        #[cfg(feature = "cozo-native")]
                        if let Some(db) = &self.db {
                            // Best-effort apply script; ignore errors to avoid crashing during dev
                            let _ = db.run_script(&sql, Default::default(), ScriptMutability::Mutable);
                        }
                    }
                    Err(e) => warn!("failed reading migration {}: {:?}", file.display(), e),
                }
            }
        }
        Ok(())
    }

    // Phase 2: Materialize hot latest window
    pub async fn refresh_hot_latest(&self, cutoff_ts: u64) -> Result<u64> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = format!(r#"
// Clear current window and reinsert from base events for stability
:delete hot_latest {{ event_id: event_id, pubkey: pubkey, kind: kind, created_at: created_at }} WHERE created_at >= {cutoff}
:insert hot_latest {{ event_id: id, pubkey: pubkey, kind: kind, created_at: created_at }} := event{{ id: id, pubkey: pubkey, kind: kind, created_at: created_at }}, created_at >= {cutoff}
"#, cutoff = cutoff_ts);
            let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
            let cnt_q = format!("?[c] := count() group () from (?[x] := hot_latest{{ created_at: created_at }}, created_at >= {})", cutoff_ts);
            if let Ok(res) = db.run_script(&cnt_q, Default::default(), ScriptMutability::Immutable) {
                if let Some(rows) = res.rows.get(0) { if let Some(row) = rows.get(0) {
                    if let Ok(v) = serde_json::to_value(row) { if let Some(a) = v.as_array() { if let Some(c) = a.get(0).and_then(|x| x.as_u64()) { return Ok(c); }}}
                }}
            }
        }
        Ok(0)
    }

    pub async fn refresh_latest_by_kind(&self, cutoff_ts: u64) -> Result<u64> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = format!(":delete latest_by_kind {{ kind: kind, event_id: event_id, created_at: created_at }} WHERE created_at >= {}", cutoff_ts);
            let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
            let ins = format!(":insert latest_by_kind {{ event_id: id, kind: kind, created_at: created_at }} := event{{ id: id, kind: kind, created_at: created_at }}, created_at >= {}", cutoff_ts);
            let _ = db.run_script(&ins, Default::default(), ScriptMutability::Mutable);
            let cnt_q = format!("?[c] := count() group () from (?[x] := latest_by_kind{{ created_at: created_at }}, created_at >= {})", cutoff_ts);
            if let Ok(res) = db.run_script(&cnt_q, Default::default(), ScriptMutability::Immutable) {
                if let Some(rows) = res.rows.get(0) { if let Some(row) = rows.get(0) {
                    if let Ok(v) = serde_json::to_value(row) { if let Some(a) = v.as_array() { if let Some(c) = a.get(0).and_then(|x| x.as_u64()) { return Ok(c); }}}
                }}
            }
        }
        Ok(0)
    }

    pub async fn refresh_latest_by_author(&self, cutoff_ts: u64) -> Result<u64> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = format!(":delete latest_by_author {{ pubkey: pubkey, event_id: event_id, created_at: created_at }} WHERE created_at >= {}", cutoff_ts);
            let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
            let ins = format!(":insert latest_by_author {{ event_id: id, pubkey: pubkey, created_at: created_at }} := event{{ id: id, pubkey: pubkey, created_at: created_at }}, created_at >= {}", cutoff_ts);
            let _ = db.run_script(&ins, Default::default(), ScriptMutability::Mutable);
            let cnt_q = format!("?[c] := count() group () from (?[x] := latest_by_author{{ created_at: created_at }}, created_at >= {})", cutoff_ts);
            if let Ok(res) = db.run_script(&cnt_q, Default::default(), ScriptMutability::Immutable) {
                if let Some(rows) = res.rows.get(0) { if let Some(row) = rows.get(0) {
                    if let Ok(v) = serde_json::to_value(row) { if let Some(a) = v.as_array() { if let Some(c) = a.get(0).and_then(|x| x.as_u64()) { return Ok(c); }}}
                }}
            }
        }
        Ok(0)
    }

    // Phase 2: Derivation verification for recent events (idempotent)
    pub async fn verify_derivations_since(&self, since_ts: u64) -> Result<u64> {
        let mut verified = 0u64;
        // get ids since cutoff from Cozo or memory
        let ids: Vec<String> = {
            #[cfg(feature = "cozo-native")]
            if let Some(db) = &self.db {
                let script = format!("?[id] := event{{ id: id, created_at: created_at }}, created_at >= {}", since_ts);
                if let Ok(res) = db.run_script(&script, Default::default(), ScriptMutability::Immutable) {
                    if let Some(rows) = res.rows.get(0) {
                        let mut out = Vec::new();
                        for row in rows { if let Ok(v) = serde_json::to_value(row) { if let Some(a) = v.as_array() { if let Some(id) = a.get(0).and_then(|x| x.as_str()) { out.push(id.to_string()); }}}}
                        out
                    } else { Vec::new() }
                } else { Vec::new() }
            } else { Vec::new() }
            #[cfg(not(feature = "cozo-native"))]
            {
                let guard = self.mem_events.lock().unwrap();
                guard.iter().filter(|e| e.created_at >= since_ts).map(|e| e.id.clone()).collect()
            }
        };
        for id in ids {
            let _ = self.refresh_edges_for_event(&id).await; // idempotent
            verified += 1;
        }
        Ok(verified)
    }

    // Phase 2: Durability snapshot/export (best-effort JSONL copy)
    pub async fn snapshot_export(&self, dir: &std::path::Path) -> Result<std::path::PathBuf> {
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_| StoreError::Internal)?.as_secs();
        let mut out = dir.to_path_buf();
        std::fs::create_dir_all(&out).map_err(|_| StoreError::Internal)?;
        out.push(format!("snapshot-{}.jsonl", ts));
        // Copy JSONL event log as a baseline snapshot
        std::fs::copy(&self.events_path, &out).map_err(|_| StoreError::Internal)?;
        Ok(out)
    }

    // Phase 2: Health check
    pub async fn health_check(&self) -> Result<()> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = "?[x] := event{ id: x }, limit: 1";
            let _ = db.run_script(script, Default::default(), ScriptMutability::Immutable).map_err(|_| StoreError::Internal)?;
        }
        Ok(())
    }

    // event persistence
    pub async fn put_event(&self, e: NostrEvent) -> Result<()> {
        // Append to disk (JSONL) and push into memory
        {
            // skip if duplicate id already known
            {
                let mut ids = self.mem_ids.lock().unwrap();
                if !ids.insert(e.id.clone()) {
                    return Ok(());
                }
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.events_path)
                .map_err(|_| StoreError::Internal)?;
            let line = serde_json::to_string(&e).map_err(|_| StoreError::Internal)?;
            file.write_all(line.as_bytes()).map_err(|_| StoreError::Internal)?;
            file.write_all(b"\n").map_err(|_| StoreError::Internal)?;
            file.flush().map_err(|_| StoreError::Internal)?;
        }
        let mut guard = self.mem_events.lock().unwrap();
        guard.push(e);
        // Mirror to Cozo if available
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            if let Some(ev) = guard.last() {
                let tags_json = serde_json::to_string(&ev.tags).unwrap_or("[]".to_string());
                // DB-level dedup guard: skip insert if id exists in Cozo
                let exists_q = format!("?[x] := event{{ id: '{}' }}", ev.id);
                let exists = db
                    .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                    .ok()
                    .and_then(|r| r.rows.get(0).cloned())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                if !exists {
                    let mut script = format!(
                        ":insert event {{ id: '{}', kind: {}, pubkey: '{}', created_at: {}, content: {}, sig: '{}', expires_at: null, tags_json: {} }};",
                        ev.id,
                        ev.kind,
                        ev.pubkey,
                        ev.created_at,
                        serde_json::to_string(&ev.content).unwrap_or("\"\"".to_string()),
                        ev.sig,
                        tags_json
                    );
                    // mirror flattened tags
                    for (pos, t) in ev.tags.iter().enumerate() {
                        if t.len() >= 2 {
                            let name = &t[0];
                            let value = &t[1];
                            script.push_str(&format!(
                                ":insert tag {{ event_id: '{}', name: '{}', value: '{}', pos: {} }};",
                                ev.id, name, value, pos
                            ));
                        }
                    }
                    let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                }
            }
        }
        Ok(())
    }
    pub async fn put_events_bulk<I: Iterator<Item = NostrEvent>>(&self, iter: I) -> Result<u64> {
        let mut guard = self.mem_events.lock().unwrap();
        let mut ids = self.mem_ids.lock().unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events_path)
            .map_err(|_| StoreError::Internal)?;
        let mut c = 0u64;
        // Track accepted (new) events in this batch for Cozo mirroring
        let mut accepted: Vec<NostrEvent> = Vec::new();
        for e in iter {
            if ids.insert(e.id.clone()) {
                let line = serde_json::to_string(&e).map_err(|_| StoreError::Internal)?;
                file.write_all(line.as_bytes()).map_err(|_| StoreError::Internal)?;
                file.write_all(b"\n").map_err(|_| StoreError::Internal)?;
                guard.push(e.clone());
                accepted.push(e);
                c += 1;
            }
        }
        file.flush().map_err(|_| StoreError::Internal)?;
        // Batch mirror to Cozo for throughput
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            if !accepted.is_empty() {
                // Check which IDs already exist in Cozo to avoid duplicates
                let ids_list = accepted
                    .iter()
                    .map(|e| format!("\"{}\"", e.id))
                    .collect::<Vec<_>>()
                    .join(",");
                let exists_q = format!(
                    "?[id] := event{{ id: id }}, id in [{}]",
                    ids_list
                );
                let mut existing: std::collections::HashSet<String> = std::collections::HashSet::new();
                if let Ok(res) = db.run_script(&exists_q, Default::default(), ScriptMutability::Immutable) {
                    if let Some(rows) = res.rows.get(0) {
                        for row in rows {
                            if let Ok(val) = serde_json::to_value(row) {
                                if let Some(arr) = val.as_array() {
                                    if let Some(idv) = arr.get(0).and_then(|v| v.as_str()) {
                                        existing.insert(idv.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                let to_insert: Vec<NostrEvent> = accepted
                    .into_iter()
                    .filter(|e| !existing.contains(&e.id))
                    .collect();
                if !to_insert.is_empty() {
                    let script = write_batch::build_insert_script(&to_insert);
                    let t0 = std::time::Instant::now();
                    tracing::trace!(target="graph_cozo", "cozo tx begin: batch_insert");
                    let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                    tracing::trace!(target="graph_cozo", "cozo tx end: batch_insert");
                    let dt = t0.elapsed().as_millis();
                    let _ = crate::metrics::Metrics::record_tx_time_ms(dt);
                }
            }
        }
        Ok(c)
    }

    // Remove duplicate events by id (keep earliest created_at)
    pub async fn dedup_events(&self) -> Result<()> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = r#"
// Remove duplicate events by id (keep the earliest created_at)
?[id, min_created] := agg min(created_at) by id from (?[id, created_at] := event{id: id, created_at: created_at})
:delete event { id: id, created_at: created_at } WHERE created_at > min_created
"#;
            let _ = db.run_script(script, Default::default(), ScriptMutability::Mutable);
        }
        Ok(())
    }

    // nostr filter -> events
    pub async fn query_events(&self, filters: &[NostrFilter], limit: usize) -> Result<Vec<NostrEvent>> {
        // Prefer Cozo as source of truth if available; fall back to in-memory
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            // Fetch extra from Cozo (prelimit) to compensate for Rust-side refinement
            let prelimit = limit.saturating_mul(3).max(100);
            if let Ok(mut all) = Self::cozo_query_filtered(db, filters, prelimit) {
                // Filter in Rust using existing predicate
                let mut out: Vec<NostrEvent> = Vec::new();
                if filters.is_empty() {
                    out.append(&mut all);
                } else {
                    for e in all.into_iter() {
                        for f in filters {
                            if filter_is_empty(f) || matches_filter(&e, f) { out.push(e); break; }
                        }
                    }
                }
                let mut seen: HashSet<String> = HashSet::new();
                out.retain(|e| seen.insert(e.id.clone()));
                out.sort_by(|a,b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
                return Ok(out.into_iter().take(limit).collect());
            }
        }

        // Fallback: query in-memory snapshot
        let guard = self.mem_events.lock().unwrap();
        let mut out: Vec<NostrEvent> = Vec::new();
        if filters.is_empty() {
            out.extend(guard.iter().cloned());
        } else {
            for e in guard.iter() {
                for f in filters {
                    if filter_is_empty(f) || matches_filter(e, f) { out.push(e.clone()); break; }
                }
            }
        }
        let mut seen: HashSet<String> = HashSet::new();
        out.retain(|e| seen.insert(e.id.clone()));
        out.sort_by(|a,b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
        Ok(out.into_iter().take(limit).collect())
    }

    // graph/interpreter

    // graph/interpreter
    pub async fn refresh_edges_for_event(&self, event_id: &EventId) -> Result<()> {
        // Find event from in-memory cache
        let ev_opt = {
            let guard = self.mem_events.lock().unwrap();
            guard.iter().find(|e| &e.id == event_id).cloned()
        };
        let Some(ev) = ev_opt else { return Ok(()) };
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            // Insert follow edges from kind 3 contact list (#p tags)
            if ev.kind == 3 {
                for t in &ev.tags {
                    if t.get(0).map(|s| s.as_str()) == Some("p") {
                        if let Some(dst) = t.get(1) {
                            let ctx = t.get(2).cloned().unwrap_or_default();
                            // idempotent: skip if exists
                            let exists_q = format!(
                                "?[x] := edge_follow{{ src: \"{}\", dst: \"{}\", ctx: \"{}\", created_at: {} }}",
                                ev.pubkey, dst, ctx, ev.created_at
                            );
                            let exists = db
                                .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                                .ok()
                                .and_then(|r| r.rows.get(0).cloned())
                                .map(|v| !v.is_empty())
                                .unwrap_or(false);
                            if !exists {
                                let script = format!(
                                    ":insert edge_follow {{ src: '{}', dst: '{}', ctx: '{}', created_at: {} }};",
                                    ev.pubkey, dst, ctx, ev.created_at
                                );
                                let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                            }
                        }
                    }
                }
            }
            // Insert mute edges from kind 10000 (list mutes, #p tags)
            if ev.kind == 10000 {
                for t in &ev.tags {
                    if t.get(0).map(|s| s.as_str()) == Some("p") {
                        if let Some(dst) = t.get(1) {
                            let exists_q = format!(
                                "?[x] := edge_mute{{ src: \"{}\", dst: \"{}\", created_at: {} }}",
                                ev.pubkey, dst, ev.created_at
                            );
                            let exists = db
                                .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                                .ok()
                                .and_then(|r| r.rows.get(0).cloned())
                                .map(|v| !v.is_empty())
                                .unwrap_or(false);
                            if !exists {
                                let script = format!(
                                    ":insert edge_mute {{ src: '{}', dst: '{}', created_at: {} }};",
                                    ev.pubkey, dst, ev.created_at
                                );
                                let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                            }
                        }
                    }
                }
            }
            // Insert reactions from kind 7 (#e event id tag); emoji from content if present
            if ev.kind == 7 {
                let mut dst_event: Option<String> = None;
                for t in &ev.tags {
                    if t.get(0).map(|s| s.as_str()) == Some("e") {
                        if let Some(eid) = t.get(1) { dst_event = Some(eid.clone()); break; }
                    }
                }
                if let Some(dst_eid) = dst_event {
                    let emoji = ev.content.clone();
                    let esc = emoji.replace('"', "\\\"");
                    // idempotent: skip if exists
                    let exists_q = format!(
                        "?[x] := reaction{{ src: \"{}\", dst_event: \"{}\", emoji: \"{}\", created_at: {} }}",
                        ev.pubkey, dst_eid, esc, ev.created_at
                    );
                    let exists = db
                        .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                        .ok()
                        .and_then(|r| r.rows.get(0).cloned())
                        .map(|v| !v.is_empty())
                        .unwrap_or(false);
                    if !exists {
                        let script = format!(
                            ":insert reaction {{ src: '{}', dst_event: '{}', emoji: '{}', created_at: {} }};",
                            ev.pubkey, dst_eid, esc, ev.created_at
                        );
                        let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                    }
                }
            }
            // Insert reports from kind 1984 (report events, #p ratee)
            if ev.kind == 1984 {
                for t in &ev.tags {
                    if t.get(0).map(|s| s.as_str()) == Some("p") {
                        if let Some(dst) = t.get(1) {
                            let typ = ev.content.replace('"', "\\\"");
                            let exists_q = format!(
                                "?[x] := edge_report{{ src: \"{}\", dst: \"{}\", typ: \"{}\", created_at: {} }}",
                                ev.pubkey, dst, typ, ev.created_at
                            );
                            let exists = db
                                .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                                .ok()
                                .and_then(|r| r.rows.get(0).cloned())
                                .map(|v| !v.is_empty())
                                .unwrap_or(false);
                            if !exists {
                                let script = format!(
                                    ":insert edge_report {{ src: '{}', dst: '{}', typ: '{}', created_at: {} }};",
                                    ev.pubkey, dst, typ, ev.created_at
                                );
                                let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                            }
                        }
                    }
                }
            }
            // TODO: mutes/reports mapping can be added later when event kinds are finalized
        }
        Ok(())
    }
    pub async fn derive_all_edges(&self) -> Result<u64> {
        // Copy event IDs to avoid holding the lock across awaits
        let ids: Vec<String> = {
            let guard = self.mem_events.lock().unwrap();
            guard.iter().map(|e| e.id.clone()).collect()
        };
        let mut c = 0u64;
        for id in ids.iter() {
            let _ = self.refresh_edges_for_event(id).await;
            c += 1;
        }
        Ok(c)
    }

    // graph traversals
    pub async fn neighbors(&self, pubkey: &Hex, _max_dos: u8) -> Result<Vec<Hex>> {
        // MVP: direct neighbors (outgoing follows)
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = format!(
                "?[dst] := edge_follow{{ src: \"{}\", dst: dst }}",
                pubkey
            );
            if let Ok(res) = db.run_script(&script, Default::default(), ScriptMutability::Immutable) {
                let mut out: Vec<Hex> = Vec::new();
                if let Some(rows) = res.rows.get(0) {
                    for row in rows {
                        if let Ok(val) = serde_json::to_value(row) {
                            if let Some(arr) = val.as_array() { if let Some(d) = arr.get(0).and_then(|v| v.as_str()) { out.push(d.to_string()); } }
                        }
                    }
                }
                return Ok(out);
            }
        }
        Ok(vec![])
    }

    // graph rank lifecycle moved to graphrank module
}

fn matches_filter(e: &NostrEvent, f: &NostrFilter) -> bool {
    // authors
    if let Some(authors) = &f.authors {
        if !authors.iter().any(|a| a == &e.pubkey) { return false; }
    }
    // kinds
    if let Some(kinds) = &f.kinds {
        if !kinds.iter().any(|k| *k == e.kind) { return false; }
    }
    // ids (prefix match)
    if let Some(ids) = &f.ids {
        let id = &e.id;
        if !ids.iter().any(|p| id.starts_with(p)) { return false; }
    }
    // since/until
    if let Some(since) = f.since { if e.created_at < since { return false; } }
    if let Some(until) = f.until { if e.created_at > until { return false; } }
    // tags (basic: each (#x, vals) requires at least one matching tag value)
    if let Some(tag_filters) = &f.tags {
        // AND across provided tag filters
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

fn filter_is_empty(f: &NostrFilter) -> bool {
    f.ids.as_ref().map(|v| v.is_empty()).unwrap_or(true)
        && f.authors.as_ref().map(|v| v.is_empty()).unwrap_or(true)
        && f.kinds.as_ref().map(|v| v.is_empty()).unwrap_or(true)
        && f.since.is_none()
        && f.until.is_none()
        && f.limit.is_none()
        && f.tags.as_ref().map(|v| v.is_empty()).unwrap_or(true)
}

// === Cozo helpers (only compiled when cozo-native feature is enabled) ===
#[cfg(feature = "cozo-native")]
impl CozoStore {
    fn load_from_cozo_into_memory(
        db: &DbInstance,
        mem_events: &Arc<Mutex<Vec<NostrEvent>>>,
        mem_ids: &Arc<Mutex<HashSet<String>>>,
    ) -> std::result::Result<usize, ()> {
        let script = "?[id, kind, pubkey, created_at, content, sig, tags_json] := event{id: id, kind: kind, pubkey: pubkey, created_at: created_at, content: content, sig: sig, tags_json: tags_json}";
        let res = db.run_script(script, Default::default(), ScriptMutability::Immutable).map_err(|_| ())?;
        let mut count = 0usize;
        if let Some(rows) = res.rows.get(0) {
            let mut guard = mem_events.lock().unwrap();
            let mut ids_guard = mem_ids.lock().unwrap();
            for row in rows {
                if let Ok(obj) = serde_json::to_value(row) {
                    if let Some(arr) = obj.as_array() {
                        if arr.len() == 7 {
                            let id = arr[0].as_str().unwrap_or_default().to_string();
                            if ids_guard.insert(id.clone()) {
                                let kind = arr[1].as_i64().unwrap_or(1) as u64;
                                let pubkey = arr[2].as_str().unwrap_or_default().to_string();
                                let created_at = arr[3].as_i64().unwrap_or(0) as u64;
                                let content = arr[4].as_str().unwrap_or_default().to_string();
                                let sig = arr[5].as_str().unwrap_or_default().to_string();
                                let tags_json = arr[6].as_str().unwrap_or("[]");
                                let tags: Vec<Vec<String>> = serde_json::from_str(tags_json).unwrap_or_default();
                                guard.push(NostrEvent { id, pubkey, created_at, kind, tags, content, sig });
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    fn seed_cozo_from_memory(&self) -> std::result::Result<(), ()> {
        let guard = self.mem_events.lock().unwrap();
        if let Some(db) = &self.db {
            for ev in guard.iter() {
                let tags_json = serde_json::to_string(&ev.tags).unwrap_or("[]".to_string());
                // Skip if event already exists in Cozo
                let exists_q = format!("?[x] := event{{ id: '{}' }}", ev.id);
                let exists = db
                    .run_script(&exists_q, Default::default(), ScriptMutability::Immutable)
                    .ok()
                    .and_then(|r| r.rows.get(0).cloned())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);
                if !exists {
                    let mut script = format!(
                        ":insert event {{ id: '{}', kind: {}, pubkey: '{}', created_at: {}, content: {}, sig: '{}', expires_at: null, tags_json: {} }};",
                        ev.id,
                        ev.kind,
                        ev.pubkey,
                        ev.created_at,
                        serde_json::to_string(&ev.content).unwrap_or("\"\"".to_string()),
                        ev.sig,
                        tags_json
                    );
                    for (pos, t) in ev.tags.iter().enumerate() {
                        if t.len() >= 2 {
                            let name = &t[0];
                            let value = &t[1];
                            script.push_str(&format!(
                                ":insert tag {{ event_id: '{}', name: '{}', value: '{}', pos: {} }};",
                                ev.id, name, value, pos
                            ));
                        }
                    }
                    let _ = db.run_script(&script, Default::default(), ScriptMutability::Mutable);
                }
            }
        }
        Ok(())
    }

    fn cozo_scan_all(db: &DbInstance) -> std::result::Result<Vec<NostrEvent>, ()> {
        let script = "?[id, kind, pubkey, created_at, content, sig, tags_json] := event{id: id, kind: kind, pubkey: pubkey, created_at: created_at, content: content, sig: sig, tags_json: tags_json}";
        let res = db.run_script(script, Default::default(), ScriptMutability::Immutable).map_err(|_| ())?;
        let mut out = Vec::new();
        if let Some(rows) = res.rows.get(0) {
            for row in rows {
                if let Ok(obj) = serde_json::to_value(row) {
                    if let Some(arr) = obj.as_array() {
                        if arr.len() == 7 {
                            let id = arr[0].as_str().unwrap_or_default().to_string();
                            let kind = arr[1].as_i64().unwrap_or(1) as u64;
                            let pubkey = arr[2].as_str().unwrap_or_default().to_string();
                            let created_at = arr[3].as_i64().unwrap_or(0) as u64;
                            let content = arr[4].as_str().unwrap_or_default().to_string();
                            let sig = arr[5].as_str().unwrap_or_default().to_string();
                            let tags_json = arr[6].as_str().unwrap_or("[]");
                            let tags: Vec<Vec<String>> = serde_json::from_str(tags_json).unwrap_or_default();
                            out.push(NostrEvent { id, pubkey, created_at, kind, tags, content, sig });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn cozo_query_filtered(db: &DbInstance, filters: &[NostrFilter], prelimit: usize) -> std::result::Result<Vec<NostrEvent>, ()> {
        // Combine simple dimensions across filters: authors, kinds, since, until
        // This is an OR-approximation that will be refined by Rust-side predicate afterwards.
        use std::collections::BTreeSet;
        let mut authors = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        let mut since_min: Option<u64> = None;
        let mut until_max: Option<u64> = None;
        let mut id_prefixes: Vec<String> = Vec::new();
        // tag filters collected as (name -> set of values)
        use std::collections::BTreeMap;
        let mut tag_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for f in filters {
            if let Some(aset) = &f.authors { for a in aset { authors.insert(a.clone()); } }
            if let Some(kset) = &f.kinds { for k in kset { kinds.insert(*k); } }
            if let Some(s) = f.since { since_min = Some(since_min.map_or(s, |cur| cur.min(s))); }
            if let Some(u) = f.until { until_max = Some(until_max.map_or(u, |cur| cur.max(u))); }
            if let Some(ids) = &f.ids { for p in ids { if !p.is_empty() { id_prefixes.push(p.clone()); } } }
            if let Some(tags) = &f.tags {
                for (name, vals) in tags.iter() {
                    let key = name.trim_start_matches('#').to_string();
                    let entry = tag_map.entry(key).or_default();
                    for v in vals { entry.insert(v.clone()); }
                }
            }
        }
        let mut conds: Vec<String> = Vec::new();
        if !authors.is_empty() {
            let list = authors.into_iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",");
            conds.push(format!("pubkey in [{}]", list));
        }
        if !kinds.is_empty() {
            let list = kinds.into_iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",");
            conds.push(format!("kind in [{}]", list));
        }
        if let Some(since) = since_min { conds.push(format!("created_at >= {}", since)); }
        if let Some(until) = until_max { conds.push(format!("created_at <= {}", until)); }
        if !id_prefixes.is_empty() {
            let ors = id_prefixes.into_iter().map(|p| format!("starts_with(id, \"{}\")", p)).collect::<Vec<_>>().join(" or ");
            conds.push(format!("({})", ors));
        }

        // Build base script with optional tag join
        let mut script = String::from("?[id, kind, pubkey, created_at, content, sig, tags_json] := event{ id: id, kind: kind, pubkey: pubkey, created_at: created_at, content: content, sig: sig, tags_json: tags_json }");
        // If tags requested, join tag and build OR over (name,value)
        if !tag_map.is_empty() {
            script.push_str(", tag{ event_id: id, name: name, value: value }");
            let mut pieces: Vec<String> = Vec::new();
            for (name, values) in tag_map.into_iter() {
                let vlist = values.into_iter().map(|v| format!("\"{}\"", v)).collect::<Vec<_>>().join(",");
                pieces.push(format!("(name == \"{}\" and value in [{}])", name, vlist));
            }
            if !pieces.is_empty() {
                script.push_str(", ");
                script.push_str(&format!("({})", pieces.join(" or ")));
            }
        }
        for c in conds { script.push_str(", "); script.push_str(&c); }
        // Prefer newest first and cap rows early; tie-break on id for stability
        script.push_str(", order_by: -created_at, id");
        script.push_str(&format!(", limit: {}", prelimit));

        let t0 = std::time::Instant::now();
        tracing::trace!(target="graph_cozo", "cozo query begin");
        let res = db.run_script(&script, Default::default(), ScriptMutability::Immutable).map_err(|_| ())?;
        tracing::trace!(target="graph_cozo", "cozo query end");
        let dt = t0.elapsed().as_millis();
        let _ = crate::metrics::Metrics::record_query_latency_ms(dt);
        let mut out = Vec::new();
        if let Some(rows) = res.rows.get(0) {
            for row in rows {
                if let Ok(obj) = serde_json::to_value(row) {
                    if let Some(arr) = obj.as_array() {
                        if arr.len() == 7 {
                            let id = arr[0].as_str().unwrap_or_default().to_string();
                            let kind = arr[1].as_i64().unwrap_or(1) as u64;
                            let pubkey = arr[2].as_str().unwrap_or_default().to_string();
                            let created_at = arr[3].as_i64().unwrap_or(0) as u64;
                            let content = arr[4].as_str().unwrap_or_default().to_string();
                            let sig = arr[5].as_str().unwrap_or_default().to_string();
                            let tags_json = arr[6].as_str().unwrap_or("[]");
                            let tags: Vec<Vec<String>> = serde_json::from_str(tags_json).unwrap_or_default();
                            out.push(NostrEvent { id, pubkey, created_at, kind, tags, content, sig });
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}
