use crate::{CozoStore, Result, StoreError};
use crate::types::{Worldview, GraphRankSettings, Scorecard, Timestamp};
use nostr_model::Hex;
use tracing::{error, info};

impl CozoStore {
    pub async fn worldview_get(&self, observer: &Hex, context: &str) -> Result<Option<Worldview>> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            // Use double-quoted string literals; escape backslashes and quotes
            let observer_q = format!("\"{}\"", observer);
            let context_esc = context.replace('\\', "\\\\").replace('"', "\\\"");
            let context_q = format!("\"{}\"", context_esc);
            let script = format!(
                "?[observer, context, settings_json, calculating_ts, calculated_ts] := worldview{{ observer: {observer_q}, context: {context_q}, settings_json: settings_json, calculating_ts: calculating_ts, calculated_ts: calculated_ts }};",
                observer_q = observer_q,
                context_q = context_q,
            );
            info!("cozo worldview_get: script_len={} script=<<{}>>", script.len(), script);
            let res = db
                .run_script(&script, Default::default(), cozo::ScriptMutability::Immutable)
                .map_err(|_| StoreError::Internal)?;
            if let Some(rows) = res.rows.get(0) {
                if let Some(row) = rows.first() {
                    if let Ok(val) = serde_json::to_value(row) {
                        if let Some(arr) = val.as_array() {
                            if arr.len() == 5 {
                                let settings_json: serde_json::Value = serde_json::from_str(
                                    arr[2].as_str().unwrap_or("{}"),
                                )
                                .unwrap_or_default();
                                let calculating_ts = arr[3].as_i64().map(|v| v as u64);
                                let calculated_ts = arr[4].as_i64().map(|v| v as u64);
                                return Ok(Some(Worldview {
                                    observer: observer.clone(),
                                    context: context.to_string(),
                                    settings_json,
                                    calculating_ts,
                                    calculated_ts,
                                }));
                            }
                        }
                    }
                }
            }
            return Ok(None);
        }
        Ok(None)
    }

    pub async fn worldview_put(&self, wv: &Worldview) -> Result<()> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            // Prepare both single- and double-quoted forms
            let observer_s = wv.observer.to_string();
            let observer_q = format!("'{}'", observer_s);
            let context_q = format!("'{}'", wv.context.replace('\'', "''"));
            // Serialize settings JSON and store as String
            let settings_text = serde_json::to_string(&wv.settings_json).unwrap_or("{}".to_string());
            let settings_q = format!("'{}'", settings_text.replace('\'', "''"));
            // Double-quoted, fully escaped variants for Cozo string literals
            let observer_dq = format!("\"{}\"", observer_s);
            let context_dq = format!("\"{}\"", wv.context.replace('\\', "\\\\").replace('"', "\\\""));
            let settings_dq = format!("\"{}\"", settings_text.replace('\\', "\\\\").replace('"', "\\\""));
            // Main attempt: build constant rows, expose as entry relation ?, then :put from ?
            let ins = format!(
                "{{\nrows[observer, context, settings_json, calculating_ts, calculated_ts] <- [[{observer}, {context}, {settings}, null, null]];\n?[observer, context, settings_json, calculating_ts, calculated_ts] := rows;\n:put worldview ?;\n}}",
                observer = observer_dq,
                context = context_dq,
                settings = settings_dq,
            );
            info!("cozo worldview_put: script_len={} script=<<{}>>", ins.len(), ins);
            if let Err(e) = db.run_script(&ins, Default::default(), cozo::ScriptMutability::Mutable) {
                error!("cozo worldview_put insert error: {:?}\nscript=<<{}>>", e, ins);
                // Fallback 1: minimal, settings_json empty string
                let observer_dq = format!("\"{}\"", observer_s);
                let context_dq = format!("\"{}\"", wv.context.replace('\\', "\\\\").replace('"', "\\\""));
                let fallback1 = format!(
                    ":put worldview {{ observer: {observer}, context: {context}, settings_json: \"\" }};",
                    observer = observer_dq,
                    context = context_dq,
                );
                info!("cozo worldview_put: fallback1 script_len={} script=<<{}>>", fallback1.len(), fallback1);
                match db.run_script(&fallback1, Default::default(), cozo::ScriptMutability::Mutable) {
                    Ok(_) => {}
                    Err(e2) => {
                        error!("cozo worldview_put fallback1 error: {:?}\nscript=<<{}>>", e2, fallback1);
                        // Fallback 2: double-quoted full payload; add nullable ints as null
                        let settings_dq = format!("\"{}\"", serde_json::to_string(&wv.settings_json).unwrap_or("{}".to_string()).replace('\\', "\\\\").replace('"', "\\\""));
                        let fallback2 = format!(
                            ":put worldview {{ observer: {observer}, context: {context}, settings_json: {settings}, calculating_ts: null, calculated_ts: null }};",
                            observer = observer_dq,
                            context = context_dq,
                            settings = settings_dq,
                        );
                        info!("cozo worldview_put: fallback2 script_len={} script=<<{}>>", fallback2.len(), fallback2);
                        if let Err(e3) = db.run_script(&fallback2, Default::default(), cozo::ScriptMutability::Mutable) {
                            error!("cozo worldview_put fallback2 error: {:?}\nscript=<<{}>>", e3, fallback2);
                            // Fallback 3: list form insert
                            let fallback3 = format!(
                                ":put worldview [{{ observer: {observer}, context: {context}, settings_json: {settings}, calculating_ts: null, calculated_ts: null }}];",
                                observer = observer_dq,
                                context = context_dq,
                                settings = settings_dq,
                            );
                            info!("cozo worldview_put: fallback3 script_len={} script=<<{}>>", fallback3.len(), fallback3);
                            if let Err(e4) = db.run_script(&fallback3, Default::default(), cozo::ScriptMutability::Mutable) {
                                error!("cozo worldview_put fallback3 error: {:?}\nscript=<<{}>>", e4, fallback3);
                                return Err(StoreError::Internal);
                            }
                        }
                    }
                }
            }
            return Ok(());
        }
        Ok(())
    }

    pub async fn grapevine_generate(&self, observer: &Hex, context: &str, _settings: GraphRankSettings) -> Result<Timestamp> {
        let ts = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()) as u64;
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            // status_json expects a String column; pass a single-quoted JSON text
            let status_q = "'{}'".to_string();
            let script = format!(
                r#":insert grapevine {{ observer: '{observer}', context: '{context}', ts: {ts}, status_json: {status_q}, expires: null }};"#,
                observer = observer,
                context = context,
                ts = ts,
                status_q = status_q,
            );
            let res = db.run_script(&script, Default::default(), cozo::ScriptMutability::Mutable);
            if let Err(e) = res {
                error!("cozo grapevine_generate error: {:?}\nscript=<<{}>>", e, script);
                return Err(StoreError::Internal);
            }
        }
        Ok(ts)
    }

    pub async fn scorecards_get(&self, observer: &Hex, context: &str, ts: Option<Timestamp>) -> Result<Option<Vec<Scorecard>>> {
        let Some(tsv) = ts else { return Ok(None) };
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let script = format!(
                "?[subject, score, confidence, interpretersums_json] := scorecard{{ observer: \"{}\", context: \"{}\", ts: {}, subject: subject, score: score, confidence: confidence, interpretersums_json: interpretersums_json }}",
                observer, context, tsv
            );
            let res = db
                .run_script(&script, Default::default(), cozo::ScriptMutability::Immutable)
                .map_err(|_| StoreError::Internal)?;
            let mut out: Vec<Scorecard> = Vec::new();
            if let Some(rows) = res.rows.get(0) {
                for row in rows {
                    if let Ok(val) = serde_json::to_value(row) {
                        if let Some(arr) = val.as_array() {
                            if arr.len() == 4 {
                                let subject = arr[0].as_str().unwrap_or_default().to_string();
                                let score = arr[1].as_f64().unwrap_or(0.0);
                                let confidence = arr[2].as_f64().unwrap_or(0.0);
                                let interpretersums_json: serde_json::Value = serde_json::from_str(
                                    arr[3].as_str().unwrap_or("{}"),
                                )
                                .unwrap_or_default();
                                out.push(Scorecard { subject, score, confidence, interpretersums_json });
                            }
                        }
                    }
                }
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    pub async fn scorecards_put(&self, observer: &Hex, context: &str, ts: Timestamp, list: &[Scorecard]) -> Result<()> {
        #[cfg(feature = "cozo-native")]
        if let Some(db) = &self.db {
            let observer_q = format!("'{}'", observer);
            let context_q = format!("'{}'", context.replace('\'', "''"));
            let mut script = format!(
                ":delete scorecard {{ observer: observer, context: context, ts: ts, subject: subject, score: score, confidence: confidence, interpretersums_json: interpretersums_json }} WHERE observer = {observer_q} and context = {context_q} and ts = {ts};\n",
                observer_q = observer_q,
                context_q = context_q,
                ts = ts,
            );
            for sc in list.iter() {
                let subj_q = format!("'{}'", sc.subject.replace('\'', "''"));
                let score_v = sc.score;
                let conf_v = sc.confidence;
                let sums_text = serde_json::to_string(&sc.interpretersums_json).unwrap_or("{}".to_string());
                let sums_q = format!("'{}'", sums_text.replace('\'', "''"));
                script.push_str(&format!(
                    ":insert scorecard {{ observer: {observer_q}, context: {context_q}, ts: {ts}, subject: {subj_q}, score: {score_v}, confidence: {conf_v}, interpretersums_json: {sums_q} }};\n",
                    observer_q = observer_q,
                    context_q = context_q,
                    ts = ts,
                    subj_q = subj_q,
                    score_v = score_v,
                    conf_v = conf_v,
                    sums_q = sums_q,
                ));
            }
            let res = db.run_script(&script, Default::default(), cozo::ScriptMutability::Mutable);
            if let Err(e) = res {
                error!("cozo scorecards_put error: {:?}\nscript=<<{}>>", e, script);
                return Err(StoreError::Internal);
            }
            return Ok(());
        }
        Ok(())
    }
}
