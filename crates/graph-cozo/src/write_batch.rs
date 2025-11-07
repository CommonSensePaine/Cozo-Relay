#![allow(dead_code)]

use crate::*;

#[cfg(feature = "cozo-native")]
pub fn build_insert_script(events: &[nostr_model::NostrEvent]) -> String {
    let mut script = String::new();
    for ev in events {
        let tags_json = serde_json::to_string(&ev.tags).unwrap_or("[]".to_string());
        let content = serde_json::to_string(&ev.content).unwrap_or("\"\"".to_string());
        script.push_str(&format!(
            ":insert event {{ id: '{}', kind: {}, pubkey: '{}', created_at: {}, content: {}, sig: '{}', expires_at: null, tags_json: {} }};",
            ev.id, ev.kind, ev.pubkey, ev.created_at, content, ev.sig, tags_json
        ));
        // Incremental materializations
        script.push_str(&format!(
            ":insert hot_latest {{ event_id: '{}', pubkey: '{}', kind: {}, created_at: {} }};",
            ev.id, ev.pubkey, ev.kind, ev.created_at
        ));
        script.push_str(&format!(
            ":insert latest_by_kind {{ event_id: '{}', kind: {}, created_at: {} }};",
            ev.id, ev.kind, ev.created_at
        ));
        script.push_str(&format!(
            ":insert latest_by_author {{ event_id: '{}', pubkey: '{}', created_at: {} }};",
            ev.id, ev.pubkey, ev.created_at
        ));
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
    }
    script
}
