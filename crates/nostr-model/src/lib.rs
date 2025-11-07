use serde::{Deserialize, Serialize};

pub type Hex = String;
pub type EventId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NostrEvent {
    pub id: EventId,
    pub pubkey: Hex,
    pub created_at: u64,
    pub kind: u64,
    pub tags: Vec<Vec<String>>, // simple representation
    pub content: String,
    pub sig: String,
}

impl NostrEvent {
    pub fn verify_id(&self) -> bool { true }
    pub fn verify_sig(&self) -> bool { true }
    pub fn expired(&self, _now: u64) -> bool { false }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NostrFilter {
    pub ids: Option<Vec<String>>,         // hex or prefixes
    pub authors: Option<Vec<Hex>>,        // pubkeys
    pub kinds: Option<Vec<u64>>,          // kinds
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")] 
    pub tags: Option<Vec<(String, Vec<String>)>>, // e.g. ("#p", [pubkey])
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthMsg { pub challenge: String, pub sig: String }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NostrMsg {
    Event(NostrEvent),
    Req { sub_id: String, filters: Vec<NostrFilter> },
    Close { sub_id: String },
    Auth(AuthMsg),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelayInfoConfig {
    pub name: Option<String>,
    pub description: Option<String>,
    pub pubkey: Option<Hex>,
    pub contact: Option<String>,
}

pub fn relay_info_document(cfg: &RelayInfoConfig) -> serde_json::Value {
    serde_json::json!({
        "name": cfg.name.clone().unwrap_or_else(|| "Cozo Social Graph Relay".into()),
        "description": cfg.description.clone().unwrap_or_default(),
        "pubkey": cfg.pubkey.clone().unwrap_or_default(),
        "contact": cfg.contact.clone().unwrap_or_default(),
        "software": "cozo-social-graph-relay",
        "version": "0.1.0",
    })
}
