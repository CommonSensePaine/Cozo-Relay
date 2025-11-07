use serde::{Deserialize, Serialize};

pub mod network {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Ws {
        pub address: String,
        pub port: u16,
        pub max_event_bytes: Option<usize>,
        pub max_ws_message_bytes: Option<usize>,
        pub max_ws_frame_bytes: Option<usize>,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Http {
        pub address: String,
        pub port: u16,
        pub cors: Option<bool>,
    }
}

pub mod database {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Cozo {
        pub path: String,
    }
}

pub mod limits {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Limits {
        pub messages_per_sec: Option<u32>,
        pub subscriptions_per_min: Option<u32>,
        pub db_conns_per_client: Option<u32>,
        pub broadcast_buffer: usize,
        pub event_persist_buffer: usize,
    }
}

pub mod authorization {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Auth {
        pub nip42_auth: bool,
        pub pubkey_whitelist: Option<Vec<String>>,
    }
}

pub mod verification {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Nip05 {
        pub mode: String,
        pub domain_whitelist: Option<Vec<String>>,
        pub domain_blacklist: Option<Vec<String>>,
        pub verify_expiration: Option<String>,
        pub verify_update_frequency: Option<String>,
        pub max_consecutive_failures: Option<u32>,
    }
}

pub mod payments {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PayToRelay {
        pub enabled: bool,
        pub admission_cost: u64,
        pub cost_per_event: u64,
        pub node_url: String,
        pub api_secret: String,
        pub terms_message: String,
        pub sign_ups: bool,
        pub direct_message: bool,
        pub secret_key: Option<String>,
        pub processor: Option<String>,
        pub rune_path: Option<String>,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub ws: network::Ws,
    pub http: network::Http,
    pub database: database::Cozo,
    pub limits: limits::Limits,
    pub authorization: authorization::Auth,
    pub verification: Option<verification::Nip05>,
    pub payments: Option<payments::PayToRelay>,
}
