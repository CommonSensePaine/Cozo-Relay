#![allow(dead_code)]

pub struct Cursor {
    pub created_at: u64,
    pub id: String,
}

impl Cursor {
    pub fn encode(&self) -> String { format!("{}:{}", self.created_at, self.id) }
    pub fn decode(s: &str) -> Option<Self> {
        let mut parts = s.splitn(2, ':');
        let a = parts.next()?.parse::<u64>().ok()?;
        let b = parts.next()?.to_string();
        Some(Self { created_at: a, id: b })
    }
}
