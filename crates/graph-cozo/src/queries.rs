#![allow(dead_code)]

use crate::*;

pub struct QueryBuilder;

impl QueryBuilder {
    pub fn order_by_created_then_id(desc: bool) -> String {
        if desc { String::from("-created_at, -id") } else { String::from("created_at, id") }
    }
}
