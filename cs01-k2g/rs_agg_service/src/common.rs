use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod message {
    include!(concat!(env!("OUT_DIR"), "/kafka_pg_grafana.rs"));
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AggregatedData {
    pub data: HashMap<i64, u64>,
}

#[derive(Clone)]
pub struct AppState {
    pub aggregated_data: AggregatedData,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            aggregated_data: AggregatedData::default(),
        }
    }
}
