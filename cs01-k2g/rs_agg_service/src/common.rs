use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{broadcast, Mutex};

pub mod message {
    include!(concat!(env!("OUT_DIR"), "/kafka_pg_grafana.rs"));
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AggregatedData {
    pub data: HashMap<i64, u64>,
}

pub struct AppState {
    pub aggregated_data: Mutex<AggregatedData>,
    pub tx: broadcast::Sender<(i64, u64)>,
}

impl AppState {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(100); // Buffer for 100 messages
        Self {
            aggregated_data: Mutex::new(AggregatedData::default()),
            tx,
        }
    }
}
