use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{broadcast, Mutex};
use tracing::error;

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

pub fn safe_broadcast<T: Clone + std::fmt::Debug>(sender: &broadcast::Sender<T>, message: T) {
    if let Err(e) = sender.send(message) {
        if sender.receiver_count() > 0 {
            error!("Failed to send message to broadcast channel, but receivers exist: {:?}", e);
        } else {
            // No receivers, so it's expected to fail. Do nothing.
        }
    }
}
