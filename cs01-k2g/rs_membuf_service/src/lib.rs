use axum::{extract::State, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use rdkafka::{consumer::{Consumer, StreamConsumer}, ClientConfig, Message as KafkaMessage};
use serde::{Deserialize, Serialize};
use std::{sync::{Arc, Mutex}, time::Duration};
use tokio::time::sleep;
use prost::Message; // Add this line

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kafka_pg_grafana.rs"));
}

pub use proto::Message as ProtoMessage; // Alias proto::Message to avoid conflict and make it public

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
    pub key: i32,
    pub value: String,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub received_at: DateTime<Utc>,
}

impl From<ProtoMessage> for StoredMessage {
    fn from(msg: ProtoMessage) -> Self {
        let timestamp_ms = msg.timestamp;
        let timestamp_secs = timestamp_ms / 1000;
        let nanos = (timestamp_ms % 1000) * 1_000_000;
        let timestamp = DateTime::<Utc>::from_timestamp(timestamp_secs, nanos as u32).unwrap_or_else(|| Utc::now());

        StoredMessage {
            timestamp,
            key: msg.key,
            value: msg.value,
            received_at: Utc::now(),
        }
    }
}

// App state
pub type SharedMessageBuffer = Arc<Mutex<Vec<StoredMessage>>>;

pub fn app(app_state: SharedMessageBuffer) -> Router {
    Router::new()
        .route("/fetch", get(fetch_messages))
        .with_state(app_state)
}

pub async fn fetch_messages(State(state): State<SharedMessageBuffer>) -> Json<Vec<StoredMessage>> {
    let buffer = state.lock().unwrap();
    Json(buffer.clone())
}

pub async fn run_consumer_task(consumer_state: SharedMessageBuffer) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:9092")
        .set("group.id", "membuf_service_group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&["t1"]).expect("Can't subscribe to topic");

    loop {
        match consumer.recv().await {
            Ok(m) => {
                let payload = match m.payload() {
                    Some(p) => p,
                    None => continue,
                };

                match ProtoMessage::decode(payload) {
                    Ok(proto_message) => {
                        let stored_message: StoredMessage = proto_message.into();
                        consumer_state.lock().unwrap().push(stored_message);
                    }
                    Err(e) => {
                        eprintln!("Failed to decode protobuf message: {:?}", e);
                    }
                }
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}

pub async fn run_cleanup_task(cleanup_state: SharedMessageBuffer) {
    loop {
        sleep(Duration::from_secs(60)).await;
        let mut buffer = cleanup_state.lock().unwrap();
        let one_minute_ago = Utc::now() - chrono::Duration::minutes(1);
        buffer.retain(|msg| msg.received_at >= one_minute_ago);
        println!("Cleaned up old messages. Current buffer size: {}", buffer.len());
    }
}
