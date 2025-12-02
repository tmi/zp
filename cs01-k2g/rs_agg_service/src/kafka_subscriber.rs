use prost::Message;
use crate::common::{self, AppState};
use anyhow::Result;
use chrono::Utc;
use rdkafka::{consumer::{Consumer, StreamConsumer}, ClientConfig, Message as KafkaMessage};
use std::sync::Arc;
use tracing::info;

pub async fn run_kafka_consumer(app_state: Arc<AppState>) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:9092")
        .set("group.id", "rs_agg_service_group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&["t1"]).expect("Can't subscribe to topic");

    info!("Kafka consumer started. Subscribed to topic 't1'");

    let mut current_second_timestamp_ms: i64 = Utc::now().timestamp_millis() / 1000 * 1000; // Rounded down to the nearest second, in milliseconds
    let mut message_count_in_second: u64 = 0;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match common::message::Message::decode(payload) {
                        Ok(decoded_message) => {
                            let message_timestamp_ms = decoded_message.timestamp;

                            if message_timestamp_ms < current_second_timestamp_ms + 1000 { // 1 second in milliseconds
                                message_count_in_second += 1;
                            } else {
                                let mut app_state_lock = app_state.aggregated_data.lock().await;
                                app_state_lock.data.insert(current_second_timestamp_ms, message_count_in_second);
                                crate::common::safe_broadcast(&app_state.tx, (current_second_timestamp_ms, message_count_in_second));

                                current_second_timestamp_ms = message_timestamp_ms / 1000 * 1000; // Update T
                                message_count_in_second = 1; // Reset C and count the current message
                            }
                        },
                        Err(e) => {
                            eprintln!("Failed to decode message: {:?}", e);
                        }
                    }
                }
            },
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}