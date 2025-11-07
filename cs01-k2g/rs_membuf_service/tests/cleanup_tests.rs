use rs_membuf_service::proto;
use rs_membuf_service::{
    StoredMessage,
    SharedMessageBuffer,
};
use chrono::{DateTime, Utc};
use std::{sync::{Arc, Mutex}, time::Duration};
use tokio::time::sleep;

// Helper function to create a StoredMessage for testing
fn create_test_stored_message(timestamp_ms: i64, received_at: DateTime<Utc>) -> StoredMessage {
    let timestamp_secs = timestamp_ms / 1000;
    let nanos = (timestamp_ms % 1000) * 1_000_000;
    let timestamp = DateTime::<Utc>::from_timestamp(timestamp_secs, nanos as u32).unwrap_or_else(|| Utc::now());

    StoredMessage {
        timestamp,
        key: 1,
        value: "test".to_string(),
        received_at,
    }
}

#[tokio::test]
async fn test_cleanup_task() {
    let app_state: SharedMessageBuffer = Arc::new(Mutex::new(Vec::new()));
    let cleanup_state: SharedMessageBuffer = Arc::clone(&app_state);

    // Add messages: one old, one new
    let now = Utc::now();
    let old_message_time = now - chrono::Duration::minutes(2);
    let new_message_time = now - chrono::Duration::seconds(30);

    app_state.lock().unwrap().push(create_test_stored_message(1000, old_message_time));
    app_state.lock().unwrap().push(create_test_stored_message(2000, new_message_time));

    assert_eq!(app_state.lock().unwrap().len(), 2);

    // Manually run the cleanup logic (simulate the background task)
    let mut buffer = cleanup_state.lock().unwrap();
    let one_minute_ago = Utc::now() - chrono::Duration::minutes(1);
    buffer.retain(|msg| msg.received_at >= one_minute_ago);

    // Check if the old message was removed
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer[0].key, 1); // The new message should remain
    assert_eq!(buffer[0].received_at, new_message_time);
}

#[tokio::test]
async fn test_cleanup_empty_buffer() {
    let app_state: SharedMessageBuffer = Arc::new(Mutex::new(Vec::new()));
    let cleanup_state: SharedMessageBuffer = Arc::clone(&app_state);

    assert_eq!(app_state.lock().unwrap().len(), 0);

    // Manually run the cleanup logic
    let mut buffer = cleanup_state.lock().unwrap();
    let one_minute_ago = Utc::now() - chrono::Duration::minutes(1);
    buffer.retain(|msg| msg.received_at >= one_minute_ago);

    assert_eq!(buffer.len(), 0);
}

#[tokio::test]
async fn test_cleanup_all_old_messages() {
    let app_state: SharedMessageBuffer = Arc::new(Mutex::new(Vec::new()));
    let cleanup_state: SharedMessageBuffer = Arc::clone(&app_state);

    let now = Utc::now();
    let old_message_time1 = now - chrono::Duration::minutes(2);
    let old_message_time2 = now - chrono::Duration::minutes(3);

    app_state.lock().unwrap().push(create_test_stored_message(1000, old_message_time1));
    app_state.lock().unwrap().push(create_test_stored_message(2000, old_message_time2));

    assert_eq!(app_state.lock().unwrap().len(), 2);

    // Manually run the cleanup logic
    let mut buffer = cleanup_state.lock().unwrap();
    let one_minute_ago = Utc::now() - chrono::Duration::minutes(1);
    buffer.retain(|msg| msg.received_at >= one_minute_ago);

    assert_eq!(buffer.len(), 0);
}
