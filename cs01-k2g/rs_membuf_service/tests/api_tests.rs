use axum::{body::Body, http::{Request, StatusCode}, Router};
use http_body_util::BodyExt;
use serde_json::Value;
use rs_membuf_service::{
    app,
    SharedMessageBuffer,
    StoredMessage,
};
use std::sync::{Arc, Mutex};
use chrono::{Utc};
use tower::util::ServiceExt; // Add this import

async fn setup_app_with_messages(messages: Vec<StoredMessage>) -> Router {
    let app_state: SharedMessageBuffer = Arc::new(Mutex::new(messages));
    app(app_state)
}

#[tokio::test]
async fn test_fetch_messages_empty() {
    let app = setup_app_with_messages(vec![]).await;

    let response = app
        .oneshot(Request::builder().uri("/fetch").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_fetch_messages_with_data() {
    let now = Utc::now();
    let messages = vec![
        StoredMessage {
            timestamp: now - chrono::Duration::seconds(10),
            key: 1,
            value: "hello".to_string(),
            received_at: now - chrono::Duration::seconds(10),
        },
        StoredMessage {
            timestamp: now - chrono::Duration::seconds(5),
            key: 2,
            value: "world".to_string(),
            received_at: now - chrono::Duration::seconds(5),
        },
    ];
    let app = setup_app_with_messages(messages.clone()).await;

    let response = app
        .oneshot(Request::builder().uri("/fetch").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Vec<StoredMessage> = serde_json::from_slice(&body).unwrap();

    assert_eq!(json.len(), 2);
    assert_eq!(json[0].key, messages[0].key);
    assert_eq!(json[1].value, messages[1].value);
}