use rs_membuf_service::{
    app,
    run_consumer_task,
    run_cleanup_task,
    SharedMessageBuffer,
};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let app_state: SharedMessageBuffer = Arc::new(Mutex::new(Vec::new()));

    // Kafka Consumer Task
    let consumer_state = Arc::clone(&app_state);
    tokio::spawn(run_consumer_task(consumer_state));

    // Background Cleanup Task
    let cleanup_state = Arc::clone(&app_state);
    tokio::spawn(run_cleanup_task(cleanup_state));

    // Axum server
    let app = app(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    println!("Listening on 0.0.0.0:8000");
    axum::serve(listener, app).await.unwrap();
}