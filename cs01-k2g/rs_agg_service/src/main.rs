use anyhow::Result;
use axum::{routing::get, Router};
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod common;
mod kafka_subscriber;
mod cleanup_task;
mod fetch_endpoint;
mod updates_endpoint;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    info!("Starting rs_agg_service...");

    let app_state = Arc::new(common::AppState::new());

    let app = Router::new()
        .route("/fetch", get(fetch_endpoint::fetch_aggregated_data))
        .route("/updates", get(updates_endpoint::ws_handler))
        .with_state(app_state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("Failed to bind to port 3000");
    info!("HTTP server listening on {}", listener.local_addr()?);

    tokio::spawn(kafka_subscriber::run_kafka_consumer(app_state.clone()));
    tokio::spawn(cleanup_task::cleanup_task(app_state.clone()));

    axum::serve(listener, app).await?;

    Ok(())
}