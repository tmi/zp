use anyhow::Result;
use axum::{routing::get, Router};
use clap::Parser;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod common;
mod kafka_subscriber;
    mod cleanup_task;
    mod fetch_endpoint;
    mod updates_endpoint;
    mod simulator;

    #[derive(Parser, Debug)]
    #[command(author, version, about, long_about = None)]
    struct Cli {
        /// Kafka bootstrap server URL
        #[arg(long, default_value = "kafka:9092")]
        kafka: String,

        /// Binding URL for the TCP listener
        #[arg(long, default_value = "0.0.0.0:3000")]
        url: String,

        /// If true, do not spawn kafka subscriber, but instead spawn a tokio task which every 10-20 milliseconds (random) sends to the app state tx a value (current timestamp, 10-100)
        #[arg(long, default_value_t = false)]
        simulate: bool,
    }

    #[tokio::main]
    async fn main() -> Result<()> {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        let cli = Cli::parse();

        info!("Starting rs_agg_service with config: {:?}", cli);

        let app_state = Arc::new(common::AppState::new());

        let app = Router::new()
            .route("/fetch", get(fetch_endpoint::fetch_aggregated_data))
            .route("/updates", get(updates_endpoint::ws_handler))
            .with_state(app_state.clone());

        let listener = tokio::net::TcpListener::bind(&cli.url)
            .await
            .expect(&format!("Failed to bind to {}", &cli.url));
        info!("HTTP server listening on {}", listener.local_addr()?);

        if cli.simulate {
            tokio::spawn(simulator::run_simulator(app_state.clone()));
        } else {
            tokio::spawn(kafka_subscriber::run_kafka_consumer(app_state.clone()));
        }
        tokio::spawn(cleanup_task::cleanup_task(app_state.clone()));

        axum::serve(listener, app).await?;

        Ok(())
    }