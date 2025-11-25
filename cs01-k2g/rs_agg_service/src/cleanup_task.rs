use crate::common::AppState;
use chrono::{Duration, Utc};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn cleanup_task(app_state: Arc<Mutex<AppState>>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        info!("Running cleanup task...");
        let mut app_state_lock = app_state.lock().await;
        let ten_minutes_ago = (Utc::now() - Duration::minutes(10)).timestamp_millis();
        app_state_lock.aggregated_data.data.retain(|timestamp, _| *timestamp > ten_minutes_ago);
        info!("Cleanup task finished. Current data points: {}", app_state_lock.aggregated_data.data.len());
    }
}