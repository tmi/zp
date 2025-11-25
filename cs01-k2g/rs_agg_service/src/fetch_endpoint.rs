use crate::common::AppState;
use axum::{extract::State, Json};
use chrono::{Duration, Utc};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub async fn fetch_aggregated_data(State(app_state): State<Arc<Mutex<AppState>>>) -> Json<HashMap<i64, u64>> {
    let mut app_state_lock = app_state.lock().await;
    let ten_minutes_ago = (Utc::now() - Duration::minutes(10)).timestamp_millis();

    app_state_lock.aggregated_data.data.retain(|timestamp, _| *timestamp > ten_minutes_ago);

    Json(app_state_lock.aggregated_data.data.clone())
}