use crate::common::AppState;
use axum::{extract::State, Json};
use chrono::{Duration, Utc};
use std::{collections::HashMap, sync::Arc};

pub async fn fetch_aggregated_data(State(app_state): State<Arc<AppState>>) -> Json<HashMap<i64, u64>> {
    let mut app_state_lock = app_state.aggregated_data.lock().await;
    let ten_minutes_ago = (Utc::now() - Duration::minutes(10)).timestamp_millis();

    app_state_lock.data.retain(|timestamp, _| *timestamp > ten_minutes_ago);

    Json(app_state_lock.data.clone())
}