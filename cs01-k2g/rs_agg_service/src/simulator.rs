use crate::common::AppState;
use anyhow::Result;
use std::sync::Arc;
use tokio::time::Duration;
use rand::Rng;
use chrono::Utc;
use tracing::info;
use tokio::runtime::Handle;

pub async fn run_simulator(app_state: Arc<AppState>) -> Result<()> {
    info!("Starting simulator...");
    let app_state_clone = app_state.clone();
    tokio::task::spawn_blocking(move || {
        let runtime_handle = Handle::current();
        let mut rng = rand::thread_rng();
        loop {
            let delay = rng.gen_range(10..=20);
            std::thread::sleep(Duration::from_millis(delay));

            let value = rng.gen_range(10..=100);
            let timestamp = Utc::now().timestamp_millis() as i64;

            // Block on the async send operation using the runtime handle
            runtime_handle.block_on(async {
                crate::common::safe_broadcast(&app_state_clone.tx, (timestamp, value));
            });
        }
    }).await?;
    Ok(())
}
