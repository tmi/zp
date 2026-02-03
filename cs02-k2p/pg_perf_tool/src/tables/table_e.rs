use async_trait::async_trait;
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::Client;
use rand::{thread_rng, Rng};

use super::TableOperations;

pub struct TableE;

#[async_trait]
impl TableOperations for TableE {
    async fn create(&self, client: &mut Client) {
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS tablee (
                    block_id VARCHAR,
                    outer_id VARCHAR,
                    outer_ts TIMESTAMPTZ,
                    inner_ids VARCHAR[],
                    volumes INT[],
                    thresholds REAL[],
                    PRIMARY KEY (block_id, outer_id)
                )",
            )
            .await
            .unwrap();
        println!("Created tablee");
    }

    async fn clean(&self, client: &mut Client) {
        client
            .execute("TRUNCATE TABLE tablee", &[])
            .await
            .unwrap();
        println!("Cleaned tablee");
    }

    async fn fill(&self, client: &mut Client, n: i32, block_size: i32, outer_size: i32) {
        let blocks = {
            let mut rng = thread_rng();
            (0..n)
                .map(|i| {
                    let outer_structs = (0..block_size)
                        .map(|j| {
                            let inner_structs = (0..outer_size)
                                .map(|k| {
                                    json!({
                                        "inner_id": format!("inner_{}_{}_{}", i, j, k),
                                        "volume": rng.gen_range(0..=100),
                                        "threshold": rng.gen_range(0.0..=1.0),
                                    })
                                })
                                .collect::<Vec<_>>();
                            (
                                format!("outer_{}_{}", i, j), // outer_id
                                SystemTime::now(),            // outer_ts
                                inner_structs,
                            )
                        })
                        .collect::<Vec<_>>();
                    (format!("block_{}", i), outer_structs) // block_id
                })
                .collect::<Vec<_>>()
        };

        let start = Instant::now();

        for (block_id, outer_structs) in blocks {
            for (outer_id, outer_ts, inner_structs) in outer_structs {
                let mut inner_ids = Vec::new();
                let mut volumes = Vec::new();
                let mut thresholds = Vec::new();
                for inner in inner_structs {
                    inner_ids.push(inner["inner_id"].as_str().unwrap().to_string());
                    volumes.push(inner["volume"].as_i64().unwrap() as i32);
                    thresholds.push(inner["threshold"].as_f64().unwrap() as f32);
                }
                client
                    .execute(
                        "INSERT INTO tablee (block_id, outer_id, outer_ts, inner_ids, volumes, thresholds) VALUES ($1, $2, $3, $4, $5, $6)",
                        &[&block_id, &outer_id, &outer_ts, &inner_ids, &volumes, &thresholds],
                    )
                    .await
                    .unwrap();
            }
        }

        let duration = start.elapsed();
        println!("Inserted {} blocks in {:?}", n, duration);
    }

    async fn query(&self, client: &mut Client) {
        let start = Instant::now();
        let rows = client
            .query(
                "SELECT
                    s.volume
                FROM (
                    SELECT unnest(volumes) as volume, unnest(thresholds) as threshold FROM tablee
                ) as s
                WHERE s.threshold > 0.5",
                &[],
            )
            .await
            .unwrap();

        let duration = start.elapsed();

        let mut total_volume = 0;
        for row in &rows {
            let volume: i32 = row.get("volume");
            total_volume += volume;
        }

        println!("Query took {:?}", duration);
        println!("Count: {}", rows.len());
        println!("Total volume: {}", total_volume);
    }
}