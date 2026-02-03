use async_trait::async_trait;
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::Client;
use rand::{thread_rng, Rng};

use super::TableOperations;

pub struct TableC;

#[async_trait]
impl TableOperations for TableC {
    fn table_name(&self) -> &str {
        "tablec"
    }

    fn create_string(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS tablec (
            block_id VARCHAR,
            outer_id VARCHAR,
            outer_ts TIMESTAMPTZ,
            inner_id VARCHAR,
            volume INT,
            threshold REAL,
            PRIMARY KEY (block_id, outer_id, inner_id)
        )"
    }

    fn query_string(&self) -> &str {
        "SELECT volume FROM tablec WHERE threshold > 0.5"
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
                for inner in inner_structs {
                    let volume = inner["volume"].as_i64().unwrap() as i32;
                    let threshold = inner["threshold"].as_f64().unwrap() as f32;
                    client
                        .execute(
                            "INSERT INTO tablec (block_id, outer_id, outer_ts, inner_id, volume, threshold) VALUES ($1, $2, $3, $4, $5, $6)",
                            &[&block_id, &outer_id, &outer_ts, &inner["inner_id"].as_str().unwrap(), &volume, &threshold],
                        )
                        .await
                        .unwrap();
                }
            }
        }

        let duration = start.elapsed();
        println!("Inserted {} blocks in {:?}", n, duration);
    }
}
