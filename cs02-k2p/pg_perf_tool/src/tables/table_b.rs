use async_trait::async_trait;
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::Client;
use rand::{thread_rng, Rng};

use super::TableOperations;

pub struct TableB;

#[async_trait]
impl TableOperations for TableB {
    fn table_name(&self) -> &str {
        "tableb"
    }

    fn create_string(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS tableb (
            block_id VARCHAR,
            outer_id VARCHAR,
            outer_ts TIMESTAMPTZ,
            inner_structs JSONB,
            PRIMARY KEY (block_id, outer_id)
        )"
    }

    fn query_string(&self) -> &str {
        "SELECT
            (d->>'volume')::INT as volume
        FROM (
            SELECT jsonb_array_elements(inner_structs) as d from tableb
        ) as inner_structs
        WHERE (d->>'threshold')::REAL > 0.5"
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
                let data = json!(inner_structs);
                client
                    .execute(
                        "INSERT INTO tableb (block_id, outer_id, outer_ts, inner_structs) VALUES ($1, $2, $3, $4)",
                        &[&block_id, &outer_id, &outer_ts, &data],
                    )
                    .await
                    .unwrap();
            }
        }

        let duration = start.elapsed();
        println!("Inserted {} blocks in {:?}", n, duration);
    }
}
