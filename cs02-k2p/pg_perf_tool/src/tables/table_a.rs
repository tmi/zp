use async_trait::async_trait;
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::Client;
use rand::{thread_rng, Rng};

use super::TableOperations;

pub struct TableA;

#[async_trait]
impl TableOperations for TableA {
    fn table_name(&self) -> &str {
        "tablea"
    }

    fn create_string(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS tablea (
            block_id VARCHAR PRIMARY KEY,
            data JSONB
        )"
    }

    fn query_string(&self) -> &str {
        "SELECT
            (d->>'volume')::INT as volume
        FROM (
            SELECT jsonb_array_elements(d->2) as d
            FROM (
                SELECT jsonb_array_elements(data) as d from tablea
            ) as outer_structs
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
            let data = json!(outer_structs);
            client
                .execute(
                    "INSERT INTO tablea (block_id, data) VALUES ($1, $2)",
                    &[&block_id, &data],
                )
                .await
                .unwrap();
        }

        let duration = start.elapsed();
        println!("Inserted {} blocks in {:?}", n, duration);
    }
}
