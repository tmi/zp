use async_trait::async_trait;
use std::time::Instant;
use tokio_postgres::Client;

use super::TableOperations;

pub struct TableD;

#[async_trait]
impl TableOperations for TableD {
    async fn create(&self, client: &mut Client) {
        client
            .batch_execute("CREATE TYPE inner_struct AS (inner_id VARCHAR, volume INT, threshold REAL)")
            .await
            .ok();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS tabled (
                    block_id VARCHAR,
                    outer_id VARCHAR,
                    outer_ts TIMESTAMPTZ,
                    inner_structs inner_struct[],
                    PRIMARY KEY (block_id, outer_id)
                )",
            )
            .await
            .unwrap();
        println!("Created tabled");
    }

    async fn clean(&self, client: &mut Client) {
        client
            .execute("TRUNCATE TABLE tabled", &[])
            .await
            .unwrap();
        println!("Cleaned tabled");
    }

    async fn fill(&self, _client: &mut Client, _n: i32, _block_size: i32, _outer_size: i32) {
        panic!("fill for table d is not implemented");
    }

    async fn query(&self, client: &mut Client) {
        let start = Instant::now();
        let rows = client
            .query(
                "SELECT
                    (s.inner).volume
                FROM (
                    SELECT unnest(inner_structs) as inner FROM tabled
                ) as s
                WHERE (s.inner).threshold > 0.5",
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