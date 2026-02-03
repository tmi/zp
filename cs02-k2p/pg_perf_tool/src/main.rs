use clap::{Parser, Subcommand};
use rand::{thread_rng, Rng};
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::{
    types::{to_sql_checked, IsNull, ToSql, Type},
    Client, NoTls,
};
use bytes::BytesMut;

//
// This is the best attempt to implement the fill for table d.
//
// #[derive(Debug)]
// struct InnerStruct {
//     inner_id: String,
//     volume: i32,
//     threshold: f32,
// }
//
// impl ToSql for InnerStruct {
//     fn to_sql(
//         &self,
//         ty: &Type,
//         out: &mut BytesMut,
//     ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
//         out.extend_from_slice(&3i32.to_be_bytes());
//         self.inner_id.to_sql(ty, out)?;
//         self.volume.to_sql(ty, out)?;
//         self.threshold.to_sql(ty, out)?;
//         Ok(IsNull::No)
//     }

//     fn accepts(ty: &Type) -> bool {
//         matches!(ty.name(), "inner_struct")
//     }

//     to_sql_checked!();
// }

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
enum Commands {
    /// Creates the tables
    Create,
    /// Cleans the specified table
    Clean {
        #[arg(value_name = "TABLE")]
        table: Table,
    },
    /// Fills the specified table with random data
    Fill {
        #[arg(value_name = "TABLE")]
        table: Table,
        #[arg(value_name = "N")]
        n: i32,
        #[arg(value_name = "BLOCK_SIZE")]
        block_size: i32,
        #[arg(value_name = "OUTER_SIZE")]
        outer_size: i32,
    },
    /// Queries the specified table
    Query {
        #[arg(value_name = "TABLE")]
        table: Table,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Table {
    TableA,
    TableB,
    TableC,
    TableD,
    TableE,
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

async fn connect() -> Client {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password=postgres", NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Create => {
            let mut client = connect().await;
            create_tables(&mut client).await;
        }
        Commands::Clean { table } => {
            let mut client = connect().await;
            clean_table(&mut client, table).await;
        }
        Commands::Fill {
            table,
            n,
            block_size,
            outer_size,
        } => {
            let mut client = connect().await;
            fill_table(&mut client, table, *n, *block_size, *outer_size).await;
        }
        Commands::Query { table } => {
            let mut client = connect().await;
            query_table(&mut client, table).await;
        }
    }
}

async fn create_tables(client: &mut Client) {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS tablea (
                block_id VARCHAR PRIMARY KEY,
                data JSONB
            )",
        )
        .await
        .unwrap();
    println!("Created tablea");

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS tableb (
                block_id VARCHAR,
                outer_id VARCHAR,
                outer_ts TIMESTAMPTZ,
                inner_structs JSONB,
                PRIMARY KEY (block_id, outer_id)
            )",
        )
        .await
        .unwrap();
    println!("Created tableb");

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS tablec (
                block_id VARCHAR,
                outer_id VARCHAR,
                outer_ts TIMESTAMPTZ,
                inner_id VARCHAR,
                volume INT,
                threshold REAL,
                PRIMARY KEY (block_id, outer_id, inner_id)
            )",
        )
        .await
        .unwrap();
    println!("Created tablec");

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

async fn clean_table(client: &mut Client, table: &Table) {
    client
        .execute(format!("TRUNCATE TABLE {}", table).as_str(), &[])
        .await
        .unwrap();
    println!("Cleaned table {}", table);
}

async fn fill_table(client: &mut Client, table: &Table, n: i32, block_size: i32, outer_size: i32) {
    let mut rng = thread_rng();

    let blocks = (0..n)
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
        .collect::<Vec<_>>();

    let start = Instant::now();

    for (block_id, outer_structs) in blocks {
        match table {
            Table::TableA => {
                let data = json!(outer_structs);
                client
                    .execute(
                        "INSERT INTO tablea (block_id, data) VALUES ($1, $2)",
                        &[&block_id, &data],
                    )
                    .await
                    .unwrap();
            }
            Table::TableB => {
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
            Table::TableC => {
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
            Table::TableD => {
                panic!("fill for table d is not implemented");
            }
            Table::TableE => {
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
        }
    }

    let duration = start.elapsed();
    println!("Inserted {} blocks in {:?}", n, duration);
}

async fn query_table(client: &mut Client, table: &Table) {
    let start = Instant::now();
    let rows = match table {
        Table::TableA => {
            client
                .query(
                    "SELECT
                        (d->>'volume')::INT as volume
                    FROM (
                        SELECT jsonb_array_elements(d->2) as d
                        FROM (
                            SELECT jsonb_array_elements(data) as d from tablea
                        ) as outer_structs
                    ) as inner_structs
                    WHERE (d->>'threshold')::REAL > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        Table::TableB => {
            client
                .query(
                    "SELECT
                        (d->>'volume')::INT as volume
                    FROM (
                        SELECT jsonb_array_elements(inner_structs) as d from tableb
                    ) as inner_structs
                    WHERE (d->>'threshold')::REAL > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        Table::TableC => {
            client
                .query(
                    "SELECT volume FROM tablec WHERE threshold > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        Table::TableD => {
            client
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
                .unwrap()
        }
        Table::TableE => {
            client
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
                .unwrap()
        }
    };

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
