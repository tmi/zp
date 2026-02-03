use clap::{Parser, Subcommand};
use rand::Rng;
use serde_json::json;
use std::time::{Instant, SystemTime};
use tokio_postgres::{Client, NoTls};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Creates the tables
    Create,
    /// Cleans the specified table
    Clean {
        #[arg(value_name = "TABLE")]
        table: String,
    },
    /// Fills the specified table with random data
    Fill {
        #[arg(value_name = "TABLE")]
        table: String,
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
        table: String,
    },
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
            "CREATE TABLE IF NOT EXISTS table_a (
                block_id VARCHAR PRIMARY KEY,
                data JSONB
            )",
        )
        .await
        .unwrap();
    println!("Created table_a");

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS table_b (
                block_id VARCHAR,
                outer_id VARCHAR,
                outer_ts TIMESTAMPTZ,
                inner_structs JSONB,
                PRIMARY KEY (block_id, outer_id)
            )",
        )
        .await
        .unwrap();
    println!("Created table_b");

    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS table_c (
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
    println!("Created table_c");
}

async fn clean_table(client: &mut Client, table: &str) {
    client
        .execute(format!("TRUNCATE TABLE {}", table).as_str(), &[])
        .await
        .unwrap();
    println!("Cleaned table {}", table);
}

async fn fill_table(client: &mut Client, table: &str, n: i32, block_size: i32, outer_size: i32) {
    let mut rng = rand::thread_rng();

    let blocks = (0..n)
        .map(|i| {
            let outer_structs = (0..block_size)
                .map(|j| {
                    let inner_structs = (0..outer_size)
                        .map(|k| {
                            json!({
                                "inner_id": format!("inner_{}_{}_{}", i, j, k),
                                "volume": rng.gen_range(0..=100),
                                "threshold": rng.gen_range(0.0..=1.0)
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
            "table_a" => {
                let data = json!(outer_structs);
                client
                    .execute(
                        "INSERT INTO table_a (block_id, data) VALUES ($1, $2)",
                        &[&block_id, &data],
                    )
                    .await
                    .unwrap();
            }
            "table_b" => {
                for (outer_id, outer_ts, inner_structs) in outer_structs {
                    let data = json!(inner_structs);
                    client
                        .execute(
                            "INSERT INTO table_b (block_id, outer_id, outer_ts, inner_structs) VALUES ($1, $2, $3, $4)",
                            &[&block_id, &outer_id, &outer_ts, &data],
                        )
                        .await
                        .unwrap();
                }
            }
            "table_c" => {
                for (outer_id, outer_ts, inner_structs) in outer_structs {
                    for inner in inner_structs {
                        let volume = inner["volume"].as_i64().unwrap() as i32;
                        let threshold = inner["threshold"].as_f64().unwrap() as f32;
                        client
                            .execute(
                                "INSERT INTO table_c (block_id, outer_id, outer_ts, inner_id, volume, threshold) VALUES ($1, $2, $3, $4, $5, $6)",
                                &[&block_id, &outer_id, &outer_ts, &inner["inner_id"].as_str().unwrap(), &volume, &threshold],
                            )
                            .await
                            .unwrap();
                    }
                }
            }
            _ => panic!("Unknown table"),
        }
    }

    let duration = start.elapsed();
    println!("Inserted {} blocks in {:?}", n, duration);
}

async fn query_table(client: &mut Client, table: &str) {
    let start = Instant::now();
    let rows = match table {
        "table_a" => {
            client
                .query(
                    "SELECT
                        (d->>'volume')::INT as volume
                    FROM (
                        SELECT jsonb_array_elements(d->2) as d
                        FROM (
                            SELECT jsonb_array_elements(data) as d from table_a
                        ) as outer_structs
                    ) as inner_structs
                    WHERE (d->>'threshold')::REAL > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        "table_b" => {
            client
                .query(
                    "SELECT
                        (d->>'volume')::INT as volume
                    FROM (
                        SELECT jsonb_array_elements(inner_structs) as d from table_b
                    ) as inner_structs
                    WHERE (d->>'threshold')::REAL > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        "table_c" => {
            client
                .query(
                    "SELECT volume FROM table_c WHERE threshold > 0.5",
                    &[],
                )
                .await
                .unwrap()
        }
        _ => panic!("Unknown table"),
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