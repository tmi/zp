use clap::{Parser, Subcommand};
use tokio_postgres::{Client, NoTls};

mod tables;
use tables::{
    table_a::TableA, table_b::TableB, table_c::TableC, table_d::TableD, table_e::TableE,
    util::{generic_clean, generic_create, generic_query},
    TableOperations,
};

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
    let tables: Vec<Box<dyn TableOperations>> = vec![
        Box::new(TableA),
        Box::new(TableB),
        Box::new(TableC),
        Box::new(TableD),
        Box::new(TableE),
    ];

    match &cli.command {
        Commands::Create => {
            let mut client = connect().await;
            client
                .batch_execute(
                    "CREATE TYPE inner_struct AS (inner_id VARCHAR, volume INT, threshold REAL)",
                )
                .await
                .ok();
            for table in tables {
                generic_create(&mut client, table.create_string(), table.table_name()).await;
            }
        }
        Commands::Clean { table } => {
            let mut client = connect().await;
            let table_op = get_table_operations(table);
            generic_clean(&mut client, table_op.table_name()).await;
        }
        Commands::Fill {
            table,
            n,
            block_size,
            outer_size,
        } => {
            let mut client = connect().await;
            get_table_operations(table)
                .fill(&mut client, *n, *block_size, *outer_size)
                .await;
        }
        Commands::Query { table } => {
            let mut client = connect().await;
            let table_op = get_table_operations(table);
            generic_query(&mut client, table_op.query_string()).await;
        }
    }
}

fn get_table_operations(table: &Table) -> Box<dyn TableOperations> {
    match table {
        Table::TableA => Box::new(TableA),
        Table::TableB => Box::new(TableB),
        Table::TableC => Box::new(TableC),
        Table::TableD => Box::new(TableD),
        Table::TableE => Box::new(TableE),
    }
}
