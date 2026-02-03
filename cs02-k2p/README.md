# PostgreSQL Performance Case Study

This project is a case study for PostgreSQL performance, comparing three different data representations for a nested data structure.

## Project Structure

- `docker-compose.yml`: Defines the PostgreSQL service using the `postgres:13` image.
- `pg_perf_tool/`: A Rust CLI tool for interacting with the database.

## `pg_perf_tool`

The `pg_perf_tool` is a CLI tool built with Clap for managing the database tables and running performance tests.

### Commands

- `create`: Creates the three tables (`table_a`, `table_b`, `table_c`) with different schemas.
- `clean <table>`: Deletes all rows in the specified table.
- `fill <table> <N> <blockSize> <outerSize>`: Fills the specified table with generated data.
  - `N`: The number of blocks to generate.
  - `blockSize`: The number of outer structs per block.
  - `outerSize`: The number of inner structs per outer struct.
- `query <table>`: Queries the specified table to calculate the total volume and count of inner structs with a threshold greater than 0.5.

### Database Schemas

- **`table_a`**: Stores the entire block as a single JSONB object.
- **`table_b`**: Stores one row per outer struct, with the inner structs as a JSONB array.
- **`table_c`**: Stores one row per inner struct, with all data denormalized.

### Known Issues

- The `rand` crate used in this project has some deprecation warnings that appear during compilation. These warnings do not affect the functionality of the tool and have been ignored for now.

## Getting Started

1.  **Start the database:**
    ```bash
    docker compose up -d
    ```

2.  **Build the tool:**
    ```bash
    cd pg_perf_tool
    cargo build
    ```

3.  **Run commands:**
    ```bash
    ./target/debug/pg_perf_tool <command>
    ```

4.  **Stop the database:**
    ```bash
    docker compose down
    ```
