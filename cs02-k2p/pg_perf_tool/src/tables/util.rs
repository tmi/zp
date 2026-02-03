use std::time::Instant;
use tokio_postgres::Client;

pub async fn generic_create(client: &mut Client, create_string: &str, table_name: &str) {
    client.batch_execute(create_string).await.unwrap();
    println!("Created {}", table_name);
}

pub async fn generic_clean(client: &mut Client, table_name: &str) {
    client
        .execute(format!("TRUNCATE TABLE {}", table_name).as_str(), &[])
        .await
        .unwrap();
    println!("Cleaned {}", table_name);
}

pub async fn generic_query(client: &mut Client, query_string: &str) {
    let start = Instant::now();
    let rows = client.query(query_string, &[]).await.unwrap();
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
