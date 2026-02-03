use async_trait::async_trait;
use tokio_postgres::Client;

pub mod table_a;
pub mod table_b;
pub mod table_c;
pub mod table_d;
pub mod table_e;

#[async_trait]
pub trait TableOperations {
    async fn create(&self, client: &mut Client);
    async fn clean(&self, client: &mut Client);
    async fn fill(&self, client: &mut Client, n: i32, block_size: i32, outer_size: i32);
    async fn query(&self, client: &mut Client);
}
