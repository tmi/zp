use async_trait::async_trait;
use tokio_postgres::Client;

pub mod table_a;
pub mod table_b;
pub mod table_c;
pub mod table_d;
pub mod table_e;
pub mod util;

#[async_trait]
pub trait TableOperations {
    fn table_name(&self) -> &str;
    fn create_string(&self) -> &str;
    fn query_string(&self) -> &str;
    async fn fill(&self, client: &mut Client, n: i32, block_size: i32, outer_size: i32);
}