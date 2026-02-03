use async_trait::async_trait;
use tokio_postgres::Client;

use super::TableOperations;

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

pub struct TableD;

#[async_trait]
impl TableOperations for TableD {
    fn table_name(&self) -> &str {
        "tabled"
    }

    fn create_string(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS tabled (
            block_id VARCHAR,
            outer_id VARCHAR,
            outer_ts TIMESTAMPTZ,
            inner_structs inner_struct[],
            PRIMARY KEY (block_id, outer_id)
        )"
    }

    fn query_string(&self) -> &str {
        "SELECT
            (s.inner).volume
        FROM (
            SELECT unnest(inner_structs) as inner FROM tabled
        ) as s
        WHERE (s.inner).threshold > 0.5"
    }

    async fn fill(&self, _client: &mut Client, _n: i32, _block_size: i32, _outer_size: i32) {
        panic!("fill for table d is not implemented");
    }
}