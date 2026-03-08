*Status*: Complete

# Summary

This project is a case study evaluating PostgreSQL performance across five different schema designs for representing nested data. It features a Rust-based CLI tool, `pg_perf_tool`, which automates table creation, data generation, and performance benchmarking for each schema.

## Key Takeaways

- **Schema Design impact**: The representation of nested data significantly affects both ingestion and query performance.
- **Ingestion vs. Querying**: Table A (full JSONB) is fastest for insertion but significantly slower for complex queries. Conversely, Table C (denormalized) and Table E (split primitive arrays) are slower to populate but provide the best query performance.
- **Postgres Arrays**: Using native PostgreSQL arrays (Table E) proved to be slightly more efficient than denormalization (Table C) or JSONB arrays (Table B) for the specific workload tested.
- **Performance results**:
  - **Table A (JSONB block)**: Insertion: ~50ms, Query: ~196ms
  - **Table B (JSONB array)**: Insertion: ~364ms, Query: ~5ms
  - **Table C (Denormalized)**: Insertion: ~4.9s, Query: ~2.4ms
  - **Table E (Primitive arrays)**: Insertion: ~322ms, Query: ~2.1ms
