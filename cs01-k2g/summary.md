*Status*: Complete

# Summary

This project demonstrates and benchmarks different architectures for visualizing real-time data from Kafka in Grafana. It compares a direct Kafka-to-Grafana visualization approach against a pipeline where data is ingested into PostgreSQL (optionally with TimescaleDB) before being queried by Grafana. The project includes a Rust-based aggregation service (`rs_agg_service`), a Python-based visualization client (`py_agg_client`), and various utility scripts for data production and transformation.

## Key Takeaways

- **Performance Difference**: PostgreSQL is significantly faster than the direct Kafka datasource for Grafana at high message volumes, with Kafka being approximately 100x slower in fetch speed during benchmarks.
- **Scalability**: In the tested setup, PostgreSQL started to show observable lag at message rates of approximately 60k/s, while Kafka showed no lag but much slower retrieval.
- **Architecture**: Storing Kafka data in a relational database like PostgreSQL/TimescaleDB provides better query performance and flexibility for Grafana dashboards compared to direct Kafka queries.
- **Direct Visualization**: The project also demonstrates a custom path for direct visualization using a Rust service and a Python-based frontend, bypassing traditional dashboarding tools.
