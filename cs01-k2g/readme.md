Demonstrates how to visualize kafka data in grafana, possibly going through a postgres/timescale first.

# Environment setup
```
# TODO docker compose
network=devel-network
docker network create $network
docker run --rm  --name kafka --network $network -e KAFKA_LISTENERS='PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093' -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_NODE_ID=1 -e KAFKA_ADVERTISED_LISTENER=PLAINTEXT://kafka:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka:4.1.0
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic t1 --partitions 1 --bootstrap-server kafka:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic t1-json --partitions 1 --bootstrap-server kafka:9092
docker run --rm --name postgres --network $network -e POSTGRES_PASSWORD=pgt postgres
docker run --rm --name grafana --network $network -p 3000:3000 -e GF_PLUGINS_PREINSTALL=hamedkarbasi93-kafka-datasource grafana/grafana-enterprise
```

# Demonstration
```
cd python
N=10
uv run python ./producer_postgres.py create_table postgresql://postgres:pgt@postgres
uv run python ./producer.py kafka t1 $N
uv run python ./transformer.py transform kafka t1 t2 $N
uv run python ./producer_postgres.py run kafka t1 postgresql://postgres:pgt@postgres $N
uv run python ./configure_grafana_datasource.py
```
then open grafana at :3000, with admin/admin, and go to dashboards

# Performance notes
The `perf_bench` script is able to benchmark all modes, with the following definition:
- postgres: fetch all messages in a given time window, then sleep(x), then fetch all messages from the same time window. Extra messages represent `lag`.
- kafka: note last offset at the start, then seek to beginning of given time window, then fetch all messages (windowStart, lastAtStart), then keep fetching (lastAtStart, windowEnd). The second set of messages represents `lag`.

Only at message rates 60k/s we started observing any lag -- 1s of messages in postgres case, 0 in kafka case.

This does not take into account the fetch speed itself -- kafka is ~100x slower at higher message volumes.

# Next steps
1. Investigate options for proto-kafka reading from grafana
2. Install timescale, define the table as a hypertable, add some aggregations
