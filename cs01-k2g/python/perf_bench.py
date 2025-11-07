import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import fire
import orjson
import psycopg2
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING, TopicPartition

# Assuming message_pb2 is available in the same directory
import message_pb2

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class PerfBench:
    def __init__(self):
        pass

    def postgres(self, db_conn_string: str = "postgresql://postgres:pgt@postgres", db_table: str = "kafka_messages", window_length_minutes: int = 1):
        """
        Benchmarks the PostgreSQL pipeline.
        """
        s = datetime.now()
        conn = None
        try:
            conn = psycopg2.connect(db_conn_string)
            cur = conn.cursor()

            # Query for rows within the window (S - window_length_minutes, S)
            time_threshold = s - timedelta(minutes=window_length_minutes)
            time_threshold_ms = int(time_threshold.timestamp() * 1000)
            s_ms = int(s.timestamp() * 1000)
            cur.execute(
                f"SELECT timestamp, key, value FROM {db_table} WHERE timestamp BETWEEN %s AND %s",
                (time_threshold_ms, s_ms)
            )
            rows = cur.fetchall()
            count1 = len(rows)
            e = datetime.now()
            time_delta = e - s
            diff_s_e = (time_delta.days * 86400 + time_delta.seconds) * 1000 + time_delta.microseconds / 1000

            print(f"First query: {count1} rows, time taken: {diff_s_e:.1f} ms")

            time.sleep(2)

            # Run the same query again
            s_re_query = datetime.now()
            cur.execute(
                f"SELECT timestamp, key, value FROM {db_table} WHERE timestamp BETWEEN %s AND %s",
                (time_threshold_ms, s_ms)
            )
            rows_re_query = cur.fetchall()
            count2 = len(rows_re_query)
            e_re_query = datetime.now()
            time_delta_re_query = e_re_query - s_re_query
            diff_s_e_re_query = (time_delta_re_query.days * 86400 + time_delta_re_query.seconds) * 1000 + time_delta_re_query.microseconds / 1000

            print(f"Second query: {count2} rows, time taken: {diff_s_e_re_query:.1f} ms")

            print(f"First row count: {count1}")
            print(f"Difference between row counts: {count2 - count1}")
            print(f"Time taken for first query (E-S): {diff_s_e:.1f} ms")

        except Exception:
            logger.exception("Error during postgres benchmark")
        finally:
            if conn:
                conn.close()

    def kafkaJson(self, kafka_brokers: str = "kafka:9092", kafka_topic: str = "t1-json", window_length_minutes: int = 1):
        """
        Benchmarks the Kafka JSON pipeline.
        """
        s = datetime.now()

        conf = {
            'bootstrap.servers': kafka_brokers,
            'group.id': 'perf_bench_group',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)

        try:
            consumer.subscribe([kafka_topic])

            # Wait for partition assignment
            partitions = []
            while not partitions:
                consumer.poll(1.0) # Poll with a timeout to allow for group rebalance and assignment
                partitions = consumer.assignment()
                if not partitions:
                    logger.warning("Waiting for partition assignment...")

            partition = partitions[0] # Assuming single partition as per problem description

            low, high = consumer.get_watermark_offsets(partition)

            count1 = 0
            start_time_window = s - timedelta(minutes=window_length_minutes)
            
            # First Pass: Consume messages up to the high watermark, counting those in (start_time_window, s)
            consumer.seek(TopicPartition(kafka_topic, partition.partition, OFFSET_BEGINNING)) # Seek to beginning for the first pass
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    # No more messages in the current poll interval, check if we reached high offset
                    current_position = consumer.position([partition])[0].offset
                    if current_position >= high:
                        break
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event, means we reached the end
                        break
                    else:
                        raise KafkaException(msg.error())
                
                if msg.offset() >= high: # Break if we pass the high offset
                    break

                try:
                    decoded_msg = orjson.loads(msg.value().decode('utf-8'))
                    msg_timestamp_str = decoded_msg.get("timestamp")
                    if msg_timestamp_str:
                        msg_timestamp = datetime.fromtimestamp(msg_timestamp_str / 1000)
                        if start_time_window <= msg_timestamp <= s:
                            count1 += 1
                except Exception:
                    logger.exception("Error decoding or processing message")
            
            e = datetime.now()
            time_delta = e - s
            diff_s_e = (time_delta.days * 86400 + time_delta.seconds) * 1000 + time_delta.microseconds / 1000

            print(f"First consumption: {count1} messages, time taken: {diff_s_e:.1f} ms")

            time.sleep(2)

            # Second Pass: Continue from where the first pass left off, count messages in (start_time_window, s)
            # and break after passing 's' timestamp.
            count2 = 0
            s_re_consume = datetime.now()
            # DO NOT seek to beginning - continue from current position
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    # No more messages in the current poll interval
                    break
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event
                        break
                    else:
                        raise KafkaException(msg.error())
                
                try:
                    decoded_msg = orjson.loads(msg.value().decode('utf-8'))
                    msg_timestamp_str = decoded_msg.get("timestamp")
                    if msg_timestamp_str:
                        msg_timestamp = datetime.fromtimestamp(msg_timestamp_str / 1000)
                        if msg_timestamp > s: # Break after passing 's' timestamp
                            break
                        if start_time_window <= msg_timestamp <= s:
                            count2 += 1
                except Exception:
                    logger.exception("Error decoding or processing message")
            
            e_re_consume = datetime.now()
            time_delta_re_consume = e_re_consume - s_re_consume
            diff_s_e_re_consume = (time_delta_re_consume.days * 86400 + time_delta_re_consume.seconds) * 1000 + time_delta_re_consume.microseconds / 1000

            print(f"Second consumption: {count2} messages, time taken: {diff_s_e_re_consume:.1f} ms")

            print(f"First message count: {count1}")
            print(f"Number of messages in the second step (timestamp in (start, s)): {count2}")
            print(f"Time taken for first consumption (E-S): {diff_s_e:.1f} ms")

        except KafkaException:
            logger.exception("Kafka error during kafkaJson benchmark")
        except Exception:
            logger.exception("Error during kafkaJson benchmark")
        finally:
            consumer.close()


def main():
    fire.Fire(PerfBench)

if __name__ == "__main__":
    main()
