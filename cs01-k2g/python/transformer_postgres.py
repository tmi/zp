import logging
import fire
import psycopg2
import sys
from confluent_kafka import Consumer, KafkaException
from message_pb2 import Message # ty: ignore[unresolved-import]

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class TransformerPostgresCLI:
    def __init__(self):
        pass

    def create_table(self, db_conn_string: str):
        """
        Creates a PostgreSQL table to store Kafka messages.

        Args:
            db_conn_string: PostgreSQL connection string.
        """
        conn = None
        try:
            conn = psycopg2.connect(db_conn_string)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kafka_messages (
                    timestamp BIGINT NOT NULL,
                    key INTEGER NOT NULL,
                    value TEXT NOT NULL
                )
            """)
            conn.commit()
            logger.warning("Table 'kafka_messages' created or already exists.")
        except Exception:
            logger.exception("Error creating table")
        finally:
            if conn:
                conn.close()

    def run(self, kafka_brokers: str = "kafka:9092", topic: str = "t1", db_conn_string: str = "postgresql://postgres:pgt@postgres", end_after: int | None = None):
        """
        Consumes messages from Kafka and inserts them into a PostgreSQL table.

        Args:
            kafka_brokers: Comma-separated Kafka broker addresses (e.g., "localhost:9092").
            topic: The Kafka topic to consume from.
            db_conn_string: PostgreSQL connection string.
            end_after: Number of messages to consume before exiting. If None, consumes forever.
        """
        conn = None
        consumer = None
        try:
            conn = psycopg2.connect(db_conn_string)
            cur = conn.cursor()

            conf = {
                'bootstrap.servers': kafka_brokers,
                'group.id': 'postgres_transformer_group',
                'auto.offset.reset': 'earliest'
            }
            consumer = Consumer(conf)
            consumer.subscribe([topic])

            messages_processed = 0
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' % \
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    try:
                        message_proto = Message()
                        message_proto.ParseFromString(msg.value())
                        cur.execute("INSERT INTO kafka_messages (timestamp, key, value) VALUES (%s, %s, %s)",
                                    (message_proto.timestamp, message_proto.key, message_proto.value))
                        conn.commit()
                    except Exception:
                        logger.exception("Error processing message or inserting into DB")

                    messages_processed += 1
                    if end_after is not None and messages_processed >= end_after:
                        print(f"Processed {messages_processed} messages, exiting.")
                        break

        except KeyboardInterrupt:
            sys.stderr.write('% Aborted by user\n')
        except Exception:
            logger.exception("An error occurred")
        finally:
            if consumer:
                consumer.close()
            if conn:
                conn.close()

if __name__ == '__main__':
    fire.Fire(TransformerPostgresCLI)
