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

    def create_table(self, db_conn_string: str = "postgresql://postgres:pgt@postgres"):
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
        Consumes messages from Kafka and inserts them into a PostgreSQL table in batches.

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
            batch_size = 100
            message_batch = []

            while True:
                # Initial poll with a short timeout to check for messages
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    # If no message on initial poll, check if we should exit
                    if end_after is not None and messages_processed >= end_after:
                        logger.warning(f"Processed {messages_processed} messages, exiting.")
                        break
                    continue # No messages, continue to next iteration of main loop

                message_batch = []
                # Process the first message
                if not msg.error():
                    try:
                        message_proto = Message()
                        message_proto.ParseFromString(msg.value())
                        message_batch.append((message_proto.timestamp, message_proto.key, message_proto.value))
                    except Exception:
                        logger.exception("Error processing message or adding to batch")
                else:
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' % \
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())

                # Continuously poll with timeout 0 until batch is full or no more messages
                while len(message_batch) < batch_size:
                    msg = consumer.poll(timeout=0) # Non-blocking poll
                    if msg is None:
                        break # No more messages, break from inner loop
                    
                    if not msg.error():
                        try:
                            message_proto = Message()
                            message_proto.ParseFromString(msg.value())
                            message_batch.append((message_proto.timestamp, message_proto.key, message_proto.value))
                        except Exception:
                            logger.exception("Error processing message or adding to batch")
                    else:
                        if msg.error().code() == KafkaException._PARTITION_EOF:
                            sys.stderr.write('%% %s [%d] reached end at offset %d\n' % \
                                             (msg.topic(), msg.partition(), msg.offset()))
                        elif msg.error():
                            raise KafkaException(msg.error())

                # After inner loop, insert the collected batch
                if message_batch:
                    try:
                        cur.executemany("INSERT INTO kafka_messages (timestamp, key, value) VALUES (%s, %s, %s)", message_batch)
                        conn.commit()
                        messages_processed += len(message_batch)
                        logger.debug(f"Inserted batch of {len(message_batch)} messages. Total processed: {messages_processed}")
                        message_batch = [] # Clear batch after insert
                    except Exception:
                        logger.exception("Error inserting batch into DB")
                
                # Check end_after after each batch insert
                if end_after is not None and messages_processed >= end_after:
                    logger.warning(f"Processed {messages_processed} messages, exiting.")
                    break

            # No need for a final batch insert check here, as the logic ensures batches are inserted
            # either when full or when no more messages are immediately available.


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
