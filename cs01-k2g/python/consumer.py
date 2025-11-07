import fire
import sys
from confluent_kafka import Consumer, KafkaException
from message_pb2 import Message # ty: ignore[unresolved-import]

class ConsumerCLI:
    def __init__(self):
        pass

    def consume(self, kafka_brokers: str, topic: str, end_after: int | None = None):
        """
        Consumes messages from a Kafka topic and prints them to stdout.

        Args:
            kafka_brokers: Comma-separated Kafka broker addresses (e.g., "localhost:9092").
            topic: The Kafka topic to consume from.
            end_after: Number of messages to consume before exiting. If None, consumes forever.
        """
        conf = {
            'bootstrap.servers': kafka_brokers,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        }

        consumer = Consumer(conf)

        try:
            consumer.subscribe([topic])

            messages_consumed = 0
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Proper message
                    print(f"Received message: Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
                    try:
                        message_proto = Message()
                        message_proto.ParseFromString(msg.value())
                        print(f"  Timestamp: {message_proto.timestamp}, Key: {message_proto.key}, Value: {message_proto.value}")
                    except Exception as e:
                        print(f"  Error deserializing message: {e}")

                    messages_consumed += 1
                    if end_after is not None and messages_consumed >= end_after:
                        print(f"Consumed {messages_consumed} messages, exiting.")
                        break

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
        finally:
            consumer.close()

if __name__ == '__main__':
    fire.Fire(ConsumerCLI)
