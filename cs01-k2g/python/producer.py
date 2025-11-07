import fire
import time
import random
from confluent_kafka import Producer
from message_pb2 import Message # ty: ignore[unresolved-import]

class ProducerCLI:
    def __init__(self):
        pass

    def produce(self, kafka_brokers: str, topic: str, num_messages: int | None = None):
        """
        Produces random messages to a Kafka topic.

        Args:
            kafka_brokers: Comma-separated Kafka broker addresses (e.g., "localhost:9092").
            topic: The Kafka topic to produce to.
            num_messages: Number of messages to produce. If None, produces forever.
        """
        conf = {
            'bootstrap.servers': kafka_brokers,
            'client.id': 'my_producer'
        }

        producer = Producer(conf)

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

        i = 0
        while True:
            message_proto = Message()
            message_proto.timestamp = int(time.time() * 1000)  # milliseconds
            message_proto.key = i
            message_proto.value = f"Random value {random.randint(0, 1000)}"

            producer.produce(topic, key=str(message_proto.key).encode('utf-8'), value=message_proto.SerializeToString(), callback=delivery_report)
            producer.poll(0)  # Serve delivery callback queue.
            time.sleep(0.1) # Sleep for 0.1 seconds

            i += 1
            if num_messages is not None and i >= num_messages:
                print(f"Produced {i} messages, exiting.")
                break

        producer.flush() # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.

if __name__ == '__main__':
    fire.Fire(ProducerCLI)
