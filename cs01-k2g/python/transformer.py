import fire
import orjson
import sys
from confluent_kafka import Consumer, Producer, KafkaException
from message_pb2 import Message

class TransformerCLI:
    def __init__(self):
        pass

    def transform(self, kafka_brokers: str, input_topic: str, output_topic: str, end_after: int = None):
        """
        Reads proto messages from an input topic, transforms them to JSON, and writes to an output topic.

        Args:
            kafka_brokers: Comma-separated Kafka broker addresses (e.g., "localhost:9092").
            input_topic: The Kafka topic to consume proto messages from.
            output_topic: The Kafka topic to produce JSON messages to.
            end_after: Number of messages to transform before exiting. If None, transforms forever.
        """
        consumer_conf = {
            'bootstrap.servers': kafka_brokers,
            'group.id': 'transformer_group',
            'auto.offset.reset': 'earliest'
        }
        producer_conf = {
            'bootstrap.servers': kafka_brokers,
            'client.id': 'transformer_producer'
        }

        consumer = Consumer(consumer_conf)
        producer = Producer(producer_conf)

        def delivery_report(err, msg):
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

        try:
            consumer.subscribe([input_topic])

            messages_transformed = 0
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
                        
                        json_message = {
                            "timestamp": message_proto.timestamp,
                            "key": message_proto.key,
                            "value": message_proto.value
                        }
                        
                        producer.produce(output_topic, key=str(message_proto.key).encode('utf-8'), value=orjson.dumps(json_message), callback=delivery_report)
                        producer.poll(0)
                        print(f"Transformed and produced message: Key: {message_proto.key}")
                    except Exception as e:
                        print(f"Error processing message or transforming: {e}")

                    messages_transformed += 1
                    if end_after is not None and messages_transformed >= end_after:
                        print(f"Transformed {messages_transformed} messages, exiting.")
                        break

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if consumer:
                consumer.close()
            if producer:
                producer.flush()

if __name__ == '__main__':
    fire.Fire(TransformerCLI)
