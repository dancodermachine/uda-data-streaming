import asyncio
from dataclasses import asdict, dataclass, field
import json
import os
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faker import Faker


faker = Faker()

# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
# Merge this into every Producer/Consumer/AdminClient config below.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}
TOPIC_NAME = "com.udacity.exercise4.clicks"
SCHEMA_REGISTRY_URL = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]
# Confluent Cloud Schema Registry uses its own API key, separate from the Kafka one.
SCHEMA_REGISTRY_CONFIG = {
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": (
        f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_KEY"]}:'
        f'{os.environ["CONFLUENT_SCHEMA_REGISTRY_SECRET"]}'
    ),
}


@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    # Avro schema as a JSON string, as expected by AvroSerializer
    schema = """{
        "type": "record",
        "name": "click_event",
        "namespace": "com.udacity.lesson3.exercise4",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"},
            {
                "name": "attributes",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "attribute",
                        "fields": [
                            {"name": "element", "type": "string"},
                            {"name": "content", "type": "string"}
                        ]
                    }
                }
            }
        ]
    }"""


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    schema_registry = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    # Registers the schema on first use and prefixes each message with its schema ID
    serializer = AvroSerializer(schema_registry, ClickEvent.schema)
    context = SerializationContext(topic_name, MessageField.VALUE)

    p = Producer(KAFKA_CLIENT_CONFIG)
    while True:
        p.produce(
            topic=topic_name,
            value=serializer(asdict(ClickEvent()), context),
        )
        await asyncio.sleep(1.0)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    schema_registry = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
    # Looks up the writer schema in the registry by the ID embedded in each message
    deserializer = AvroDeserializer(schema_registry)
    context = SerializationContext(topic_name, MessageField.VALUE)

    c = Consumer({**KAFKA_CLIENT_CONFIG, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(deserializer(message.value(), context))
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()
