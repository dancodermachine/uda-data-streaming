import asyncio

from dataclasses import asdict, dataclass, field
from io import BytesIO
import json
import os
import random

from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer


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
TOPIC_NAME = "com.udacity.exercise3.clicks"


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


    # Update this Avro schema to include a map of attributes
    schema = parse_schema(
        {
            "type": "record",
            "name": "click_event",
            "namespace": TOPIC_NAME,
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
                #
                # Add the attributes map!
                #
                {
                    "name": "attributes",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "attribute",
                            "fields": [
                                {"name": "element", "type": "string"},
                                {"name": "content", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        }
    )

    def serialize(self) -> bytes:
        """
        Serialize the ClickEvent into Avro format for sending to Kafka.

        Returns
        -------
        bytes
            Avro-encoded representation of this ClickEvent.
        """
        out = BytesIO()
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()


async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of click events into a Kafka topic.

    Configures a Kafka producer using ``KAFKA_CLIENT_CONFIG`` and publishes
    an Avro-serialized ``ClickEvent`` once per second to the given topic.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to produce messages into.

    Returns
    -------
    None
        Runs indefinitely, producing messages until the enclosing task is
        cancelled.
    """
    p = Producer(KAFKA_CLIENT_CONFIG)
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)


def main() -> None:
    """
    Run the exercise.

    Returns
    -------
    None
    """
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name: str) -> None:
    """
    Run the producer task against the given topic.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to produce into.

    Returns
    -------
    None
    """
    await asyncio.create_task(produce(topic_name))


if __name__ == "__main__":
    main()
