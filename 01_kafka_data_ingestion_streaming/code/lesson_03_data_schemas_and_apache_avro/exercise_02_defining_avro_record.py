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
TOPIC_NAME = "com.udacity.exercise2.clicks"


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    # Define an Avro Schema for this ClickEvent
    # This will not produce any output, but you can use `kafka-console-consumer` to check that messages are being produced.
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
        #
        # Rewrite the serializer to send data in Avro format.
        # Use BytesIO for your output buffer. Once you have an output buffer instance, call
        # `getvalue() to retrieve the data inside the buffer.
        # HINT: This exercise will not print to the console. Use the `kafka-console-consumer` to view the messages.

        out = BytesIO() # to keep things in memory, not on disk
        # `writer` is a fastavro utility that actually will take an input chunk of data and write it out
        # in the format that we expect.
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()
           
        # return json.dumps(
        #    {"uri": self.uri, "timestamp": self.timestamp, "email": self.email}
        #)


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