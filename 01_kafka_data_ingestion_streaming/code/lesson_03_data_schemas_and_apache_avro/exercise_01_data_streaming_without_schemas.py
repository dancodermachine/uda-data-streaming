import asyncio
from dataclasses import dataclass, field
import json
import os
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
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
TOPIC_NAME = "com.udacity.exercise1.clicks"


async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of click events into a Kafka topic.

    Configures a Kafka producer using ``KAFKA_CLIENT_CONFIG`` and publishes
    a serialized ``ClickEvent`` once per second to the given topic.

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


async def consume(topic_name: str) -> None:
    """
    Consume and print messages from a Kafka topic.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to consume messages from.

    Returns
    -------
    None
        Runs indefinitely, polling for messages until the enclosing task is
        cancelled.
    """
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
                values = json.loads(message.value())
                click_event = ClickEvent(
                    email=values["email"],
                    timestamp=values["timestamp"],
                    uri=values["uri"],
                )
                print(f"consumed message {click_event}")
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main() -> None:
    """
    Check for the topic and create it if it does not exist, then run the
    producer and consumer.

    Returns
    -------
    None
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)

    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name: str) -> None:
    """
    Run the producer and consumer tasks concurrently against the same topic.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to produce into and consume from.

    Returns
    -------
    None
    """
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)

    def serialize(self) -> str:
        """
        Serialize the ClickEvent as JSON for sending to Kafka.

        Returns
        -------
        str
            JSON-encoded representation of this ClickEvent.
        """
        email_key = random.choice(["email", "user_email"])
        return json.dumps(
            {"uri": self.uri, "timestamp": self.timestamp, email_key: self.email}
        )

    @classmethod
    def deserialize(cls, json_data: str) -> "ClickEvent":
        """
        Deserialize a ClickEvent from its JSON representation.

        Parameters
        ----------
        json_data : str
            JSON-encoded representation of a ClickEvent.

        Returns
        -------
        ClickEvent
            The deserialized ClickEvent instance.
        """
        click_json = json.loads(json_data)
        return cls(
            email=click_json["email"],
            timestamp=click_json["timestamp"],
            uri=click_json["uri"],
        )


if __name__ == "__main__":
    main()
