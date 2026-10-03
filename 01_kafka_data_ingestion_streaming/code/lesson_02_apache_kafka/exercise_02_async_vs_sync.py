import os
from dataclasses import dataclass, field
import json
import random
from datetime import datetime, timezone

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
TOPIC_NAME = "org.udacity.exercise2.purchases"


@dataclass
class Purchase:
    """
    A randomly generated purchase event.

    Parameters
    ----------
    username : str
        Fake username of the purchaser.
    currency : str
        Fake ISO currency code for the purchase.
    amount : int
        Purchase amount, in the smallest unit of ``currency``.
    """

    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self) -> str:
        """
        Serialize the object in JSON string format.

        Returns
        -------
        str
            JSON-encoded representation of this ``Purchase``.
        """
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


def produce_sync(topic_name: str) -> None:
    """
    Produce data synchronously into the Kafka topic.

    Making the producer sync: ``flush`` is what makes a producer
    synchronous. It tells the client to send the message and wait for
    confirmation before proceeding (``p.flush``).

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to produce messages into.

    Returns
    -------
    None
        Runs indefinitely, producing messages until interrupted.
    """
    p = Producer(KAFKA_CLIENT_CONFIG)
    start_time = datetime.now(timezone.utc)
    curr_iteration = 0

    while True:
        p.produce(topic_name, Purchase().serialize())
        p.flush() # comment it to make it async, else use it for sync
        if curr_iteration % 1_000 == 0:
            elapsed = (datetime.now(timezone.utc) - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds: {elapsed}")
        curr_iteration += 1


def main() -> None:
    """
    Check for the topic and create it if it does not exist, then run the
    synchronous producer.

    Returns
    -------
    None
    """
    create_topic(TOPIC_NAME)
    try:
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


def create_topic(topic_name: str) -> None:
    """
    Create the topic with the given topic name.

    Parameters
    ----------
    topic_name : str
        Name of the topic to create.

    Returns
    -------
    None
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)
    futures = client.create_topics(
        [NewTopic(topic=topic_name, num_partitions=5, replication_factor=3)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")


if __name__ == "__main__":
    main()
