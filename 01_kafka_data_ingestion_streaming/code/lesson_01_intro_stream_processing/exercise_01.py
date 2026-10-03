import os
import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "my-first-python-topic"

# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
# Merge this into every Producer/Consumer/AdminClient config below.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}

async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of messages into a Kafka topic.

    Configures a Kafka producer using ``KAFKA_CLIENT_CONFIG`` and publishes
    one message per second to the given topic, using an incrementing
    iteration counter as both the message key and value.

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

    curr_iteration = 0
    while True:
        p.produce(topic_name, key=str(curr_iteration), value=f"Message: {curr_iteration}")
        p.poll(0)

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name: str) -> None:
    """
    Consume and print messages from a Kafka topic.

    Configures a Kafka consumer using ``KAFKA_CLIENT_CONFIG`` plus a
    ``group.id``, subscribes to the given topic, and continuously polls for
    new messages, printing the key and value of each successfully consumed
    message.

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
    c = Consumer({
        **KAFKA_CLIENT_CONFIG,
        "group.id": "my-first-python-consumer-group",
        "auto.offset.reset": "earliest"})

    c.subscribe([topic_name])

    try:
        while True:
            message = c.poll(1.0)

            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")

            await asyncio.sleep(1)
    finally:
        c.close()


async def produce_consume() -> None:
    """
    Run the producer and consumer concurrently against the same topic.

    Schedules ``produce`` and ``consume`` as concurrent asyncio tasks against
    ``TOPIC_NAME`` and awaits both.

    Returns
    -------
    None
    """
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main() -> None:
    """
    Run the exercise end-to-end.

    Creates the Kafka topic via ``AdminClient``, runs the producer/consumer
    event loop until interrupted, and deletes the topic on shutdown.

    * num_partitions: number of partitions to split the topic's log into, enabling parallel reads/writes and ordering only within each partition.
    * replication_factor: number of broker copies kept for each partition, for fault tolerance if a broker goes down.

    Returns
    -------
    None
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)

    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=3)

    futures = client.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"topic {topic_name} created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        for f in client.delete_topics([TOPIC_NAME]).values():
            f.result()


if __name__ == "__main__":
    main()