# NOT WORKING AS EXPECTED

import os
import asyncio
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "org.udacity.exercise3.producer_config"



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
    messages as fast as possible to the given topic, using an incrementing
    iteration counter as both the message key and value.

    Producer:
    * linger.ms: Alias for queue.buffering.max.ms Delay milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective batches of messages to accumulate at the expense of increased message delivery latency.
    * batch.num.messages: Maximum number of messages batched into a single MessageSet sent to the broker.
    * queue.buffering.max.messages: Max number of messages allowed on the producer queue.
    * queue.buffering.max.kbytes: Max total message size sum allowed on the producer queue.

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
    p = Producer({
        **KAFKA_CLIENT_CONFIG,
        "linger.ms": "10000",
        "batch.num.messages": "10000",
        # "queue.buffering.max.messages": "10000000",
        # "queue.buffering.max.kbytes": "2097151",
    }) # Many options/combinations you can tweek

    start_time = datetime.now(timezone.utc)
    curr_iteration = 0

    while True:
        p.produce(topic_name, key=str(curr_iteration), value=f"Message: {curr_iteration}")
        if curr_iteration % 100_000 == 0:
            elapsed = (datetime.now(timezone.utc) - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds: {elapsed}" )
        curr_iteration += 1
        p.poll(0)  # checks for messages the broker has confirmed and clears them from the waiting list; without this, the waiting list keeps growing until it's full and errors out
        await asyncio.sleep(0)  # yields to the event loop so the concurrent consumer task can still run


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
        "group.id": "0",
        "auto.offset.reset": "earliest"})

    c.subscribe([topic_name])

    try:
        while True:
            message = c.poll(0)  # non-blocking: a real timeout here would freeze the event loop and starve the concurrent producer task

            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")

            await asyncio.sleep(0)  # yields to the event loop so the concurrent producer task can still run
    finally:
        c.close()


async def produce_consume(topic_name: str) -> None:
    """
    Run the producer and consumer concurrently against the same topic.

    Schedules ``produce`` and ``consume`` as concurrent asyncio tasks against
    the given topic and awaits both.

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


def main() -> None:
    """
    Run the exercise end-to-end.

    Creates the Kafka topic via ``AdminClient``, then runs the
    producer/consumer event loop until interrupted.

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
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == "__main__":
    main()