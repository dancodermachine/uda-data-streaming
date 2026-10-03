import os
import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
# Merge this into every Producer/Consumer/AdminClient config below.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}

def topic_exists(client: AdminClient, topic_name: str) -> bool:
    """
    Check whether a topic already exists on the cluster.

    Parameters
    ----------
    client : AdminClient
        Admin client connected to the target Kafka cluster.
    topic_name : str
        Name of the topic to check for.

    Returns
    -------
    bool
        ``True`` if a topic with the given name exists on the cluster,
        ``False`` otherwise.
    """
    cluster_metadata = client.list_topics(timeout=5)
    return topic_name in cluster_metadata.topics

def create_topic(client: AdminClient, topic_name: str) -> None:
    """
    Create a topic with the given name on the cluster.
    * cleanup.policy: Designates the retention policy to use on old log segments. 
    * compression.type: Final compression type.
    * delete.retention.ms: The amount of time to retain delete tombstone markers for log compacted topics.
    * file.delete.delay.ms: The time to wait before deleting a file from the filesystem. 

    Parameters
    ----------
    client : AdminClient
        Admin client connected to the target Kafka cluster.
    topic_name : str
        Name of the topic to create.

    Returns
    -------
    None
    """
    futures = client.create_topics(
        [
            NewTopic(
                topic_name,
                num_partitions=1,
                replication_factor=3,
                config={
                    # keeps the latest values for each key
                    "cleanup.policy": "compact", # log compaction on the topic
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                },
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise


def main() -> None:
    """
    Check for the topic and create it if it does not exist, then run the
    producer/consumer event loop.

    Returns
    -------
    None
    """
    client = AdminClient(KAFKA_CLIENT_CONFIG)

    topic_name = "my-second-python-topic"
    exists = topic_exists(client, topic_name)
    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce_consume(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")


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


async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of messages into a Kafka topic.

    Configures a Kafka producer using ``KAFKA_CLIENT_CONFIG`` and publishes
    one message every half second to the given topic, using an incrementing
    iteration counter as the message value.

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

    def delivery_report(err, msg) -> None:
        if err is not None:
            print(f"delivery failed for record {msg.key()}: {err}")
        else:
            print(f"record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    curr_iteration = 0
    while True:
        p.produce(
            topic_name,
            key=b"counter",
            value=f"iteration {curr_iteration}".encode("utf-8"),
            callback=delivery_report,
        )
        p.poll(0)
        curr_iteration += 1
        await asyncio.sleep(0.5)


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
    c = Consumer({**KAFKA_CLIENT_CONFIG, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(2.5)


if __name__ == "__main__":
    main()
