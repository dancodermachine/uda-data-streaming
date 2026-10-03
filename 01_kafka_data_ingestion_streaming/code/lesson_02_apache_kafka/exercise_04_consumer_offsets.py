# on_assign below always resets to OFFSET_BEGINNING, so every consumer run replays
# the topic from the start regardless of its last committed offset ("part 2" of the
# exercise). To see "part 1" instead -- the consumer resuming from its last committed
# offset -- comment out the `partition.offset = OFFSET_BEGINNING` line in on_assign.
#
# Either way, the producer must keep running continuously in its own terminal while
# only the consumer restarts, otherwise both counters reset together and the printed
# numbers won't correspond to anything meaningful. CLI role selection in main() lets
# you run them as independent processes:
#
#     python exercise_04_consumer_offsets.py produce   # leave running in one terminal
#     python exercise_04_consumer_offsets.py consume   # kill/restart this one

import os
import sys
import asyncio

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING

# Confluent Cloud requires SASL/SSL auth on top of `bootstrap.servers`.
# Merge this into every Producer/Consumer/AdminClient config below.
KAFKA_CLIENT_CONFIG = {
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_KAFKA_KEY"],
    "sasl.password": os.environ["CONFLUENT_KAFKA_SECRET"],
}
TOPIC_NAME = "com.udacity.exercise4.iterations"


async def consume(topic_name: str) -> None:
    """
    Consume and print messages from a Kafka topic, resetting to the
    beginning of the topic on every assignment.

    * auto.offset.reset: Action to take when there is no initial offset in
      offset store or the desired offset is out of range: 'smallest',
      'earliest' - automatically reset the offset to the smallest offset.

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
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # Set the offset reset to earliest
    c = Consumer(
        {
            **KAFKA_CLIENT_CONFIG,
            "group.id": "0-take3",
            "auto.offset.reset": "earliest",
        }
    )

    # Configure the on_assign callback
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    """
    Callback for when topic assignment takes place.

    Resets each assigned partition's offset to the beginning and assigns
    the consumer those partitions, so every run replays the topic from the
    start regardless of any previously committed offset.

    Parameters
    ----------
    consumer : Consumer
        Consumer instance the partitions are being assigned to.
    partitions : list[TopicPartition]
        Partitions assigned to this consumer.

    Returns
    -------
    None
    """
    # Most applications won't neccesarily have to do this, especially stream
    # processing applications that only care about data in a certain time frame.
    # However, there are many important use cases of Kafka where you do need to
    # see all of the data available on a topic.
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    consumer.assign(partitions)


def main() -> None:
    """
    Run the exercise.

    With no arguments, runs the producer and consumer together in this
    process (both restart together, so the producer's in-memory counter
    resets every run). Passing ``produce`` or ``consume`` on the command
    line instead runs only that side, so it can be started/stopped
    independently in its own terminal -- keep the producer running
    continuously and restart only the consumer to see the effect of
    ``on_assign``'s offset handling in isolation.

    Returns
    -------
    None
    """
    role = sys.argv[1] if len(sys.argv) > 1 else None

    try:
        if role == "produce":
            asyncio.run(produce(TOPIC_NAME))
        elif role == "consume":
            asyncio.run(consume(TOPIC_NAME))
        else:
            asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of messages into a Kafka topic.

    Configures a Kafka producer using ``KAFKA_CLIENT_CONFIG`` and publishes
    one message every tenth of a second to the given topic, using an
    incrementing iteration counter as the message value.

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
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.1)


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


if __name__ == "__main__":
    main()

