import asyncio
from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://localhost:8082"
TOPIC_NAME = "lesson4.solution7.click_events"
CONSUMER_GROUP = f"solution7-consumer-group-{random.randint(0,10000)}"


async def consume() -> None:
    """
    Consume Avro click events from Kafka through REST Proxy.

    Creates a REST Proxy consumer instance in ``CONSUMER_GROUP`` (Avro
    format, starting from the earliest offset), subscribes it to
    ``TOPIC_NAME`` and repeatedly fetches and prints records. REST Proxy
    looks up each record's schema in Schema Registry and returns it decoded
    as JSON. The consumer instance is deleted from REST Proxy on exit.

    All REST Proxy calls run in a worker thread so they don't block
    ``produce`` running on the same event loop.

    Returns
    -------
    None
        Runs indefinitely, fetching records until the enclosing task is
        cancelled or a REST Proxy request fails.
    """
    # Define a consumer name
    consumer_name = "solution7-consumer"
    # Define the appropriate headers
    # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
        "Content-Type": "application/vnd.kafka.v2+json"
    }
    # Define the consumer group creation payload, use avro
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)
    data = {
        "name": consumer_name,
        "format": "avro",
        # Without this a new consumer starts at the latest offset and skips
        # everything produced before it subscribed
        "auto.offset.reset": "earliest",
    }
    # requests is blocking, so run it in a thread to let produce() keep running
    resp = await asyncio.to_thread(
        requests.post,
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers,
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return
    print("REST Proxy consumer group created")

    resp_data = resp.json()
    # REST Proxy keeps the consumer instance alive on the server until it is
    # deleted (or times out), so always clean it up when we stop
    try:
        #
        # Create the subscription payload
        # See: https://docs.confluent.io/current/kafka-rest/api.html#consumers
        #
        data = {
            "topics": [TOPIC_NAME]
        }
        resp = await asyncio.to_thread(
            requests.post,
            f"{resp_data['base_uri']}/subscription",
            data=json.dumps(data),
            headers=headers,
        )
        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to subscribe REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return
        print("REST Proxy consumer subscription created")
        while True:
            #
            # Set the Accept header to the same data type as the consumer was created with
            # See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
            #
            accept_headers = {
                "Accept": "application/vnd.kafka.avro.v2+json"
            }
            resp = await asyncio.to_thread(
                requests.get,
                f"{resp_data['base_uri']}/records?timeout=10000",
                headers=accept_headers,
            )
            try:
                resp.raise_for_status()
            except:
                print(
                    f"Failed to fetch records with REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
                )
                return
            print("Consumed records via REST Proxy:")
            print(f"{json.dumps(resp.json())}")
            await asyncio.sleep(0.1)
    finally:
        # Plain blocking call: awaiting inside finally is unreliable while the
        # task is being cancelled on Ctrl+C
        requests.delete(resp_data["base_uri"], headers=headers)
        print("REST Proxy consumer deleted")


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    # Avro schema as a JSON string, as expected by AvroSerializer
    schema = """{
        "type": "record",
        "name": "click_event",
        "namespace": "com.udacity.lesson3.exercise2",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"}
        ]
    }"""


async def produce(topic_name: str) -> None:
    """
    Produce a continuous stream of Avro click events into a Kafka topic.

    Serializes a fake ``ClickEvent`` with ``AvroSerializer`` (which registers
    ``ClickEvent.schema`` with Schema Registry on first use) and produces it
    every tenth of a second, directly to Kafka rather than through REST
    Proxy.

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
    schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    # Registers the schema on first use and prefixes each message with its schema ID
    serializer = AvroSerializer(schema_registry, ClickEvent.schema)
    context = SerializationContext(topic_name, MessageField.VALUE)

    p = Producer({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    while True:
        p.produce(
            topic=topic_name,
            value=serializer(asdict(ClickEvent()), context),
        )
        await asyncio.sleep(0.1)


async def produce_consume(topic_name: str) -> None:
    """
    Run the producer and REST Proxy consumer concurrently.

    Schedules ``produce`` and ``consume`` as concurrent asyncio tasks and
    awaits both.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to produce into.

    Returns
    -------
    None
    """
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume())
    await t1
    await t2


def main() -> None:
    """
    Run the exercise until interrupted with Ctrl+C.

    Returns
    -------
    None
    """
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()