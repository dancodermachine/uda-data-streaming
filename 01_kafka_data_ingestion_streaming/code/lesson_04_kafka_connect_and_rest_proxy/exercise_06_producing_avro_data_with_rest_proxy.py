from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://localhost:8082"

# It is a string!
AVRO_SCHEMA = """{
    "type": "record",
    "name": "click_event",
    "fields": [
        {"name": "email", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "uri", "type": "string"},
        {"name": "number", "type": "int"}
    ]
}"""


def produce() -> None:
    """
    Produce a single Avro click event to Kafka through REST Proxy.

    Builds one fake ``ClickEvent`` and POSTs it with ``AVRO_SCHEMA`` to the
    ``lesson4.exercise6.click_events`` topic. REST Proxy registers the
    schema with Schema Registry, encodes the record as Avro and produces
    it, then this prints REST Proxy's response (the partition and offset
    the record landed on, plus the schema ID).

    Returns
    -------
    None
    """

    # Set the appropriate headers
    # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
        "Content-Type": "application/vnd.kafka.avro.v2+json"
    }
    # Update the below payload to include the Avro Schema string
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    data = {
        "value_schema": AVRO_SCHEMA,
        "records": [{"value": asdict(ClickEvent())}]
    }
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/lesson4.exercise6.click_events",
        data=json.dumps(data),
        headers=headers,
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))


def main() -> None:
    """
    Produce an Avro click event through REST Proxy every half second.

    Runs until interrupted with Ctrl+C.

    Returns
    -------
    None
    """
    try:
        while True:
            produce()
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
