import json
import requests

REST_PROXY_URL = "http://localhost:8082"

def get_topics() -> list[str]:
    """
    Fetch and print the names of all topics from REST Proxy.

    Returns
    -------
    list[str]
        Names of every topic in the cluster, or an empty list if the request
        failed.
    """
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics")

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()


def get_topic(topic_name: str) -> None:
    """
    Fetch and print the details of a single topic from REST Proxy.

    Prints the topic's configuration and its partitions, as returned by
    REST Proxy.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic to describe.

    Returns
    -------
    None
    """
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_brokers() -> None:
    """
    Fetch and print the IDs of the brokers in the cluster from REST Proxy.

    Returns
    -------
    None
    """
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--brokers
    resp = requests.get(f"{REST_PROXY_URL}/brokers")  

    try:
        resp.raise_for_status()
    except:
        print("Failed to get brokers {json.dumps(resp.json(), indent=2)})")

    print("Fetched brokers from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_partitions(topic_name: str) -> None:
    """
    Fetch and print partition information for a topic from REST Proxy.

    For each partition, prints its number, its leader broker and its
    replicas.

    Parameters
    ----------
    topic_name : str
        Name of the Kafka topic whose partitions to list.

    Returns
    -------
    None
    """
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics-(string-topic_name)-partitions
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get partitions {json.dumps(resp.json(), indent=2)})")

    print("Fetched partitions from Kafka:")
    print(json.dumps(resp.json(), indent=2))


if __name__ == "__main__":
    topics = get_topics()
    get_topic(topics[0])
    get_brokers()
    get_partitions(topics[-1])
