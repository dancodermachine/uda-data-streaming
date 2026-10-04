# JDBC source connector that loads data from the Postgres purchase table into Kafka
import asyncio
import json
import requests


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "exercise3"


def configure_connector() -> None:
    """
    Register the JDBC source connector with Kafka Connect.

    Checks the Kafka Connect REST API for an existing connector named
    ``CONNECTOR_NAME`` and returns early if one exists. Otherwise POSTs the
    connector config, telling the Connect worker to poll the Postgres table
    and produce each new row (detected by its incrementing ``id``) to a
    topic. This only registers the connector; the Connect worker does the
    actual reading and producing.

    Returns
    -------
    None
        Exits the process with status 1 if Kafka Connect rejects the
        connector config.
    """
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # Complete the Kafka Connect Config below for a JDBC source connector.
    # You should whitelist the `clicks` table, use incrementing mode and the
    # incrementing column name should be id.
    #
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": "clicks-jdbc",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # which tool to use: one that copies rows from a database into Kafka
                    "topic.prefix": "connect-",  # text added to the start of each table's name to form the Kafka topic name
                    "mode": "incrementing",  # how it spots new rows, e.g. by watching a number that goes up with each new row (bulk|incrementing|timestamp)
                    "incrementing.column.name": "id",  # the column holding that ever-increasing number (usually the id)
                    "table.whitelist": "purchases",  # which database tables to copy (all others are ignored)
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/classroom",
                    "connection.user": "root",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()