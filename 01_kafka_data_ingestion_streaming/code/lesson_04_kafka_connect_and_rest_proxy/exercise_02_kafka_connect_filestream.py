import asyncio
import json
import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "exercise2"


def configure_connector() -> None:
    """
    Register the FileStreamSource connector with Kafka Connect.

    Checks the Kafka Connect REST API for an existing connector named
    ``CONNECTOR_NAME`` and returns early if one exists. Otherwise POSTs the
    connector config, telling the Connect worker to tail the log file and
    produce each new line to the topic. This only registers the connector;
    the Connect worker does the actual reading and producing.

    Returns
    -------
    None

    Raises
    ------
    requests.HTTPError
        If Kafka Connect rejects the connector config.
    """
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # Complete the Kafka Connect Config below.
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/filestream_connector.html#filesource-connector
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "FileStreamSource",  # connector plugin to use: reads lines from a file into Kafka
                    "topic": "kafka_connect_and_rest_proxy.exercise_02.logs",  # Kafka topic the file's lines are written to
                    "tasks.max": 1,  # how many tasks it should run in the server
                    "file": f"/tmp/{CONNECTOR_NAME}.log",  # file the connector reads from (the source)
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    print("connector created successfully")


async def log() -> None:
    """
    Continually append numbered lines to the connector's source file.

    Truncates ``/tmp/{CONNECTOR_NAME}.log`` and writes one ``log number N``
    line per second, flushing after each write so the connector sees it
    immediately.

    Returns
    -------
    None
        Runs indefinitely, writing lines until the enclosing task is
        cancelled.
    """
    with open(f"/tmp/{CONNECTOR_NAME}.log", "w") as f:
        iteration = 0
        while True:
            f.write(f"log number {iteration}\n")
            f.flush()
            await asyncio.sleep(1.0)
            iteration += 1


async def log_task() -> None:
    """
    Run the file writer and register the connector.

    Schedules ``log`` as an asyncio task, registers the connector (a
    blocking call, so ``log`` only starts writing once it returns), then
    awaits the writer.

    Returns
    -------
    None
    """
    task = asyncio.create_task(log())
    configure_connector()
    await task


def run() -> None:
    """
    Run the exercise until interrupted with Ctrl+C.

    Returns
    -------
    None
    """
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    run()
