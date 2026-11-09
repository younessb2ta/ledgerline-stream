"""Native single-node Kafka (KRaft mode) for the streaming demo.

No ZooKeeper, no Docker. The release tarball is downloaded once into
``~/.local/ledgerline-stream``; data lives beside it and the topic is created
with three partitions so the Flink source has real parallelism.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from . import tools

TOPIC = "ledger.events"
BOOTSTRAP = "127.0.0.1:9092"
CONTROL_EVENT = "stream_flush"
# Far enough out that it closes any window a generator could produce. Sent
# only after the last real event, so everything after it is late by definition.
FLUSH_TS = "9999-12-31T23:59:59Z"
CONTROLLER = "127.0.0.1:9093"
DATA_DIR = tools.TOOLS / "kafka-data"
CONFIG = tools.TOOLS / "kafka-kraft.properties"


def java_env() -> dict[str, str]:
    java_home = Path.home() / ".local" / "java" / "17"
    env = {**os.environ, "JAVA_HOME": str(java_home)}
    env["PATH"] = f"{java_home}/bin:" + env.get("PATH", "")
    return env


def ensure_installed() -> None:
    tools.download(tools.KAFKA_URL, tools.KAFKA_HOME)


def write_config() -> None:
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(
        "\n".join(
            [
                "process.roles=broker,controller",
                "node.id=1",
                f"controller.quorum.voters=1@{CONTROLLER}",
                f"listeners=PLAINTEXT://{BOOTSTRAP},CONTROLLER://{CONTROLLER}",
                f"advertised.listeners=PLAINTEXT://{BOOTSTRAP}",
                "controller.listener.names=CONTROLLER",
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                f"log.dirs={DATA_DIR}",
                "num.partitions=3",
                "default.replication.factor=1",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "group.initial.rebalance.delay.ms=0",
                "auto.create.topics.enable=false",
                "log.retention.hours=24",
            ]
        )
        + "\n"
    )


def cluster_id_file() -> Path:
    return tools.TOOLS / "kafka-cluster-id"


def format_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    marker = DATA_DIR / "meta.properties"
    if marker.exists():
        return
    if cluster_id_file().exists():
        cluster_id = cluster_id_file().read_text().strip()
    else:
        cluster_id = str(uuid.uuid4())
        cluster_id_file().write_text(cluster_id)
    tools.run(
        [
            str(tools.kafka_bin("kafka-storage.sh")),
            "format",
            "-t",
            cluster_id,
            "-c",
            str(CONFIG),
        ],
        env=java_env(),
    )


def _topic_exists() -> bool:
    result = subprocess.run(
        [
            str(tools.kafka_bin("kafka-topics.sh")),
            "--bootstrap-server",
            BOOTSTRAP,
            "--list",
        ],
        capture_output=True,
        text=True,
        env=java_env(),
    )
    return result.returncode == 0


def up() -> None:
    ensure_installed()
    write_config()
    format_storage()
    if _topic_exists():
        print("kafka already running")
        ensure_topic()
        return
    tools.RUN.mkdir(parents=True, exist_ok=True)
    with (tools.RUN / "kafka.log").open("w") as log:
        process = subprocess.Popen(
            [str(tools.kafka_bin("kafka-server-start.sh")), str(CONFIG)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=java_env(),
            start_new_session=True,
        )
    tools.write_pid("kafka", process.pid)
    tools.wait_for("kafka broker", _topic_exists, timeout=90)
    ensure_topic()


def ensure_topic(partitions: int = 1) -> None:
    """Create the topic if missing.

    One partition is the default on purpose. The replay demos compress 14 days
    of event time into a few seconds of wall clock; with several partitions the
    source can read one a batch ahead of another, and a 10 s watermark bound
    then reports cross-partition skew as lateness. A single partition gives a
    total order, so event time and arrival time agree. Pass ``partitions=3``
    for a throughput run where exact reconciliation is not the point.
    """
    result = subprocess.run(
        [
            str(tools.kafka_bin("kafka-topics.sh")),
            "--bootstrap-server",
            BOOTSTRAP,
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ],
        capture_output=True,
        text=True,
        env=java_env(),
    )
    print((result.stdout or result.stderr).strip())


def down() -> None:
    pid = tools.read_pid("kafka")
    if pid is None:
        # fall back to the release helper, which finds the JVM by classpath
        if tools.KAFKA_HOME.exists() and _topic_exists():
            tools.run([str(tools.kafka_bin("kafka-server-stop.sh"))], check=False, env=java_env())
        else:
            print("kafka is not running")
        return
    with contextlib.suppress(OSError):
        os.kill(pid, signal.SIGTERM)
    for _ in range(60):
        if not tools.process_alive(pid):
            break
        time.sleep(0.5)
    tools.pid_file("kafka").unlink(missing_ok=True)
    print("kafka stopped")


def status() -> None:
    print("installed:", tools.KAFKA_HOME.exists())
    print("running:", _topic_exists())


def topics() -> None:
    tools.run(
        [
            str(tools.kafka_bin("kafka-topics.sh")),
            "--bootstrap-server",
            BOOTSTRAP,
            "--describe",
            "--topic",
            TOPIC,
        ],
        env=java_env(),
    )


def delete_group(group: str = "ledgerline-stream") -> None:
    """Forget committed offsets so a re-run consumes the topic from the start."""
    result = subprocess.run(
        [
            str(tools.kafka_bin("kafka-consumer-groups.sh")),
            "--bootstrap-server",
            BOOTSTRAP,
            "--delete",
            "--group",
            group,
        ],
        capture_output=True,
        text=True,
        env=java_env(),
    )
    output = (result.stdout or result.stderr).strip()
    print(output or f"consumer group {group}: nothing to delete")


def flush_marker(partition: int, *, event_ts: str = FLUSH_TS, marker_id: str | None = None) -> dict:
    """Build the control record that advances the watermark past every window.

    Kept separate from the producer so its shape can be tested without a
    broker. It deliberately looks like a regular event: the same JSON contract
    has to pass through ``JsonToEvent`` and both timestamp assigners. No sink
    stores it; ``IsLedgerEvent`` and ``IsMoneyEvent`` drop it first.
    """
    return {
        "event_id": marker_id or f"stream-flush-{partition}-{uuid.uuid4()}",
        "event_type": CONTROL_EVENT,
        "event_ts": event_ts,
        "order_id": CONTROL_EVENT,
        "customer_id": CONTROL_EVENT,
        "product_id": CONTROL_EVENT,
        "quantity": 0,
        "unit_price_cents": 0,
        "amount_cents": 0,
        "currency": "USD",
        "schema_version": 1,
    }


def send_flush_marker(
    *, bootstrap: str = BOOTSTRAP, topic: str = TOPIC, event_ts: str = FLUSH_TS
) -> None:
    """Publish the end-of-replay marker that closes every open window.

    Kafka never signals "no more records", so a watermark can only advance
    past the last real event if something newer arrives. The marker carries a
    far-future timestamp, flows through both watermark assigners, and is
    filtered out before the ledger sink. Only send it once no further real
    events will be produced: everything after it is late by definition.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    partitions = sorted(producer.list_topics(topic, timeout=15).topics[topic].partitions)
    if not partitions:
        raise SystemExit(f"topic {topic} has no partitions")
    # One marker per partition. The job's watermark is the minimum across
    # partitions, so a single marker would leave the other partitions holding
    # the watermark at the last real event and the final windows would stay
    # open.
    for partition in partitions:
        marker = flush_marker(partition, event_ts=event_ts)
        producer.produce(topic, json.dumps(marker).encode("utf-8"), partition=partition)
    if producer.flush(30) != 0:
        raise SystemExit("flush marker was not acknowledged by the broker")
    print(f"sent {len(partitions)} flush markers to {bootstrap}/{topic} ({event_ts})")


def reset_topic(partitions: int = 1) -> None:
    """Delete and recreate the topic so every demo starts from an empty log."""
    subprocess.run(
        [
            str(tools.kafka_bin("kafka-topics.sh")),
            "--bootstrap-server",
            BOOTSTRAP,
            "--delete",
            "--topic",
            TOPIC,
        ],
        capture_output=True,
        text=True,
        env=java_env(),
    )
    for _ in range(60):
        listed = subprocess.run(
            [str(tools.kafka_bin("kafka-topics.sh")), "--bootstrap-server", BOOTSTRAP, "--list"],
            capture_output=True,
            text=True,
            env=java_env(),
        )
        if TOPIC not in listed.stdout:
            break
        time.sleep(0.5)
    ensure_topic(partitions)


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    command = argv[0] if argv else "status"
    if command == "up":
        up()
    elif command == "down":
        down()
    elif command == "status":
        status()
    elif command == "topics":
        topics()
    elif command == "flush":
        send_flush_marker()
    elif command == "install":
        ensure_installed()
        write_config()
        format_storage()
        print("kafka installed at", tools.KAFKA_HOME)
    else:
        raise SystemExit(f"unknown command: {command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
