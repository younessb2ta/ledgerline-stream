from __future__ import annotations

import socket

from stack.cluster import parse_flink_processes
from stack.kafka import broker_listening

FLINK_HOME = "/Users/dev/.local/ledgerline-stream/flink-1.20.3"


def java_command(pid: int, main_class: str, home: str = FLINK_HOME) -> str:
    return f"  {pid} {home}/bin/java -classpath {home}/lib/* {main_class}"


def test_broker_listening_is_true_while_a_socket_accepts():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        assert broker_listening("127.0.0.1", port) is True


def test_broker_listening_is_false_when_the_port_is_closed():
    """The cold-start preflight must not block on a broker that is not running."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        port = server.getsockname()[1]
    assert broker_listening("127.0.0.1", port) is False


def test_parse_flink_processes_finds_taskmanagers_and_the_jobmanager():
    ps_output = "\n".join(
        [
            java_command(101, "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"),
            java_command(
                102,
                "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint",
            ),
        ]
    )
    assert parse_flink_processes(ps_output, FLINK_HOME) == [
        (101, "TaskManagerRunner"),
        (102, "StandaloneSessionClusterEntrypoint"),
    ]


def test_parse_flink_processes_ignores_other_installations_and_jobs():
    ps_output = "\n".join(
        [
            java_command(
                201,
                "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
                home="/opt/flink-1.20.3",
            ),
            java_command(
                202,
                "org.apache.flink.runtime.taskexecutor.TaskManagerRunner",
                home=f"{FLINK_HOME}-backup",
            ),
            java_command(203, "com.ledgerline.stream.LedgerJob"),
            java_command(204, "org.apache.flink.client.cli.CliFrontend run"),
        ]
    )
    assert parse_flink_processes(ps_output, FLINK_HOME) == []


def test_parse_flink_processes_tolerates_blank_and_malformed_lines():
    assert parse_flink_processes("\nnot-a-pid TaskManagerRunner\n", FLINK_HOME) == []
