"""Start/stop the embedded Postgres server (TCP on 127.0.0.1:5433).

`pgserver` ships portable Postgres 16 binaries and an initdb wrapper, but its
managed server listens on a Unix socket only. Java's JDBC driver needs TCP, so
this module keeps the bundled binaries and runs `pg_ctl` itself with
`-h 127.0.0.1 -p 5433`.

    uv run python -m stack.postgres up|down|status|init|psql
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from . import tools


def _bin(name: str) -> Path:
    return tools.postgres_bin() / name


def initialized() -> bool:
    return (tools.PGDATA / "PG_VERSION").exists()


def running() -> bool:
    if not initialized():
        return False
    result = subprocess.run(
        [str(_bin("pg_ctl")), "-D", str(tools.PGDATA), "status"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def init() -> None:
    if initialized():
        return
    tools.PGDATA.parent.mkdir(parents=True, exist_ok=True)
    tools.run(
        [
            str(_bin("initdb")),
            "-D",
            str(tools.PGDATA),
            "-U",
            tools.PG_USER,
            "--auth=trust",
            "--encoding=UTF8",
            "--locale=C",
        ]
    )


def up() -> None:
    init()
    if not running():
        tools.run(
            [
                str(_bin("pg_ctl")),
                "-D",
                str(tools.PGDATA),
                "-o",
                f"-h 127.0.0.1 -p {tools.PG_PORT} -k {tools.PGDATA}",
                "-l",
                str(tools.PGDATA / "log"),
                "-w",
                "start",
            ]
        )
    _createdb_if_missing()
    apply_schema()
    print(
        f"postgres ready: postgresql://{tools.PG_USER}@127.0.0.1:{tools.PG_PORT}/{tools.PG_DATABASE}"
    )


def down() -> None:
    if running():
        tools.run([str(_bin("pg_ctl")), "-D", str(tools.PGDATA), "-m", "fast", "stop"])
    else:
        print("postgres is not running")


def status() -> None:
    print("initialized:", initialized())
    print("running:", running())
    if running():
        print(f"jdbc: {tools.PG_JDBC}")


def _createdb_if_missing() -> None:
    exists = subprocess.run(
        [
            str(_bin("psql")),
            "-h",
            "127.0.0.1",
            "-p",
            str(tools.PG_PORT),
            "-U",
            tools.PG_USER,
            "-d",
            "postgres",
            "-tAc",
            f"SELECT 1 FROM pg_database WHERE datname = '{tools.PG_DATABASE}'",
        ],
        capture_output=True,
        text=True,
    )
    if exists.stdout.strip() != "1":
        tools.run(
            [
                str(_bin("createdb")),
                "-h",
                "127.0.0.1",
                "-p",
                str(tools.PG_PORT),
                "-U",
                tools.PG_USER,
                tools.PG_DATABASE,
            ]
        )


def apply_schema() -> None:
    tools.run(
        [
            str(_bin("psql")),
            "-h",
            "127.0.0.1",
            "-p",
            str(tools.PG_PORT),
            "-U",
            tools.PG_USER,
            "-d",
            tools.PG_DATABASE,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(tools.REPO / "db" / "schema.sql"),
        ]
    )


def psql(statement: str) -> None:
    tools.run(
        [
            str(_bin("psql")),
            "-h",
            "127.0.0.1",
            "-p",
            str(tools.PG_PORT),
            "-U",
            tools.PG_USER,
            "-d",
            tools.PG_DATABASE,
            "-c",
            statement,
        ]
    )


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    command = argv[0] if argv else "status"
    if command == "up":
        up()
    elif command == "down":
        down()
    elif command == "status":
        status()
    elif command == "init":
        init()
        print("initialized", tools.PGDATA)
    elif command == "psql":
        psql(" ".join(argv[1:]))
    else:
        raise SystemExit(f"unknown command: {command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
