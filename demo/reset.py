"""Reset the streaming stack to a clean, reproducible state.

Cancels jobs, empties every table, deletes local checkpoints, and forgets the
Kafka consumer group's committed offsets. Without the last step a re-run would
consume nothing, which is the classic way a streaming demo quietly stops
proving anything.

    uv run python -m demo.reset
"""

from __future__ import annotations

import argparse
import shutil
import sys

import psycopg

from stack import cluster, kafka, tools

TABLES = (
    "ledger_events",
    "insert_attempts",
    "customer_balances",
    "revenue_windows",
    "balance_snapshots",
    "late_events",
    "revenue_windows_restated",
    "pipeline_events",
)


def reset(*, dsn: str, group: str) -> None:
    cluster.cancel_all()

    with psycopg.connect(dsn) as conn:
        conn.execute("truncate " + ", ".join(TABLES) + " restart identity")
        conn.commit()
    print(f"truncated {len(TABLES)} tables")

    if tools.CHECKPOINTS.exists():
        shutil.rmtree(tools.CHECKPOINTS)
    tools.CHECKPOINTS.mkdir(parents=True, exist_ok=True)
    print(f"cleared {tools.CHECKPOINTS}")

    kafka.reset_topic()
    kafka.delete_group(group)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=f"postgresql://{tools.PG_USER}@127.0.0.1:{tools.PG_PORT}/{tools.PG_DATABASE}",
    )
    parser.add_argument("--group", default="ledgerline-stream")
    args = parser.parse_args(argv)
    reset(dsn=args.dsn, group=args.group)
    print("reset complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
