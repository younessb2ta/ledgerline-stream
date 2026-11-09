"""Waits that make a bounded replay safe to reconcile.

The end-of-replay marker closes the windows, but the projection is only
readable once three things have happened: the marker crossed the window
operator, the sinks flushed the batch that followed it, and the balance
snapshot for that checkpoint was written. Waiting for *any* checkpoint is not
enough — it can complete before the barrier that follows the marker, which is
exactly how an earlier version of the late-data demo reconciled a projection
that was still one batch short.

    uv run python -m demo.settle
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from stack import cluster, tools

DSN = f"postgresql://{tools.PG_USER}@127.0.0.1:{tools.PG_PORT}/{tools.PG_DATABASE}"


def wait_for_snapshot(checkpoint_id: int, *, dsn: str = DSN, timeout: float = 60.0) -> None:
    """Wait until ``BalanceSink.notifyCheckpointComplete`` wrote its snapshot."""
    if not checkpoint_id:
        return

    def ready() -> bool:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            row = conn.execute(
                "select count(*) from balance_snapshots where checkpoint_id = %s",
                (checkpoint_id,),
            ).fetchone()
        return bool(row) and int(row[0]) > 0

    tools.wait_for(f"balance snapshot for checkpoint {checkpoint_id}", ready, timeout=timeout)


def settle(job_id: str, *, dsn: str = DSN, timeout: float = 240.0) -> int:
    """Wait for the flush marker to pass and be checkpointed; return the id."""
    final = cluster.wait_for_flush(job_id, timeout=timeout)
    checkpoint_id = int(final.get("id", 0) or 0)
    wait_for_snapshot(checkpoint_id, dsn=dsn)
    return checkpoint_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", default=None, help="defaults to the active job")
    parser.add_argument("--timeout", type=float, default=240.0)
    args = parser.parse_args(argv)

    job_id = args.job_id
    if job_id is None:
        job = cluster.active_job()
        if job is None:
            raise SystemExit("no active job")
        job_id = job["jid"]

    checkpoint_id = settle(job_id, timeout=args.timeout)
    print(f"flushed through checkpoint {checkpoint_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
