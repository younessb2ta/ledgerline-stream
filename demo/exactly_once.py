"""Exactly-once demo: kill a TaskManager while events are in flight.

The claim under test is not "Flink never redelivers". It does. The claim is
that redelivery is harmless:

* ``ledger_events`` inserts with ``ON CONFLICT (event_id) DO NOTHING``
* ``customer_balances`` writes an absolute running value, not an increment
* ``revenue_windows`` upserts on ``window_start``

A ``BEFORE INSERT`` trigger on ``ledger_events`` counts every attempt, so the
artifact shows both sides: the source replayed records, and the number of
distinct rows did not move.

    uv run python -m demo.exactly_once
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from demo import reset
from reconciliation import reconcile
from stack import cluster, kafka, tools

ARTIFACTS = tools.REPO / "artifacts"
DSN = f"postgresql://{tools.PG_USER}@127.0.0.1:{tools.PG_PORT}/{tools.PG_DATABASE}"


def count(sql: str) -> int:
    with psycopg.connect(DSN, connect_timeout=5) as conn:
        return int(conn.execute(sql).fetchone()[0])


def job_running(job_id: str) -> bool:
    return any(job["jid"] == job_id and job["state"] == "RUNNING" for job in cluster.jobs())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", default="2026-11-01")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--rate", type=float, default=80.0, help="events per second")
    parser.add_argument(
        "--settle", type=float, default=3.0, help="seconds after checkpoint before the kill"
    )
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or ARTIFACTS / f"exactly-once-{stamp}.json"

    print("== reset")
    reset.reset(dsn=DSN, group="ledgerline-stream")

    print("== submit")
    job_id = cluster.submit()
    tools.wait_for("job running", lambda: job_running(job_id), timeout=120)
    initial = cluster.latest_completed_checkpoint(job_id)
    initial_id = int(initial.get("id", 0))
    print(f"job {job_id}, checkpoint baseline {initial_id}")

    eventforge = Path(sys.executable).parent / "eventforge"
    command = [
        str(eventforge),
        "kafka",
        "--seed",
        str(args.seed),
        "--start",
        args.start,
        "--days",
        str(args.days),
        "--bootstrap",
        kafka.BOOTSTRAP,
        "--topic",
        kafka.TOPIC,
        "--rate",
        str(args.rate),
        "--sort",
    ]
    print("== produce in the background")
    producer = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    victim = None
    replaced = False
    pid = None
    restored: dict = {}
    killed_at = datetime.now(UTC)
    restored_at = datetime.now(UTC)
    events_before_kill = 0
    attempts_before_kill = 0
    checkpoint_before: dict = {}
    try:
        checkpoint_before = cluster.wait_for_checkpoint(job_id, after_id=initial_id, timeout=120)
        time.sleep(args.settle)
        events_before_kill = count("select count(*) from ledger_events")
        attempts_before_kill = count("select coalesce(sum(attempts), 0) from insert_attempts")

        busy = cluster.busy_taskmanagers()
        if not busy:
            raise SystemExit("no TaskManager is hosting the job")
        victim = busy[0]
        killed_at = datetime.now(UTC)
        pid = cluster.kill_taskmanager(victim)

        print("== waiting for restore")
        restored = cluster.wait_for_restore(job_id, timeout=180)
        restored_at = datetime.now(UTC)
        print(f"restored in {(restored_at - killed_at).total_seconds():.1f}s")

        print("== waiting for the producer")
        output = producer.communicate(timeout=300)[0].strip()
        print(output.splitlines()[-1] if output else "producer finished")

        print("== flush open windows")
        kafka.send_flush_marker()
        cluster.wait_for_checkpoint(
            job_id, after_id=int(checkpoint_before.get("id", 0)), timeout=180
        )

        print("== reconcile")
        code = reconcile.main(
            [
                "--days",
                str(args.days),
                "--seed",
                str(args.seed),
                "--start",
                args.start,
                "--out",
                str(out),
            ]
        )
    finally:
        if producer.poll() is None:
            producer.terminate()
        if victim is not None and len(cluster.taskmanagers()) < 2:
            print("== restarting the killed TaskManager")
            cluster.start_taskmanager()
            tools.wait_for(
                "two TaskManagers",
                lambda: len(cluster.taskmanagers()) >= 2,
                timeout=120,
            )
            replaced = True

    report = json.loads(out.read_text())
    report["failover"] = {
        "job_id": job_id,
        "victim_taskmanager": victim.get("id") if victim else None,
        "victim_pid": pid if victim else None,
        "checkpoint_before_kill": checkpoint_before.get("id"),
        "checkpoint_after_restore": restored.get("counts", {}).get("completed"),
        "restores": restored.get("counts", {}).get("restored"),
        "restore_seconds": round((restored_at - killed_at).total_seconds(), 1),
        "events_before_kill": events_before_kill,
        "attempts_before_kill": attempts_before_kill,
        "taskmanager_replaced": replaced,
        "note": (
            "The source resumed from the checkpoint offset, so events processed "
            "after that checkpoint were delivered again. The insert-attempt "
            "trigger shows the replay; distinct rows and balances are unchanged."
        ),
    }
    out.write_text(json.dumps(report, indent=2))
    print(f"\nwrote {out}")
    return code


if __name__ == "__main__":
    sys.exit(main())
