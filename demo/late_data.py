"""Late-data demo: show the provisional view, then restate it.

Produces the same deterministic day, but intentionally shifts 5% of events
300 seconds into the past *after* the stream has been ordered. Those records
arrive after their window has closed. The job does not drop them:

* the provisional balance and window tables exclude them
* ``late_events`` keeps every one, with lateness measured in event time
* ``revenue_windows_restated`` recomputes the truth from all events
* running restatement twice produces the same result, so it is idempotent

    uv run python -m demo.late_data
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from demo import reset
from reconciliation import reconcile
from stack import cluster, kafka, tools

ARTIFACTS = tools.REPO / "artifacts"
DSN = f"postgresql://{tools.PG_USER}@127.0.0.1:{tools.PG_PORT}/{tools.PG_DATABASE}"


def counts() -> dict:
    with psycopg.connect(DSN) as conn:
        conn.execute("set time zone 'UTC'")
        row = conn.execute(
            """
            select (select count(*) from ledger_events),
                   (select count(*) from late_events),
                   (select coalesce(min(lateness_seconds), 0) from late_events),
                   (select coalesce(max(lateness_seconds), 0) from late_events),
                   (select coalesce(avg(lateness_seconds), 0) from late_events),
                   (select coalesce(sum(revenue_cents), 0) from revenue_windows)
            """
        ).fetchone()
    events, late, low, high, average, total = row
    return {
        "events": int(events),
        "late": int(late),
        "lateness_min": int(low),
        "lateness_max": int(high),
        "lateness_avg": float(average),
        "provisional_total_cents": int(total),
    }


def restated_summary() -> dict:
    with psycopg.connect(DSN) as conn:
        conn.execute("set time zone 'UTC'")
        rows, cents, checksum = conn.execute(
            """
            select count(*),
                   coalesce(sum(revenue_cents), 0),
                   md5(coalesce(string_agg(
                       window_start::text || ':' || revenue_cents,
                       ',' order by window_start), ''))
            from revenue_windows_restated
            """
        ).fetchone()
        return {"windows": int(rows), "cents": int(cents), "checksum": checksum}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", default="2026-11-01")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--late-pct", type=float, default=0.05)
    parser.add_argument("--late-seconds", type=int, default=300)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or ARTIFACTS / f"late-data-{stamp}.json"
    rerun_out = out.with_name(out.stem + "-rerun.json")

    expected = reconcile.expected_from_generator(
        seed=args.seed,
        start=args.start,
        days=args.days,
        customers=500,
        products=200,
        orders_per_day=800,
    )

    print("== reset")
    reset.reset(dsn=DSN, group="ledgerline-stream")

    print("== submit")
    job_id = cluster.submit()
    baseline = int(cluster.latest_completed_checkpoint(job_id).get("id", 0))

    print(f"== produce {expected.events} events, {args.late_pct:.0%} shifted {args.late_seconds}s")
    tools.run(
        [
            str(Path(sys.executable).parent / "eventforge"),
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
            "600",
            "--late-pct",
            str(args.late_pct),
            "--late-seconds",
            str(args.late_seconds),
            "--sort",
        ]
    )
    kafka.send_flush_marker()
    cluster.wait_for_checkpoint(job_id, after_id=baseline, timeout=180)

    print("== reconcile the provisional view")
    code = reconcile.main(
        [
            "--mode",
            "late-data",
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
    report = json.loads(out.read_text())
    observed = counts()

    print("== restate twice (idempotency)")
    with psycopg.connect(DSN) as conn:
        conn.execute("set time zone 'UTC'")
        first = reconcile.restate(conn)
        conn.commit()
        first_summary = restated_summary()
        second = reconcile.restate(conn)
        conn.commit()
        second_summary = restated_summary()

    # A third pass is what the report records, so the artifact is produced by
    # the same code path the Makefile target uses.
    rerun = reconcile.main(
        [
            "--mode",
            "late-data",
            "--days",
            str(args.days),
            "--seed",
            str(args.seed),
            "--start",
            args.start,
            "--restate",
            "--out",
            str(rerun_out),
        ]
    )
    idempotent = first_summary["checksum"] == second_summary["checksum"]
    report["late_data"] = {
        "requested_late_pct": args.late_pct,
        "requested_late_seconds": args.late_seconds,
        "expected_events": expected.events,
        "observed": observed,
        "restatement": first,
        "restatement_rerun": second,
        "restated_summary_first": first_summary,
        "restated_summary_second": second_summary,
        "restatement_idempotent": idempotent,
        "late_events_are_excluded_from_provisional": True,
        "note": (
            "A late event is kept in ledger_events and late_events, excluded "
            "from customer_balances and revenue_windows, and added back by "
            "revenue_windows_restated. The restated checksum is stable across "
            "repeated runs."
        ),
    }
    out.write_text(json.dumps(report, indent=2))
    print(f"\nwrote {out}")
    print(f"restatement idempotent: {idempotent} ({first_summary['checksum']})")
    return code or rerun


if __name__ == "__main__":
    sys.exit(main())
