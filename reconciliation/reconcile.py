"""Compare the streaming projection with the generator's ground truth.

Two independent questions, deliberately kept separate:

1. **Completeness** — did the job record every event exactly once? Compared
   against `eventforge`, which is deterministic, so the expected IDs and sums
   are known rather than assumed.
2. **Projection correctness** — do `customer_balances` and `revenue_windows`
   equal what a fresh replay of `ledger_events` produces? Late events are
   excluded from the provisional projection by design; they are added back by
   `--restate`.

Exit code is 0 only when every check passes, so this can gate the demo.

    uv run python -m reconciliation.reconcile --days 3
    uv run python -m reconciliation.reconcile --days 3 --restate
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DSN = "postgresql://postgres@127.0.0.1:5433/ledgerline"
DEFAULT_OUT = ROOT / "artifacts" / "reconciliation.json"


@dataclass
class Expected:
    events: int
    event_ids: set[str]
    per_customer_cents: dict[str, int]
    total_revenue_cents: int


def expected_from_generator(
    *, seed: int, start: str, days: int, customers: int, products: int, orders_per_day: int
) -> Expected:
    from eventforge.generator import GeneratorConfig, generate_day

    cfg = GeneratorConfig(
        seed=seed,
        start_date=date.fromisoformat(start),
        days=days,
        n_customers=customers,
        n_products=products,
        orders_per_day=orders_per_day,
    )
    per_customer: defaultdict[str, int] = defaultdict(int)
    ids: set[str] = set()
    total_events = 0
    for offset in range(cfg.days):
        day = cfg.start_date + timedelta(days=offset)
        data = generate_day(cfg, day)
        truth = data.ground_truth
        total_events += truth["total_events"]
        ids.update(event.event_id for event in data.events)
        for customer_id, cents in truth["per_customer_balance_cents"].items():
            per_customer[customer_id] += cents
    return Expected(
        events=total_events,
        event_ids=ids,
        per_customer_cents=dict(per_customer),
        total_revenue_cents=sum(per_customer.values()),
    )


def _accepted_filter(include_late: bool) -> str:
    if include_late:
        return ""
    return "and not exists (select 1 from late_events as l where l.event_id = e.event_id)"


def projection(conn: psycopg.Connection, *, include_late: bool) -> tuple[dict[str, int], dict]:
    where = _accepted_filter(include_late)
    balances = dict(
        conn.execute(
            f"""
            select e.customer_id,
                   sum(case when e.event_type = 'refund_issued'
                            then -e.amount_cents else e.amount_cents end)::bigint
            from ledger_events as e
            where e.event_type in ('order_paid', 'refund_issued')
              {where}
            group by e.customer_id
            """
        ).fetchall()
    )
    windows = {
        row[0]: {"revenue_cents": int(row[1]), "event_count": int(row[2])}
        for row in conn.execute(
            f"""
            select date_trunc('minute', e.event_ts) as window_start,
                   sum(case when e.event_type = 'refund_issued'
                            then -e.amount_cents else e.amount_cents end)::bigint,
                   count(*)::bigint
            from ledger_events as e
            where e.event_type in ('order_paid', 'refund_issued')
              {where}
            group by 1
            """
        ).fetchall()
    }
    return balances, windows


def compare_balances(expected: dict[str, int], observed: dict[str, int]) -> dict:
    keys = set(expected) | set(observed)
    mismatches = {
        key: {"expected": expected.get(key, 0), "observed": observed.get(key, 0)}
        for key in keys
        if expected.get(key, 0) != observed.get(key, 0)
    }
    return {
        "customers_expected": len(expected),
        "customers_observed": len(observed),
        "mismatches": len(mismatches),
        "max_abs_diff_cents": max(
            (abs(item["expected"] - item["observed"]) for item in mismatches.values()), default=0
        ),
        "sample": dict(list(mismatches.items())[:5]),
    }


def compare_windows(expected: dict, observed: dict) -> dict:
    keys = set(expected) | set(observed)
    mismatches = {
        str(key): {
            "expected": expected.get(key, {"revenue_cents": 0, "event_count": 0}),
            "observed": observed.get(key, {"revenue_cents": 0, "event_count": 0}),
        }
        for key in keys
        if expected.get(key) != observed.get(key)
    }
    return {
        "windows_expected": len(expected),
        "windows_observed": len(observed),
        "mismatches": len(mismatches),
        "max_abs_diff_cents": max(
            (
                abs(item["expected"]["revenue_cents"] - item["observed"]["revenue_cents"])
                for item in mismatches.values()
            ),
            default=0,
        ),
        "sample": dict(list(mismatches.items())[:5]),
    }


def restate(conn: psycopg.Connection) -> dict:
    """Recompute windows from all events and store the authoritative result."""
    conn.execute("delete from revenue_windows_restated")
    conn.execute(
        """
        insert into revenue_windows_restated
            (window_start, window_end, revenue_cents, event_count, restated_at)
        select date_trunc('minute', event_ts) as window_start,
               date_trunc('minute', event_ts) + interval '1 minute' as window_end,
               sum(case when event_type = 'refund_issued'
                        then -amount_cents else amount_cents end)::bigint,
               count(*)::bigint,
               now()
        from ledger_events
        where event_type in ('order_paid', 'refund_issued')
        group by 1
        """
    )
    provisional = {
        row[0]: int(row[1])
        for row in conn.execute(
            "select window_start, revenue_cents from revenue_windows"
        ).fetchall()
    }
    restated = {
        row[0]: int(row[1])
        for row in conn.execute(
            "select window_start, revenue_cents from revenue_windows_restated"
        ).fetchall()
    }
    keys = set(provisional) | set(restated)
    diffs = {
        str(key): {"provisional": provisional.get(key, 0), "restated": restated.get(key, 0)}
        for key in keys
        if provisional.get(key, 0) != restated.get(key, 0)
    }
    missing = sum(diff["restated"] - diff["provisional"] for diff in diffs.values())
    return {
        "windows_provisional": len(provisional),
        "windows_restated": len(restated),
        "windows_with_diff": len(diffs),
        "revenue_delta_cents": missing,
        "sample": dict(list(diffs.items())[:5]),
    }


def latest_snapshot(conn: psycopg.Connection) -> dict:
    row = conn.execute("select max(checkpoint_id) from balance_snapshots").fetchone()
    if not row or row[0] is None:
        return {"checkpoint_id": None, "mismatches": None}
    checkpoint_id = int(row[0])
    snapshot = dict(
        conn.execute(
            "select customer_id, balance_cents from balance_snapshots where checkpoint_id = %s",
            (checkpoint_id,),
        ).fetchall()
    )
    live = dict(conn.execute("select customer_id, balance_cents from customer_balances").fetchall())
    mismatches = sum(1 for key in set(snapshot) | set(live) if snapshot.get(key) != live.get(key))
    return {
        "checkpoint_id": checkpoint_id,
        "customers": len(snapshot),
        "mismatches": mismatches,
    }


def build_report(args: argparse.Namespace, conn: psycopg.Connection) -> dict:
    conn.execute("set time zone 'UTC'")
    expected = expected_from_generator(
        seed=args.seed,
        start=args.start,
        days=args.days,
        customers=args.customers,
        products=args.products,
        orders_per_day=args.orders_per_day,
    )

    event_rows = conn.execute(
        "select count(*), count(distinct event_id) from ledger_events"
    ).fetchone()
    recorded, distinct = int(event_rows[0]), int(event_rows[1])
    late_rows = conn.execute(
        """
        select count(*),
               coalesce(sum(
                   case when payload ->> 'event_type' = 'refund_issued'
                        then -(payload ->> 'amount_cents')::bigint
                        when payload ->> 'event_type' = 'order_paid'
                        then  (payload ->> 'amount_cents')::bigint
                        else 0 end), 0)
        from late_events
        """
    ).fetchone()
    late_count, late_signed_cents = int(late_rows[0]), int(late_rows[1])

    # The trigger on ledger_events records every INSERT attempt, including the
    # ones ON CONFLICT DO NOTHING skips. It is how a failover demo proves that
    # the source replayed records and the sink absorbed them.
    try:
        write_rows = conn.execute(
            """
            select coalesce(sum(attempts), 0),
                   coalesce(sum(greatest(attempts - 1, 0)), 0),
                   count(*) filter (where attempts > 1)
            from insert_attempts
            """
        ).fetchone()
        write_attempts, replayed_attempts, events_replayed = (int(value) for value in write_rows)
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        write_attempts = replayed_attempts = events_replayed = None

    observed_ids = {row[0] for row in conn.execute("select event_id from ledger_events").fetchall()}

    provisional_balances, provisional_windows = projection(conn, include_late=False)
    live_balances = dict(
        conn.execute("select customer_id, balance_cents from customer_balances").fetchall()
    )
    live_windows = {
        row[0]: {"revenue_cents": int(row[1]), "event_count": int(row[2])}
        for row in conn.execute(
            "select window_start, revenue_cents, event_count from revenue_windows"
        ).fetchall()
    }

    # Late events are excluded from the provisional projection on purpose, so
    # the expected provisional total is the ground truth minus their signed
    # amounts. With no late data this reduces to the plain ground truth.
    expected_provisional_cents = expected.total_revenue_cents - late_signed_cents
    balance_replay = compare_balances(provisional_balances, live_balances)
    window_replay = compare_windows(provisional_windows, live_windows)

    checks = [
        {
            "name": "events_recorded",
            "expected": expected.events,
            "observed": recorded,
            "pass": recorded == expected.events,
        },
        {
            "name": "events_distinct",
            "expected": expected.events,
            "observed": distinct,
            "pass": distinct == expected.events,
        },
        {
            "name": "duplicate_rows",
            "expected": 0,
            "observed": recorded - distinct,
            "pass": recorded == distinct,
        },
        {
            "name": "event_id_sha256",
            "expected": _sha256(expected.event_ids)[:16],
            "observed": _sha256(observed_ids)[:16],
            "pass": _sha256(expected.event_ids) == _sha256(observed_ids),
        },
        {
            "name": "customer_balances_match_replay",
            "expected": len(provisional_balances),
            "observed": balance_replay["mismatches"],
            "pass": balance_replay["mismatches"] == 0,
        },
        {
            "name": "revenue_windows_match_replay",
            "expected": len(provisional_windows),
            "observed": window_replay["mismatches"],
            "pass": window_replay["mismatches"] == 0,
        },
        {
            "name": "provisional_balance_total_cents",
            "expected": expected_provisional_cents,
            "observed": sum(live_balances.values()),
            "pass": sum(live_balances.values()) == expected_provisional_cents,
        },
        {
            "name": "provisional_window_total_cents",
            "expected": expected_provisional_cents,
            "observed": sum(item["revenue_cents"] for item in live_windows.values()),
            "pass": sum(item["revenue_cents"] for item in live_windows.values())
            == expected_provisional_cents,
        },
    ]

    snapshot = latest_snapshot(conn)
    report = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "mode": args.mode,
        "window": {
            "seed": args.seed,
            "start": args.start,
            "days": args.days,
            "customers": args.customers,
            "products": args.products,
            "orders_per_day": args.orders_per_day,
        },
        "expected": {
            "events": expected.events,
            "customers": len(expected.per_customer_cents),
            "total_revenue_cents": expected.total_revenue_cents,
        },
        "observed": {
            "events": recorded,
            "distinct_events": distinct,
            "customers_with_balance": len(live_balances),
            "balance_total_cents": sum(live_balances.values()),
            "revenue_windows": len(live_windows),
            "window_total_cents": sum(item["revenue_cents"] for item in live_windows.values()),
            "late_events": late_count,
            "late_signed_cents": late_signed_cents,
            "write_attempts": write_attempts,
            "replayed_attempts": replayed_attempts,
            "events_replayed": events_replayed,
        },
        "checks": checks,
        "balance_replay": balance_replay,
        "window_replay": window_replay,
        "latest_snapshot": snapshot,
    }
    if args.restate:
        report["restatement"] = restate(conn)
        conn.commit()
    return report


def _sha256(ids: set[str]) -> str:
    import hashlib

    return hashlib.sha256("\n".join(sorted(ids)).encode("utf-8")).hexdigest()


def render(report: dict) -> str:
    lines = [
        f"reconciliation ({report['mode']})  {report['generated_at']}",
        f"expected: {report['expected']['events']} events, "
        f"{report['expected']['customers']} customers, "
        f"{report['expected']['total_revenue_cents']} cents revenue",
        f"observed: {report['observed']['events']} events "
        f"({report['observed']['distinct_events']} distinct), "
        f"{report['observed']['customers_with_balance']} balances, "
        f"{report['observed']['revenue_windows']} windows, "
        f"{report['observed']['late_events']} late",
    ]
    if report["observed"].get("write_attempts") is not None:
        lines.append(
            f"writes: {report['observed']['write_attempts']} insert attempts for "
            f"{report['observed']['events']} events, "
            f"{report['observed']['replayed_attempts']} replayed and absorbed"
        )
    lines += [
        "",
        f"{'check':<38}{'result':<8}{'expected':>14}{'observed':>14}",
    ]
    for check in report["checks"]:
        passed = check["pass"]
        result = "info" if passed is None else ("PASS" if passed else "FAIL")
        lines.append(
            f"{check['name']:<38}{result:<8}"
            f"{str(check['expected'])[:14]:>14}{str(check['observed'])[:14]:>14}"
        )
    snapshot = report["latest_snapshot"]
    if snapshot["checkpoint_id"] is not None:
        lines.append(
            f"\nlatest checkpoint {snapshot['checkpoint_id']}: "
            f"{snapshot['mismatches']} balance mismatches vs the snapshot"
        )
    if "restatement" in report:
        restatement = report["restatement"]
        lines.append(
            f"restatement: {restatement['windows_with_diff']} windows differ, "
            f"{restatement['revenue_delta_cents']} cents added by late data"
        )
    verdict = all(check["pass"] for check in report["checks"] if check["pass"] is not None)
    lines.append("\nRESULT: " + ("PASS" if verdict else "FAIL"))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--mode", default="exactly-once", choices=["exactly-once", "late-data"])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", default="2026-11-01")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--orders-per-day", type=int, default=800)
    parser.add_argument("--restate", action="store_true", help="recompute windows from all events")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    with psycopg.connect(args.dsn) as conn:
        report = build_report(args, conn)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2))
    print(render(report))
    print(f"\nwrote {args.out}")
    verdict = all(check["pass"] for check in report["checks"] if check["pass"] is not None)
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
