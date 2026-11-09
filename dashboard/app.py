"""Ledgerline live dashboard.

Reads the provisional projection from Postgres and the job's health from the
Flink REST API. The two are deliberately separate: the database says what the
ledger believes, Flink says whether the pipeline is keeping its promises about
checkpoints and lateness.

    uv run streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg
import requests
import streamlit as st

# Streamlit executes this file as __main__, so the repository root is not on
# sys.path. health.py is still importable as `dashboard.health` for tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.health import human_age, summarize_checkpoints  # noqa: E402

DSN = os.environ.get("LEDGERLINE_DSN", "postgresql://postgres@127.0.0.1:5433/ledgerline")
FLINK = os.environ.get("LEDGERLINE_FLINK", "http://127.0.0.1:8081")

st.set_page_config(page_title="Ledgerline — provisional ledger", layout="wide")


def query(sql: str, params: tuple | None = None) -> list[tuple]:
    with psycopg.connect(DSN, connect_timeout=3) as conn:
        conn.execute("set time zone 'UTC'")
        return conn.execute(sql, params or ()).fetchall()


def frame(sql: str, params: tuple | None = None, columns: list[str] | None = None) -> pd.DataFrame:
    rows = query(sql, params)
    return pd.DataFrame(rows, columns=columns)


def flink(path: str) -> dict:
    response = requests.get(f"{FLINK}{path}", timeout=2)
    response.raise_for_status()
    return response.json()


def active_job() -> dict | None:
    try:
        jobs = flink("/jobs/overview").get("jobs", [])
    except requests.RequestException:
        return None
    states = ("RUNNING", "RESTARTING", "INITIALIZING", "CREATED", "FAILING")
    active = [job for job in jobs if job["state"] in states]
    return active[-1] if active else None


def checkpoint_health(job_id: str) -> dict:
    try:
        data = flink(f"/jobs/{job_id}/checkpoints")
    except requests.RequestException:
        return {}
    return summarize_checkpoints(data, now_ms=time.time() * 1000)


def seconds(value: float | None) -> str:
    return human_age(value)


@st.fragment(run_every="2s")
def live() -> None:
    job = active_job()
    job_state = job["state"] if job else "NO JOB"
    health = checkpoint_health(job["jid"]) if job else {}

    header = st.columns([2, 1, 1, 1, 1])
    header[0].markdown("## Ledgerline")
    header[1].metric("Flink job", job_state)
    header[2].metric("Checkpoint age", seconds(health.get("age_seconds")))
    header[3].metric("Restores", health.get("restored", "—"))
    header[4].metric("Checkpoints", health.get("completed", "—"))

    try:
        totals = query(
            """
            select (select count(*) from ledger_events),
                   (select count(distinct event_id) from ledger_events),
                   (select count(*) from late_events),
                   (select coalesce(sum(balance_cents), 0) from customer_balances),
                   (select count(*) from customer_balances),
                   (select count(*) from revenue_windows),
                   (select coalesce(sum(revenue_cents), 0) from revenue_windows)
            """
        )[0]
    except psycopg.Error as error:
        st.error(f"Postgres is not reachable: {error}")
        return

    events, distinct, late, balance_total, customers, windows, window_total = totals

    metrics = st.columns(6)
    metrics[0].metric("Events", f"{events:,}")
    metrics[1].metric("Distinct", f"{distinct:,}")
    metrics[2].metric("Late", f"{late:,}")
    metrics[3].metric("Customers", f"{customers:,}")
    metrics[4].metric("Balance total", f"${balance_total / 100:,.2f}")
    metrics[5].metric("Windows", f"{windows:,}")

    left, right = st.columns([3, 2])

    with left:
        st.subheader("Revenue per minute")
        revenue = frame(
            """
            select window_start, revenue_cents
            from revenue_windows
            order by window_start desc
            limit 120
            """,
            columns=["window_start", "revenue_cents"],
        )
        if revenue.empty:
            st.info("No closed windows yet. The watermark has not crossed a window end.")
        else:
            revenue = revenue.sort_values("window_start")
            revenue["revenue"] = revenue["revenue_cents"] / 100
            st.bar_chart(revenue.set_index("window_start")["revenue"], height=280)
            st.caption(
                f"Showing {len(revenue)} of {windows:,} closed windows · "
                f"provisional total ${window_total / 100:,.2f}"
            )

        st.subheader("Largest balances")
        top = frame(
            """
            select customer_id, balance_cents, last_event_ts
            from customer_balances
            order by balance_cents desc
            limit 10
            """,
            columns=["customer", "balance", "last event"],
        )
        if not top.empty:
            top["balance"] = top["balance"].map(lambda cents: f"${cents / 100:,.2f}")
            st.dataframe(top, width="stretch", hide_index=True)

    with right:
        st.subheader("Checkpoint health")
        if health:
            st.dataframe(
                pd.DataFrame(
                    [
                        ["job", job["jid"]],
                        ["state", job_state],
                        ["latest checkpoint", health.get("latest_id")],
                        ["age", seconds(health.get("age_seconds"))],
                        ["completed", health.get("completed")],
                        ["failed", health.get("failed")],
                        ["restored", health.get("restored")],
                    ],
                    columns=["signal", "value"],
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("Flink is not reachable.")

        st.subheader("Late events")
        late_rows = frame(
            """
            select event_id,
                   event_ts,
                   lateness_seconds,
                   payload ->> 'event_type' as event_type,
                   (payload ->> 'amount_cents')::bigint as amount_cents
            from late_events
            order by event_ts desc
            limit 10
            """,
            columns=["event", "event time", "late by (s)", "type", "amount"],
        )
        if late_rows.empty:
            st.success("No event has missed its window.")
        else:
            late_rows["amount"] = late_rows["amount"].map(lambda cents: f"${cents / 100:,.2f}")
            st.dataframe(late_rows, width="stretch", hide_index=True)
            st.caption("These are excluded from the provisional view until restatement.")

        st.subheader("Provisional vs replayed")
        replay = query(
            """
            select count(*),
                   coalesce(sum(case when event_type = 'refund_issued'
                                     then -amount_cents else amount_cents end), 0)
            from ledger_events
            where event_type in ('order_paid', 'refund_issued')
              and not exists (
                  select 1 from late_events as l where l.event_id = ledger_events.event_id
              )
            """
        )[0]
        restated = query(
            "select count(*), coalesce(sum(revenue_cents), 0) from revenue_windows_restated"
        )[0]
        attempts = query(
            """
            select coalesce(sum(attempts), 0),
                   coalesce(sum(greatest(attempts - 1, 0)), 0)
            from insert_attempts
            """
        )[0]
        st.dataframe(
            pd.DataFrame(
                [
                    ["provisional windows", f"{windows:,}", f"${window_total / 100:,.2f}"],
                    ["replayed events", f"{replay[0]:,}", f"${replay[1] / 100:,.2f}"],
                    ["restated windows", f"{restated[0]:,}", f"${restated[1] / 100:,.2f}"],
                ],
                columns=["view", "rows", "revenue"],
            ),
            width="stretch",
            hide_index=True,
        )
        st.caption(
            f"{int(attempts[0]):,} insert attempts for {int(events):,} events · "
            f"{int(attempts[1]):,} replayed writes absorbed by the primary key"
        )


live()
st.caption(
    "Provisional means watermark-closed: a window appears only after the "
    "watermark passes its end plus the allowed lateness. Late events are "
    "recorded, excluded from this view, and added back by restatement."
)
