"""Pure helpers behind the dashboard's Flink health panel.

They live outside ``app.py`` so they can be tested without starting Streamlit
or reaching a running cluster. ``app.py`` turns the same REST payload into
widgets; the arithmetic and formatting happen here.
"""

from __future__ import annotations


def checkpoint_age_seconds(latest: dict, *, now_ms: float) -> float | None:
    """Seconds since the latest completed checkpoint, or ``None`` if there is none."""
    triggered = latest.get("trigger_timestamp")
    if not triggered:
        return None
    return max((now_ms - triggered) / 1000.0, 0.0)


def summarize_checkpoints(payload: dict, *, now_ms: float) -> dict:
    """Pull the signals the dashboard shows out of the checkpoint REST payload."""
    latest = (payload.get("latest") or {}).get("completed") or {}
    counts = payload.get("counts") or {}
    return {
        "latest_id": latest.get("id"),
        "age_seconds": checkpoint_age_seconds(latest, now_ms=now_ms),
        "completed": counts.get("completed"),
        "failed": counts.get("failed"),
        "restored": counts.get("restored"),
    }


def human_age(value: float | None) -> str:
    if value is None:
        return "—"
    if value < 90:
        return f"{value:.0f}s"
    return f"{value / 60:.1f}m"
