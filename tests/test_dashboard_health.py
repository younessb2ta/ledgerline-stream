from __future__ import annotations

from dashboard.health import checkpoint_age_seconds, human_age, summarize_checkpoints


def test_checkpoint_age_is_measured_from_the_trigger_timestamp():
    latest = {"id": 7, "trigger_timestamp": 1_000_000}

    assert checkpoint_age_seconds(latest, now_ms=1_045_500) == 45.5


def test_checkpoint_age_is_none_without_a_completed_checkpoint():
    assert checkpoint_age_seconds({}, now_ms=1_000_000) is None


def test_age_never_goes_negative_when_clocks_disagree():
    latest = {"trigger_timestamp": 2_000_000}

    assert checkpoint_age_seconds(latest, now_ms=1_999_000) == 0.0


def test_summarize_exposes_the_failover_signals():
    payload = {
        "counts": {"completed": 9, "failed": 1, "restored": 2},
        "latest": {"completed": {"id": 9, "trigger_timestamp": 1_000_000}},
    }

    summary = summarize_checkpoints(payload, now_ms=1_030_000)

    assert summary == {
        "latest_id": 9,
        "age_seconds": 30.0,
        "completed": 9,
        "failed": 1,
        "restored": 2,
    }


def test_summarize_tolerates_a_cluster_without_checkpoints():
    summary = summarize_checkpoints({"counts": {}, "latest": {}}, now_ms=1_000_000)

    assert summary["latest_id"] is None
    assert summary["age_seconds"] is None
    assert summary["restored"] is None


def test_human_age_switches_from_seconds_to_minutes():
    assert human_age(None) == "—"
    assert human_age(5) == "5s"
    assert human_age(90) == "1.5m"
