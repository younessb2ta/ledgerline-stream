from __future__ import annotations

import hashlib
from datetime import date

from eventforge.generator import GeneratorConfig, generate_day

from reconciliation import reconcile

SMALL = {
    "seed": 7,
    "start": "2026-11-01",
    "days": 1,
    "customers": 25,
    "products": 12,
    "orders_per_day": 40,
}


def _small_day():
    cfg = GeneratorConfig(
        seed=SMALL["seed"],
        start_date=date.fromisoformat(SMALL["start"]),
        days=SMALL["days"],
        n_customers=SMALL["customers"],
        n_products=SMALL["products"],
        orders_per_day=SMALL["orders_per_day"],
    )
    return cfg, generate_day(cfg, cfg.start_date)


def test_expected_matches_the_generator_ground_truth():
    cfg, data = _small_day()
    expected = reconcile.expected_from_generator(**SMALL)
    truth = data.ground_truth

    assert expected.events == truth["total_events"]
    assert expected.event_ids == {event.event_id for event in data.events}
    assert expected.total_revenue_cents == sum(truth["per_customer_balance_cents"].values())


def test_expected_ids_are_stable_across_calls():
    first = reconcile.expected_from_generator(**SMALL)
    second = reconcile.expected_from_generator(**SMALL)

    assert first.event_ids == second.event_ids
    assert reconcile._sha256(first.event_ids) == reconcile._sha256(second.event_ids)


def test_compare_balances_reports_only_the_differences():
    result = reconcile.compare_balances({"a": 100, "b": 50}, {"a": 100, "c": 5})

    assert result["customers_expected"] == 2
    assert result["customers_observed"] == 2
    assert result["mismatches"] == 2
    assert result["max_abs_diff_cents"] == 50
    assert set(result["sample"]) == {"b", "c"}


def test_compare_windows_uses_missing_side_as_zero():
    result = reconcile.compare_windows(
        {"2026-11-01 00:00": {"revenue_cents": 120, "event_count": 2}},
        {},
    )

    assert result["mismatches"] == 1
    assert result["max_abs_diff_cents"] == 120
    assert result["sample"]["2026-11-01 00:00"]["observed"] == {
        "revenue_cents": 0,
        "event_count": 0,
    }


def test_sha256_is_order_independent_and_content_sensitive():
    digest = hashlib.sha256("\n".join(["a", "b"]).encode()).hexdigest()

    assert reconcile._sha256({"b", "a"}) == digest
    assert reconcile._sha256({"a"}) != reconcile._sha256({"a", "b"})


def test_render_marks_a_clean_report_as_pass():
    report = {
        "mode": "exactly-once",
        "generated_at": "2026-11-09T00:00:00+00:00",
        "expected": {"events": 2, "customers": 2, "total_revenue_cents": 300},
        "observed": {
            "events": 2,
            "distinct_events": 2,
            "customers_with_balance": 2,
            "revenue_windows": 1,
            "late_events": 0,
            "write_attempts": 2,
            "replayed_attempts": 0,
        },
        "checks": [{"name": "events_recorded", "expected": 2, "observed": 2, "pass": True}],
        "latest_snapshot": {"checkpoint_id": 3, "mismatches": 0},
    }

    text = reconcile.render(report)

    assert "RESULT: PASS" in text
    assert "writes: 2 insert attempts for 2 events, 0 replayed" in text
    assert "latest checkpoint 3: 0 balance mismatches" in text


def test_render_fails_when_any_check_fails():
    report = {
        "mode": "late-data",
        "generated_at": "2026-11-09T00:00:00+00:00",
        "expected": {"events": 2, "customers": 2, "total_revenue_cents": 300},
        "observed": {
            "events": 2,
            "distinct_events": 2,
            "customers_with_balance": 2,
            "revenue_windows": 1,
            "late_events": 1,
        },
        "checks": [{"name": "events_recorded", "expected": 2, "observed": 1, "pass": False}],
        "latest_snapshot": {"checkpoint_id": None, "mismatches": 0},
    }

    assert "RESULT: FAIL" in render_safely(report)


def render_safely(report: dict) -> str:
    """Render without the insert-attempt fields that older reports lack."""
    report["observed"]["write_attempts"] = None
    return reconcile.render(report)
