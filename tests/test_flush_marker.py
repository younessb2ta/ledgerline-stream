from __future__ import annotations

import json
from datetime import datetime

from stack import kafka


def test_flush_marker_matches_the_event_contract():
    marker = kafka.flush_marker(0, marker_id="marker-1")

    assert marker["event_id"] == "marker-1"
    assert marker["event_type"] == kafka.CONTROL_EVENT
    assert marker["event_ts"] == kafka.FLUSH_TS
    # JsonToEvent deserializes into the same POJO as real events.
    required = {
        "event_id",
        "event_type",
        "event_ts",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price_cents",
        "amount_cents",
        "currency",
        "schema_version",
    }
    assert set(marker) == required
    assert json.loads(json.dumps(marker)) == marker


def test_flush_timestamp_is_far_beyond_any_generated_event():
    generated = datetime.fromisoformat("2026-11-01T00:00:00+00:00")
    flush = datetime.fromisoformat(kafka.FLUSH_TS.replace("Z", "+00:00"))

    assert flush.year == 9999
    assert flush > generated


def test_marker_ids_are_unique_per_partition():
    first = kafka.flush_marker(0)
    second = kafka.flush_marker(0)
    other = kafka.flush_marker(1)

    assert first["event_id"] != second["event_id"]
    assert first["event_id"].startswith("stream-flush-0-")
    assert other["event_id"].startswith("stream-flush-1-")
