from __future__ import annotations

from stack import cluster, kafka


def test_current_input_watermark_uses_the_max_input_channel():
    entries = [
        {"id": "0.currentInputWatermark", "value": "1000"},
        {"id": "0.currentOutputWatermark", "value": "999999"},
        {"id": "1.currentInputWatermark", "value": "2000"},
    ]

    assert cluster.current_input_watermark(entries) == 2000


def test_current_input_watermark_tolerates_an_empty_payload():
    assert cluster.current_input_watermark([]) is None
    assert cluster.current_input_watermark([{"id": "0.currentInputWatermark", "value": ""}]) is None


def test_current_input_watermark_preserves_long_min_value():
    # Flink reports Long.MIN_VALUE before the first watermark. It must not be
    # mistaken for "no watermark" or for a small number.
    entries = [{"id": "0.currentInputWatermark", "value": str(-(2**63))}]

    assert cluster.current_input_watermark(entries) == -(2**63)


def test_flush_watermark_target_is_just_below_the_marker():
    # The job subtracts its 10 s out-of-orderness, so the target must leave
    # room below the marker but stay far above any real event.
    assert kafka.FLUSH_WATERMARK_TARGET < kafka.FLUSH_TS_MILLIS
    assert kafka.FLUSH_TS_MILLIS - kafka.FLUSH_WATERMARK_TARGET <= 60_000


def test_flush_watermark_target_is_far_beyond_generated_event_time():
    newest_generated = 253_402_300_799_000 - 60_000 - 14 * 24 * 3600 * 1000

    assert newest_generated < kafka.FLUSH_WATERMARK_TARGET
