import json

import pytest

from air_quality_pipeline.consumer import process_message
from air_quality_pipeline.events import classify, decode_reading, make_reading
from air_quality_pipeline.storage import connect, save_reading


class Message:
    def __init__(self, reading, *, key=None, offset=4):
        self.reading = reading
        self._key = key if key is not None else reading.sensor_id.encode()
        self._offset = offset

    def value(self):
        return self.reading.to_bytes()

    def key(self):
        return self._key

    def topic(self):
        return "air-quality-readings"

    def partition(self):
        return 1

    def offset(self):
        return self._offset


def test_event_round_trip_and_utc_timestamp():
    reading = make_reading("station-01", 22.5, 650.0)
    assert decode_reading(reading.to_bytes()) == reading
    assert reading.observed_at.endswith("+00:00")


@pytest.mark.parametrize("pm25,co2,expected", [
    (34.9, 999.0, "NORMAL"),
    (35.0, 500.0, "ALERT"),
    (21.0, 1000.0, "ALERT"),
])
def test_alert_boundaries(pm25, co2, expected):
    assert classify(make_reading("station-01", pm25, co2)) == expected


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), 249, 5001])
def test_invalid_co2_rejected(bad_value):
    with pytest.raises(ValueError):
        make_reading("station-01", 20.0, bad_value)


@pytest.mark.parametrize("bad_value", [-1, 1001])
def test_invalid_pm25_rejected(bad_value):
    with pytest.raises(ValueError):
        make_reading("station-01", bad_value, 600)


def test_missing_field_rejected():
    payload = json.loads(make_reading("station-01", 20, 600).to_bytes())
    del payload["sensor_id"]
    with pytest.raises(ValueError):
        decode_reading(json.dumps(payload).encode())


def test_save_is_idempotent_after_replay(tmp_path):
    connection = connect(str(tmp_path / "air_quality.db"))
    reading = make_reading("station-01", 39, 600)
    assert save_reading(connection, reading, "air-quality-readings", 1, 4)
    assert not save_reading(connection, reading, "air-quality-readings", 1, 4)
    assert connection.execute("SELECT COUNT(*), status FROM readings").fetchone() == (1, "ALERT")
    connection.close()


def test_duplicate_event_id_at_new_offset_is_ignored(tmp_path):
    connection = connect(str(tmp_path / "air_quality.db"))
    reading = make_reading("station-01", 20, 600)
    assert save_reading(connection, reading, "air-quality-readings", 1, 4)
    assert not save_reading(connection, reading, "air-quality-readings", 1, 5)
    assert connection.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 1
    connection.close()


def test_process_rejects_key_mismatch_before_storage(tmp_path):
    connection = connect(str(tmp_path / "air_quality.db"))
    reading = make_reading("station-01", 20, 600)
    with pytest.raises(ValueError, match="key does not match"):
        process_message(connection, Message(reading, key=b"station-02"))
    assert connection.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 0
    connection.close()
