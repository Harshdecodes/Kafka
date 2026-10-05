"""Air-quality event format and deterministic demo alert rules."""

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4


@dataclass(frozen=True)
class SensorReading:
    event_id: str
    sensor_id: str
    observed_at: str
    pm25_ug_m3: float
    co2_ppm: float

    def to_bytes(self) -> bytes:
        return json.dumps(self.__dict__, separators=(",", ":"), allow_nan=False).encode("utf-8")


def make_reading(sensor_id: str, pm25_ug_m3: float, co2_ppm: float) -> SensorReading:
    reading = SensorReading(
        event_id=str(uuid4()),
        sensor_id=sensor_id,
        observed_at=datetime.now(timezone.utc).isoformat(),
        pm25_ug_m3=pm25_ug_m3,
        co2_ppm=co2_ppm,
    )
    validate(reading)
    return reading


def decode_reading(payload: bytes) -> SensorReading:
    try:
        obj = json.loads(payload)
        if not isinstance(obj, dict) or set(obj) != set(SensorReading.__dataclass_fields__):
            raise ValueError("event must contain exactly the five documented fields")
        reading = SensorReading(**obj)
        validate(reading)
        return reading
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, KeyError) as exc:
        raise ValueError("invalid sensor event") from exc


def validate(reading: SensorReading) -> None:
    if not isinstance(reading.event_id, str) or not isinstance(reading.observed_at, str):
        raise ValueError("event_id and observed_at must be strings")
    try:
        UUID(reading.event_id)
        timestamp = datetime.fromisoformat(reading.observed_at)
    except (TypeError, ValueError) as exc:
        raise ValueError("event_id or observed_at is invalid") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")
    if not isinstance(reading.sensor_id, str) or not reading.sensor_id.strip():
        raise ValueError("sensor_id must be a nonempty string")
    for name, value in (("pm25_ug_m3", reading.pm25_ug_m3), ("co2_ppm", reading.co2_ppm)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{name} must be a finite number")
    if not 0 <= reading.pm25_ug_m3 <= 1000:
        raise ValueError("pm25_ug_m3 must be between 0 and 1000")
    if not 250 <= reading.co2_ppm <= 5000:
        raise ValueError("co2_ppm must be between 250 and 5000")


def classify(reading: SensorReading) -> str:
    return "ALERT" if reading.pm25_ug_m3 >= 35 or reading.co2_ppm >= 1000 else "NORMAL"
