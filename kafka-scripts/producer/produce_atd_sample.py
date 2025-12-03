#!/usr/bin/env python3
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

from kafka import KafkaProducer


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=32768,
    )


def make_event(sensor: str, ts: datetime, volume: int, **kwargs) -> Dict:
    payload = {
        "record_id": f"{sensor}-{int(ts.timestamp())}",
        "atd_device_id": sensor,
        "intersection_name": kwargs.get("intersection_name", "Demo-Intersection"),
        "direction": kwargs.get("direction", "N"),
        "movement": kwargs.get("movement", "through"),
        "heavy_vehicle": kwargs.get("heavy_vehicle", 0),
        "volume": volume,
        "speed_average": kwargs.get("speed_average", 45.0),
        "speed_stddev": kwargs.get("speed_stddev", 5.0),
        "seconds_in_zone_average": kwargs.get("seconds_in_zone_average", 2.0),
        "seconds_in_zone_stddev": kwargs.get("seconds_in_zone_stddev", 0.3),
        "event_ts_iso": ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    return payload


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
    topic = os.environ.get("ATD_TOPIC", "traffic_raw_sensor")

    producer = build_producer(bootstrap)

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    # Generate samples across current hour and next hour for two sensors
    samples = []
    for i in range(0, 12):  # 12 samples 5 minutes apart
        ts = now + timedelta(minutes=5 * i)
        samples.append(make_event("atd-1001", ts, volume=10 + (i % 5)))
        samples.append(make_event("atd-1002", ts, volume=5 + (i % 3)))

    for ev in samples:
        key = ev["atd_device_id"]
        producer.send(topic, key=key, value=ev)
        time.sleep(0.05)

    producer.flush()
    print(f"Sent {len(samples)} ATD sample records to {topic} @ {bootstrap}")


if __name__ == "__main__":
    main()
