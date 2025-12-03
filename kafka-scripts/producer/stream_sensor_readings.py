#!/usr/bin/env python3
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
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


def generate_reading(atd_device_id: str, extra: Dict[str, float] | None = None) -> Dict:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "record_id": f"{atd_device_id}-{int(datetime.now(timezone.utc).timestamp())}",
        "atd_device_id": atd_device_id,
        "intersection_name": (extra or {}).get("intersection_name", "Demo-Intersection"),
        "direction": (extra or {}).get("direction", random.choice(["N","S","E","W"])),
        "movement": (extra or {}).get("movement", random.choice(["through","left","right"])),
        "heavy_vehicle": (extra or {}).get("heavy_vehicle", random.randint(0, 5)),
        "volume": random.randint(0, 30),
        "speed_average": round(random.uniform(20, 80), 1),
        "speed_stddev": round(random.uniform(1, 10), 1),
        "seconds_in_zone_average": round(random.uniform(0.5, 5.0), 2),
        "seconds_in_zone_stddev": round(random.uniform(0.1, 1.0), 2),
        "event_ts_iso": now_iso,
    }
    return payload


def parse_args():
    ap = argparse.ArgumentParser(description="Stream synthetic sensor readings to Kafka periodically")
    ap.add_argument("--topic", default="traffic_raw_sensor", help="Kafka topic name")
    ap.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        help="Kafka bootstrap servers (comma-separated)",
    )
    ap.add_argument("--device-id", default="atd-1001", help="ATD device ID to use as the Kafka key")
    ap.add_argument("--interval", type=float, default=5.0, help="Seconds between messages")
    ap.add_argument("--jitter", type=float, default=0.5, help="Random jitter added/subtracted to interval")
    ap.add_argument("--count", type=int, default=0, help="Number of messages to send (0 = infinite)")
    ap.add_argument("--debug", action="store_true", help="Print each produced message to stdout")
    ap.add_argument(
        "--extra",
        default=None,
        help="Optional JSON string of extra fields to include in each reading",
    )
    return ap.parse_args()


def main():
    args = parse_args()

    # Validate extra fields if provided
    extra_fields: Dict | None = None
    if args.extra:
        try:
            extra_fields = json.loads(args.extra)
            if not isinstance(extra_fields, dict):
                raise ValueError("--extra must be a JSON object")
        except Exception as e:
            raise SystemExit(f"Invalid --extra JSON: {e}")

    producer = build_producer(args.bootstrap)
    print(
        f"Streaming ATD-shaped readings to '{args.topic}' at {args.bootstrap} every ~{args.interval}s (count={args.count or '∞'})"
    )

    sent = 0
    try:
        while True:
            reading = generate_reading(args.device_id, extra=extra_fields)
            producer.send(args.topic, key=args.device_id, value=reading)
            if args.debug:
                try:
                    print(json.dumps({"key": args.device_id, "value": reading}))
                except Exception:
                    print({"key": args.device_id, "value": reading})
            sent += 1

            # Stop if count reached (when count > 0)
            if args.count and sent >= args.count:
                break

            # Sleep with optional jitter
            sleep_s = max(0.0, args.interval + random.uniform(-args.jitter, args.jitter))
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        print("Interrupted by user; flushing pending messages...")
    finally:
        producer.flush()
        print(f"Sent {sent} messages to {args.topic}")


if __name__ == "__main__":
    main()
