#!/usr/bin/env python3
import argparse
import csv
import json
import os
import time
from typing import Iterable, Dict

from kafka import KafkaProducer


def iter_csv(path: str) -> Iterable[Dict[str, str]]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def iter_json_lines(path: str) -> Iterable[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=32768,
    )


def main():
    ap = argparse.ArgumentParser(description="Produce sample traffic data to Kafka")
    ap.add_argument("--file", default="data/sample/traffic_sample.csv", help="CSV or JSON lines file relative to working dir")
    ap.add_argument("--topic", default="traffic_raw_sensor", help="Kafka topic name")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"), help="Bootstrap servers")
    ap.add_argument("--sleep", type=float, default=0.2, help="Seconds to sleep between records")
    ap.add_argument("--limit", type=int, default=0, help="Max records to send (0 = all)")
    ap.add_argument("--key-field", default="sensor_id", help="Field used as message key if present")
    args = ap.parse_args()

    path = args.file
    if not os.path.exists(path):
        raise SystemExit(f"Input file not found: {path}")

    if path.endswith(".csv"):
        records = iter_csv(path)
    else:
        records = iter_json_lines(path)

    producer = build_producer(args.bootstrap)
    sent = 0
    for rec in records:
        key = rec.get(args.key_field)
        producer.send(args.topic, key=key, value=rec)
        sent += 1
        if args.limit and sent >= args.limit:
            break
        if args.sleep:
            time.sleep(args.sleep)

    producer.flush()
    print(f"Sent {sent} records to {args.topic} @ {args.bootstrap}")


if __name__ == "__main__":
    main()
