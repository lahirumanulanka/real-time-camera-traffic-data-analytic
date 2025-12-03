#!/usr/bin/env python3
import argparse
import json
import os
from kafka import KafkaConsumer


def build_consumer(bootstrap: str, topic: str, group_id: str, offset_reset: str = "latest", consumer_timeout_ms: int | None = None) -> KafkaConsumer:
    params = dict(
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    if consumer_timeout_ms and consumer_timeout_ms > 0:
        params["consumer_timeout_ms"] = consumer_timeout_ms
    return KafkaConsumer(topic, **params)


def parse_args():
    ap = argparse.ArgumentParser(description="Consume metrics snapshots from Kafka")
    ap.add_argument("--topic", default="traffic_metrics", help="Kafka topic to consume from")
    ap.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        help="Kafka bootstrap servers (comma-separated)",
    )
    ap.add_argument("--group-id", default="metrics-consumer", help="Kafka consumer group id")
    ap.add_argument("--offset-reset", choices=["earliest", "latest"], default="latest", help="Where to start if no committed offset")
    ap.add_argument("--limit", type=int, default=0, help="Max number of messages to read (0 = infinite)")
    ap.add_argument("--idle-exit-ms", type=int, default=0, help="Exit after this many ms of idle (0 = never)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    ap.add_argument("--write", default=None, help="Optional file path to append messages as JSON Lines")
    return ap.parse_args()


def main():
    args = parse_args()
    timeout = args.idle_exit_ms if args.idle_exit_ms and args.idle_exit_ms > 0 else None
    consumer = build_consumer(args.bootstrap, args.topic, args.group_id, args.offset_reset, timeout)

    print(f"Consuming metrics from '{args.topic}' at {args.bootstrap} (group={args.group_id}, offset_reset={args.offset_reset})", flush=True)
    out_f = None
    try:
        if args.write:
            out_f = open(args.write, "a", encoding="utf-8")

        count = 0
        for msg in consumer:
            key = msg.key
            value = msg.value
            payload = {"key": key, "value": value}
            if args.pretty:
                print(json.dumps(payload, indent=2), flush=True)
            else:
                print(json.dumps(payload), flush=True)

            if out_f:
                out_f.write(json.dumps(payload) + "\n")

            count += 1
            if args.limit and count >= args.limit:
                break
    finally:
        if out_f:
            out_f.close()
        consumer.close()
        print("Metrics consumer stopped.")


if __name__ == "__main__":
    main()
