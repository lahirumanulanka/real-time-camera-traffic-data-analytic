#!/usr/bin/env python3
import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Tuple

from kafka import KafkaConsumer, KafkaProducer

# Dataset schema (traffic_counts_kafka.jsonl):
#   record_id, atd_device_id, intersection_name, direction, movement,
#   heavy_vehicle, volume, speed_average, speed_stddev,
#   seconds_in_zone_average, seconds_in_zone_stddev, event_ts_iso
# This processor computes hourly average vehicle count per sensor using:
#   event_ts_iso (timestamp), atd_device_id (sensor id), volume (count)


def parse_ts_any(record: Dict) -> datetime:
    ts_val = record.get("event_ts_iso") or ""
        # Strict to new dataset: only event_ts_iso
    try:
        # Expect ISO with timezone or 'Z'
        if isinstance(ts_val, str) and ts_val.endswith("Z"):
            # Convert trailing Z to +00:00 for fromisoformat
            ts_val = ts_val[:-1] + "+00:00"
        if isinstance(ts_val, str) and ts_val:
            return datetime.fromisoformat(ts_val)
    except Exception:
        pass
    # Fallback: assume UTC now
    return datetime.now(timezone.utc)


class MetricsState:
    def __init__(self, expected_interval_s: float = 5.0):
        # Hourly average vehicle count per sensor
        # key: (sensor_id, yyyy-mm-ddTHH) -> sum_count, n
        self.hourly_counts: Dict[Tuple[str, str], Tuple[int, int]] = defaultdict(lambda: (0, 0))

        # Daily peak traffic volume across all sensors
        # key: yyyy-mm-dd -> (peak_value, sensor_id, ts)
        self.daily_peak: Dict[str, Tuple[int, str, str]] = {}

        # Daily sensor availability: based on presence per-minute
        # key: (sensor_id, yyyy-mm-dd) -> set of minute buckets that had data
        self.daily_minutes_present: Dict[Tuple[str, str], set] = defaultdict(set)

        self.expected_interval_s = expected_interval_s
        self._lock = threading.Lock()

    def update(self, record: Dict):
        sensor_id = record.get("atd_device_id")
            # Strict to new dataset: only atd_device_id
        if not sensor_id:
            return
        ts = parse_ts_any(record)
        vc_raw = record.get("volume")
            # Strict to new dataset: only volume
        try:
            vehicle_count = int(vc_raw) if vc_raw is not None else 0
        except Exception:
            vehicle_count = 0

        day_key = ts.date().isoformat()  # yyyy-mm-dd
        hour_key = f"{day_key}T{ts.hour:02d}"  # yyyy-mm-ddTHH
        minute_key = f"{ts.hour:02d}:{ts.minute:02d}"  # HH:MM

        with self._lock:
            # Hourly averages per sensor
            key_h = (sensor_id, hour_key)
            s, n = self.hourly_counts[key_h]
            self.hourly_counts[key_h] = (s + vehicle_count, n + 1)

            # Daily peak across all sensors
            peak = self.daily_peak.get(day_key)
            if peak is None or vehicle_count > peak[0]:
                self.daily_peak[day_key] = (vehicle_count, sensor_id, ts.isoformat())

            # Availability per minute presence
            key_d = (sensor_id, day_key)
            self.daily_minutes_present[key_d].add(minute_key)

    def snapshot(self) -> Dict:
        """Create a metrics snapshot for emission."""
        with self._lock:
            now = datetime.now(timezone.utc)
            day_key = now.date().isoformat()

            # Build hourly averages for all seen hours
            hourly_avg = []
            for (sensor_id, hour_key), (s, n) in self.hourly_counts.items():
                avg = (s / n) if n else 0.0
                hourly_avg.append({
                    "atd_device_id": sensor_id,
                    "hour": hour_key,  # yyyy-mm-ddTHH
                    "avg_vehicle_count": round(avg, 2),
                    "samples": n,
                })

            # Daily peak across all sensors
            peak_val, peak_sensor, peak_ts = 0, None, None
            if day_key in self.daily_peak:
                peak_val, peak_sensor, peak_ts = self.daily_peak[day_key]
            daily_peak = {
                "day": day_key,
                "peak_vehicle_count": peak_val,
                "atd_device_id": peak_sensor,
                "ts": peak_ts,
            }

            # Availability: fraction of minutes with data so far today
            # Total minutes elapsed today
            minutes_elapsed = now.hour * 60 + now.minute + 1  # include current minute
            availability = []
            for (sensor_id, dkey), minutes_set in self.daily_minutes_present.items():
                if dkey != day_key:
                    continue
                pct = (len(minutes_set) / minutes_elapsed * 100.0) if minutes_elapsed > 0 else 0.0
                availability.append({
                    "atd_device_id": sensor_id,
                    "day": dkey,
                    "availability_pct": round(pct, 2),
                    "minutes_with_data": len(minutes_set),
                    "minutes_elapsed": minutes_elapsed,
                })

            return {
                "generated_at": now.isoformat(),
                "hourly_average_vehicle_count": hourly_avg,
                "daily_peak_traffic_volume": daily_peak,
                "daily_sensor_availability": availability,
            }


def build_consumer(bootstrap: str, topic: str, group_id: str, offset_reset: str = "latest") -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,
    )


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def parse_args():
    ap = argparse.ArgumentParser(description="Consume sensor readings and emit real-time metrics")
    ap.add_argument("--input-topic", default="traffic_raw_sensor", help="Kafka topic to consume from")
    ap.add_argument("--output-topic", default="traffic_metrics", help="Kafka topic to publish metrics")
    ap.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        help="Kafka bootstrap servers (comma-separated)",
    )
    ap.add_argument("--group-id", default="traffic-metrics-processor", help="Kafka consumer group id")
    ap.add_argument("--offset-reset", choices=["earliest", "latest"], default="latest", help="Where to start if no committed offset")
    ap.add_argument("--emit-interval", type=float, default=30.0, help="Seconds between metrics emissions")
    ap.add_argument("--expected-interval", type=float, default=5.0, help="Expected sensor message spacing (seconds)")
    ap.add_argument("--log-to-console", action="store_true", help="Print metrics snapshot to console when emitting")
    ap.add_argument("--log-input", action="store_true", help="Print each consumed input record")
    return ap.parse_args()


def main():
    args = parse_args()

    consumer = build_consumer(args.bootstrap, args.input_topic, args.group_id, args.offset_reset)
    producer = build_producer(args.bootstrap)
    state = MetricsState(expected_interval_s=args.expected_interval)

    stop_event = threading.Event()

    def handle_sigint(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    last_emit = time.time()
    print(
        f"Processing from '{args.input_topic}' and emitting to '{args.output_topic}' every {args.emit_interval}s"
    )

    try:
        while not stop_event.is_set():
            for msg in consumer:
                rec = msg.value
                if args.log_input:
                    try:
                        print(json.dumps({"in_key": msg.key, "in_value": rec}))
                    except Exception:
                        print({"in_key": msg.key, "in_value": rec})
                state.update(rec)

                # Emit periodically
                now = time.time()
                if now - last_emit >= args.emit_interval:
                    snapshot = state.snapshot()
                    producer.send(args.output_topic, key="metrics", value=snapshot)
                    producer.flush()
                    if args.log_to_console:
                        print(json.dumps(snapshot, indent=2))
                    last_emit = now

            # If consumer times out, still consider emitting
            now = time.time()
            if now - last_emit >= args.emit_interval:
                snapshot = state.snapshot()
                producer.send(args.output_topic, key="metrics", value=snapshot)
                producer.flush()
                if args.log_to_console:
                    print(json.dumps(snapshot, indent=2))
                last_emit = now

    except KeyboardInterrupt:
        pass
    finally:
        # Final emit on shutdown
        snapshot = state.snapshot()
        producer.send(args.output_topic, key="metrics", value=snapshot)
        producer.flush()
        if args.log_to_console:
            print(json.dumps(snapshot, indent=2))
        consumer.close()
        print("Processor stopped.")


if __name__ == "__main__":
    sys.exit(main())
