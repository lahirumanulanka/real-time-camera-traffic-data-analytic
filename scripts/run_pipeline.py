#!/usr/bin/env python3
import os
import sys
import time
import subprocess
import threading
from pathlib import Path


def stream_output(proc: subprocess.Popen, label: str):
    try:
        for line in iter(proc.stdout.readline, b""):
            if not line:
                break
            try:
                text = line.decode("utf-8", errors="replace").rstrip()
            except Exception:
                text = str(line).rstrip()
            print(f"[{label}] {text}")
    except Exception as e:
        print(f"[{label}] <stream error> {e}")


def main():
    repo_root = Path(__file__).resolve().parents[1]
    python_path = repo_root / ".venv" / "Scripts" / "python.exe"
    if not python_path.exists():
        print("Python from venv not found. Please create/activate .venv first.")
        sys.exit(1)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

    # Fresh consumer group for processor
    from uuid import uuid4
    group_id = f"traffic-metrics-processor-atd-{uuid4()}"

    cmds = {
        "processor": [
            str(python_path),
            str(repo_root / "kafka-scripts" / "processor" / "realtime_metrics.py"),
            "--bootstrap", bootstrap,
            "--input-topic", "traffic_raw_sensor",
            "--output-topic", "traffic_metrics",
            "--emit-interval", "5",
            "--offset-reset", "earliest",
            "--group-id", group_id,
            "--log-to-console",
            "--log-input",
        ],
        "consumer_raw": [
            str(python_path),
            str(repo_root / "kafka-scripts" / "consumer" / "consume_raw_sensor.py"),
            "--bootstrap", bootstrap,
            "--topic", "traffic_raw_sensor",
            "--offset-reset", "earliest",
            "--group-id", f"raw-consumer-{uuid4()}",
            "--pretty",
        ],
        "consumer_metrics": [
            str(python_path),
            str(repo_root / "kafka-scripts" / "consumer" / "consume_metrics.py"),
            "--bootstrap", bootstrap,
            "--topic", "traffic_metrics",
            "--offset-reset", "earliest",
            "--group-id", f"metrics-consumer-{uuid4()}",
            "--pretty",
        ],
        "producer": [
            str(python_path),
            str(repo_root / "kafka-scripts" / "producer" / "stream_sensor_readings.py"),
            "--bootstrap", bootstrap,
            "--topic", "traffic_raw_sensor",
            "--interval", "1",
            "--count", "10",
            "--device-id", "atd-7007",
            "--debug",
        ],
    }

    procs: dict[str, subprocess.Popen] = {}
    threads: list[threading.Thread] = []

    try:
        # Start processor and consumers first
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        for name in ["processor", "consumer_raw", "consumer_metrics"]:
            proc = subprocess.Popen(
                cmds[name],
                cwd=str(repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
            )
            procs[name] = proc
            t = threading.Thread(target=stream_output, args=(proc, name), daemon=True)
            t.start()
            threads.append(t)

        # Give them a moment to subscribe
        time.sleep(2)

        # Start producer
        prod = subprocess.Popen(
            cmds["producer"],
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )
        procs["producer"] = prod
        t = threading.Thread(target=stream_output, args=(prod, "producer"), daemon=True)
        t.start()
        threads.append(t)

        # Wait until producer finishes
        prod.wait(timeout=60)

        # Let processor emit a couple snapshots after production
        time.sleep(10)

    except Exception as e:
        print(f"[runner] Error: {e}")
    finally:
        # Terminate long-running processes (processor and consumers)
        for name in ["processor", "consumer_raw", "consumer_metrics"]:
            proc = procs.get(name)
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=10)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        print("[runner] Completed. Processes stopped.")


if __name__ == "__main__":
    main()
