import subprocess
import sys

TOPICS = [
    {"name": "traffic_raw", "partitions": 1, "replication": 1},
    {"name": "traffic_metrics", "partitions": 1, "replication": 1},
]

BOOTSTRAP = "localhost:9092"

def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def detect_topics_bin() -> str:
    # Try Bitnami path first
    check = run(["docker", "exec", "kafka", "bash", "-lc", "test -x /opt/bitnami/kafka/bin/kafka-topics.sh"])
    if check.returncode == 0:
        return "/opt/bitnami/kafka/bin/kafka-topics.sh"
    # Try Confluent path
    check = run(["docker", "exec", "kafka", "bash", "-lc", "command -v kafka-topics || test -x /usr/bin/kafka-topics"])
    if check.returncode == 0:
        # Prefer plain binary name if present in PATH
        which = run(["docker", "exec", "kafka", "bash", "-lc", "command -v kafka-topics || echo /usr/bin/kafka-topics"])
        return which.stdout.strip() or "/usr/bin/kafka-topics"
    raise RuntimeError("Could not find kafka-topics in 'kafka' container.")

def create_topic(bin_path: str, name: str, partitions: int, replication: int) -> None:
    cmd = [
        "docker", "exec", "-i", "kafka", bin_path,
        "--bootstrap-server", BOOTSTRAP,
        "--create",
        "--topic", name,
        "--partitions", str(partitions),
        "--replication-factor", str(replication),
    ]
    res = run(cmd)
    print(res.stdout)

def list_topics(bin_path: str) -> None:
    cmd = [
        "docker", "exec", "-i", "kafka", bin_path,
        "--bootstrap-server", BOOTSTRAP,
        "--list",
    ]
    res = run(cmd)
    print(res.stdout)

def main() -> int:
    try:
        bin_path = detect_topics_bin()
    except Exception as e:
        print(f"Error: {e}")
        return 1
    for t in TOPICS:
        print(f"Creating topic: {t['name']}")
        create_topic(bin_path, t["name"], t["partitions"], t["replication"])
    print("Listing topics:")
    list_topics(bin_path)
    return 0

if __name__ == "__main__":
    sys.exit(main())
