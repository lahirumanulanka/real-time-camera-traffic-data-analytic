# Real-Time Camera Traffic Data Analytic — Containers

This repo includes a Docker Compose stack to run Kafka (+ Zookeeper), Kafka UI, PostgreSQL, Grafana, and a placeholder Python app container for your scripts.

## Prerequisites
- Docker Desktop installed and running
- Windows PowerShell (commands below use PowerShell)

## Start (isolated from other projects)
Use a unique compose project name to avoid collisions with other stacks:

```powershell
cd "c:\Users\ASUS\Documents\real-time-camera-traffic-data-analytic"
# choose your own project name if you prefer
$PROJECT="traffic-analytics";
docker compose -p $PROJECT up -d
```

## Stop and Remove
```powershell
$PROJECT="traffic-analytics";
docker compose -p $PROJECT down
```

## Check Status and Logs
```powershell
$PROJECT="traffic-analytics";
docker compose -p $PROJECT ps

docker compose -p $PROJECT logs kafka -f
```

## Services
- Zookeeper: `localhost:2181`
- Kafka (internal): `kafka:9092` (for containers)
- Kafka (host): `localhost:29092` (for local clients)
- Kafka UI: http://localhost:8080
- PostgreSQL: `localhost:5432` (db: `trafficdb`, user: `traffic`, pass: `traffic`)
- Grafana: http://localhost:3000 (admin/admin)
 - Kafka Connect: http://localhost:8083 (REST API)

## Project Scripts
- Mounts `./kafka-scripts` and `./data` into the `app` container at `/app/...`.
- For now, the `app` container idles (`sleep infinity`). Replace the command to run your orchestrator (`run_all.py`) when it exists.

Example (when ready):
```yaml
  app:
    image: python:3.11-slim
    working_dir: /app
    volumes:
      - ./kafka-scripts:/app/kafka-scripts
      - ./data:/app/data
    command: ["python", "kafka-scripts/run_all.py"]
```

## Notes
- Kafka topics can be auto-created; you can also manage them via Kafka UI.
- Grafana will look for provisioning in `grafana/provisioning`. Add `datasource.yml` and `dashboards.yml` when ready.
- This stack is isolated by the compose project name (e.g., `traffic-analytics`) so it won't collide with other projects using Kafka/Postgres/Grafana on your machine.

## Kafka Connect
### Build & Start with Connect
```powershell
cd "c:\Users\ASUS\Documents\real-time-camera-traffic-data-analytic"
$PROJECT="traffic-analytics";
docker compose -p $PROJECT up -d --build kafka-connect
```

### List Connectors (empty initially)
```powershell
Invoke-RestMethod -Uri http://localhost:8083/connectors
```

### Register FileStream Source (reads JSON lines file)
```powershell
Invoke-RestMethod -Method Post -ContentType 'application/json' -InFile .\kafka-connect\connectors\filestream_raw_source.json -Uri http://localhost:8083/connectors
```

### Register Postgres Sink (aggregated topic to DB)
```powershell
Invoke-RestMethod -Method Post -ContentType 'application/json' -InFile .\kafka-connect\connectors\postgres_sink_traffic_processed.json -Uri http://localhost:8083/connectors
```

### Check Connector Status
```powershell
Invoke-RestMethod -Uri http://localhost:8083/connectors/filestream_raw_source/status
Invoke-RestMethod -Uri http://localhost:8083/connectors/postgres_sink_traffic_processed/status
```

### Remove a Connector
```powershell
Invoke-RestMethod -Method Delete -Uri http://localhost:8083/connectors/filestream_raw_source
```

### Important
- Ensure `/data/dataset/traffic_counts_kafka.json` exists and contains one JSON object per line for the FileStream connector.
- Messages sent to `traffic_processed_aggregate` should use a key with `sensor_id` to allow the sink upsert to work (pk mode record_key).
- Adjust retention or partition counts using `kafka-topics` as needed.

## Data Ingestion

### Fetch latest Camera Traffic Counts (>=5,000 rows)

This project can pull the latest records from Austin's Camera Traffic Counts open dataset via the Socrata API and save a CSV locally.

Steps (Windows PowerShell):

1. Ensure your Python environment is active. If you use the repo's virtual environment:

```powershell
# Activate venv (adjust path if needed)
& .\.venv\Scripts\Activate.ps1
```

2. Install required packages:

```powershell
pip install sodapy pandas
```

3. (Optional but recommended) Set a Socrata app token to improve reliability:

```powershell
$env:SOCRATA_APP_TOKEN = "your-app-token-here"
```

Create a token at https://data.austintexas.gov/profile/edit/developer_settings

4. Run the fetch script to download at least 5,000 latest rows:

```powershell
python .\scripts\fetch_camera_counts.py 5000
```

This writes `data/dataset/Camera_Traffic_Counts_latest.csv` and prints a small preview.

Notes:
- You can change the target row count by passing a different number (e.g., `python .\scripts\fetch_camera_counts.py 10000`).
- The script pages results ordered by `count_time` descending to prioritize the latest records.
