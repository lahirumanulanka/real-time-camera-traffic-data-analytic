# Real‑Time Camera Traffic Analytics (Student Guide)

Simple setup to stream fake traffic sensor data into Kafka, save it in Postgres, and see live charts in Grafana.

## What you need
- Docker Desktop running
- Windows PowerShell (commands below use PowerShell)
- Python 3.11+ (to run producer/consumer locally)
- Git LFS (large files tracked in this repo)

## Quick start
1) Start the stack (Kafka, Zookeeper, Postgres, Grafana):

```powershell
cd "c:\Users\ASUS\Documents\real-time-camera-traffic-data-analytic"
docker compose up -d
```

2) Create the database tables:

```powershell
# Raw readings table
docker exec -i postgres psql -U trafficuser -d trafficdb -f /scripts/create_readings_table.sql

# Metrics tables (hourly avg, daily peak, availability)
python .\kafka-scripts\db\init_db.py
```

3) Install Python packages (first time only):

```powershell
& .\.venv\Scripts\Activate.ps1
pip install -r .\requirements.txt
```

4) Run the consumer (saves data + metrics into Postgres):

```powershell
python .\kafka-scripts\consumer\consumer_metrics.py
```

5) Run the producer (sends new readings every 5 seconds):

```powershell
python .\kafka-scripts\producer\producer_live.py
```

6) Open Grafana:
- URL: http://localhost:3000
- Login: `admin` / `admin`
- Dashboard: “Real‑Time Traffic Metrics” (auto refresh 5s)
- If you see no data, set time range to “Last 15m”.

## Services (from docker‑compose)
- Zookeeper: `localhost:2181`
- Kafka: `localhost:9092`
- Postgres: `localhost:5432` (db `trafficdb`, user `trafficuser`, pass `trafficpass`)
- Grafana: `http://localhost:3000` (admin/admin)

## Useful checks
See if tables exist:
```powershell
docker exec -i postgres psql -U trafficuser -d trafficdb -c "\\dt"
```
Recent raw data in the last 10 minutes:
```powershell
docker exec -i postgres psql -U trafficuser -d trafficdb -c "SELECT COUNT(*) AS last10m FROM traffic_readings WHERE read_time >= NOW() - INTERVAL '10 minutes';"
```

## Troubleshooting (quick)
- Grafana loads but panels are empty:
  - Make sure producer and consumer are running.
  - Set time range to “Last 15m” and click Refresh.
  - Restart Grafana to reload provisioning:
    ```powershell
    docker restart grafana
    ```
- Points show outside time range or chart flickers:
  - Producer now sends timestamps in UTC with a `Z` suffix.
  - Dashboard “live” mode is off and refresh is 5s.
- Kafka connection issues:
  - Kafka runs on `localhost:9092`.
  - Wait ~15–30s after `docker compose up -d` for services to be healthy.

## Git LFS (large files)
This repo tracks big datasets with Git LFS. After clone:
```powershell
git lfs install
git lfs pull
```

## Extras
- The dataset files live under `data/dataset/`.
- You can fetch your own CSV via `scripts/fetch_camera_counts.py` if you want.
