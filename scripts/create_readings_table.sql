-- Create table for raw traffic readings used for real-time dashboards
CREATE TABLE IF NOT EXISTS traffic_readings (
  device_id TEXT NOT NULL,
  read_time TIMESTAMPTZ NOT NULL,
  volume NUMERIC NOT NULL,
  PRIMARY KEY (device_id, read_time)
);

-- Index to accelerate time-range queries per device
CREATE INDEX IF NOT EXISTS idx_traffic_readings_time_device
  ON traffic_readings (read_time, device_id);
