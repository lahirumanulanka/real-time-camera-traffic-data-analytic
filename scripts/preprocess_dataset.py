import os
from pathlib import Path
import pandas as pd
import orjson


def preprocess_dataset():
    # Paths
    repo_root = Path(__file__).resolve().parents[1]
    input_csv = repo_root / "data" / "dataset" / "Camera_Traffic_Counts_latest.csv"
    output_json = repo_root / "data" / "dataset" / "traffic_cleaned.json"
    output_csv = repo_root / "data" / "dataset" / "Camera_Traffic_Counts_preprocessed.csv"

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found at {input_csv}")

    # Load
    df = pd.read_csv(input_csv)

    # Initial diagnostics
    print(f"Initial shape: {df.shape}")
    print("Missing values per column:\n", df.isna().sum())

    # Remove rows missing critical fields
    required_cols = ["atd_device_id", "read_date", "volume"]
    df = df.dropna(subset=required_cols)

    # Convert read_date to datetime and drop NaT
    df["read_date"] = pd.to_datetime(df["read_date"], errors="coerce")
    df = df.dropna(subset=["read_date"]).copy()

    # Convert heavy_vehicle to boolean
    if "heavy_vehicle" in df.columns:
        # Handle various truthy/falsey representations
        def to_bool(val):
            if pd.isna(val):
                return False
            if isinstance(val, (int, float)):
                return bool(int(val))
            s = str(val).strip().lower()
            return s in {"1", "true", "t", "yes", "y"}

        df["heavy_vehicle"] = df["heavy_vehicle"].map(to_bool)

    # Sort by read_date
    df = df.sort_values(by="read_date")

    # Rename columns for Kafka-friendly names
    rename_map = {
        "atd_device_id": "device_id",
        "speed_average": "speed_avg",
        "speed_stddev": "speed_stddev",
        "seconds_in_zone_average": "seconds_in_zone_avg",
        "seconds_in_zone_stddev": "seconds_in_zone_stddev",
    }
    df = df.rename(columns=rename_map)

    # Ensure read_date is JSON-serializable (ISO string)
    if "read_date" in df.columns:
        df["read_date"] = df["read_date"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Convert to records (list of dicts)
    records = df.to_dict(orient="records")

    # Save compact JSON via orjson
    output_json.parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "wb") as f:
        f.write(orjson.dumps(records, option=orjson.OPT_SORT_KEYS))

    # Save cleaned CSV
    df.to_csv(output_csv, index=False)

    print(f"Saved JSON: {output_json}")
    print(f"Saved CSV: {output_csv}")
    print(f"Final shape: {df.shape}")


if __name__ == "__main__":
    preprocess_dataset()
