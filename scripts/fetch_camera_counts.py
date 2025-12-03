import os
import sys
import csv
from datetime import datetime
from typing import List, Dict

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    from sodapy import Socrata
except ImportError:
    Socrata = None

DATASET_ID = "sh59-i6y9"  # Camera Traffic Counts
DEFAULT_ROW_TARGET = 5000
OUTPUT_PATH = os.path.join("data", "dataset", "Camera_Traffic_Counts_latest.csv")


def ensure_dependencies():
    missing = []
    if pd is None:
        missing.append("pandas")
    if Socrata is None:
        missing.append("sodapy")
    if missing:
        print(
            "Missing packages: {}. Install with: pip install {}".format(
                ", ".join(missing), " ".join(missing)
            )
        )
        sys.exit(1)


def get_client() -> Socrata:
    app_token = os.environ.get("SOCRATA_APP_TOKEN")
    # Public dataset, token recommended but optional
    client = Socrata("data.austintexas.gov", app_token)
    return client


def fetch_latest_counts(target_rows: int = DEFAULT_ROW_TARGET) -> List[Dict]:
    client = get_client()

    # Order by the dataset's timestamp to get latest first. Inspect fields:
    # Common fields include: count_time, camera_id, direction, count
    # We'll order by count_time descending and page until we reach target_rows.
    rows: List[Dict] = []
    limit = 1000
    offset = 0

    while len(rows) < target_rows:
        batch = client.get(
            DATASET_ID,
            limit=limit,
            offset=offset,
            order="read_date DESC"
        )
        if not batch:
            break
        rows.extend(batch)
        offset += limit

    return rows[:target_rows]


def write_csv(records: List[Dict], output_path: str = OUTPUT_PATH):
    if not records:
        print("No records to write.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Normalize columns
    # Collect all keys to ensure consistent columns
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"Wrote {len(records)} rows to {output_path}")



def to_dataframe(records: List[Dict]):
    if pd is None:
        return None
    df = pd.DataFrame(records)
    # Try to parse timestamps if present
    for col in ["count_time", "created_at", "updated_at"]:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
    return df


def main():
    ensure_dependencies()

    target = DEFAULT_ROW_TARGET
    # Allow override via CLI arg
    if len(sys.argv) > 1:
        try:
            target = int(sys.argv[1])
        except ValueError:
            print("First argument must be an integer for target rows.")
            sys.exit(2)

    records = fetch_latest_counts(target)
    if not records:
        print("No records downloaded from Socrata API.")
        sys.exit(3)

    write_csv(records, OUTPUT_PATH)

    df = to_dataframe(records)
    if df is not None:
        # Show a small preview
        print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
