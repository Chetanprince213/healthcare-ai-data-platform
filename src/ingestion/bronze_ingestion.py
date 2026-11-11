
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def ingest_csv(filename):
    source_path = RAW_DIR / filename

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    source_bytes = source_path.read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()

    ingestion_time = datetime.now(
        timezone.utc
    ).isoformat()

    dataset_name = source_path.stem

    output_dir = BRONZE_DIR / dataset_name
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use source hash to make reprocessing idempotent.
    output_path = output_dir / f"{source_hash[:16]}.jsonl"

    if output_path.exists():
        print(f"SKIPPED: {filename} already ingested")
        return output_path

    row_count = 0

    with source_path.open(
        "r",
        newline="",
        encoding="utf-8-sig"
    ) as source_file, output_path.open(
        "w",
        encoding="utf-8"
    ) as output_file:

        reader = csv.DictReader(source_file)

        if not reader.fieldnames:
            raise ValueError(f"Missing CSV header: {filename}")

        for row in reader:
            record = {
                **row,
                "_source_file": filename,
                "_source_sha256": source_hash,
                "_ingested_at": ingestion_time
            }

            output_file.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )
            row_count += 1

    print(
        f"INGESTED: {filename} -> "
        f"{output_path.name} ({row_count} rows)"
    )

    return output_path


if __name__ == "__main__":
    ingest_csv("patients.csv")
    ingest_csv("appointments.csv")
    print("Bronze ingestion completed!")
