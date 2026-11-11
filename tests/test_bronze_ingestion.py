
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"


def read_bronze(dataset):
    files = list(
        (BRONZE_DIR / dataset).glob("*.jsonl")
    )

    assert files, f"No Bronze files found for {dataset}"

    records = []

    for filepath in files:
        with filepath.open(encoding="utf-8") as file:
            records.extend(
                json.loads(line)
                for line in file
                if line.strip()
            )

    return records


def test_patient_bronze_records():
    records = read_bronze("patients")

    assert len(records) == 1000

    for record in records:
        assert record["patient_id"]
        assert record["_source_file"] == "patients.csv"
        assert record["_source_sha256"]
        assert record["_ingested_at"]


def test_appointment_bronze_records():
    records = read_bronze("appointments")

    assert len(records) == 3000

    for record in records:
        assert record["appointment_id"]
        assert record["patient_id"]
        assert record["_source_file"] == "appointments.csv"
        assert record["_ingested_at"]
