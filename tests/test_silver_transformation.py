
import csv
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"
SCRIPT = (
    BASE_DIR
    / "src"
    / "transformation"
    / "silver_transformation.py"
)


def read_csv(filename):
    with (SILVER_DIR / filename).open(
        newline="",
        encoding="utf-8"
    ) as file:
        return list(csv.DictReader(file))


def test_silver_pipeline_runs():
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        check=True
    )

    assert "Silver transformation completed!" in result.stdout


def test_unique_patient_ids():
    patients = read_csv("patients.csv")
    ids = [p["patient_id"] for p in patients]

    assert len(ids) == len(set(ids))


def test_appointment_referential_integrity():
    patients = read_csv("patients.csv")
    appointments = read_csv("appointments.csv")

    patient_ids = {p["patient_id"] for p in patients}

    assert all(
        a["patient_id"] in patient_ids
        for a in appointments
    )


def test_appointment_statuses():
    appointments = read_csv("appointments.csv")

    valid = {"Completed", "Cancelled", "No-Show"}

    assert all(
        a["status"] in valid
        for a in appointments
    )
