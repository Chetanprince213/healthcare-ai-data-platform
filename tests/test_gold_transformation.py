
import csv
import sqlite3
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = BASE_DIR / "data" / "gold"

SCRIPT = (
    BASE_DIR
    / "src"
    / "transformation"
    / "gold_transformation.py"
)


def test_gold_pipeline_runs():
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        check=True
    )

    assert "Gold transformation completed!" in result.stdout


def test_database_record_counts():
    with sqlite3.connect(
        GOLD_DIR / "healthcare_analytics.db"
    ) as connection:
        patients = connection.execute(
            "SELECT COUNT(*) FROM patients"
        ).fetchone()[0]

        appointments = connection.execute(
            "SELECT COUNT(*) FROM appointments"
        ).fetchone()[0]

    assert patients == 1000
    assert appointments == 3000


def test_department_kpis():
    with (
        GOLD_DIR / "department_kpis.csv"
    ).open(newline="", encoding="utf-8") as file:
        records = list(csv.DictReader(file))

    assert len(records) == 5

    total = sum(
        int(record["total_appointments"])
        for record in records
    )

    assert total == 3000

    for record in records:
        rate = float(record["no_show_rate_pct"])
        assert 0 <= rate <= 100


def test_monthly_totals():
    with (
        GOLD_DIR / "monthly_appointments.csv"
    ).open(newline="", encoding="utf-8") as file:
        records = list(csv.DictReader(file))

    total = sum(
        int(record["total_appointments"])
        for record in records
    )

    assert total == 3000
