
import importlib.util
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPT = (
    BASE_DIR / "src" / "monitoring" / "healthcare_monitor.py"
)

spec = importlib.util.spec_from_file_location(
    "healthcare_monitor", SCRIPT
)

monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


def test_valid_patient_data():
    df = pd.DataFrame([
        {"patient_id": "P001", "age": "30"},
        {"patient_id": "P002", "age": "45"},
    ])

    metrics = monitor.calculate_quality_metrics(df)

    assert metrics["total_records"] == 2
    assert metrics["missing_patient_ids"] == 0
    assert metrics["duplicate_patient_ids"] == 0
    assert metrics["invalid_ages"] == 0


def test_invalid_patient_data():
    df = pd.DataFrame([
        {"patient_id": "P001", "age": "30"},
        {"patient_id": "P001", "age": "-5"},
        {"patient_id": "", "age": "150"},
    ])

    metrics = monitor.calculate_quality_metrics(df)

    assert metrics["total_records"] == 3
    assert metrics["missing_patient_ids"] == 1
    assert metrics["duplicate_patient_ids"] == 1
    assert metrics["invalid_ages"] == 2


def test_audit_logging(tmp_path):
    source_file = tmp_path / "patients.csv"
    database_file = tmp_path / "monitoring.db"

    pd.DataFrame([
        {"patient_id": "P001", "age": "30"},
        {"patient_id": "P002", "age": "45"},
    ]).to_csv(source_file, index=False)

    monitor.run_monitoring(
        source_file=source_file,
        database_file=database_file
    )

    with sqlite3.connect(database_file) as conn:
        audit = conn.execute("""
            SELECT status FROM pipeline_audit
        """).fetchone()

        metric_count = conn.execute("""
            SELECT COUNT(*) FROM quality_metrics
        """).fetchone()[0]

    assert audit[0] == "SUCCESS"
    assert metric_count == 4
