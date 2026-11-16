"""Verify real Delta Lake execution evidence."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

REPORT = (
    ROOT
    / "data"
    / "quality_reports"
    / "delta_lake_validation.json"
)

DELTA_PATH = (
    ROOT
    / "data"
    / "delta_lake"
    / "patient_appointments"
)


def load_report():
    return json.loads(
        REPORT.read_text(encoding="utf-8")
    )


def test_real_delta_execution_succeeded():
    report = load_report()

    assert report["implementation"] == "real_local_delta_lake"
    assert report["passed"] is True


def test_two_versions_written():
    report = load_report()

    assert report["version_zero_records"] == 5
    assert report["version_one_records"] == 6


def test_transaction_history():
    report = load_report()

    versions = {
        entry["version"]
        for entry in report["transaction_history"]
    }

    assert {0, 1}.issubset(versions)


def test_time_travel_verified():
    report = load_report()

    assert report["checks"]["version_zero_time_travel"]
    assert report["checks"]["historical_versions_remain_readable"]


def test_delta_transaction_log_exists():
    assert (DELTA_PATH / "_delta_log").is_dir()

    assert list(
        (DELTA_PATH / "_delta_log").glob("*.json")
    )


def test_no_destructive_vacuum():
    report = load_report()

    assert report["checks"]["no_vacuum_executed"]
