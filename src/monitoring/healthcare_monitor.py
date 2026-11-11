
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]

SOURCE_FILE = BASE_DIR / "data" / "raw" / "patients.csv"
DATABASE_FILE = (
    BASE_DIR / "data" / "monitoring" / "monitoring.db"
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def create_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_audit (
            run_id TEXT PRIMARY KEY,
            pipeline_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL,
            error_message TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS quality_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            recorded_at TEXT NOT NULL
        )
    """)

    conn.commit()


def calculate_quality_metrics(df):
    if "patient_id" not in df.columns:
        raise ValueError("Missing patient_id column")

    patient_ids = df["patient_id"].fillna("").astype(str).str.strip()

    total_records = len(df)
    missing_ids = int(patient_ids.eq("").sum())

    duplicate_ids = int(
        patient_ids[patient_ids.ne("")].duplicated().sum()
    )

    metrics = {
        "total_records": total_records,
        "missing_patient_ids": missing_ids,
        "duplicate_patient_ids": duplicate_ids,
    }

    if "age" in df.columns:
        ages = pd.to_numeric(df["age"], errors="coerce")

        invalid_ages = (
            ages.isna() | (ages < 0) | (ages > 120)
        ).sum()

        metrics["invalid_ages"] = int(invalid_ages)

    return metrics


def save_metrics(conn, run_id, metrics):
    for metric_name, metric_value in metrics.items():
        conn.execute("""
            INSERT INTO quality_metrics (
                run_id,
                metric_name,
                metric_value,
                recorded_at
            )
            VALUES (?, ?, ?, ?)
        """, (
            run_id,
            metric_name,
            float(metric_value),
            utc_now()
        ))

    conn.commit()


def run_monitoring(
    source_file=SOURCE_FILE,
    database_file=DATABASE_FILE
):
    source_file = Path(source_file)
    database_file = Path(database_file)

    database_file.parent.mkdir(
        parents=True, exist_ok=True
    )

    run_id = str(uuid.uuid4())

    with sqlite3.connect(database_file) as conn:
        create_tables(conn)

        conn.execute("""
            INSERT INTO pipeline_audit (
                run_id,
                pipeline_name,
                start_time,
                status
            )
            VALUES (?, ?, ?, ?)
        """, (
            run_id,
            "healthcare_data_quality",
            utc_now(),
            "RUNNING"
        ))

        conn.commit()

        try:
            if not source_file.exists():
                raise FileNotFoundError(source_file)

            df = pd.read_csv(
                source_file,
                dtype=str,
                keep_default_na=False
            )

            metrics = calculate_quality_metrics(df)
            save_metrics(conn, run_id, metrics)

            failures = [
                name
                for name, value in metrics.items()
                if name != "total_records" and value > 0
            ]

            if metrics["total_records"] == 0:
                failures.append("empty_dataset")

            status = "FAILED" if failures else "SUCCESS"

            conn.execute("""
                UPDATE pipeline_audit
                SET end_time = ?,
                    status = ?,
                    error_message = ?
                WHERE run_id = ?
            """, (
                utc_now(),
                status,
                ", ".join(failures) if failures else None,
                run_id
            ))

            conn.commit()

            print("Healthcare Data Quality Results")
            print("--------------------------------")

            for name, value in metrics.items():
                print(f"{name}: {value}")

            print(f"Pipeline status: {status}")
            print(f"Run ID: {run_id}")

            if failures:
                raise SystemExit(1)

            return metrics

        except Exception as error:
            conn.execute("""
                UPDATE pipeline_audit
                SET end_time = ?,
                    status = 'FAILED',
                    error_message = ?
                WHERE run_id = ?
            """, (
                utc_now(),
                str(error),
                run_id
            ))

            conn.commit()
            raise


if __name__ == "__main__":
    run_monitoring()
