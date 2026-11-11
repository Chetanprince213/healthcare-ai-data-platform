
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]

SOURCE_FILE = BASE_DIR / "data" / "raw" / "patients.csv"
DATABASE_FILE = BASE_DIR / "data" / "warehouse" / "healthcare.db"

DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)


def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_patient (
            patient_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            effective_from TEXT NOT NULL,
            effective_to TEXT,
            is_current INTEGER NOT NULL,
            version INTEGER NOT NULL
        )
    """)

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
        idx_dim_patient_current
        ON dim_patient(patient_id)
        WHERE is_current = 1
    """)


def load_source():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Missing source file: {SOURCE_FILE}"
        )

    df = pd.read_csv(
        SOURCE_FILE,
        dtype=str,
        keep_default_na=False
    )

    if "patient_id" not in df.columns:
        raise ValueError("Missing patient_id column")

    if df["patient_id"].eq("").any():
        raise ValueError("Empty patient_id detected")

    if df["patient_id"].duplicated().any():
        raise ValueError("Duplicate patient_id detected")

    return df


def process_scd2(conn, source_df, batch_time=None):
    import json

    if batch_time is None:
        batch_time = datetime.now(
            timezone.utc
        ).isoformat()

    inserts = 0
    updates = 0
    unchanged = 0

    with conn:
        for _, row in source_df.iterrows():
            patient_id = row["patient_id"]

            attributes = {
                key: value
                for key, value in row.to_dict().items()
                if key != "patient_id"
            }

            attributes_json = json.dumps(
                attributes,
                sort_keys=True
            )

            existing = conn.execute("""
                SELECT patient_sk, attributes_json, version
                FROM dim_patient
                WHERE patient_id = ?
                  AND is_current = 1
            """, (patient_id,)).fetchone()

            if existing is None:
                conn.execute("""
                    INSERT INTO dim_patient (
                        patient_id,
                        attributes_json,
                        effective_from,
                        effective_to,
                        is_current,
                        version
                    )
                    VALUES (?, ?, ?, NULL, 1, 1)
                """, (
                    patient_id,
                    attributes_json,
                    batch_time
                ))

                inserts += 1

            elif existing[1] != attributes_json:
                conn.execute("""
                    UPDATE dim_patient
                    SET effective_to = ?,
                        is_current = 0
                    WHERE patient_sk = ?
                """, (
                    batch_time,
                    existing[0]
                ))

                conn.execute("""
                    INSERT INTO dim_patient (
                        patient_id,
                        attributes_json,
                        effective_from,
                        effective_to,
                        is_current,
                        version
                    )
                    VALUES (?, ?, ?, NULL, 1, ?)
                """, (
                    patient_id,
                    attributes_json,
                    batch_time,
                    existing[2] + 1
                ))

                updates += 1

            else:
                unchanged += 1

    return inserts, updates, unchanged


def main():
    source_df = load_source()

    with sqlite3.connect(DATABASE_FILE) as conn:
        create_table(conn)

        inserts, updates, unchanged = process_scd2(
            conn,
            source_df
        )

        total_versions = conn.execute("""
            SELECT COUNT(*) FROM dim_patient
        """).fetchone()[0]

        current_records = conn.execute("""
            SELECT COUNT(*)
            FROM dim_patient
            WHERE is_current = 1
        """).fetchone()[0]

    print("SCD Type 2 processing completed!")
    print(f"INSERT: {inserts}")
    print(f"UPDATE: {updates}")
    print(f"UNCHANGED: {unchanged}")
    print(f"Total historical versions: {total_versions}")
    print(f"Current patient records: {current_records}")


if __name__ == "__main__":
    main()
