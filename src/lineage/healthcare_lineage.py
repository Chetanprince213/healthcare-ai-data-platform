
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

DATABASE_FILE = (
    BASE_DIR / "data" / "lineage" / "lineage.db"
)

PIPELINE = [
    {
        "name": "raw_patients",
        "layer": "RAW",
        "location": "data/raw/patients.csv",
        "upstream": None,
        "transformation": "Synthetic data generation",
    },
    {
        "name": "bronze_patients",
        "layer": "BRONZE",
        "location": "data/bronze",
        "upstream": "raw_patients",
        "transformation": "Raw data ingestion",
    },
    {
        "name": "silver_patients",
        "layer": "SILVER",
        "location": "data/silver",
        "upstream": "bronze_patients",
        "transformation": "Cleaning and standardization",
    },
    {
        "name": "gold_analytics",
        "layer": "GOLD",
        "location": "data/gold",
        "upstream": "silver_patients",
        "transformation": "Healthcare analytics aggregation",
    },
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def create_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_name TEXT PRIMARY KEY,
            layer TEXT NOT NULL,
            location TEXT NOT NULL,
            transformation TEXT NOT NULL,
            registered_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS lineage_edges (
            upstream_dataset TEXT NOT NULL,
            downstream_dataset TEXT NOT NULL,
            PRIMARY KEY (
                upstream_dataset,
                downstream_dataset
            )
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS lineage_runs (
            run_id TEXT PRIMARY KEY,
            recorded_at TEXT NOT NULL,
            status TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        )
    """)

    conn.commit()


def register_pipeline(conn, pipeline=PIPELINE):
    for dataset in pipeline:
        conn.execute("""
            INSERT INTO datasets (
                dataset_name,
                layer,
                location,
                transformation,
                registered_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dataset_name)
            DO UPDATE SET
                layer = excluded.layer,
                location = excluded.location,
                transformation = excluded.transformation
        """, (
            dataset["name"],
            dataset["layer"],
            dataset["location"],
            dataset["transformation"],
            utc_now()
        ))

        upstream = dataset["upstream"]

        if upstream:
            conn.execute("""
                INSERT OR IGNORE INTO lineage_edges (
                    upstream_dataset,
                    downstream_dataset
                )
                VALUES (?, ?)
            """, (
                upstream,
                dataset["name"]
            ))

    conn.commit()


def record_lineage_run(conn, status, metadata):
    run_id = str(uuid.uuid4())

    conn.execute("""
        INSERT INTO lineage_runs (
            run_id,
            recorded_at,
            status,
            metadata_json
        )
        VALUES (?, ?, ?, ?)
    """, (
        run_id,
        utc_now(),
        status,
        json.dumps(metadata, sort_keys=True)
    ))

    conn.commit()
    return run_id


def get_upstream_datasets(conn, dataset_name):
    rows = conn.execute("""
        WITH RECURSIVE upstream_tree(dataset_name) AS (
            SELECT upstream_dataset
            FROM lineage_edges
            WHERE downstream_dataset = ?

            UNION

            SELECT e.upstream_dataset
            FROM lineage_edges e
            JOIN upstream_tree t
              ON e.downstream_dataset = t.dataset_name
        )
        SELECT dataset_name
        FROM upstream_tree
        ORDER BY dataset_name
    """, (dataset_name,)).fetchall()

    return [row[0] for row in rows]


def main():
    DATABASE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with sqlite3.connect(DATABASE_FILE) as conn:
        create_tables(conn)
        register_pipeline(conn)

        run_id = record_lineage_run(
            conn,
            "REGISTERED",
            {
                "pipeline": "healthcare_data_pipeline",
                "dataset_count": len(PIPELINE),
                "note": "Dataset lineage metadata registered"
            }
        )

        upstream = get_upstream_datasets(
            conn,
            "gold_analytics"
        )

        dataset_count = conn.execute("""
            SELECT COUNT(*) FROM datasets
        """).fetchone()[0]

        edge_count = conn.execute("""
            SELECT COUNT(*) FROM lineage_edges
        """).fetchone()[0]

    print("Healthcare Data Lineage Registered!")
    print(f"Datasets: {dataset_count}")
    print(f"Lineage relationships: {edge_count}")
    print(f"Gold upstream datasets: {upstream}")
    print(f"Registration run ID: {run_id}")


if __name__ == "__main__":
    main()
