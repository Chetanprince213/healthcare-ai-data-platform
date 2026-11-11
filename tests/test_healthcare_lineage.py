
import importlib.util
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

SCRIPT = (
    BASE_DIR / "src" / "lineage" / "healthcare_lineage.py"
)

spec = importlib.util.spec_from_file_location(
    "healthcare_lineage",
    SCRIPT
)

lineage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lineage)


def test_dataset_lineage_registration():
    with sqlite3.connect(":memory:") as conn:
        lineage.create_tables(conn)
        lineage.register_pipeline(conn)

        count = conn.execute("""
            SELECT COUNT(*) FROM datasets
        """).fetchone()[0]

        assert count == 4

        upstream = lineage.get_upstream_datasets(
            conn,
            "gold_analytics"
        )

        assert set(upstream) == {
            "raw_patients",
            "bronze_patients",
            "silver_patients"
        }


def test_lineage_registration_is_idempotent():
    with sqlite3.connect(":memory:") as conn:
        lineage.create_tables(conn)

        lineage.register_pipeline(conn)
        lineage.register_pipeline(conn)

        datasets = conn.execute("""
            SELECT COUNT(*) FROM datasets
        """).fetchone()[0]

        edges = conn.execute("""
            SELECT COUNT(*) FROM lineage_edges
        """).fetchone()[0]

        assert datasets == 4
        assert edges == 3


def test_lineage_run_metadata():
    with sqlite3.connect(":memory:") as conn:
        lineage.create_tables(conn)

        run_id = lineage.record_lineage_run(
            conn,
            "REGISTERED",
            {"source": "test"}
        )

        row = conn.execute("""
            SELECT status, metadata_json
            FROM lineage_runs
            WHERE run_id = ?
        """, (run_id,)).fetchone()

        assert row[0] == "REGISTERED"
        assert '"source": "test"' in row[1]
