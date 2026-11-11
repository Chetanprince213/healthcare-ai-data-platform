
import importlib.util
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

SCRIPT = (
    BASE_DIR / "src" / "scd" / "patient_scd2.py"
)

spec = importlib.util.spec_from_file_location(
    "patient_scd2",
    SCRIPT
)

scd2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scd2)


def test_scd_type_2_history():
    conn = sqlite3.connect(":memory:")

    try:
        scd2.create_table(conn)

        first_snapshot = pd.DataFrame([
            {
                "patient_id": "P001",
                "city": "Bengaluru"
            }
        ])

        second_snapshot = pd.DataFrame([
            {
                "patient_id": "P001",
                "city": "Mysuru"
            }
        ])

        result1 = scd2.process_scd2(
            conn,
            first_snapshot,
            "2026-01-01T00:00:00+00:00"
        )

        result2 = scd2.process_scd2(
            conn,
            second_snapshot,
            "2026-02-01T00:00:00+00:00"
        )

        assert result1 == (1, 0, 0)
        assert result2 == (0, 1, 0)

        rows = conn.execute("""
            SELECT
                attributes_json,
                effective_from,
                effective_to,
                is_current,
                version
            FROM dim_patient
            WHERE patient_id = 'P001'
            ORDER BY version
        """).fetchall()

        assert len(rows) == 2

        assert '"Bengaluru"' in rows[0][0]
        assert rows[0][2] == (
            "2026-02-01T00:00:00+00:00"
        )
        assert rows[0][3] == 0
        assert rows[0][4] == 1

        assert '"Mysuru"' in rows[1][0]
        assert rows[1][2] is None
        assert rows[1][3] == 1
        assert rows[1][4] == 2

        # Rerunning unchanged data must not add versions
        result3 = scd2.process_scd2(
            conn,
            second_snapshot,
            "2026-03-01T00:00:00+00:00"
        )

        assert result3 == (0, 0, 1)

        total = conn.execute("""
            SELECT COUNT(*) FROM dim_patient
        """).fetchone()[0]

        assert total == 2

    finally:
        conn.close()
