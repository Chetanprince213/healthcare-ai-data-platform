"""Automated warehouse correctness and reliability tests."""

import csv

import duckdb
import pytest

from src.warehouse.healthcare_warehouse import (
    initialize_warehouse,
    execute_query,
    benchmark_query,
    create_analytics_objects,
    compare_query_plans,
    inspect_warehouse_metadata,
    read_sql
)


@pytest.fixture
def patient_source(tmp_path):

    path = tmp_path / "patients.csv"

    with path.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "patient_id",
            "region",
            "age",
            "prior_visits",
            "no_show"
        ])

        writer.writerows([
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1],
            ["P003", "N", 50, 4, 0]
        ])

    return path


def test_warehouse_initialization(patient_source):

    with duckdb.connect(":memory:") as con:

        result = initialize_warehouse(
            con, patient_source
        )

        assert result["total_records"] == 3
        assert result["unique_patients"] == 3
        assert result["invalid_records"] == 0
        assert result["status"] == "PASSED"


def test_catalog_schema_resolution(
    patient_source, tmp_path
):

    database = tmp_path / "healthcare_analytics.duckdb"

    with duckdb.connect(str(database)) as con:

        initialize_warehouse(
            con, patient_source
        )

        catalog = con.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        count = con.execute(
            "SELECT COUNT(*) FROM healthcare.patients"
        ).fetchone()[0]

        assert catalog == "healthcare_analytics"
        assert count == 3


def test_idempotent_ingestion(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        result = initialize_warehouse(
            con, patient_source
        )

        assert result["total_records"] == 3


def test_regional_aggregation(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        result = execute_query(
            con,
            read_sql(
                "regional_patient_metrics.sql"
            )
        )

        assert sum(
            row["total_patients"]
            for row in result
        ) == 3

        assert result[0]["region"] == "N"
        assert result[0]["patient_volume_rank"] == 1


def test_duplicate_rejection(patient_source):

    with patient_source.open(
        "a", encoding="utf-8"
    ) as file:

        file.write("P001,N,35,2,0\n")

    with duckdb.connect(":memory:") as con:

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )


def test_invalid_age_rejection(patient_source):

    with patient_source.open(
        "a", encoding="utf-8"
    ) as file:

        file.write("P004,N,150,2,0\n")

    with duckdb.connect(":memory:") as con:

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )


def test_invalid_no_show_rejection(patient_source):

    with patient_source.open(
        "a", encoding="utf-8"
    ) as file:

        file.write("P004,N,35,2,5\n")

    with duckdb.connect(":memory:") as con:

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )


def test_negative_visits_rejection(patient_source):

    with patient_source.open(
        "a", encoding="utf-8"
    ) as file:

        file.write("P004,N,35,-1,0\n")

    with duckdb.connect(":memory:") as con:

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )


def test_missing_identifier_rejection(patient_source):

    with patient_source.open(
        "a", encoding="utf-8"
    ) as file:

        file.write(",N,35,2,0\n")

    with duckdb.connect(":memory:") as con:

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )


def test_missing_source(tmp_path):

    with duckdb.connect(":memory:") as con:

        with pytest.raises(FileNotFoundError):

            initialize_warehouse(
                con, tmp_path / "missing.csv"
            )


def test_transaction_rollback(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        with patient_source.open(
            "a", encoding="utf-8"
        ) as file:

            file.write("P001,N,35,2,0\n")

        with pytest.raises(ValueError):

            initialize_warehouse(
                con, patient_source
            )

        count = con.execute(
            "SELECT COUNT(*) FROM healthcare.patients"
        ).fetchone()[0]

        assert count == 3


def test_analytics_view(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        create_analytics_objects(con, 10)

        result = con.execute(
            """
            SELECT age_group
            FROM healthcare.patient_analytics
            WHERE patient_id = 'P001'
            """
        ).fetchone()[0]

        assert result == "30-44"


def test_benchmark_dataset(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        rows = create_analytics_objects(
            con, 10
        )

        assert rows == 30


def test_query_plan_generation(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        create_analytics_objects(con, 10)

        plans = compare_query_plans(con)

        assert plans["full_scan"]
        assert plans["filtered_scan"]


def test_compute_profile_consistency(
    patient_source, tmp_path
):

    database = tmp_path / "benchmark.duckdb"

    with duckdb.connect(str(database)) as con:

        initialize_warehouse(
            con, patient_source
        )

    query = """
        SELECT COUNT(*) AS total
        FROM healthcare.patients
    """

    small = benchmark_query(
        database,
        query,
        {
            "name": "small",
            "threads": 1,
            "memory_limit": "512MB"
        },
        2
    )

    medium = benchmark_query(
        database,
        query,
        {
            "name": "medium",
            "threads": 2,
            "memory_limit": "1GB"
        },
        2
    )

    assert small["result"] == medium["result"]
    assert small["result"] == [{"total": 3}]
    assert small["average_ms"] >= 0
    assert medium["average_ms"] >= 0


def test_metadata_inspection(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        create_analytics_objects(con, 10)

        metadata = inspect_warehouse_metadata(con)

        names = {
            item["table_name"]
            for item in metadata["objects"]
        }

        assert "patients" in names
        assert "patient_analytics" in names
        assert "benchmark_patients" in names


def test_utilization_ranking(patient_source):

    with duckdb.connect(":memory:") as con:

        initialize_warehouse(
            con, patient_source
        )

        result = execute_query(
            con,
            read_sql(
                "patient_utilization_ranking.sql"
            )
        )

        north = [
            row for row in result
            if row["region"] == "N"
        ]

        assert north[0]["patient_id"] == "P003"
        assert north[0]["patient_rank"] == 1
        assert north[1]["patient_rank"] == 2


def test_read_only_connection(
    patient_source, tmp_path
):

    database = tmp_path / "readonly.duckdb"

    with duckdb.connect(str(database)) as con:

        initialize_warehouse(
            con, patient_source
        )

    with duckdb.connect(
        str(database),
        read_only=True
    ) as con:

        with pytest.raises(duckdb.Error):

            con.execute(
                "DELETE FROM healthcare.patients"
            )
