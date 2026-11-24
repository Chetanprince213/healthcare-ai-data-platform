"""Staged ingestion correctness and reliability tests."""

import csv

import duckdb
import pytest

from src.snowflake.staged_ingestion import (
    EXPECTED_COLUMNS,
    file_hash,
    initialize_database,
    stage_source,
    inspect_csv,
    ingest_file,
    poll_once,
    poll_stage,
    export_parquet
)


def create_csv(path, rows, columns=None):

    path.parent.mkdir(
        parents=True, exist_ok=True
    )

    with path.open(
        "w", encoding="utf-8", newline=""
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            columns or EXPECTED_COLUMNS
        )

        writer.writerows(rows)

    return path


@pytest.fixture
def environment(tmp_path):

    landing = tmp_path / "landing"

    quarantine = tmp_path / "quarantine"

    landing.mkdir()

    quarantine.mkdir()

    connection = duckdb.connect(":memory:")

    initialize_database(connection)

    yield connection, landing, quarantine

    connection.close()


def count_patients(connection):

    return connection.execute(
        "SELECT COUNT(*) FROM ingestion.patients"
    ).fetchone()[0]


def test_file_hash(tmp_path):

    first = tmp_path / "first.txt"

    second = tmp_path / "second.txt"

    first.write_text("same content")

    second.write_text("same content")

    assert file_hash(first) == file_hash(second)

    assert len(file_hash(first)) == 64


def test_stage_source(tmp_path):

    source = create_csv(
        tmp_path / "source.csv",
        [["P001", "N", 35, 2, 0]]
    )

    staged = stage_source(
        source, tmp_path / "landing"
    )

    assert staged.exists()

    assert file_hash(source) == file_hash(staged)

    assert stage_source(
        source, tmp_path / "landing"
    ) == staged


def test_csv_row_count(tmp_path):

    source = create_csv(
        tmp_path / "patients.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1]
        ]
    )

    assert inspect_csv(source) == 2


def test_bulk_copy(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "patients.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1]
        ]
    )

    result = ingest_file(
        con, source, quarantine
    )

    assert result["status"] == "LOADED"

    assert result["inserted_rows"] == 2

    assert count_patients(con) == 2


def test_duplicate_file_skip(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(con, source, quarantine)

    result = ingest_file(
        con, source, quarantine
    )

    assert result["status"] == "SKIPPED"

    assert count_patients(con) == 1


def test_renamed_duplicate_file(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "original.csv",
        [["P001", "N", 35, 2, 0]]
    )

    duplicate = landing / "renamed.csv"

    duplicate.write_bytes(source.read_bytes())

    ingest_file(con, source, quarantine)

    result = ingest_file(
        con, duplicate, quarantine
    )

    assert result["status"] == "SKIPPED"

    assert count_patients(con) == 1


def test_incremental_ingestion(environment):

    con, landing, quarantine = environment

    first = create_csv(
        landing / "first.csv",
        [["P001", "N", 35, 2, 0]]
    )

    second = create_csv(
        landing / "second.csv",
        [["P002", "S", 45, 3, 1]]
    )

    ingest_file(con, first, quarantine)

    result = ingest_file(
        con, second, quarantine
    )

    assert result["inserted_rows"] == 1

    assert count_patients(con) == 2


def test_existing_record_deduplication(environment):

    con, landing, quarantine = environment

    first = create_csv(
        landing / "first.csv",
        [["P001", "N", 35, 2, 0]]
    )

    second = create_csv(
        landing / "second.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1]
        ]
    )

    ingest_file(con, first, quarantine)

    result = ingest_file(
        con, second, quarantine
    )

    assert result["duplicate_rows"] == 1

    assert result["inserted_rows"] == 1

    assert count_patients(con) == 2


def test_invalid_schema(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "invalid.csv",
        [["P001"]],
        columns=["wrong_column"]
    )

    result = ingest_file(
        con, source, quarantine
    )

    assert result["status"] == "QUARANTINED"

    assert count_patients(con) == 0

    assert list(quarantine.glob("*.csv"))


@pytest.mark.parametrize(
    "row",
    [
        ["P001", "N", 150, 2, 0],
        ["P001", "N", 35, -1, 0],
        ["P001", "N", 35, 2, 5],
        ["", "N", 35, 2, 0],
        ["P001", "", 35, 2, 0],
        ["P001", "N", "invalid", 2, 0],
        ["P001", "N", "35.5", 2, 0]
    ]
)
def test_invalid_records(environment, row):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "invalid.csv",
        [row]
    )

    result = ingest_file(
        con, source, quarantine
    )

    assert result["status"] == "QUARANTINED"

    assert count_patients(con) == 0


def test_duplicate_ids_within_file(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "duplicates.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P001", "N", 35, 2, 0]
        ]
    )

    result = ingest_file(
        con, source, quarantine
    )

    assert result["status"] == "QUARANTINED"

    assert count_patients(con) == 0


def test_transaction_rollback(environment):

    con, landing, quarantine = environment

    first = create_csv(
        landing / "valid.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(con, first, quarantine)

    invalid = create_csv(
        landing / "invalid.csv",
        [
            ["P002", "S", 45, 3, 1],
            ["P003", "N", 999, 2, 0]
        ]
    )

    result = ingest_file(
        con, invalid, quarantine
    )

    assert result["status"] == "QUARANTINED"

    assert count_patients(con) == 1


def test_conflicting_patient_rejection(environment):

    con, landing, quarantine = environment

    first = create_csv(
        landing / "first.csv",
        [["P001", "N", 35, 2, 0]]
    )

    second = create_csv(
        landing / "second.csv",
        [["P001", "N", 50, 2, 0]]
    )

    ingest_file(con, first, quarantine)

    result = ingest_file(
        con, second, quarantine
    )

    assert result["status"] == "QUARANTINED"

    assert count_patients(con) == 1


def test_load_history(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(con, source, quarantine)

    history = con.execute(
        """
        SELECT
            source_rows,
            inserted_rows,
            status
        FROM ingestion.load_history
        """
    ).fetchone()

    assert history == (1, 1, "LOADED")


def test_error_audit(environment):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "invalid.csv",
        [["P001", "N", 999, 2, 0]]
    )

    ingest_file(con, source, quarantine)

    count = con.execute(
        "SELECT COUNT(*) FROM ingestion.load_errors"
    ).fetchone()[0]

    assert count == 1

    assert list(
        quarantine.glob("*.error.json")
    )


def test_polling_discovery(environment):

    con, landing, quarantine = environment

    create_csv(
        landing / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    results = poll_once(
        con, landing, quarantine
    )

    assert len(results) == 1

    assert results[0]["status"] == "LOADED"


def test_partial_file_ignored(environment):

    con, landing, quarantine = environment

    (landing / "incomplete.part").write_text(
        "partial file"
    )

    results = poll_once(
        con, landing, quarantine
    )

    assert results == []


def test_repeated_polling(environment):

    con, landing, quarantine = environment

    create_csv(
        landing / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    results = poll_stage(
        con,
        landing,
        quarantine,
        cycles=3,
        interval_seconds=0
    )

    assert results[0]["status"] == "LOADED"

    assert results[1]["status"] == "SKIPPED"

    assert results[2]["status"] == "SKIPPED"

    assert count_patients(con) == 1


def test_parquet_export(environment, tmp_path):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "patients.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1]
        ]
    )

    ingest_file(con, source, quarantine)

    destination = tmp_path / "patients.parquet"

    exported = export_parquet(
        con, destination
    )

    assert destination.exists()

    assert exported == 2


def test_repeatable_parquet_export(
    environment, tmp_path
):

    con, landing, quarantine = environment

    source = create_csv(
        landing / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(con, source, quarantine)

    destination = tmp_path / "patients.parquet"

    assert export_parquet(
        con, destination
    ) == 1

    assert export_parquet(
        con, destination
    ) == 1
