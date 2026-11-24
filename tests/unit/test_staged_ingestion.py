"""Transactional staged ingestion reliability tests."""

import csv

import duckdb
import pytest

from src.snowflake.staged_ingestion import (
    EXPECTED_COLUMNS,
    DataValidationError,
    file_hash,
    stage_source,
    inspect_csv,
    initialize_database,
    ingest_file,
    poll_once,
    poll_stage,
    export_parquet
)


@pytest.fixture
def environment(tmp_path):

    connection = duckdb.connect(":memory:")

    initialize_database(connection)

    yield connection, tmp_path

    connection.close()


def create_file(path, rows, columns=None):

    with path.open(
        "w", encoding="utf-8", newline=""
    ) as output:

        writer = csv.writer(output)

        writer.writerow(
            columns or EXPECTED_COLUMNS
        )

        writer.writerows(rows)

    return path


def count(connection):

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


def test_csv_count(tmp_path):

    source = create_file(
        tmp_path / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    assert inspect_csv(source) == 1


def test_empty_file(tmp_path):

    source = create_file(
        tmp_path / "empty.csv", []
    )

    with pytest.raises(DataValidationError):

        inspect_csv(source)


def test_bulk_copy(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["inserted_rows"] == 1

    assert count(con) == 1


def test_duplicate_file(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(
        con, source, folder / "quarantine"
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["status"] == "SKIPPED"

    assert count(con) == 1


def test_renamed_duplicate_file(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    duplicate = folder / "renamed.csv"

    duplicate.write_bytes(source.read_bytes())

    assert file_hash(source) == file_hash(duplicate)

    ingest_file(
        con, source, folder / "quarantine"
    )

    result = ingest_file(
        con, duplicate, folder / "quarantine"
    )

    assert result["status"] == "SKIPPED"

    assert count(con) == 1


def test_incremental_loading(environment):

    con, folder = environment

    for identifier in ["P001", "P002"]:

        source = create_file(
            folder / f"{identifier}.csv",
            [[identifier, "N", 35, 2, 0]]
        )

        ingest_file(
            con, source, folder / "quarantine"
        )

    assert count(con) == 2


def test_record_deduplication(environment):

    con, folder = environment

    first = create_file(
        folder / "first.csv",
        [["P001", "N", 35, 2, 0]]
    )

    second = create_file(
        folder / "second.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P002", "S", 45, 3, 1]
        ]
    )

    ingest_file(
        con, first, folder / "quarantine"
    )

    result = ingest_file(
        con, second, folder / "quarantine"
    )

    assert result["duplicate_rows"] == 1

    assert result["inserted_rows"] == 1

    assert count(con) == 2


def test_invalid_schema(environment):

    con, folder = environment

    source = create_file(
        folder / "invalid.csv",
        [["P001"]],
        columns=["incorrect_column"]
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["status"] == "QUARANTINED"

    assert count(con) == 0


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
def test_invalid_data(environment, row):

    con, folder = environment

    source = create_file(
        folder / "invalid.csv", [row]
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["status"] == "QUARANTINED"

    assert count(con) == 0

    assert list(
        (folder / "quarantine").glob("*.error.json")
    )


def test_duplicate_identifiers(environment):

    con, folder = environment

    source = create_file(
        folder / "duplicates.csv",
        [
            ["P001", "N", 35, 2, 0],
            ["P001", "N", 35, 2, 0]
        ]
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["status"] == "QUARANTINED"

    assert count(con) == 0


def test_transaction_rollback(environment):

    con, folder = environment

    first = create_file(
        folder / "valid.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(
        con, first, folder / "quarantine"
    )

    source = create_file(
        folder / "invalid.csv",
        [
            ["P002", "N", 35, 2, 0],
            ["P003", "N", 999, 2, 0]
        ]
    )

    result = ingest_file(
        con, source, folder / "quarantine"
    )

    assert result["status"] == "QUARANTINED"

    assert count(con) == 1


def test_conflicting_records(environment):

    con, folder = environment

    first = create_file(
        folder / "first.csv",
        [["P001", "N", 35, 2, 0]]
    )

    second = create_file(
        folder / "second.csv",
        [["P001", "N", 45, 2, 0]]
    )

    ingest_file(
        con, first, folder / "quarantine"
    )

    result = ingest_file(
        con, second, folder / "quarantine"
    )

    assert result["status"] == "QUARANTINED"

    assert count(con) == 1


def test_partial_file_ignored(environment):

    con, folder = environment

    (folder / "pending.part").write_text(
        "incomplete data"
    )

    assert poll_once(
        con, folder, folder / "quarantine"
    ) == []


def test_atomic_staging(environment):

    con, folder = environment

    source = create_file(
        folder / "source.csv",
        [["P001", "N", 35, 2, 0]]
    )

    staged = stage_source(
        source, folder / "landing"
    )

    assert file_hash(source) == file_hash(staged)

    assert stage_source(
        source, folder / "landing"
    ) == staged


def test_polling(environment):

    con, folder = environment

    create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    results = poll_stage(
        con,
        folder,
        folder / "quarantine",
        cycles=3,
        interval_seconds=0
    )

    assert results[0]["status"] == "LOADED"

    assert results[1]["status"] == "SKIPPED"

    assert results[2]["status"] == "SKIPPED"

    assert count(con) == 1


def test_parquet_reconciliation(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(
        con, source, folder / "quarantine"
    )

    destination = folder / "patients.parquet"

    assert export_parquet(con, destination) == 1

    assert destination.exists()


def test_repeatable_export(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(
        con, source, folder / "quarantine"
    )

    destination = folder / "patients.parquet"

    assert export_parquet(con, destination) == 1

    assert export_parquet(con, destination) == 1


def test_load_history(environment):

    con, folder = environment

    source = create_file(
        folder / "patients.csv",
        [["P001", "N", 35, 2, 0]]
    )

    ingest_file(
        con, source, folder / "quarantine"
    )

    result = con.execute("""
        SELECT source_rows, inserted_rows, status
        FROM ingestion.load_history
    """).fetchone()

    assert result == (1, 1, "LOADED")


def test_error_audit(environment):

    con, folder = environment

    source = create_file(
        folder / "invalid.csv",
        [["P001", "N", 999, 2, 0]]
    )

    ingest_file(
        con, source, folder / "quarantine"
    )

    result = con.execute(
        "SELECT COUNT(*) FROM ingestion.load_errors"
    ).fetchone()[0]

    assert result == 1
