"""Transactional incremental healthcare file ingestion."""

import csv
import hashlib
import json
import logging
import shutil
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]

LOGGER = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "patient_id",
    "region",
    "age",
    "prior_visits",
    "no_show"
]


class DataValidationError(ValueError):
    """Invalid staged healthcare data."""


def utc_now():

    return datetime.now(timezone.utc).isoformat()


def file_hash(path):

    digest = hashlib.sha256()

    with Path(path).open("rb") as source:

        for block in iter(
            lambda: source.read(65536), b""
        ):

            digest.update(block)

    return digest.hexdigest()


def sql_literal(value):

    return "'" + str(value).replace("'", "''") + "'"


def write_json(path, payload):

    path = Path(path)

    path.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8"
    )

    temporary.replace(path)


def stage_source(source, landing):

    source = Path(source)

    landing = Path(landing)

    landing.mkdir(
        parents=True, exist_ok=True
    )

    digest = file_hash(source)

    destination = landing / (
        source.stem + "_" + digest[:16] + ".csv"
    )

    if destination.exists():

        if file_hash(destination) != digest:

            raise RuntimeError(
                "Staged file fingerprint mismatch"
            )

        return destination

    temporary = destination.with_suffix(".part")

    shutil.copy2(source, temporary)

    if file_hash(temporary) != digest:

        temporary.unlink(missing_ok=True)

        raise RuntimeError(
            "Source changed during staging"
        )

    temporary.replace(destination)

    return destination


def inspect_csv(path):

    with Path(path).open(
        encoding="utf-8-sig",
        newline=""
    ) as source:

        reader = csv.reader(source, strict=True)

        if next(reader, None) != EXPECTED_COLUMNS:

            raise DataValidationError(
                "Unexpected source schema"
            )

        count = 0

        for row in reader:

            if len(row) != len(EXPECTED_COLUMNS):

                raise DataValidationError(
                    "Unexpected column count"
                )

            count += 1

    if not count:

        raise DataValidationError(
            "Empty source file"
        )

    return count


def initialize_database(connection):

    connection.execute("""
        CREATE SCHEMA IF NOT EXISTS ingestion;

        CREATE TABLE IF NOT EXISTS ingestion.patients (
            patient_id VARCHAR PRIMARY KEY,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL,
            source_file VARCHAR NOT NULL,
            source_hash VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            CHECK (age BETWEEN 0 AND 120),
            CHECK (prior_visits >= 0),
            CHECK (no_show IN (0, 1))
        );

        CREATE TABLE IF NOT EXISTS ingestion.load_history (
            file_hash VARCHAR PRIMARY KEY,
            file_name VARCHAR NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            source_rows BIGINT NOT NULL,
            inserted_rows BIGINT NOT NULL,
            duplicate_rows BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ingestion.load_errors (
            error_id VARCHAR PRIMARY KEY,
            file_hash VARCHAR,
            file_name VARCHAR NOT NULL,
            error_message VARCHAR NOT NULL,
            failed_at TIMESTAMP NOT NULL
        );
    """)


def quarantine_file(
    connection,
    path,
    quarantine,
    digest,
    error
):

    quarantine = Path(quarantine)

    quarantine.mkdir(
        parents=True, exist_ok=True
    )

    destination = quarantine / (
        digest[:16] + "_" + path.name
    )

    shutil.copy2(path, destination)

    write_json(
        destination.with_suffix(".error.json"),
        {
            "file_name": path.name,
            "file_hash": digest,
            "error": str(error),
            "failed_at": utc_now()
        }
    )

    connection.execute("""
        INSERT INTO ingestion.load_errors
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        str(uuid.uuid4()),
        digest,
        path.name,
        str(error)
    ])

    path.unlink()

    LOGGER.warning(
        "File quarantined | file=%s | error=%s",
        path.name,
        error
    )


def ingest_file(connection, path, quarantine):

    path = Path(path)

    digest = file_hash(path)

    previous = connection.execute("""
        SELECT COUNT(*)
        FROM ingestion.load_history
        WHERE file_hash = ?
          AND status = 'LOADED'
    """, [digest]).fetchone()[0]

    if previous:

        LOGGER.info(
            "Previously loaded file skipped | file=%s",
            path.name
        )

        return {
            "file_name": path.name,
            "status": "SKIPPED",
            "inserted_rows": 0
        }

    transaction = False

    try:

        source_rows = inspect_csv(path)

        connection.execute("BEGIN TRANSACTION")

        transaction = True

        connection.execute("""
            CREATE OR REPLACE TEMP TABLE staged_patients (
                patient_id VARCHAR,
                region VARCHAR,
                age VARCHAR,
                prior_visits VARCHAR,
                no_show VARCHAR
            )
        """)

        connection.execute(
            "COPY staged_patients FROM "
            + sql_literal(path.resolve().as_posix())
            + " (FORMAT CSV, HEADER TRUE)"
        )

        staged_count = connection.execute(
            "SELECT COUNT(*) FROM staged_patients"
        ).fetchone()[0]

        if staged_count != source_rows:

            raise DataValidationError(
                "Source-to-stage reconciliation failed"
            )

        invalid = connection.execute("""
            SELECT COUNT(*)
            FROM staged_patients
            WHERE patient_id IS NULL
               OR TRIM(patient_id) = ''
               OR region IS NULL
               OR TRIM(region) = ''
               OR age IS NULL
               OR NOT regexp_full_match(
                   TRIM(age), '[0-9]+'
               )
               OR TRY_CAST(age AS INTEGER) IS NULL
               OR TRY_CAST(age AS INTEGER)
                    NOT BETWEEN 0 AND 120
               OR prior_visits IS NULL
               OR NOT regexp_full_match(
                   TRIM(prior_visits), '[0-9]+'
               )
               OR TRY_CAST(prior_visits AS INTEGER)
                    IS NULL
               OR no_show IS NULL
               OR TRIM(no_show) NOT IN ('0', '1')
        """).fetchone()[0]

        if invalid:

            raise DataValidationError(
                f"Invalid records: {invalid}"
            )

        connection.execute("""
            CREATE OR REPLACE TEMP TABLE validated_patients AS
            SELECT
                TRIM(patient_id) AS patient_id,
                TRIM(region) AS region,
                CAST(age AS INTEGER) AS age,
                CAST(prior_visits AS INTEGER) AS prior_visits,
                CAST(no_show AS INTEGER) AS no_show
            FROM staged_patients
        """)

        unique = connection.execute("""
            SELECT COUNT(DISTINCT patient_id)
            FROM validated_patients
        """).fetchone()[0]

        if unique != source_rows:

            raise DataValidationError(
                "Duplicate patient identifiers"
            )

        conflicts = connection.execute("""
            SELECT COUNT(*)
            FROM validated_patients s
            JOIN ingestion.patients t
              ON s.patient_id = t.patient_id
            WHERE s.region IS DISTINCT FROM t.region
               OR s.age IS DISTINCT FROM t.age
               OR s.prior_visits IS DISTINCT FROM t.prior_visits
               OR s.no_show IS DISTINCT FROM t.no_show
        """).fetchone()[0]

        if conflicts:

            raise DataValidationError(
                "Conflicting existing patient records"
            )

        duplicates = connection.execute("""
            SELECT COUNT(*)
            FROM validated_patients s
            JOIN ingestion.patients t
              ON s.patient_id = t.patient_id
        """).fetchone()[0]

        before = connection.execute(
            "SELECT COUNT(*) FROM ingestion.patients"
        ).fetchone()[0]

        connection.execute("""
            INSERT INTO ingestion.patients
            SELECT
                s.patient_id,
                s.region,
                s.age,
                s.prior_visits,
                s.no_show,
                ?,
                ?,
                CURRENT_TIMESTAMP
            FROM validated_patients s
            WHERE NOT EXISTS (
                SELECT 1
                FROM ingestion.patients t
                WHERE t.patient_id = s.patient_id
            )
        """, [path.name, digest])

        after = connection.execute(
            "SELECT COUNT(*) FROM ingestion.patients"
        ).fetchone()[0]

        inserted = after - before

        if inserted + duplicates != source_rows:

            raise DataValidationError(
                "Target reconciliation failed"
            )

        connection.execute("""
            INSERT INTO ingestion.load_history
            VALUES (
                ?, ?, ?, ?, ?, ?,
                CURRENT_TIMESTAMP,
                'LOADED'
            )
        """, [
            digest,
            path.name,
            path.stat().st_size,
            source_rows,
            inserted,
            duplicates
        ])

        connection.execute("COMMIT")

        transaction = False

        LOGGER.info(
            "File loaded | file=%s | inserted=%s",
            path.name,
            inserted
        )

        return {
            "file_name": path.name,
            "status": "LOADED",
            "source_rows": source_rows,
            "inserted_rows": inserted,
            "duplicate_rows": duplicates
        }

    except (
        DataValidationError,
        UnicodeError,
        csv.Error,
        duckdb.ConversionException,
        duckdb.InvalidInputException
    ) as error:

        if transaction:

            connection.execute("ROLLBACK")

        quarantine_file(
            connection,
            path,
            quarantine,
            digest,
            error
        )

        return {
            "file_name": path.name,
            "status": "QUARANTINED",
            "inserted_rows": 0,
            "error": str(error)
        }

    except Exception:

        if transaction:

            connection.execute("ROLLBACK")

        raise


def poll_once(connection, landing, quarantine):

    return [
        ingest_file(connection, path, quarantine)
        for path in sorted(
            Path(landing).glob("*.csv")
        )
        if path.is_file()
    ]


def poll_stage(
    connection,
    landing,
    quarantine,
    cycles,
    interval_seconds
):

    if cycles < 1 or interval_seconds < 0:

        raise ValueError(
            "Invalid polling configuration"
        )

    results = []

    for cycle in range(cycles):

        LOGGER.info(
            "Polling cycle %s of %s",
            cycle + 1,
            cycles
        )

        results.extend(
            poll_once(
                connection,
                landing,
                quarantine
            )
        )

        if cycle + 1 < cycles:

            time.sleep(interval_seconds)

    return results


def export_parquet(connection, destination):

    destination = Path(destination)

    destination.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporary = destination.with_suffix(".tmp.parquet")

    temporary.unlink(missing_ok=True)

    connection.execute("""
        COPY (
            SELECT
                patient_id,
                region,
                age,
                prior_visits,
                no_show
            FROM ingestion.patients
            ORDER BY patient_id
        ) TO """
        + sql_literal(temporary.resolve().as_posix())
        + " (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    exported = connection.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(temporary)]
    ).fetchone()[0]

    target = connection.execute(
        "SELECT COUNT(*) FROM ingestion.patients"
    ).fetchone()[0]

    if exported != target:

        raise RuntimeError(
            "Parquet reconciliation failed"
        )

    temporary.replace(destination)

    return exported


def run_pipeline():

    config = json.loads(
        (
            ROOT / "configs/ingestion/staged_ingestion.json"
        ).read_text(encoding="utf-8")
    )

    log_path = ROOT / config["log_path"]

    log_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                log_path, encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )

    start = time.perf_counter()

    started_at = utc_now()

    run_id = str(uuid.uuid4())

    source = ROOT / config["source_path"]

    landing = ROOT / config["landing_path"]

    quarantine = ROOT / config["quarantine_path"]

    database = ROOT / config["database_path"]

    database.parent.mkdir(
        parents=True, exist_ok=True
    )

    source_rows = inspect_csv(source)

    staged = stage_source(source, landing)

    digest = file_hash(staged)

    with duckdb.connect(
        str(database),
        config={
            "threads": str(config["threads"]),
            "memory_limit": config["memory_limit"]
        }
    ) as connection:

        initialize_database(connection)

        results = poll_stage(
            connection,
            landing,
            quarantine,
            config["max_poll_cycles"],
            config["poll_interval_seconds"]
        )

        if any(
            result["status"] == "QUARANTINED"
            for result in results
        ):

            raise RuntimeError(
                "Rejected files require investigation"
            )

        history = connection.execute("""
            SELECT source_rows
            FROM ingestion.load_history
            WHERE file_hash = ?
        """, [digest]).fetchone()

        if history != (source_rows,):

            raise RuntimeError(
                "Source load history mismatch"
            )

        target, unique = connection.execute("""
            SELECT
                COUNT(*),
                COUNT(DISTINCT patient_id)
            FROM ingestion.patients
        """).fetchone()

        if target != unique:

            raise RuntimeError(
                "Target uniqueness validation failed"
            )

        exported = export_parquet(
            connection,
            ROOT / config["export_path"]
        )

        history_count = connection.execute(
            "SELECT COUNT(*) FROM ingestion.load_history"
        ).fetchone()[0]

        regional_metrics = [
            {
                "region": region,
                "total_patients": count
            }
            for region, count in connection.execute("""
                SELECT region, COUNT(*)
                FROM ingestion.patients
                GROUP BY region
                ORDER BY region
            """).fetchall()
        ]

    report = {
        "run_id": run_id,
        "implementation_mode": config["implementation_mode"],
        "started_at": started_at,
        "completed_at": utc_now(),
        "duration_seconds": round(
            time.perf_counter() - start, 4
        ),
        "status": "SUCCESS",
        "source_records": source_rows,
        "target_records": target,
        "unique_patients": unique,
        "exported_records": exported,
        "load_history_records": history_count,
        "files_loaded": sum(
            item["status"] == "LOADED"
            for item in results
        ),
        "duplicate_file_detections": sum(
            item["status"] == "SKIPPED"
            for item in results
        ),
        "inserted_records": sum(
            item["inserted_rows"]
            for item in results
        ),
        "quarantined_files": 0,
        "reconciliation_status": "PASSED",
        "regional_metrics": regional_metrics,
        "file_results": results
    }

    write_json(
        ROOT / config["report_path"],
        report
    )

    write_json(
        ROOT / config["validation_path"],
        {
            "component": "staged_ingestion",
            "run_id": run_id,
            "status": "PASSED",
            "source_records": source_rows,
            "target_records": target,
            "exported_records": exported
        }
    )

    LOGGER.info(
        "Ingestion completed | run_id=%s",
        run_id
    )

    return report


if __name__ == "__main__":

    try:

        run_pipeline()

    except Exception:

        LOGGER.exception(
            "Staged ingestion execution failed"
        )

        raise
