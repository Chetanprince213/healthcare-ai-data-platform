"""Transactional staged healthcare file ingestion."""

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

CONFIG_PATH = (
    ROOT / "configs/ingestion/staged_ingestion.json"
)

LOGGER = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "patient_id",
    "region",
    "age",
    "prior_visits",
    "no_show"
]


class DataValidationError(ValueError):
    """Raised when a staged file violates data rules."""


def utc_now():

    return datetime.now(timezone.utc).isoformat()


def load_config():

    return json.loads(
        CONFIG_PATH.read_text(encoding="utf-8-sig")
    )


def write_json(path, payload):

    path = Path(path)

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(payload, indent=4) + "\n",
        encoding="utf-8"
    )

    temporary.replace(path)


def configure_logging(path):

    path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
        handlers=[
            logging.FileHandler(path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def file_hash(path):
    """Calculate SHA-256 without loading entire file."""

    digest = hashlib.sha256()

    with Path(path).open("rb") as file:

        for chunk in iter(
            lambda: file.read(65536), b""
        ):

            digest.update(chunk)

    return digest.hexdigest()


def sql_literal(value):
    """Safely quote a value for DuckDB COPY statements."""

    return "'" + str(value).replace("'", "''") + "'"


def initialize_database(connection):

    connection.execute(
        "CREATE SCHEMA IF NOT EXISTS ingestion"
    )

    connection.execute(
        """
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
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion.load_history (
            file_hash VARCHAR PRIMARY KEY,
            file_name VARCHAR NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            source_rows BIGINT NOT NULL,
            inserted_rows BIGINT NOT NULL,
            duplicate_rows BIGINT NOT NULL,
            loaded_at TIMESTAMP NOT NULL,
            status VARCHAR NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion.load_errors (
            error_id VARCHAR PRIMARY KEY,
            file_hash VARCHAR,
            file_name VARCHAR NOT NULL,
            error_message VARCHAR NOT NULL,
            failed_at TIMESTAMP NOT NULL
        )
        """
    )


def stage_source(source, landing):
    """Publish a complete file through an atomic rename."""

    source = Path(source)

    landing = Path(landing)

    landing.mkdir(parents=True, exist_ok=True)

    digest = file_hash(source)

    destination = landing / (
        source.stem + "_" + digest[:16] + ".csv"
    )

    if destination.exists():

        if file_hash(destination) != digest:

            raise RuntimeError(
                "Existing staged file content mismatch"
            )

        return destination

    temporary = destination.with_suffix(".part")

    shutil.copy2(source, temporary)

    temporary.replace(destination)

    return destination


def inspect_csv(path):
    """Validate the header and count source records."""

    with Path(path).open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.reader(file)

        header = next(reader, None)

        if header != EXPECTED_COLUMNS:

            raise DataValidationError(
                f"Unexpected CSV schema: {header}"
            )

        count = 0

        for row in reader:

            if len(row) != len(EXPECTED_COLUMNS):

                raise DataValidationError(
                    "Invalid CSV column count"
                )

            count += 1

    if count == 0:

        raise DataValidationError(
            "Empty source file"
        )

    return count


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

    connection.execute(
        """
        INSERT INTO ingestion.load_errors
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            str(uuid.uuid4()),
            digest,
            path.name,
            str(error)
        ]
    )

    path.unlink()

    LOGGER.warning(
        "File quarantined | file=%s | error=%s",
        path.name,
        error
    )


def ingest_file(connection, path, quarantine):
    """Load one staged CSV transactionally."""

    path = Path(path)

    digest = file_hash(path)

    previous = connection.execute(
        """
        SELECT COUNT(*)
        FROM ingestion.load_history
        WHERE file_hash = ?
          AND status = 'LOADED'
        """,
        [digest]
    ).fetchone()[0]

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

    transaction_started = False

    try:

        source_rows = inspect_csv(path)

        connection.execute("BEGIN TRANSACTION")

        transaction_started = True

        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE staged_patients (
                patient_id VARCHAR,
                region VARCHAR,
                age VARCHAR,
                prior_visits VARCHAR,
                no_show VARCHAR
            )
            """
        )

        # Actual local DuckDB COPY operation.
        connection.execute(
            "COPY staged_patients FROM "
            + sql_literal(path.resolve().as_posix())
            + " (FORMAT CSV, HEADER TRUE)"
        )

        loaded_rows = connection.execute(
            "SELECT COUNT(*) FROM staged_patients"
        ).fetchone()[0]

        if loaded_rows != source_rows:

            raise DataValidationError(
                "Source-to-stage reconciliation failed"
            )

        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE validated_patients AS
            SELECT
                TRIM(patient_id) AS patient_id,
                TRIM(region) AS region,
                TRY_CAST(age AS INTEGER) AS age,
                TRY_CAST(prior_visits AS INTEGER) AS prior_visits,
                TRY_CAST(no_show AS INTEGER) AS no_show,
                age AS raw_age,
                prior_visits AS raw_visits,
                no_show AS raw_no_show
            FROM staged_patients
            """
        )

        invalid = connection.execute(
            """
            SELECT COUNT(*)
            FROM validated_patients
            WHERE patient_id IS NULL
               OR patient_id = ''
               OR region IS NULL
               OR region = ''
               OR age IS NULL
               OR age NOT BETWEEN 0 AND 120
               OR prior_visits IS NULL
               OR prior_visits < 0
               OR no_show IS NULL
               OR no_show NOT IN (0, 1)
               OR NOT regexp_full_match(
                   TRIM(raw_age), '[0-9]+'
               )
               OR NOT regexp_full_match(
                   TRIM(raw_visits), '[0-9]+'
               )
               OR NOT regexp_full_match(
                   TRIM(raw_no_show), '[01]'
               )
            """
        ).fetchone()[0]

        if invalid:

            raise DataValidationError(
                f"Invalid records detected: {invalid}"
            )

        unique_count = connection.execute(
            """
            SELECT COUNT(DISTINCT patient_id)
            FROM validated_patients
            """
        ).fetchone()[0]

        if unique_count != source_rows:

            raise DataValidationError(
                "Duplicate patient IDs within source file"
            )

        conflicts = connection.execute(
            """
            SELECT COUNT(*)
            FROM validated_patients s
            JOIN ingestion.patients t
              ON s.patient_id = t.patient_id
            WHERE s.region IS DISTINCT FROM t.region
               OR s.age IS DISTINCT FROM t.age
               OR s.prior_visits IS DISTINCT FROM t.prior_visits
               OR s.no_show IS DISTINCT FROM t.no_show
            """
        ).fetchone()[0]

        if conflicts:

            raise DataValidationError(
                "Conflicting existing patient records"
            )

        duplicate_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM validated_patients s
            JOIN ingestion.patients t
              ON s.patient_id = t.patient_id
            """
        ).fetchone()[0]

        before_count = connection.execute(
            "SELECT COUNT(*) FROM ingestion.patients"
        ).fetchone()[0]

        connection.execute(
            """
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
            """,
            [path.name, digest]
        )

        after_count = connection.execute(
            "SELECT COUNT(*) FROM ingestion.patients"
        ).fetchone()[0]

        inserted_rows = after_count - before_count

        if inserted_rows + duplicate_rows != source_rows:

            raise DataValidationError(
                "Stage-to-target reconciliation failed"
            )

        connection.execute(
            """
            INSERT INTO ingestion.load_history
            VALUES (
                ?, ?, ?, ?, ?, ?,
                CURRENT_TIMESTAMP, 'LOADED'
            )
            """,
            [
                digest,
                path.name,
                path.stat().st_size,
                source_rows,
                inserted_rows,
                duplicate_rows
            ]
        )

        connection.execute("COMMIT")

        transaction_started = False

        LOGGER.info(
            "File loaded | file=%s | source=%s | "
            "inserted=%s | duplicates=%s",
            path.name,
            source_rows,
            inserted_rows,
            duplicate_rows
        )

        return {
            "file_name": path.name,
            "status": "LOADED",
            "source_rows": source_rows,
            "inserted_rows": inserted_rows,
            "duplicate_rows": duplicate_rows
        }

    except (
        DataValidationError,
        UnicodeError,
        csv.Error,
        duckdb.ConversionException,
        duckdb.InvalidInputException
    ) as error:

        if transaction_started:

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

        if transaction_started:

            connection.execute("ROLLBACK")

        raise


def poll_once(connection, landing, quarantine):
    """Discover completed CSV files and process them."""

    results = []

    for path in sorted(Path(landing).glob("*.csv")):

        if path.is_file():

            results.append(
                ingest_file(
                    connection,
                    path,
                    quarantine
                )
            )

    return results


def poll_stage(
    connection,
    landing,
    quarantine,
    cycles,
    interval_seconds
):
    """Bounded polling loop for local Snowpipe analogy."""

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
    """Unload analytical data and verify exported rows."""

    destination = Path(destination)

    destination.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporary = destination.with_suffix(
        ".tmp.parquet"
    )

    if temporary.exists():

        temporary.unlink()

    connection.execute(
        """
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

    exported_rows = connection.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(temporary)]
    ).fetchone()[0]

    target_rows = connection.execute(
        "SELECT COUNT(*) FROM ingestion.patients"
    ).fetchone()[0]

    if exported_rows != target_rows:

        raise RuntimeError(
            "Parquet export reconciliation failed"
        )

    temporary.replace(destination)

    return exported_rows


def run_pipeline():

    config = load_config()

    configure_logging(
        ROOT / config["log_path"]
    )

    started_at = utc_now()

    start = time.perf_counter()

    run_id = str(uuid.uuid4())

    source = ROOT / config["source_path"]

    landing = ROOT / config["landing_path"]

    quarantine = ROOT / config["quarantine_path"]

    database = ROOT / config["database_path"]

    database.parent.mkdir(
        parents=True, exist_ok=True
    )

    LOGGER.info(
        "Ingestion started | run_id=%s",
        run_id
    )

    source_rows = inspect_csv(source)

    staged_file = stage_source(
        source,
        landing
    )

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

        target_rows = connection.execute(
            "SELECT COUNT(*) FROM ingestion.patients"
        ).fetchone()[0]

        source_history = connection.execute(
            """
            SELECT source_rows
            FROM ingestion.load_history
            WHERE file_hash = ?
            """,
            [file_hash(staged_file)]
        ).fetchone() if staged_file.exists() else None

        if (
            source_history is None
            or source_history[0] != source_rows
        ):

            raise RuntimeError(
                "Source load history reconciliation failed"
            )

        if any(
            item["status"] == "QUARANTINED"
            for item in results
        ):

            raise RuntimeError(
                "One or more files were quarantined. "
                "Review ingestion logs and error reports."
            )

        exported_rows = export_parquet(
            connection,
            ROOT / config["export_path"]
        )

        history_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM ingestion.load_history
            """
        ).fetchone()[0]

        unique_patients = connection.execute(
            """
            SELECT COUNT(DISTINCT patient_id)
            FROM ingestion.patients
            """
        ).fetchone()[0]

        if unique_patients != target_rows:

            raise RuntimeError(
                "Target uniqueness validation failed"
            )

        cursor = connection.execute(
            """
            SELECT region, COUNT(*) AS total_patients
            FROM ingestion.patients
            GROUP BY region
            ORDER BY region
            """
        )

        regional_metrics = [
            {
                "region": row[0],
                "total_patients": row[1]
            }
            for row in cursor.fetchall()
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
        "target_records": target_rows,
        "unique_patients": unique_patients,
        "exported_records": exported_rows,
        "load_history_records": history_count,
        "files_loaded": sum(
            item["status"] == "LOADED"
            for item in results
        ),
        "duplicate_file_detections": sum(
            item["status"] == "SKIPPED"
            for item in results
        ),
        "quarantined_files": sum(
            item["status"] == "QUARANTINED"
            for item in results
        ),
        "inserted_records": sum(
            item["inserted_rows"]
            for item in results
        ),
        "reconciliation_status": "PASSED",
        "regional_metrics": regional_metrics,
        "file_results": results
    }

    write_json(
        ROOT / config["report_path"],
        report
    )

    # Replacement baseline checkpoint because the
    # original starter ZIP is unavailable.
    write_json(
        ROOT / "data/training_labs/step_36_result.json",
        {
            "step": 36,
            "mode": "local_replacement_baseline",
            "result": {
                "source_records": source_rows,
                "target_records": target_rows,
                "exported_records": exported_rows,
                "status": "PASSED"
            }
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
            "Staged ingestion pipeline failed"
        )

        raise
