"""Configuration-driven healthcare analytical warehouse."""

import json
import logging
import statistics
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = (
    ROOT / "configs/warehouse/warehouse_config.json"
)

SQL_DIRECTORY = ROOT / "sql/warehouse"

LOGGER = logging.getLogger(__name__)


def load_config():
    """Load external warehouse configuration."""

    return json.loads(
        CONFIG_PATH.read_text(encoding="utf-8-sig")
    )


def read_sql(filename):
    """Load a SQL statement from the SQL directory."""

    return (
        SQL_DIRECTORY / filename
    ).read_text(encoding="utf-8-sig").strip().rstrip(";")


def configure_logging(path):
    """Configure application and execution logging."""

    path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
        handlers=[
            logging.FileHandler(
                path, encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )


def execute_query(connection, query):
    """Execute SQL and return structured records."""

    cursor = connection.execute(query)

    columns = [
        item[0] for item in cursor.description
    ]

    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def validate_patient_data(connection):
    """Validate uniqueness and patient attribute domains."""

    total, unique, invalid = connection.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT patient_id),
            COUNT(*) FILTER (
                WHERE patient_id IS NULL
                   OR TRIM(patient_id) = ''
                   OR region IS NULL
                   OR TRIM(region) = ''
                   OR age IS NULL
                   OR age NOT BETWEEN 0 AND 120
                   OR prior_visits IS NULL
                   OR prior_visits < 0
                   OR no_show IS NULL
                   OR no_show NOT IN (0, 1)
            )
        FROM healthcare.patients
        """
    ).fetchone()

    duplicates = total - unique

    if total == 0:
        raise ValueError("Empty patient dataset")

    if duplicates or invalid:
        raise ValueError(
            "Data quality validation failed: "
            f"duplicates={duplicates}, "
            f"invalid={invalid}"
        )

    return {
        "total_records": total,
        "unique_patients": unique,
        "duplicate_records": duplicates,
        "invalid_records": invalid,
        "status": "PASSED"
    }


def initialize_warehouse(connection, source_path):
    """Perform transactional full-refresh ingestion."""

    source_path = Path(source_path)

    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    connection.execute("BEGIN TRANSACTION")

    try:

        connection.execute(
            "CREATE SCHEMA IF NOT EXISTS healthcare"
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE healthcare.patients AS
            SELECT
                TRIM(patient_id) AS patient_id,
                TRIM(region) AS region,
                CAST(age AS INTEGER) AS age,
                CAST(prior_visits AS INTEGER) AS prior_visits,
                CAST(no_show AS INTEGER) AS no_show
            FROM read_csv_auto(
                ?,
                header = true,
                all_varchar = true
            )
            """,
            [str(source_path)]
        )

        metrics = validate_patient_data(connection)

        source_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM read_csv_auto(?, header = true)
            """,
            [str(source_path)]
        ).fetchone()[0]

        if source_count != metrics["total_records"]:
            raise ValueError(
                "Source-to-target reconciliation failed"
            )

        connection.execute("COMMIT")

        return metrics

    except Exception:

        connection.execute("ROLLBACK")

        raise


def create_analytics_objects(connection, multiplier):
    """Create views and a separate benchmark workload."""

    if not isinstance(multiplier, int):
        raise ValueError(
            "Benchmark multiplier must be an integer"
        )

    if not 1 <= multiplier <= 10000:
        raise ValueError(
            "Benchmark multiplier outside supported range"
        )

    view_query = read_sql(
        "patient_analytics_view.sql"
    )

    connection.execute("BEGIN TRANSACTION")

    try:

        connection.execute(
            "CREATE OR REPLACE VIEW "
            "healthcare.patient_analytics AS "
            + view_query
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE
                healthcare.benchmark_patients AS
            SELECT
                p.patient_id || '-' ||
                    CAST(r.range AS VARCHAR)
                    AS benchmark_id,
                p.region,
                p.age,
                p.prior_visits,
                p.no_show
            FROM healthcare.patients p
            CROSS JOIN range(?) r
            ORDER BY p.region, p.age
            """,
            [multiplier]
        )

        row_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM healthcare.benchmark_patients
            """
        ).fetchone()[0]

        connection.execute("COMMIT")

        return row_count

    except Exception:

        connection.execute("ROLLBACK")

        raise


def benchmark_query(
    database_path,
    query,
    profile,
    iterations
):
    """Measure query performance using resource profiles."""

    if iterations < 1:
        raise ValueError(
            "Benchmark iterations must be positive"
        )

    query = query.strip().rstrip(";")

    durations = []

    with duckdb.connect(
        str(database_path),
        read_only=True,
        config={
            "threads": str(profile["threads"]),
            "memory_limit": profile["memory_limit"]
        }
    ) as connection:

        start = time.perf_counter()

        reference = execute_query(
            connection, query
        )

        first_execution_ms = (
            time.perf_counter() - start
        ) * 1000

        for _ in range(iterations):

            start = time.perf_counter()

            result = execute_query(
                connection, query
            )

            durations.append(
                (time.perf_counter() - start) * 1000
            )

            if result != reference:
                raise RuntimeError(
                    "Inconsistent analytical query results"
                )

        plan = connection.execute(
            "EXPLAIN ANALYZE " + query
        ).fetchall()

    return {
        "profile": profile["name"],
        "threads": profile["threads"],
        "memory_limit": profile["memory_limit"],
        "iterations": iterations,
        "first_execution_ms": round(
            first_execution_ms, 4
        ),
        "minimum_ms": round(
            min(durations), 4
        ),
        "average_ms": round(
            statistics.mean(durations), 4
        ),
        "median_ms": round(
            statistics.median(durations), 4
        ),
        "maximum_ms": round(
            max(durations), 4
        ),
        "result": reference,
        "query_plan": "\n".join(
            str(row[-1]) for row in plan
        )
    }


def compare_query_plans(connection):
    """Inspect full-scan and filtered-scan execution."""

    queries = {
        "full_scan": """
            SELECT SUM(prior_visits)
            FROM healthcare.benchmark_patients
        """,
        "filtered_scan": """
            SELECT SUM(prior_visits)
            FROM healthcare.benchmark_patients
            WHERE region = 'N'
        """
    }

    plans = {}

    for name, query in queries.items():

        rows = connection.execute(
            "EXPLAIN ANALYZE " + query
        ).fetchall()

        plans[name] = "\n".join(
            str(row[-1]) for row in rows
        )

    return plans


def inspect_warehouse_metadata(connection):
    """Inspect database, schema and analytical objects."""

    catalog, schema = connection.execute(
        "SELECT current_database(), current_schema()"
    ).fetchone()

    objects = execute_query(
        connection,
        """
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = 'healthcare'
        ORDER BY table_name
        """
    )

    return {
        "catalog": catalog,
        "default_schema": schema,
        "analytics_schema": "healthcare",
        "objects": objects
    }


def run_warehouse_pipeline():
    """Execute the complete analytical warehouse workflow."""

    config = load_config()

    source = ROOT / config["source_path"]

    database = ROOT / config["database_path"]

    report_path = ROOT / config["report_path"]

    if database.stem.lower() == "healthcare":

        raise ValueError(
            "Database catalog must differ from "
            "the healthcare schema name"
        )

    configure_logging(
        ROOT / config["log_path"]
    )

    database.parent.mkdir(
        parents=True, exist_ok=True
    )

    report_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    run_id = str(uuid.uuid4())

    started_at = datetime.now(
        timezone.utc
    ).isoformat()

    start = time.perf_counter()

    LOGGER.info(
        "Warehouse pipeline started | run_id=%s",
        run_id
    )

    try:

        with duckdb.connect(
            str(database),
            config={
                "threads": "2",
                "memory_limit": "1GB"
            }
        ) as connection:

            quality = initialize_warehouse(
                connection, source
            )

            benchmark_rows = create_analytics_objects(
                connection,
                config["benchmark_multiplier"]
            )

            regional_metrics = execute_query(
                connection,
                read_sql(
                    "regional_patient_metrics.sql"
                )
            )

            utilization_ranking = execute_query(
                connection,
                read_sql(
                    "patient_utilization_ranking.sql"
                )
            )

            plans = compare_query_plans(
                connection
            )

            metadata = inspect_warehouse_metadata(
                connection
            )

        LOGGER.info(
            "Data validation passed | patients=%s",
            quality["total_records"]
        )

        LOGGER.info(
            "Benchmark workload created | rows=%s",
            benchmark_rows
        )

        regional_count = sum(
            row["total_patients"]
            for row in regional_metrics
        )

        if regional_count != quality["total_records"]:

            raise ValueError(
                "Regional reconciliation failed"
            )

        expected_benchmark_rows = (
            quality["total_records"]
            * config["benchmark_multiplier"]
        )

        if benchmark_rows != expected_benchmark_rows:

            raise ValueError(
                "Benchmark reconciliation failed"
            )

        benchmarks = []

        benchmark_sql = read_sql(
            "benchmark_workload.sql"
        )

        for profile in config["compute_profiles"]:

            result = benchmark_query(
                database,
                benchmark_sql,
                profile,
                config["benchmark_iterations"]
            )

            benchmarks.append(result)

            LOGGER.info(
                "Benchmark completed | "
                "profile=%s | average_ms=%s",
                result["profile"],
                result["average_ms"]
            )

        if not benchmarks:

            raise ValueError(
                "No compute profiles configured"
            )

        reference = benchmarks[0]["result"]

        if any(
            item["result"] != reference
            for item in benchmarks
        ):

            raise RuntimeError(
                "Compute profile reconciliation failed"
            )

        report = {
            "run_id": run_id,
            "warehouse_name": config["warehouse_name"],
            "implementation_mode": config["implementation_mode"],
            "started_at": started_at,
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "status": "SUCCESS",
            "duration_seconds": round(
                time.perf_counter() - start, 4
            ),
            "warehouse_metadata": metadata,
            "data_quality": quality,
            "regional_metrics": regional_metrics,
            "utilization_ranking": utilization_ranking,
            "benchmark_records": benchmark_rows,
            "reconciliation_status": "PASSED",
            "compute_benchmarks": benchmarks,
            "query_plans": plans
        }

        temporary_report = report_path.with_suffix(
            ".tmp"
        )

        temporary_report.write_text(
            json.dumps(report, indent=4),
            encoding="utf-8"
        )

        temporary_report.replace(report_path)

        LOGGER.info(
            "Warehouse pipeline completed | run_id=%s",
            run_id
        )

        return report

    except Exception:

        LOGGER.exception(
            "Warehouse pipeline failed | run_id=%s",
            run_id
        )

        raise


if __name__ == "__main__":
    run_warehouse_pipeline()
