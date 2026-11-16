"""Local Delta Lake ACID, time travel and optimization exercise.

Uses only synthetic healthcare data.
Requires Apache Spark, Delta Lake and a compatible Java runtime.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


ROOT = Path(__file__).resolve().parents[2]

DELTA_PATH = ROOT / "data" / "delta_lake" / "patient_appointments"

REPORT_PATH = (
    ROOT
    / "data"
    / "quality_reports"
    / "delta_lake_validation.json"
)


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("HealthcareDeltaLakeValidation")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def collect_records(dataframe) -> list[tuple]:
    return sorted(
        tuple(row)
        for row in dataframe.select(
            "patient_id",
            "region",
            "appointment_status",
        ).collect()
    )


def run_delta_validation() -> dict:
    if DELTA_PATH.exists():
        raise FileExistsError(
            f"Existing Delta table preserved: {DELTA_PATH}"
        )

    if REPORT_PATH.exists():
        raise FileExistsError(
            f"Existing validation report preserved: {REPORT_PATH}"
        )

    spark = create_spark()

    try:
        spark.sparkContext.setLogLevel("ERROR")

        schema = StructType(
            [
                StructField(
                    "patient_id",
                    StringType(),
                    False,
                ),
                StructField(
                    "region",
                    StringType(),
                    False,
                ),
                StructField(
                    "appointment_status",
                    StringType(),
                    False,
                ),
            ]
        )

        original_records = [
            ("P0001", "North", "completed"),
            ("P0002", "South", "completed"),
            ("P0003", "North", "cancelled"),
            ("P0004", "South", "completed"),
            ("P0005", "North", "completed"),
        ]

        updated_records = [
            ("P0001", "North", "completed"),
            ("P0002", "South", "completed"),
            ("P0003", "North", "completed"),
            ("P0004", "South", "completed"),
            ("P0005", "North", "completed"),
            ("P0006", "East", "scheduled"),
        ]

        original_df = spark.createDataFrame(
            original_records,
            schema,
        )

        updated_df = spark.createDataFrame(
            updated_records,
            schema,
        )

        delta_path = str(DELTA_PATH.resolve())

        # ----------------------------------------------------
        # FIRST ACID TRANSACTION: VERSION 0
        # ----------------------------------------------------

        print("\nWRITING DELTA VERSION 0")

        (
            original_df
            .coalesce(1)
            .write
            .format("delta")
            .mode("errorifexists")
            .save(delta_path)
        )

        version_zero = (
            spark.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(delta_path)
        )

        version_zero_records = collect_records(
            version_zero
        )

        assert version_zero_records == sorted(
            original_records
        ), "Version 0 does not match the original records."

        print(
            f"Version 0 records: {len(version_zero_records)}"
        )

        # ----------------------------------------------------
        # SECOND ACID TRANSACTION: VERSION 1
        # ----------------------------------------------------

        print("\nWRITING DELTA VERSION 1")

        (
            updated_df
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .save(delta_path)
        )

        version_one = (
            spark.read
            .format("delta")
            .option("versionAsOf", 1)
            .load(delta_path)
        )

        version_one_records = collect_records(
            version_one
        )

        assert version_one_records == sorted(
            updated_records
        ), "Version 1 does not match the updated records."

        print(
            f"Version 1 records: {len(version_one_records)}"
        )

        # ----------------------------------------------------
        # TIME TRAVEL
        # ----------------------------------------------------

        print("\nVERIFYING TIME TRAVEL")

        old_version_again = (
            spark.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(delta_path)
        )

        time_travel_passed = (
            collect_records(old_version_again)
            == sorted(original_records)
        )

        assert time_travel_passed, (
            "Delta Time Travel could not recover version 0."
        )

        print("Version 0 remains readable after version 1.")

        # ----------------------------------------------------
        # DELTA TRANSACTION HISTORY
        # ----------------------------------------------------

        print("\nREADING DELTA TRANSACTION HISTORY")

        delta_table = DeltaTable.forPath(
            spark,
            delta_path,
        )

        history_rows = (
            delta_table
            .history()
            .select("version", "operation", "timestamp")
            .orderBy("version")
            .collect()
        )

        history = [
            {
                "version": int(row["version"]),
                "operation": row["operation"],
                "timestamp": (
                    row["timestamp"].isoformat()
                    if row["timestamp"] is not None
                    else None
                ),
            }
            for row in history_rows
        ]

        versions = {
            entry["version"]
            for entry in history
        }

        assert 0 in versions
        assert 1 in versions

        for entry in history:
            print(
                f"Version {entry['version']}: "
                f"{entry['operation']}"
            )

        # ----------------------------------------------------
        # SAFE OPTIMIZATION
        # ----------------------------------------------------

        print("\nCHECKING DELTA COMPACTION SUPPORT")

        optimization = {
            "attempted": False,
            "completed": False,
            "reason": None,
        }

        # Delta compaction is a supported table maintenance
        # operation in compatible Delta Lake installations.
        # It does not use VACUUM.

        optimize_api = getattr(
            delta_table,
            "optimize",
            None,
        )

        if callable(optimize_api):
            optimization["attempted"] = True

            try:
                (
                    delta_table
                    .optimize()
                    .executeCompaction()
                )

                optimization["completed"] = True

                print("Delta compaction completed.")

            except Exception as exc:
                optimization["reason"] = (
                    f"{type(exc).__name__}: {exc}"
                )

                print(
                    "Compaction was not available in this "
                    "runtime. The validation will continue "
                    "without claiming optimization succeeded."
                )

        else:
            optimization["reason"] = (
                "Delta optimization API unavailable."
            )

        # ----------------------------------------------------
        # VERIFY BOTH VERSIONS AGAIN
        # ----------------------------------------------------

        print("\nREVERIFYING HISTORICAL VERSIONS")

        old_after_optimization = (
            spark.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(delta_path)
        )

        version_one_after_optimization = (
            spark.read
            .format("delta")
            .option("versionAsOf", 1)
            .load(delta_path)
        )

        historical_versions_readable = (
            collect_records(old_after_optimization)
            == sorted(original_records)
            and collect_records(version_one_after_optimization)
            == sorted(updated_records)
        )

        assert historical_versions_readable, (
            "Historical Delta versions are not readable."
        )

        transaction_log = (
            DELTA_PATH / "_delta_log"
        )

        log_exists = (
            transaction_log.is_dir()
            and any(transaction_log.glob("*.json"))
        )

        assert log_exists, (
            "Delta transaction log not found."
        )

        checks = {
            "version_zero_written": (
                len(version_zero_records) == 5
            ),
            "version_one_written": (
                len(version_one_records) == 6
            ),
            "version_zero_time_travel": time_travel_passed,
            "transaction_history_contains_both_versions": (
                0 in versions and 1 in versions
            ),
            "delta_transaction_log_exists": log_exists,
            "historical_versions_remain_readable": (
                historical_versions_readable
            ),
            "no_vacuum_executed": True,
        }

        report = {
            "timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "implementation": "real_local_delta_lake",
            "synthetic_data_only": True,
            "cloud_deployed": False,
            "version_zero_records": len(version_zero_records),
            "version_one_records": len(version_one_records),
            "transaction_history": history,
            "optimization": optimization,
            "checks": checks,
            "passed": all(checks.values()),
        }

        REPORT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        REPORT_PATH.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\nDELTA LAKE VALIDATION RESULTS")

        for name, passed in checks.items():
            print(
                f"{'PASS' if passed else 'FAIL'}: {name}"
            )

        print(
            "\nOVERALL RESULT:",
            "PASS" if report["passed"] else "FAIL",
        )

        if not report["passed"]:
            raise RuntimeError(
                "Delta Lake validation failed."
            )

        return report

    finally:
        spark.stop()


if __name__ == "__main__":
    run_delta_validation()
