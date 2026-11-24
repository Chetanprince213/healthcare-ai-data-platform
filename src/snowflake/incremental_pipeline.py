"""End-to-end incremental healthcare processing."""

import json
import logging
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src.snowflake.change_data_capture import (
    initialize_database,
    bootstrap_source,
    apply_change,
    get_patient,
    get_offset,
    read_stream,
    consume_append_only
)

from src.snowflake.incremental_refresh import (
    initialize_analytics,
    consume_changes,
    freshness_status,
    reconcile_analytics
)

from src.snowflake.temporal_history import (
    initialize_snapshots,
    create_snapshot,
    query_as_of,
    restore_snapshot
)

from src.snowflake.task_orchestration import (
    execute_dag,
    run_schedule
)


ROOT = Path(__file__).resolve().parents[2]

CONFIG = (
    ROOT
    / "configs/incremental/incremental_processing.json"
)

LOGGER = logging.getLogger(__name__)


def utc_now():

    return datetime.now(
        timezone.utc
    ).isoformat()


def write_json(path, payload):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(
            payload,
            indent=2
        ) + "\n",
        encoding="utf-8"
    )

    temporary.replace(path)


def apply_synthetic_changes(connection):

    existing = connection.execute("""
        SELECT COUNT(*)
        FROM cdc.change_events
        WHERE event_key IN (
            'synthetic:insert',
            'synthetic:update',
            'synthetic:delete'
        )
    """).fetchone()[0]

    if existing == 3:

        return 0

    if existing:

        raise RuntimeError(
            "Incomplete synthetic change batch. "
            "Review the change log before recovery."
        )

    patients = connection.execute("""
        SELECT patient_id
        FROM cdc.source_patients
        ORDER BY patient_id
        LIMIT 2
    """).fetchall()

    if len(patients) < 2:

        raise ValueError(
            "At least two source patients are required"
        )

    update_id = patients[0][0]

    delete_id = patients[1][0]

    updated = get_patient(
        connection,
        update_id
    )

    updated["prior_visits"] += 1

    updated["region"] = (
        "S"
        if updated["region"] == "N"
        else "N"
    )

    updated["no_show"] = (
        1 - updated["no_show"]
    )

    apply_change(
        connection,
        "INSERT",
        "SYNTHETIC-CDC-001",
        {
            "patient_id": "SYNTHETIC-CDC-001",
            "region": "N",
            "age": 42,
            "prior_visits": 3,
            "no_show": 0
        },
        event_key="synthetic:insert"
    )

    apply_change(
        connection,
        "UPDATE",
        update_id,
        updated,
        event_key="synthetic:update"
    )

    apply_change(
        connection,
        "DELETE",
        delete_id,
        event_key="synthetic:delete"
    )

    return 3


def verify_source_target(connection):

    mismatch = connection.execute("""
        SELECT COUNT(*)
        FROM (
            (
                SELECT *
                FROM cdc.source_patients

                EXCEPT

                SELECT *
                FROM cdc.current_patients
            )

            UNION ALL

            (
                SELECT *
                FROM cdc.current_patients

                EXCEPT

                SELECT *
                FROM cdc.source_patients
            )
        )
    """).fetchone()[0]

    if mismatch:

        raise RuntimeError(
            "Source-to-target reconciliation failed"
        )

    return "PASSED"


def run_pipeline():

    config = json.loads(
        CONFIG.read_text(encoding="utf-8")
    )

    log_path = ROOT / config["log_path"]

    log_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | %(message)s"
        ),
        handlers=[
            logging.FileHandler(
                log_path,
                encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )

    run_id = str(uuid.uuid4())

    started_at = utc_now()

    started = time.perf_counter()

    database = ROOT / config["database_path"]

    database.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    consumer = config["consumer_name"]

    LOGGER.info(
        "Pipeline started | run_id=%s",
        run_id
    )

    with duckdb.connect(
        str(database),
        config={
            "threads": str(config["threads"]),
            "memory_limit": config["memory_limit"]
        }
    ) as connection:

        initialize_database(connection)

        initialize_analytics(connection)

        initialize_snapshots(connection)

        bootstrap_records = bootstrap_source(
            connection,
            ROOT / config["source_path"]
        )

        bootstrap_refresh = consume_changes(
            connection,
            consumer,
            str(uuid.uuid4())
        )

        existing_snapshot = connection.execute("""
            SELECT event_version
            FROM temporal.snapshot_catalog
            WHERE snapshot_name = 'baseline'
        """).fetchone()

        baseline_version = (
            existing_snapshot[0]
            if existing_snapshot
            else get_offset(connection, consumer)
        )

        baseline_count = create_snapshot(
            connection,
            "baseline",
            baseline_version
        )

        new_events = apply_synthetic_changes(
            connection
        )

        pending_events = read_stream(
            connection,
            consumer
        )

        append_only_events = consume_append_only(
            connection,
            "append_only_audit_consumer"
        )

        def refresh():

            return consume_changes(
                connection,
                consumer,
                str(uuid.uuid4())
            )

        def validate():

            return {
                "source_target": verify_source_target(
                    connection
                ),
                "target_records": reconcile_analytics(
                    connection
                )
            }

        def snapshot():

            version = get_offset(
                connection,
                consumer
            )

            return create_snapshot(
                connection,
                f"analytics_version_{version}",
                version
            )

        def scheduled_execution():

            return execute_dag(
                {
                    "consume_change_stream": {
                        "dependencies": [],
                        "function": refresh
                    },
                    "validate_analytics": {
                        "dependencies": [
                            "consume_change_stream"
                        ],
                        "function": validate
                    },
                    "publish_snapshot": {
                        "dependencies": [
                            "validate_analytics"
                        ],
                        "function": snapshot
                    }
                },
                max_attempts=config["max_attempts"],
                retry_delay_seconds=(
                    config["retry_delay_seconds"]
                )
            )

        executions = run_schedule(
            scheduled_execution,
            config["schedule_cycles"],
            config["schedule_interval_seconds"]
        )

        processed = sum(
            execution["result"]["results"][
                "consume_change_stream"
            ]["processed_events"]
            for execution in executions
        )

        latest_version = get_offset(
            connection,
            consumer
        )

        historical = query_as_of(
            connection,
            baseline_version
        )

        current = query_as_of(
            connection,
            latest_version
        )

        historical_map = {
            row["patient_id"]: row
            for row in historical
        }

        current_map = {
            row["patient_id"]: row
            for row in current
        }

        inserted = sorted(
            current_map.keys()
            - historical_map.keys()
        )

        deleted = sorted(
            historical_map.keys()
            - current_map.keys()
        )

        updated = sorted(
            patient_id
            for patient_id in (
                historical_map.keys()
                & current_map.keys()
            )
            if (
                historical_map[patient_id]
                != current_map[patient_id]
            )
        )

        if not (
            len(inserted)
            == len(deleted)
            == len(updated)
            == 1
        ):

            raise RuntimeError(
                "Historical change verification failed"
            )

        restored = restore_snapshot(
            connection,
            "baseline"
        )

        recovery_mismatch = connection.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT *
                FROM temporal.recovered_patients

                EXCEPT

                SELECT
                    patient_id,
                    region,
                    age,
                    prior_visits,
                    no_show
                FROM temporal.snapshot_patients
                WHERE snapshot_name = 'baseline'
            )
        """).fetchone()[0]

        if (
            restored != baseline_count
            or recovery_mismatch
        ):

            raise RuntimeError(
                "Historical recovery verification failed"
            )

        freshness = freshness_status(
            connection,
            consumer,
            config["target_lag_seconds"]
        )

        if freshness["pending_events"]:

            raise RuntimeError(
                "Unconsumed CDC events remain"
            )

        regional_columns = [
            "region",
            "total_patients",
            "total_visits",
            "missed_appointments",
            "average_age",
            "no_show_rate_pct"
        ]

        regional_metrics = [
            dict(zip(regional_columns, row))
            for row in connection.execute("""
                SELECT *
                FROM analytics.regional_metrics
                ORDER BY region
            """).fetchall()
        ]

        event_summary = [
            {
                "operation": operation,
                "event_count": count
            }
            for operation, count
            in connection.execute("""
                SELECT operation, COUNT(*)
                FROM cdc.change_events
                GROUP BY operation
                ORDER BY operation
            """).fetchall()
        ]

        history_count = connection.execute("""
            SELECT COUNT(*)
            FROM cdc.patient_history
        """).fetchone()[0]

        total_events = connection.execute("""
            SELECT COUNT(*)
            FROM cdc.change_events
        """).fetchone()[0]

        processed_events = connection.execute("""
            SELECT COUNT(*)
            FROM cdc.processed_events
            WHERE consumer_name = ?
        """, [consumer]).fetchone()[0]

        if total_events != processed_events:

            raise RuntimeError(
                "Event reconciliation failed"
            )

        report = {
            "pipeline_name": config["pipeline_name"],
            "implementation_mode": (
                config["implementation_mode"]
            ),
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": utc_now(),
            "duration_seconds": round(
                time.perf_counter() - started,
                4
            ),
            "status": "SUCCESS",
            "bootstrap_records": bootstrap_records,
            "bootstrap_processed_events": (
                bootstrap_refresh["processed_events"]
            ),
            "new_change_events": new_events,
            "pending_events_before_refresh": (
                len(pending_events)
            ),
            "incremental_events_processed": processed,
            "append_only_events_consumed": (
                len(append_only_events)
            ),
            "total_change_events": total_events,
            "total_processed_events": processed_events,
            "baseline_version": baseline_version,
            "latest_version": latest_version,
            "baseline_records": baseline_count,
            "current_records": len(current),
            "historical_versions": history_count,
            "restored_records": restored,
            "inserted_patients": inserted,
            "updated_patients": updated,
            "deleted_patients": deleted,
            "freshness": freshness,
            "event_summary": event_summary,
            "regional_metrics": regional_metrics,
            "scheduled_executions": executions,
            "reconciliation_status": (
                verify_source_target(connection)
            ),
            "historical_recovery_status": "PASSED"
        }

    write_json(
        ROOT / config["report_path"],
        report
    )

    LOGGER.info(
        "Pipeline completed | events=%s | records=%s",
        report["total_change_events"],
        report["current_records"]
    )

    print(
        json.dumps(
            {
                "status": report["status"],
                "change_events": total_events,
                "new_events": new_events,
                "processed_incremental_events": processed,
                "historical_versions": history_count,
                "current_records": len(current),
                "restored_records": restored
            },
            indent=2
        )
    )

    return report


if __name__ == "__main__":

    try:

        run_pipeline()

    except Exception:

        LOGGER.exception(
            "Incremental processing failed"
        )

        raise
