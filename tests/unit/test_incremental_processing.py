"""Tests for CDC, incremental refresh and temporal recovery."""

import csv
import uuid

import duckdb
import pytest

from src.snowflake.change_data_capture import (
    initialize_database,
    apply_change,
    bootstrap_source,
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
    query_as_of,
    create_snapshot,
    restore_snapshot
)

from src.snowflake.task_orchestration import (
    execute_dag,
    run_schedule
)


@pytest.fixture
def connection():

    con = duckdb.connect(":memory:")

    initialize_database(con)

    initialize_analytics(con)

    initialize_snapshots(con)

    yield con

    con.close()


def patient(
    identifier="P001",
    region="N",
    age=35,
    visits=2,
    no_show=0
):

    return {
        "patient_id": identifier,
        "region": region,
        "age": age,
        "prior_visits": visits,
        "no_show": no_show
    }


def insert(con, identifier="P001"):

    return apply_change(
        con,
        "INSERT",
        identifier,
        patient(identifier)
    )


def refresh(con, fail=False):

    return consume_changes(
        con,
        "analytics",
        str(uuid.uuid4()),
        inject_failure=fail
    )


def count(con, table):

    return con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]


def test_insert_event(connection):

    insert(connection)

    assert count(
        connection,
        "cdc.change_events"
    ) == 1

    assert get_patient(
        connection,
        "P001"
    )["age"] == 35


def test_update_event(connection):

    insert(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=5)
    )

    assert get_patient(
        connection,
        "P001"
    )["prior_visits"] == 5


def test_delete_event(connection):

    insert(connection)

    apply_change(
        connection,
        "DELETE",
        "P001"
    )

    assert get_patient(
        connection,
        "P001"
    ) is None


def test_duplicate_event_key(connection):

    first = apply_change(
        connection,
        "INSERT",
        "P001",
        patient(),
        event_key="event-001"
    )

    second = apply_change(
        connection,
        "INSERT",
        "P001",
        patient(),
        event_key="event-001"
    )

    assert first == second

    assert count(
        connection,
        "cdc.change_events"
    ) == 1


def test_conflicting_event_key(connection):

    apply_change(
        connection,
        "INSERT",
        "P001",
        patient(),
        event_key="event-001"
    )

    with pytest.raises(ValueError):

        apply_change(
            connection,
            "INSERT",
            "P001",
            patient(age=50),
            event_key="event-001"
        )


def test_duplicate_patient(connection):

    insert(connection)

    with pytest.raises(ValueError):

        insert(connection)


def test_missing_update_target(connection):

    with pytest.raises(ValueError):

        apply_change(
            connection,
            "UPDATE",
            "P001",
            patient()
        )


def test_missing_delete_target(connection):

    with pytest.raises(ValueError):

        apply_change(
            connection,
            "DELETE",
            "P001"
        )


def test_invalid_operation(connection):

    with pytest.raises(ValueError):

        apply_change(
            connection,
            "INVALID",
            "P001"
        )


def test_invalid_patient(connection):

    with pytest.raises(ValueError):

        apply_change(
            connection,
            "INSERT",
            "P001",
            patient(age=999)
        )


def test_standard_stream(connection):

    insert(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=3)
    )

    apply_change(
        connection,
        "DELETE",
        "P001"
    )

    events = read_stream(
        connection,
        "analytics"
    )

    assert [
        event[2]
        for event in events
    ] == [
        "INSERT",
        "UPDATE",
        "DELETE"
    ]


def test_append_only_stream(connection):

    insert(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=3)
    )

    events = consume_append_only(
        connection,
        "append_only"
    )

    assert len(events) == 1

    assert events[0][2] == "INSERT"

    assert consume_append_only(
        connection,
        "append_only"
    ) == []


def test_independent_stream_offsets(connection):

    insert(connection)

    refresh(connection)

    assert read_stream(
        connection,
        "analytics"
    ) == []

    assert len(
        read_stream(
            connection,
            "independent_consumer"
        )
    ) == 1


def test_stream_read_does_not_advance(connection):

    insert(connection)

    read_stream(
        connection,
        "analytics"
    )

    assert get_offset(
        connection,
        "analytics"
    ) == 0


def test_incremental_merge(connection):

    insert(connection)

    result = refresh(connection)

    assert result["processed_events"] == 1

    assert count(
        connection,
        "cdc.current_patients"
    ) == 1


def test_incremental_update(connection):

    insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=9)
    )

    refresh(connection)

    visits = connection.execute("""
        SELECT prior_visits
        FROM cdc.current_patients
    """).fetchone()[0]

    assert visits == 9


def test_incremental_delete(connection):

    insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "DELETE",
        "P001"
    )

    refresh(connection)

    assert count(
        connection,
        "cdc.current_patients"
    ) == 0


def test_idempotent_refresh(connection):

    insert(connection)

    refresh(connection)

    second = refresh(connection)

    assert second["processed_events"] == 0

    assert count(
        connection,
        "cdc.processed_events"
    ) == 1


def test_transaction_rollback(connection):

    insert(connection)

    with pytest.raises(RuntimeError):

        refresh(
            connection,
            fail=True
        )

    assert get_offset(
        connection,
        "analytics"
    ) == 0

    assert count(
        connection,
        "cdc.current_patients"
    ) == 0

    assert count(
        connection,
        "cdc.processed_events"
    ) == 0

    assert refresh(
        connection
    )["processed_events"] == 1


def test_dynamic_region_refresh(connection):

    insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(region="S")
    )

    result = refresh(connection)

    assert result["affected_regions"] == 2

    rows = connection.execute("""
        SELECT region, total_patients
        FROM analytics.regional_metrics
    """).fetchall()

    assert rows == [("S", 1)]


def test_analytics_reconciliation(connection):

    insert(connection, "P001")

    insert(connection, "P002")

    refresh(connection)

    assert reconcile_analytics(
        connection
    ) == 2


def test_scd_type_two(connection):

    insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=7)
    )

    refresh(connection)

    rows = connection.execute("""
        SELECT
            prior_visits,
            is_current
        FROM cdc.patient_history
        ORDER BY valid_from_event
    """).fetchall()

    assert rows == [
        (2, False),
        (7, True)
    ]


def test_historical_query(connection):

    version = insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "UPDATE",
        "P001",
        patient(visits=10)
    )

    refresh(connection)

    historical = query_as_of(
        connection,
        version
    )

    assert historical[0]["prior_visits"] == 2


def test_deleted_patient_history(connection):

    version = insert(connection)

    refresh(connection)

    apply_change(
        connection,
        "DELETE",
        "P001"
    )

    refresh(connection)

    assert len(
        query_as_of(
            connection,
            version
        )
    ) == 1


def test_snapshot_creation(connection):

    version = insert(connection)

    refresh(connection)

    assert create_snapshot(
        connection,
        "baseline",
        version
    ) == 1


def test_snapshot_idempotency(connection):

    version = insert(connection)

    refresh(connection)

    create_snapshot(
        connection,
        "baseline",
        version
    )

    create_snapshot(
        connection,
        "baseline",
        version
    )

    assert count(
        connection,
        "temporal.snapshot_catalog"
    ) == 1


def test_snapshot_recovery(connection):

    version = insert(connection)

    refresh(connection)

    create_snapshot(
        connection,
        "baseline",
        version
    )

    apply_change(
        connection,
        "DELETE",
        "P001"
    )

    refresh(connection)

    assert restore_snapshot(
        connection,
        "baseline"
    ) == 1

    assert count(
        connection,
        "cdc.current_patients"
    ) == 0


def test_missing_snapshot(connection):

    with pytest.raises(ValueError):

        restore_snapshot(
            connection,
            "missing"
        )


def test_invalid_historical_version(connection):

    with pytest.raises(ValueError):

        query_as_of(
            connection,
            999999
        )


def test_target_lag(connection):

    insert(connection)

    refresh(connection)

    status = freshness_status(
        connection,
        "analytics",
        60
    )

    assert status["pending_events"] == 0

    assert status["status"] == "WITHIN_TARGET"


def test_pending_events(connection):

    insert(connection)

    status = freshness_status(
        connection,
        "analytics",
        60
    )

    assert status["pending_events"] == 1


def test_task_dependencies():

    order = []

    result = execute_dag({
        "validation": {
            "dependencies": ["ingestion"],
            "function": lambda: order.append(
                "validation"
            )
        },
        "ingestion": {
            "dependencies": [],
            "function": lambda: order.append(
                "ingestion"
            )
        }
    })

    assert result["status"] == "SUCCESS"

    assert order == [
        "ingestion",
        "validation"
    ]


def test_circular_dependencies():

    with pytest.raises(ValueError):

        execute_dag({
            "first": {
                "dependencies": ["second"],
                "function": lambda: None
            },
            "second": {
                "dependencies": ["first"],
                "function": lambda: None
            }
        })


def test_task_retry():

    attempts = []

    def operation():

        attempts.append(1)

        if len(attempts) == 1:

            raise OSError(
                "Transient failure"
            )

        return "SUCCESS"

    result = execute_dag(
        {
            "refresh": {
                "dependencies": [],
                "function": operation
            }
        },
        retry_delay_seconds=0
    )

    assert len(attempts) == 2

    assert result["history"][0]["status"] == "FAILED"

    assert result["history"][1]["status"] == "SUCCESS"


def test_downstream_failure_propagation():

    downstream = []

    def failure():

        raise ValueError(
            "Validation failed"
        )

    with pytest.raises(ValueError):

        execute_dag({
            "validation": {
                "dependencies": [],
                "function": failure
            },
            "publish": {
                "dependencies": ["validation"],
                "function": lambda: downstream.append(1)
            }
        })

    assert downstream == []


def test_scheduled_execution():

    result = run_schedule(
        lambda: "SUCCESS",
        cycles=3,
        interval_seconds=0
    )

    assert len(result) == 3


def test_source_bootstrap(
    connection,
    tmp_path
):

    source = tmp_path / "patients.csv"

    with source.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as output:

        writer = csv.DictWriter(
            output,
            fieldnames=[
                "patient_id",
                "region",
                "age",
                "prior_visits",
                "no_show"
            ]
        )

        writer.writeheader()

        writer.writerow(patient())

    assert bootstrap_source(
        connection,
        source
    ) == 1

    assert bootstrap_source(
        connection,
        source
    ) == 0

    assert count(
        connection,
        "cdc.change_events"
    ) == 1
