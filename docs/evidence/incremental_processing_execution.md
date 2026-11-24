# Healthcare Incremental Processing Execution

## Execution Metadata

- Execution date: 2026-11-24 10:10:31
- Run identifier: 32b576ac-52b6-46b7-b032-29814d1c663b
- Execution status: SUCCESS
- Implementation: Local DuckDB analogue

## Execution Commands

    python -m src.snowflake.incremental_pipeline

    python -m pytest tests/unit/test_incremental_processing.py -v

    python -m pytest -q

## Initial Execution

- Bootstrap records: 100
- New change events: 3
- Incremental events processed: 3

## Change Event Reconciliation

- Total change events: 103
- Total processed events: 103
- Pending events: 0


    operation event_count
    --------- -----------
    DELETE              1
    INSERT            101
    UPDATE              1




## Temporal History

- Baseline version: 100
- Latest version: 103
- Baseline records: 100
- Current records: 100
- Historical versions: 102
- Inserted patients: SYNTHETIC-CDC-001
- Updated patients: P0001
- Deleted patients: P0002
- Restored records: 100
- Historical recovery: PASSED

## Incremental Idempotency

- New events on rerun: 0
- Events processed on rerun: 0
- Duplicate historical versions: None detected
- Reconciliation: PASSED

## Refresh Freshness

- Target lag seconds: 60
- Pending event age seconds: 0.0
- Freshness status: WITHIN_TARGET

## Regional Analytics


    region total_patients total_visits missed_appointments average_age no_show_rate_pct
    ------ -------------- ------------ ------------------- ----------- ----------------
    E                  32          140                  10        48.5            31.25
    N                  35          133                  10       54.06            28.57
    S                  33          143                  19       42.39            57.58




## Scheduled Task Execution


    Cycle Task                  Attempt Status  Duration
    ----- ----                  ------- ------  --------
        1 consume_change_stream       1 SUCCESS  0.03447
        1 validate_analytics          1 SUCCESS 0.023185
        1 publish_snapshot            1 SUCCESS 0.004186
        2 consume_change_stream       1 SUCCESS 0.055525
        2 validate_analytics          1 SUCCESS 0.020724
        2 publish_snapshot            1 SUCCESS 0.004691




## Component Test Results

    ============================= test session starts =============================
    platform win32 -- Python 3.13.14, pytest-9.2.0, pluggy-1.7.0 -- C:\Users\DELL\Documents\healthcare-ai-platform\.venv\Scripts\python.exe
    cachedir: .pytest_cache
    rootdir: C:\Users\DELL\Documents\healthcare-ai-platform
    plugins: anyio-4.16.0, Faker-40.54.0
    collecting ... collected 37 items

    tests/unit/test_incremental_processing.py::test_insert_event PASSED      [  2%]
    tests/unit/test_incremental_processing.py::test_update_event PASSED      [  5%]
    tests/unit/test_incremental_processing.py::test_delete_event PASSED      [  8%]
    tests/unit/test_incremental_processing.py::test_duplicate_event_key PASSED [ 10%]
    tests/unit/test_incremental_processing.py::test_conflicting_event_key PASSED [ 13%]
    tests/unit/test_incremental_processing.py::test_duplicate_patient PASSED [ 16%]
    tests/unit/test_incremental_processing.py::test_missing_update_target PASSED [ 18%]
    tests/unit/test_incremental_processing.py::test_missing_delete_target PASSED [ 21%]
    tests/unit/test_incremental_processing.py::test_invalid_operation PASSED [ 24%]
    tests/unit/test_incremental_processing.py::test_invalid_patient PASSED   [ 27%]
    tests/unit/test_incremental_processing.py::test_standard_stream PASSED   [ 29%]
    tests/unit/test_incremental_processing.py::test_append_only_stream PASSED [ 32%]
    tests/unit/test_incremental_processing.py::test_independent_stream_offsets PASSED [ 35%]
    tests/unit/test_incremental_processing.py::test_stream_read_does_not_advance PASSED [ 37%]
    tests/unit/test_incremental_processing.py::test_incremental_merge PASSED [ 40%]
    tests/unit/test_incremental_processing.py::test_incremental_update PASSED [ 43%]
    tests/unit/test_incremental_processing.py::test_incremental_delete PASSED [ 45%]
    tests/unit/test_incremental_processing.py::test_idempotent_refresh PASSED [ 48%]
    tests/unit/test_incremental_processing.py::test_transaction_rollback PASSED [ 51%]
    tests/unit/test_incremental_processing.py::test_dynamic_region_refresh PASSED [ 54%]
    tests/unit/test_incremental_processing.py::test_analytics_reconciliation PASSED [ 56%]
    tests/unit/test_incremental_processing.py::test_scd_type_two PASSED      [ 59%]
    tests/unit/test_incremental_processing.py::test_historical_query PASSED  [ 62%]
    tests/unit/test_incremental_processing.py::test_deleted_patient_history PASSED [ 64%]
    tests/unit/test_incremental_processing.py::test_snapshot_creation PASSED [ 67%]
    tests/unit/test_incremental_processing.py::test_snapshot_idempotency PASSED [ 70%]
    tests/unit/test_incremental_processing.py::test_snapshot_recovery PASSED [ 72%]
    tests/unit/test_incremental_processing.py::test_missing_snapshot PASSED  [ 75%]
    tests/unit/test_incremental_processing.py::test_invalid_historical_version PASSED [ 78%]
    tests/unit/test_incremental_processing.py::test_target_lag PASSED        [ 81%]
    tests/unit/test_incremental_processing.py::test_pending_events PASSED    [ 83%]
    tests/unit/test_incremental_processing.py::test_task_dependencies PASSED [ 86%]
    tests/unit/test_incremental_processing.py::test_circular_dependencies PASSED [ 89%]
    tests/unit/test_incremental_processing.py::test_task_retry PASSED        [ 91%]
    tests/unit/test_incremental_processing.py::test_downstream_failure_propagation PASSED [ 94%]
    tests/unit/test_incremental_processing.py::test_scheduled_execution PASSED [ 97%]
    tests/unit/test_incremental_processing.py::test_source_bootstrap PASSED  [100%]

    ============================= 37 passed in 5.43s ==============================


## Platform Regression Results

    ........................................................................ [ 53%]
    ..............................................................           [100%]
    134 passed in 44.52s
    SUCCESS: The process with PID 7444 (child process of PID 6112) has been terminated.
    SUCCESS: The process with PID 6112 (child process of PID 8564) has been terminated.
    SUCCESS: The process with PID 8564 (child process of PID 13968) has been terminated.


## Validation Status

- Change Data Capture: PASSED
- Transactional processing: PASSED
- Incremental refresh: PASSED
- Historical recovery: PASSED
- Automated testing: PASSED

## Implementation Boundary

LOCAL SIMULATION - NOT DEPLOYED TO CLOUD.

Python and DuckDB execute the local processing pipeline.

Snowflake Streams, Tasks, Dynamic Tables, Time Travel,
zero-copy cloning and Fail-safe remain documented
technical references.

No managed cloud infrastructure was provisioned.
