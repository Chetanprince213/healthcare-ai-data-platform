# Staged Ingestion Execution Evidence

## Execution Metadata

- Date: 2026-11-24 09:55:12
- Run ID: 68f394d1-7566-40c3-8116-a29010071585
- Implementation: Local DuckDB and polling
- Pipeline status: SUCCESS
- Duration: 2.2351 seconds

## Commands Executed

    python -m src.snowflake.staged_ingestion
    python -m src.snowflake.staged_ingestion
    python -m pytest tests/unit/test_staged_ingestion.py -v
    python -m pytest -q

## Initial Execution

- Source records: 100
- Target records: 100
- Inserted records: 100
- Files loaded: 1
- Exported records: 100

## Repeat Execution

- Source records: 100
- Target records: 100
- Unique patients: 100
- Inserted records: 0
- Duplicate file detections: 3
- Quarantined files: 0
- Load history records: 1
- Exported records: 100

## Validation

- Source-to-target reconciliation: PASSED
- Target-to-Parquet reconciliation: PASSED
- Duplicate file detection: PASSED
- Idempotency: PASSED
- Baseline checkpoint: PASSED

## Regional Analytics


    region total_patients
    ------ --------------
    E                  33
    N                  33
    S                  34




## File Processing Results


    file_name                     status  inserted_rows
    ---------                     ------  -------------
    patients_cf201b267d0f4f74.csv SKIPPED             0
    patients_cf201b267d0f4f74.csv SKIPPED             0
    patients_cf201b267d0f4f74.csv SKIPPED             0




## Automated Ingestion Tests

    ============================= test session starts =============================
    platform win32 -- Python 3.13.14, pytest-9.2.0, pluggy-1.7.0 -- C:\Users\DELL\Documents\healthcare-ai-platform\.venv\Scripts\python.exe
    cachedir: .pytest_cache
    rootdir: C:\Users\DELL\Documents\healthcare-ai-platform
    plugins: anyio-4.16.0, Faker-40.54.0
    collecting ... collected 26 items

    tests/unit/test_staged_ingestion.py::test_file_hash PASSED               [  3%]
    tests/unit/test_staged_ingestion.py::test_stage_source PASSED            [  7%]
    tests/unit/test_staged_ingestion.py::test_csv_row_count PASSED           [ 11%]
    tests/unit/test_staged_ingestion.py::test_bulk_copy PASSED               [ 15%]
    tests/unit/test_staged_ingestion.py::test_duplicate_file_skip PASSED     [ 19%]
    tests/unit/test_staged_ingestion.py::test_renamed_duplicate_file PASSED  [ 23%]
    tests/unit/test_staged_ingestion.py::test_incremental_ingestion PASSED   [ 26%]
    tests/unit/test_staged_ingestion.py::test_existing_record_deduplication PASSED [ 30%]
    tests/unit/test_staged_ingestion.py::test_invalid_schema PASSED          [ 34%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row0] PASSED   [ 38%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row1] PASSED   [ 42%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row2] PASSED   [ 46%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row3] PASSED   [ 50%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row4] PASSED   [ 53%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row5] PASSED   [ 57%]
    tests/unit/test_staged_ingestion.py::test_invalid_records[row6] PASSED   [ 61%]
    tests/unit/test_staged_ingestion.py::test_duplicate_ids_within_file PASSED [ 65%]
    tests/unit/test_staged_ingestion.py::test_transaction_rollback PASSED    [ 69%]
    tests/unit/test_staged_ingestion.py::test_conflicting_patient_rejection PASSED [ 73%]
    tests/unit/test_staged_ingestion.py::test_load_history PASSED            [ 76%]
    tests/unit/test_staged_ingestion.py::test_error_audit PASSED             [ 80%]
    tests/unit/test_staged_ingestion.py::test_polling_discovery PASSED       [ 84%]
    tests/unit/test_staged_ingestion.py::test_partial_file_ignored PASSED    [ 88%]
    tests/unit/test_staged_ingestion.py::test_repeated_polling PASSED        [ 92%]
    tests/unit/test_staged_ingestion.py::test_parquet_export PASSED          [ 96%]
    tests/unit/test_staged_ingestion.py::test_repeatable_parquet_export PASSED [100%]

    ============================= 26 passed in 3.95s ==============================


## Complete Project Tests

    ........................................................................ [ 81%]
    ................                                                         [100%]
    88 passed in 37.06s
    SUCCESS: The process with PID 9412 (child process of PID 7612) has been terminated.
    SUCCESS: The process with PID 7612 (child process of PID 13700) has been terminated.
    SUCCESS: The process with PID 13700 (child process of PID 3988) has been terminated.


## Errors

No unhandled errors occurred in these successful runs.

Validation failure and quarantine behavior are covered
by automated tests.

## Implementation Scope

LOCAL SIMULATION - NOT DEPLOYED TO CLOUD.

DuckDB COPY, local file staging, incremental polling,
load history and Parquet export are implemented directly.

Snowflake stages, COPY INTO and Snowpipe are covered
through architecture documentation and SQL references.

No actual Snowflake or AWS deployment was performed.

No cloud service charges were incurred by this script.
