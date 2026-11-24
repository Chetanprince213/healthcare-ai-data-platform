# Staged Ingestion Execution

## Execution Metadata

- Execution date: 2026-11-24 10:03:36
- Run identifier: 0ada1fb8-f64d-46b3-833b-e108ffae3b4a
- Status: SUCCESS
- Duration: 2.2436 seconds

## Initial Execution

- Inserted records: 0
- Files loaded: 0

## Data Reconciliation

- Source records: 100
- Target records: 100
- Unique patients: 100
- Exported records: 100
- Records inserted on rerun: 0
- Duplicate file detections: 3
- Load history records: 1

## Regional Analytics


    region total_patients
    ------ --------------
    E                  33
    N                  33
    S                  34




## Reliability

- Idempotency: PASSED
- Source reconciliation: PASSED
- Parquet reconciliation: PASSED
- Automated tests: PASSED

## Implementation

Local staged file ingestion using Python, DuckDB and Parquet.

Snowpipe is documented through architecture and SQL
references rather than a managed cloud deployment.
