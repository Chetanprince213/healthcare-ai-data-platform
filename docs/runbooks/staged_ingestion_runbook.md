# Staged Ingestion Operational Runbook

## Execution

From the project root:

    python -m src.snowflake.staged_ingestion

## Testing

    python -m pytest tests/unit/test_staged_ingestion.py -v

## Landing Directory

    data/staging/healthcare/landing/

Only completed CSV files are processed.

Files ending in .part are ignored.

## Database

    data/staging/healthcare/staged_ingestion.duckdb

Catalog: staged_ingestion

Schema: ingestion

Tables:

- patients
- load_history
- load_errors

The catalog and schema names are intentionally different.

## Monitoring

    logs/ingestion/staged_ingestion.log

    data/staging/healthcare/reports/ingestion_report.json

## Export

    data/staging/healthcare/exports/patients.parquet

## Quarantine

    data/staging/healthcare/quarantine/

Review the associated error JSON before correcting
and restaging a rejected file.

Do not bypass data validation.

## Duplicate Files

Previously loaded file content is skipped.

This is an expected successful idempotency outcome.

## Database Locks

Use a single ingestion writer.

Close conflicting database connections before retrying.

## Recovery

Correct the underlying issue and rerun the pipeline.

Successful file loads remain recorded in load history.

Target insertion and load history are transactional.

Quarantine and export files are separate filesystem
operations.

## Security

Use synthetic healthcare data only.

Do not commit runtime databases, raw logs,
quarantine files or credentials.
