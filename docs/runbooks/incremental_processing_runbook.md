# Healthcare Incremental Processing Runbook

## Execute Pipeline

    python -m src.snowflake.incremental_pipeline

## Configuration

    configs/incremental/incremental_processing.json

## Automated Tests

    python -m pytest tests/unit/test_incremental_processing.py -v

## Operational Monitoring

    logs/incremental/incremental_processing.log

    data/incremental/reports/incremental_metrics.json

## Database

    data/incremental/healthcare_incremental.duckdb

## Change Event Inspection

    SELECT *
    FROM cdc.change_events
    ORDER BY event_id;

## Consumer Offset Inspection

    SELECT *
    FROM cdc.consumer_offsets;

## Historical Record Inspection

    SELECT *
    FROM cdc.patient_history
    ORDER BY patient_id, valid_from_event;

## Snapshot Inspection

    SELECT *
    FROM temporal.snapshot_catalog;

## Refresh Monitoring

    SELECT *
    FROM analytics.refresh_history
    ORDER BY refreshed_at DESC;

## Failure Recovery

Inspect the application traceback and execution log.

Failed consumer transactions do not advance offsets.

Correct the failure and rerun the consumer.

An incomplete synthetic source-change batch requires
inspection of the committed change events.

Do not delete consumer offsets to force reprocessing.

Do not modify the event log manually.

## Historical Recovery

Restore named snapshots into the separate recovery table.

Validate recovered records before any downstream use.

The recovery operation does not replace the current
analytical target.

## Operational Limitations

The scheduler is bounded and runs only while the
application process is active.

The database assumes a single writer.

The local implementation does not provide native
Snowflake stream retention, Fail-safe or zero-copy cloning.

## Security

Use synthetic data only.

Runtime databases, logs and generated reports must remain
outside version control.
