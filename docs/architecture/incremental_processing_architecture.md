# Healthcare Incremental Processing Architecture

## Executive Overview

The healthcare platform implements transactional change
tracking, incremental analytical refresh, historical
versioning and non-destructive snapshot recovery.

## Architecture

    Synthetic Healthcare Source
        |
        v
    Source Patient State
        |
        v
    Transactional Change Event Log
        |
        +--> Independent Append-Only Consumer
        |
        v
    Standard Change Consumer
        |
        v
    Incremental MERGE and DELETE
        |
        +--> SCD Type 2 Patient History
        |
        v
    Regional Analytical Refresh
        |
        +--> Freshness Monitoring
        |
        v
    Historical Snapshots and Recovery

## Change Data Capture

Source mutations and change events are committed together.

Each event contains:

- Unique event identifier
- Idempotency key
- Patient identifier
- Operation type
- Before image
- After image
- UTC change timestamp

The event log is append-only by application convention.

## Stream Consumption

Consumers maintain independent event offsets.

Reading events does not advance an offset.

Successful transactional consumption advances the offset.

Failed processing rolls back target mutations, historical
versions, processed-event records and consumer offsets.

The analytical target supports one designated consumer.

## Standard and Append-Only Streams

The standard local consumer processes inserts, updates
and deletes.

The append-only local consumer processes insert events
and maintains an independent offset.

Snowflake standard streams expose net table changes
between offsets, including metadata columns.

The local event log preserves individual application
events and is not identical to Snowflake stream semantics.

## Incremental Refresh

SQL MERGE synchronizes changed patient records.

Delete events remove records from the current target.

Only regions affected by change events are recalculated.

This reduces unnecessary full-table aggregation work.

## Dynamic Table Analogue

Regional metrics are persisted as derived analytical data.

Refresh is explicitly invoked by the local scheduler.

Snowflake Dynamic Tables instead use managed refresh
planning and target-lag objectives.

## Target Lag

The pipeline measures the age of the oldest pending event
and compares it against a configured freshness threshold.

This is a local monitoring metric, not Snowflake's exact
managed target-lag calculation.

## Task Orchestration

The task dependency graph contains:

- Change consumption
- Analytical validation
- Snapshot publication

Downstream tasks run only after successful dependencies.

Transient operating-system failures use bounded retries
and exponential backoff.

The scheduler executes a finite number of cycles.

It is not a persistent production scheduling service.

## Historical Versioning

SCD Type 2 history maintains:

- Patient identifier
- Historical attribute values
- Valid-from event version
- Valid-to event version
- Current-record indicator

Updates close the previous version and create a new one.

Deletes close the current version without removing history.

## Time Travel Analogue

Historical state is reconstructed using event-version
validity intervals.

Named snapshots preserve historical records.

Recovery writes into a separate recovery table without
overwriting the current analytical target.

Snapshots are physical copies, not zero-copy clones.

## Transaction Management

Change creation, event consumption and snapshot recovery
each use explicit database transactions.

The synthetic demonstration batch contains individually
committed source changes.

An incomplete demonstration batch requires review.

The local deployment assumes a single database writer.

## Data Quality

Validation includes patient uniqueness, mandatory fields,
age constraints, utilization constraints, event uniqueness,
source-target equality and historical reconciliation.

## Deployment Scope

Python and DuckDB components are executed locally.

Snowflake Streams, Tasks, Dynamic Tables, Time Travel,
zero-copy cloning and Fail-safe are technical references.

No managed Snowflake service is provisioned.

Only synthetic healthcare records are permitted.
