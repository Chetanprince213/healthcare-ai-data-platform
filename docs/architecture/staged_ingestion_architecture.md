# Healthcare Staged File Ingestion

## Overview

A transactional incremental file ingestion pipeline
implemented with Python, DuckDB and Parquet.

All data is synthetic.

## Local Architecture

    Synthetic CSV
        |
        v
    Landing Stage
        |
        v
    Polling File Discovery
        |
        v
    SHA-256 Duplicate Detection
        |
        v
    CSV Schema Validation
        |
        v
    DuckDB COPY into Temporary Table
        |
        v
    Data Quality Validation
        |
        +--> Invalid Files --> Quarantine
        |
        v
    Transactional Target Insertion
        |
        +--> Load History and Audit Records
        |
        v
    Parquet Export and Reconciliation

## Snowflake Stages

A stage identifies a file location used for loading
or unloading data.

### Internal Stages

Snowflake-managed file storage.

User stages are associated with users.

Table stages are associated with tables.

Named internal stages are independently managed objects.

### External Stages

Reference external cloud storage such as Amazon S3,
Azure Blob Storage or Google Cloud Storage.

Storage integrations support delegated access without
embedding long-lived cloud credentials in SQL.

## File Formats

Snowflake supports CSV, JSON, Parquet, Avro, ORC and XML.

File format objects describe how files are interpreted.

The local implementation ingests CSV and exports Parquet.

JSON is used for configuration, reporting and error metadata.

JSON ingestion is covered by the Snowflake reference only.

## Stage Commands

PUT uploads local files to supported internal stages.

GET downloads files from supported internal stages.

LIST displays staged files.

REMOVE deletes staged files.

PUT and GET require a compatible client.

## COPY INTO

COPY INTO table loads staged data into target tables.

COPY INTO location unloads query or table results.

Bulk loading typically uses virtual warehouse compute.

The local implementation executes actual DuckDB COPY
operations for CSV ingestion and Parquet export.

DuckDB COPY is not Snowflake COPY INTO.

## Error Handling

Snowflake COPY supports ON_ERROR options such as:

- ABORT_STATEMENT
- CONTINUE
- SKIP_FILE

VALIDATION_MODE can inspect loading errors for supported
COPY configurations without inserting the data.

The local pipeline uses whole-file rejection and quarantine.

## Snowpipe

Snowpipe provides managed continuous file ingestion
using serverless compute.

Auto-ingest can be triggered by cloud storage notifications.

For AWS, S3 event notifications and supported messaging
configuration can notify Snowpipe about new files.

Snowpipe can also receive supported API-based notifications.

Snowpipe does not require a continuously running
user-managed virtual warehouse for its ingestion compute.

Snowpipe Streaming is a separate row-oriented ingestion
capability and is not implemented in this module.

## Local Polling Analogue

The local implementation uses a bounded polling loop.

It discovers completed CSV files and processes new content.

Files are published through a temporary filename followed
by an atomic rename.

Incomplete files are ignored.

Polling is not equivalent to managed event-driven Snowpipe.

## Idempotency and Deduplication

SHA-256 fingerprints identify previously loaded content.

Renaming a file does not change its content fingerprint.

The load-history table records successful file ingestion.

Patient primary keys and duplicate checks prevent
duplicate patient records.

Conflicting existing patient attributes are rejected.

Updates and change-data-capture semantics are outside
the scope of this append-oriented module.

Snowflake native file-load tracking has its own
metadata rules and retention limitations.

The local hashing implementation is a separate design.

## Data Quality

- Expected CSV column names and order.
- Source-to-stage row reconciliation.
- Mandatory patient identifiers and regions.
- Integer domain validation.
- Valid age ranges.
- Non-negative visit counts.
- Binary no-show values.
- Duplicate patient detection.
- Existing-record conflict detection.
- Target-to-Parquet reconciliation.

## Transactional Reliability

Target insertion and successful load-history recording
occur in the same database transaction.

Invalid input causes transaction rollback.

Quarantine operations occur outside the database
transaction and are not jointly atomic with it.

The implementation assumes one ingestion writer.

## Monitoring

Structured logs capture file discovery, successful loads,
duplicate detection and validation failures.

The ingestion report records source, target and export
counts, file outcomes and execution duration.

Snowflake-specific monitoring references include:

- COPY_HISTORY
- PIPE_USAGE_HISTORY
- SYSTEM$PIPE_STATUS
- SHOW PIPES

## Cost Optimization

Snowpipe has managed ingestion charges.

Cost depends on the applicable ingestion pricing model
and workload characteristics.

Appropriate file sizes, batching and avoiding unnecessary
reprocessing can improve ingestion efficiency.

No Snowflake or AWS resources are created locally.

## Implementation Scope

LOCAL SIMULATION - NOT DEPLOYED TO CLOUD.

Implemented directly:

- Python ingestion application.
- Local file staging.
- DuckDB COPY.
- Incremental polling.
- Content-based file deduplication.
- Transactional target insertion.
- Load history.
- Error auditing and quarantine.
- Parquet export.
- Automated testing.

Snowflake stages, storage integrations, COPY INTO,
Snowpipe and cloud event notifications are studied
through documentation and unexecuted SQL references.
