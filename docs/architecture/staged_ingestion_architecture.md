# Healthcare Staged Ingestion Architecture

## Processing Flow

    Synthetic CSV
        |
        v
    Atomic Landing Stage
        |
        v
    File Discovery and SHA-256 Fingerprinting
        |
        v
    DuckDB COPY and Schema Validation
        |
        +--> Invalid Data --> Quarantine
        |
        v
    Transactional Target Loading
        |
        +--> Load History and Error Audit
        |
        v
    Parquet Export and Reconciliation

## Stage Management

Internal Snowflake stages use managed storage.

User stages and table stages are associated with
their respective objects.

Named stages are independently managed.

External stages reference cloud object storage.

Storage integrations enable delegated access without
embedding long-lived credentials.

## File Formats

CSV ingestion and Parquet export are implemented locally.

JSON is used for configuration and operational reports.

Snowflake JSON, CSV and Parquet format definitions are
included in the SQL reference.

## Bulk Loading

Snowflake COPY INTO table loads staged data.

COPY INTO location unloads table or query results.

PUT, GET, LIST and REMOVE support stage file management,
subject to client and stage compatibility.

## Continuous Ingestion

Snowpipe provides managed serverless file ingestion.

Cloud storage notifications can trigger auto-ingest.

The local application implements bounded polling rather
than managed cloud event notifications.

Snowpipe Streaming is a separate row-oriented capability.

## Error Handling

Snowflake supports ON_ERROR and VALIDATION_MODE options
for applicable loading configurations.

The local pipeline rejects invalid files and records
quarantine metadata.

## Idempotency

SHA-256 fingerprints identify duplicate file content.

Primary keys prevent duplicate patient records.

Conflicting existing records are rejected.

The local content-hash implementation differs from
Snowflake native file-load tracking.

## Transactions

Target insertion and successful load history are committed
together.

Quarantine and export filesystem operations are separate
from the database transaction.

The implementation assumes a single ingestion writer.

## Monitoring

Local monitoring includes structured logs, execution
reports, load history and error auditing.

Snowflake monitoring references include COPY_HISTORY,
PIPE_USAGE_HISTORY and SYSTEM$PIPE_STATUS.

## Cost Management

Snowpipe uses managed ingestion resources.

Appropriate file sizes, batching and duplicate prevention
can improve ingestion efficiency.

Actual charges depend on the applicable pricing model.

## Deployment Scope

Python, DuckDB, CSV and Parquet are executed locally.

Snowflake stages, storage integrations and Snowpipe are
documented through unexecuted SQL references.

No cloud infrastructure is provisioned.
