# Snowflake File Ingestion Technical Reference

## Internal and External Stages

Internal stages use Snowflake-managed storage.

External stages reference cloud object storage.

Storage integrations provide delegated access.

## Bulk Ingestion

COPY INTO loads staged files using warehouse compute.

Named file formats define source parsing behavior.

VALIDATION_MODE supports inspection of loading errors.

ON_ERROR determines applicable failure handling behavior.

## Continuous File Ingestion

Snowpipe provides managed serverless ingestion.

AUTO_INGEST can use cloud storage notifications.

Snowpipe Streaming is a separate row-oriented service.

## Monitoring

COPY_HISTORY exposes file loading information.

SYSTEM$PIPE_STATUS provides pipe execution information.

PIPE_USAGE_HISTORY provides ingestion usage information.

## Implementation Boundary

The local platform implements transactional DuckDB COPY,
polling, content fingerprinting and Parquet export.

Snowflake-specific SQL is maintained as an architecture
reference and has not been executed in a cloud account.
