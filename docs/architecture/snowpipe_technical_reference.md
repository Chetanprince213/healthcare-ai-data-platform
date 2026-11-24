# Snowflake Stages, COPY INTO and Snowpipe

## Stage

A location used to access files for loading or unloading.

Internal stages use Snowflake-managed storage.

External stages reference external cloud storage.

## File Format

Defines how staged files are interpreted.

Examples include CSV, JSON and Parquet.

## COPY INTO

Loads staged files into a target table or unloads
table and query results into a stage.

## Snowpipe

A managed continuous file ingestion service.

Auto-ingest can use cloud event notifications to
discover new staged files.

## Bulk Loading versus Snowpipe

Bulk COPY is suitable for controlled batch ingestion
using virtual warehouse compute.

Snowpipe supports managed continuous file ingestion
using serverless resources.

## Storage Integration

Provides delegated cloud storage access and avoids
embedding long-lived credentials in stage definitions.

## Duplicate Prevention

Native Snowflake load history tracks loaded files
under platform-specific rules.

The local pipeline implements SHA-256 content hashes
and target primary-key validation.

## Error Handling

ON_ERROR controls supported COPY loading behavior.

VALIDATION_MODE inspects supported loading errors.

Rejected local files are copied to quarantine with
an error report.

## Monitoring

COPY_HISTORY provides load-history information.

SYSTEM$PIPE_STATUS provides pipe status information.

PIPE_USAGE_HISTORY provides pipe usage information.

## Experience Boundary

The project implements local ingestion and polling.

Actual Snowflake-managed Snowpipe and cloud event
notifications have not been deployed.
