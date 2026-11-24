# Snowflake Incremental and Temporal Processing

## Streams

Snowflake streams track table changes using an offset
associated with the source object's change history.

Streams expose metadata including:

- METADATA$ACTION
- METADATA$ISUPDATE
- METADATA$ROW_ID

Standard streams support insert, update and delete changes.

An update is represented through insert and delete records
with update metadata.

Append-only streams track inserted rows.

Insert-only streams apply to supported external and
other compatible table types.

Streams can become stale when their required historical
change information is no longer retained.

Each independent consumer should generally have its
own stream.

A stream offset advances when its changes are consumed
through a successfully committed DML transaction.

A SELECT alone does not advance the stream offset.

## Tasks

Tasks execute SQL statements or supported procedures.

Task execution may use user-managed warehouse compute
or serverless compute.

Scheduling supports intervals and CRON expressions.

Triggered tasks can respond to available stream data.

Task graphs support parent-child dependencies.

Task history supports execution monitoring and diagnosis.

Production designs should consider retries, timeouts,
failure suspension and overlapping execution policies.

## Dynamic Tables

Dynamic Tables maintain query-defined analytical results.

Target lag defines a desired freshness objective rather
than an exact refresh schedule.

Refresh modes include:

- AUTO
- FULL
- INCREMENTAL

Initialization may occur on creation or on a scheduled
refresh.

DOWNSTREAM target lag supports dependent refresh designs.

Not all SQL expressions support incremental refresh.

Refresh history should be monitored for failures,
refresh duration and freshness.

## Time Travel

Time Travel provides access to retained historical data.

Supported historical query references include:

- TIMESTAMP
- OFFSET
- STATEMENT

AT and BEFORE provide historical query semantics.

Historical cloning and UNDROP support recovery within
applicable retention limits.

## Retention

Permanent tables generally support up to one day of
Time Travel on Standard Edition.

Eligible higher editions support configurable retention
up to 90 days for permanent tables.

Transient and temporary tables have more limited
retention and no Fail-safe.

Retention configuration must balance recovery needs,
storage consumption and compliance requirements.

## Zero-Copy Cloning

Snowflake cloning initially shares existing storage
rather than physically copying all underlying data.

Subsequent changes can increase storage consumption.

Clones support development, testing and recovery.

The local platform creates physical snapshots instead.

## Fail-safe

Fail-safe is a Snowflake-managed recovery mechanism
for eligible permanent data after Time Travel expires.

It generally provides a seven-day recovery period.

It is not directly queryable by users and is not a
replacement for a deliberate backup strategy.

## Slowly Changing Dimensions

SCD Type 1 overwrites previous attribute values.

SCD Type 2 maintains historical versions with validity
intervals and current-record indicators.

Time Travel and SCD Type 2 address different needs.

SCD Type 2 provides explicitly modeled business history.

Time Travel provides retained historical database state.

## Implementation Boundary

The local implementation provides application CDC,
transactional offsets, incremental SQL MERGE, bounded
scheduling, SCD Type 2 and historical snapshots.

Native Snowflake features are documented but not executed.
