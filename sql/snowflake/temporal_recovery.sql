-- Snowflake historical recovery reference.
-- Not executed locally.

USE DATABASE HEALTHCARE_PLATFORM;

USE SCHEMA INCREMENTAL;

-- Configure Time Travel retention.
-- Supported retention depends on edition and table type.

ALTER TABLE PATIENT_SOURCE
    SET DATA_RETENTION_TIME_IN_DAYS = 1;

SHOW TABLES LIKE 'PATIENT_SOURCE';

-- Historical query using a relative offset.

SELECT *
FROM PATIENT_SOURCE
AT (OFFSET => -3600);

-- Historical query using a timestamp.

SELECT *
FROM PATIENT_SOURCE
AT (
    TIMESTAMP => DATEADD(
        'MINUTE',
        -10,
        CURRENT_TIMESTAMP()
    )
);

-- Historical query before a specific statement.
-- Replace the placeholder with a valid query identifier.

-- SELECT *
-- FROM PATIENT_SOURCE
-- BEFORE (
--     STATEMENT => '<query-id>'
-- );

-- Zero-copy clone of the current table.

CREATE TABLE IF NOT EXISTS PATIENT_SOURCE_CLONE
    CLONE PATIENT_SOURCE;

-- Historical zero-copy clone.

-- CREATE TABLE PATIENT_HISTORICAL_CLONE
--     CLONE PATIENT_SOURCE
--     AT (OFFSET => -3600);

-- Recovery from historical data into a separate table.

-- CREATE TABLE PATIENT_RECOVERY AS
-- SELECT *
-- FROM PATIENT_SOURCE
-- AT (OFFSET => -3600);

-- Recover a dropped object within its retention period.

-- UNDROP TABLE PATIENT_SOURCE;

-- UNDROP SCHEMA INCREMENTAL;

-- UNDROP DATABASE HEALTHCARE_PLATFORM;

-- Query history helps identify recovery statements.

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(
        RESULT_LIMIT => 100
    )
);

-- Transient tables have limited Time Travel retention
-- and no Fail-safe.

CREATE TRANSIENT TABLE IF NOT EXISTS
    PATIENT_TRANSIENT_REFERENCE (
        PATIENT_ID VARCHAR
    )
    DATA_RETENTION_TIME_IN_DAYS = 1;

-- Temporary tables are session-scoped and have no Fail-safe.

CREATE TEMPORARY TABLE IF NOT EXISTS
    PATIENT_TEMPORARY_REFERENCE (
        PATIENT_ID VARCHAR
    );

-- Fail-safe is a Snowflake-managed recovery mechanism.
-- It is not a user-queryable Time Travel extension.
