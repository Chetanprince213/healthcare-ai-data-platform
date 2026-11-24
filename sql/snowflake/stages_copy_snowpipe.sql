-- SNOWFLAKE REFERENCE ONLY.
-- Not executed by the local project.
-- Actual execution requires account privileges.
-- Cloud execution may incur charges.

USE DATABASE HEALTHCARE_DB;

USE SCHEMA ANALYTICS;

-- =====================================================
-- 1. FILE FORMATS
-- =====================================================

CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;

CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_PARQUET_FORMAT
    TYPE = PARQUET;

-- =====================================================
-- 2. NAMED INTERNAL STAGE
-- =====================================================

CREATE STAGE IF NOT EXISTS HEALTHCARE_INTERNAL_STAGE
    FILE_FORMAT = HEALTHCARE_CSV_FORMAT;

SHOW STAGES;

DESC STAGE HEALTHCARE_INTERNAL_STAGE;

-- User stage: @~
-- Table stage: @%PATIENTS
-- Named stage: @HEALTHCARE_INTERNAL_STAGE

-- PUT and GET require a supported Snowflake client.
-- Replace local file paths before execution.

-- PUT file://C:/synthetic/patients.csv
--     @HEALTHCARE_INTERNAL_STAGE
--     AUTO_COMPRESS = TRUE;

LIST @HEALTHCARE_INTERNAL_STAGE;

-- GET @HEALTHCARE_INTERNAL_STAGE
--     file://C:/synthetic/downloads/;

-- REMOVE @HEALTHCARE_INTERNAL_STAGE
--     PATTERN = '.*patients.*';

-- =====================================================
-- 3. TARGET TABLE
-- =====================================================

CREATE TABLE IF NOT EXISTS PATIENTS_RAW (
    PATIENT_ID VARCHAR,
    REGION VARCHAR,
    AGE INTEGER,
    PRIOR_VISITS INTEGER,
    NO_SHOW INTEGER
);

-- =====================================================
-- 4. VALIDATE BEFORE LOADING
-- =====================================================

COPY INTO PATIENTS_RAW
FROM @HEALTHCARE_INTERNAL_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'HEALTHCARE_CSV_FORMAT'
)
VALIDATION_MODE = RETURN_ALL_ERRORS;

-- =====================================================
-- 5. BULK COPY INTO TABLE
-- =====================================================

COPY INTO PATIENTS_RAW
FROM @HEALTHCARE_INTERNAL_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'HEALTHCARE_CSV_FORMAT'
)
PATTERN = '.*[.]csv([.]gz)?'
ON_ERROR = 'ABORT_STATEMENT';

-- ON_ERROR alternatives include CONTINUE and SKIP_FILE.
-- FORCE = TRUE can bypass normal load-history checks.
-- Use FORCE carefully to avoid duplicate data.

-- =====================================================
-- 6. FILE METADATA
-- =====================================================

SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    METADATA$FILE_ROW_NUMBER AS SOURCE_ROW,
    $1 AS PATIENT_ID,
    $2 AS REGION
FROM @HEALTHCARE_INTERNAL_STAGE;

-- =====================================================
-- 7. COPY INTO LOCATION / UNLOAD
-- =====================================================

CREATE STAGE IF NOT EXISTS HEALTHCARE_EXPORT_STAGE
    FILE_FORMAT = HEALTHCARE_PARQUET_FORMAT;

COPY INTO @HEALTHCARE_EXPORT_STAGE/patients/
FROM (
    SELECT
        PATIENT_ID,
        REGION,
        AGE,
        PRIOR_VISITS,
        NO_SHOW
    FROM PATIENTS_RAW
)
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = SNAPPY
)
OVERWRITE = TRUE;

-- =====================================================
-- 8. EXTERNAL STAGE / STORAGE INTEGRATION
-- =====================================================

-- Illustrative AWS configuration only.
-- Replace all placeholders and configure IAM trust.
-- No real AWS resources are provisioned by this project.

CREATE STORAGE INTEGRATION IF NOT EXISTS HEALTHCARE_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN =
        'arn:aws:iam::000000000000:role/example-role'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://example-healthcare-bucket/landing/'
    );

DESC INTEGRATION HEALTHCARE_S3_INT;

CREATE STAGE IF NOT EXISTS HEALTHCARE_EXTERNAL_STAGE
    URL = 's3://example-healthcare-bucket/landing/'
    STORAGE_INTEGRATION = HEALTHCARE_S3_INT
    FILE_FORMAT = HEALTHCARE_CSV_FORMAT;

-- =====================================================
-- 9. SNOWPIPE AUTO-INGEST
-- =====================================================

-- AWS event notification configuration is also required.
-- Snowpipe uses managed serverless ingestion compute.

CREATE PIPE IF NOT EXISTS HEALTHCARE_PATIENT_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO PATIENTS_RAW
FROM @HEALTHCARE_EXTERNAL_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'HEALTHCARE_CSV_FORMAT'
)
ON_ERROR = 'SKIP_FILE';

SHOW PIPES;

DESC PIPE HEALTHCARE_PATIENT_PIPE;

SELECT SYSTEM$PIPE_STATUS(
    'HEALTHCARE_DB.ANALYTICS.HEALTHCARE_PATIENT_PIPE'
);

-- Pause and resume ingestion.

ALTER PIPE HEALTHCARE_PATIENT_PIPE
    SET PIPE_EXECUTION_PAUSED = TRUE;

ALTER PIPE HEALTHCARE_PATIENT_PIPE
    SET PIPE_EXECUTION_PAUSED = FALSE;

-- REFRESH can queue eligible recently staged files.
-- It is not a general unlimited historical backfill.

ALTER PIPE HEALTHCARE_PATIENT_PIPE REFRESH;

-- =====================================================
-- 10. LOAD HISTORY AND MONITORING
-- =====================================================

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'PATIENTS_RAW',
        START_TIME => DATEADD(
            'DAY', -1, CURRENT_TIMESTAMP()
        )
    )
);

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
        DATE_RANGE_START => DATEADD(
            'DAY', -1, CURRENT_TIMESTAMP()
        )
    )
);

-- Additional account-level monitoring is available
-- through ACCOUNT_USAGE views, subject to privileges
-- and metadata latency.
