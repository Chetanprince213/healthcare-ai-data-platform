# Healthcare Warehouse Execution Evidence

## Execution Metadata

- Date: 2026-11-24 09:48:54
- Run ID: 634a4745-83d1-48d4-b0da-bec66626551c
- Implementation: Local DuckDB
- Database catalog: healthcare_analytics
- Analytics schema: healthcare
- Pipeline status: SUCCESS
- Duration: 0.7759 seconds
- DuckDB version: 2.0.0

## Data Quality

- Source records: 100
- Warehouse records: 100
- Unique patients: 100
- Duplicate records: 0
- Invalid records: 0
- Validation: PASSED
- Reconciliation: PASSED
- Idempotency: PASSED
- Catalog/schema resolution: PASSED

## Synthetic Benchmark Workload

Benchmark records: 100000

Benchmark records are repeated synthetic data,
not additional independent patients.

## Warehouse Metadata


    table_catalog        table_schema table_name         table_type
    -------------        ------------ ----------         ----------
    healthcare_analytics healthcare   benchmark_patients BASE TABLE
    healthcare_analytics healthcare   patient_analytics  VIEW
    healthcare_analytics healthcare   patients           BASE TABLE




## Regional Analytics


    region total_patients average_age total_prior_visits missed_appointments no_show_rate_pct patient_volume_rank patient_distribution_pct
    ------ -------------- ----------- ------------------ ------------------- ---------------- ------------------- ------------------------
    S                  34       42.85                144                  20            58.82                   1                     34.0
    E                  33       48.09                143                  11            33.33                   2                     33.0
    N                  33        54.3                128                  10             30.3                   2                     33.0




## Patient Utilization Ranking


    patient_id region prior_visits patient_rank regional_average_visits
    ---------- ------ ------------ ------------ -----------------------
    P0008      E                 8            1                    4.33
    P0059      E                 8            2                    4.33
    P0083      E                 8            3                    4.33
    P0027      N                 8            1                    3.88
    P0042      N                 8            2                    3.88
    P0066      N                 8            3                    3.88
    P0004      S                 8            1                    4.24
    P0007      S                 8            2                    4.24
    P0025      S                 8            3                    4.24




## Compute Performance


    profile        threads memory_limit iterations first_execution_ms minimum_ms average_ms median_ms maximum_ms
    -------        ------- ------------ ---------- ------------------ ---------- ---------- --------- ----------
    small_compute        1 512MB                 5             6.2186     5.1373     5.1828    5.1719     5.2643
    medium_compute       2 1GB                   5             8.5445     5.5889     5.6503    5.6367     5.7796




## Warehouse Automated Tests

    ============================= test session starts =============================
    platform win32 -- Python 3.13.14, pytest-9.2.0, pluggy-1.7.0 -- C:\Users\DELL\Documents\healthcare-ai-platform\.venv\Scripts\python.exe
    cachedir: .pytest_cache
    rootdir: C:\Users\DELL\Documents\healthcare-ai-platform
    plugins: anyio-4.16.0, Faker-40.54.0
    collecting ... collected 18 items

    tests/unit/test_healthcare_warehouse.py::test_warehouse_initialization PASSED [  5%]
    tests/unit/test_healthcare_warehouse.py::test_catalog_schema_resolution PASSED [ 11%]
    tests/unit/test_healthcare_warehouse.py::test_idempotent_ingestion PASSED [ 16%]
    tests/unit/test_healthcare_warehouse.py::test_regional_aggregation PASSED [ 22%]
    tests/unit/test_healthcare_warehouse.py::test_duplicate_rejection PASSED [ 27%]
    tests/unit/test_healthcare_warehouse.py::test_invalid_age_rejection PASSED [ 33%]
    tests/unit/test_healthcare_warehouse.py::test_invalid_no_show_rejection PASSED [ 38%]
    tests/unit/test_healthcare_warehouse.py::test_negative_visits_rejection PASSED [ 44%]
    tests/unit/test_healthcare_warehouse.py::test_missing_identifier_rejection PASSED [ 50%]
    tests/unit/test_healthcare_warehouse.py::test_missing_source PASSED      [ 55%]
    tests/unit/test_healthcare_warehouse.py::test_transaction_rollback PASSED [ 61%]
    tests/unit/test_healthcare_warehouse.py::test_analytics_view PASSED      [ 66%]
    tests/unit/test_healthcare_warehouse.py::test_benchmark_dataset PASSED   [ 72%]
    tests/unit/test_healthcare_warehouse.py::test_query_plan_generation PASSED [ 77%]
    tests/unit/test_healthcare_warehouse.py::test_compute_profile_consistency PASSED [ 83%]
    tests/unit/test_healthcare_warehouse.py::test_metadata_inspection PASSED [ 88%]
    tests/unit/test_healthcare_warehouse.py::test_utilization_ranking PASSED [ 94%]
    tests/unit/test_healthcare_warehouse.py::test_read_only_connection PASSED [100%]

    ============================= 18 passed in 4.57s ==============================


## Complete Project Test Suite

    ..............................................................           [100%]
    62 passed in 121.83s (0:02:01)
    SUCCESS: The process with PID 4648 (child process of PID 9772) has been terminated.
    SUCCESS: The process with PID 9772 (child process of PID 5324) has been terminated.
    SUCCESS: The process with PID 5324 (child process of PID 12664) has been terminated.


## Implementation Scope

Local analytical warehouse implementation.

Snowflake-specific architecture and SQL are reference
material and were not executed in a Snowflake account.

No Snowflake deployment or credit consumption.

Benchmark results do not establish production scalability.
