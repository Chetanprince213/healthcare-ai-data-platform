# Analytical Warehouse Execution

## Execution Metadata

- Execution date: 2026-11-24 10:03:36
- Run identifier: 5de94ee3-7932-400c-bf5d-b29dccb55ae5
- Status: SUCCESS
- Patient records: 100
- Unique patients: 100
- Invalid records: 0
- Duplicate records: 0
- Benchmark records: 100000
- Reconciliation: PASSED

## Compute Benchmarks


    profile        threads memory_limit minimum_ms average_ms median_ms maximum_ms
    -------        ------- ------------ ---------- ---------- --------- ----------
    small_compute        1 512MB            5.5709     5.6427    5.6402     5.7422
    medium_compute       2 1GB              5.4835     5.6121    5.6349     5.7515




## Implementation

Local DuckDB analytical warehouse with configurable
compute profiles, analytical SQL and query benchmarking.

Snowflake architecture is documented separately.

No Snowflake deployment was performed.
