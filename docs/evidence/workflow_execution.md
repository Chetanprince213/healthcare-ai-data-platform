# Healthcare Workflow Execution

## Execution Metadata

- Execution date: 2026-11-24 10:03:36
- Run identifier: 280f89d8-c2f4-457f-b0d1-5389166bb78a
- Status: SUCCESS
- Source records: 100

## Task Execution


    task                      attempt status  duration_seconds timestamp
    ----                      ------- ------  ---------------- ---------
    bronze_source_ingestion         1 SUCCESS          0.00199 2026-11-24T04:32:45.427487+00:00
    silver_data_validation          1 SUCCESS         0.000358 2026-11-24T04:32:45.428399+00:00
    gold_regional_aggregation       1 SUCCESS         0.000102 2026-11-24T04:32:45.429224+00:00




## Validation

- Source schema validation: PASSED
- Patient uniqueness validation: PASSED
- Regional reconciliation: PASSED
- Automated tests: PASSED

## Implementation

Local Python dependency-aware workflow orchestration.
