# Healthcare Warehouse Operational Runbook

## Prerequisites

- Python virtual environment.
- Synthetic healthcare source dataset.
- Dependencies in requirements-warehouse.txt.

## Execution

From the repository root:

    python -m src.warehouse.healthcare_warehouse

## Testing

    python -m pytest tests/unit/test_healthcare_warehouse.py -v

## Runtime Artifacts

- data/warehouse/healthcare_analytics.duckdb
- data/warehouse/reports/warehouse_metrics.json
- logs/warehouse/warehouse_execution.log

## Failure Handling

### Catalog and Schema Ambiguity

The database catalog must not have the same name
as the healthcare schema.

Use healthcare_analytics.duckdb as the database file
and healthcare as the schema.

### Missing Source

Regenerate the synthetic source dataset using the
existing orchestration module.

### Invalid Source Records

Inspect duplicate identifiers, missing mandatory fields
and invalid attribute values.

### Database Lock

Close conflicting database connections before rerunning.

### Memory Pressure

Reduce the benchmark multiplier or adjust local
compute profiles within available resources.

### Reconciliation Failure

Investigate source, warehouse and aggregate record
counts before publishing downstream metrics.

## Recovery

Correct the underlying failure and rerun the pipeline.

Patient ingestion and analytical object creation use
separate transactions.

The entire pipeline is not a single atomic transaction.

## Security

Use synthetic records only.

Do not publish credentials, sensitive datasets,
database binaries or raw runtime logs.
