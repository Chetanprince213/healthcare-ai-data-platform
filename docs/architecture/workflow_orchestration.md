# Healthcare Workflow Orchestration

## Architecture

    Synthetic Healthcare Source
        |
        v
    Bronze Source Ingestion
        |
        v
    Silver Data Validation
        |
        v
    Gold Regional Aggregation

## Orchestration

Tasks execute in dependency order.

Downstream processing stops when an upstream task fails.

Operating-system errors are retried using exponential
backoff with configurable retry limits.

Deterministic validation failures fail immediately.

## Data Quality

The workflow validates source schema, mandatory identifiers,
unique patient keys, age ranges and utilization values.

Regional aggregates are reconciled against source counts.

## Observability

Execution history records run identifiers, task names,
attempt counts, timestamps, durations and outcomes.

## Reliability

Automated tests cover retry behavior, validation failures,
duplicate detection and reconciliation.

## Deployment Scope

The implementation is a local Python orchestration engine.

It is not a deployed Apache Airflow environment.
