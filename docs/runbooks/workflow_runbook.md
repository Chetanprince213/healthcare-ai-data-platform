# Healthcare Workflow Runbook

## Execution

    python -m src.orchestration.healthcare_workflow

## Source Generation

    python -m src.orchestration.synthetic_data

The generator refuses to overwrite an existing dataset.

## Configuration

    configs/orchestration/healthcare_workflow.json

## Monitoring

    logs/orchestration/workflow.log

    data/orchestration/workflow_history.json

## Recovery

Review the failed task and its recorded attempts.

Correct deterministic validation errors before retrying.

Do not bypass data quality validation.

## Automated Tests

    python -m pytest tests/unit/test_healthcare_workflow.py -v
