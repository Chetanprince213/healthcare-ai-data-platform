# Staged Ingestion Runbook

## Execution

    python -m src.snowflake.staged_ingestion

## Automated Tests

    python -m pytest tests/unit/test_staged_ingestion.py -v

## Runtime Locations

Landing: data/staging/healthcare/landing/

Quarantine: data/staging/healthcare/quarantine/

Exports: data/staging/healthcare/exports/

Reports: data/staging/healthcare/reports/

Logs: logs/ingestion/

## Recovery

Inspect operational logs and quarantine metadata.

Correct invalid input before restaging.

Previously loaded content is skipped automatically.

Use a single database writer.

Do not bypass validation or delete load history to
force duplicate ingestion.

## Security

Only synthetic healthcare records are permitted.

Runtime databases, quarantine files, credentials and
operational logs remain outside version control.
