# Great Expectations, Data Contracts and Reconciliation

## Implementation

Real local open-source healthcare data quality framework.

Completed: 2026-11-17 00:29:18 +05:30

Great Expectations version: 1.25.0

## Great Expectations

- Actual GX Core validation against pandas batches
- Required patient identifiers
- Patient ID uniqueness
- Allowed healthcare regions
- Exact expected schema
- Minimum record count
- Negative testing with invalid synthetic data

## Data Contracts

- JSON-defined schema
- Required columns
- String data types
- Non-null and non-blank patient identifiers
- Unique patient identifiers
- Allowed regions and appointment statuses

## Reconciliation

- Source and target record counts
- Full-record comparison
- Missing records
- Unexpected records
- Changed records

## Verified results

- Synthetic source records: 5
- Quality validation scenarios passed: 10
- Framework pytest suite: PASS
- Complete project pytest suite: PASS
- Overall result: PASS

## Compatibility fix

The invalid-type test converts the patient_id column to object
dtype before inserting an integer. This permits the data contract
validator to reject the invalid record instead of pandas rejecting
the test fixture during assignment.

## Machine-readable evidence

data/quality_reports/gx_contract_reconciliation.json

## Scope

REAL LOCAL OPEN-SOURCE IMPLEMENTATION.

Great Expectations Core was used locally.

No Great Expectations Cloud account was required.
No real patient data was processed.
Not deployed to a cloud service.
