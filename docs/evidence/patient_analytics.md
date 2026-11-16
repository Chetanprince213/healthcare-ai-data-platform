# Healthcare SQL Analytics — Advanced SQL Evidence

## Objective
Practice real SQL queries using synthetic healthcare data.

## Environment
- Windows PowerShell
- Python 3.13 virtual environment
- SQLite 3.50.4
- Local execution, ₹0 cost
- No cloud database deployment

## Implementation
Python script: `src/sql/patient_analytics.py`

SQL concepts implemented:

1. `JOIN` and `GROUP BY` — Department-level visit and revenue metrics.
2. Common Table Expression (CTE) — Total patient spending.
3. `ROW_NUMBER()` — Latest visit for each patient.
4. `RANK()` — Ranking patients by spending.

## Verification targets

| Metric | Expected result |
|---|---:|
| Total patients | 5 |
| Total visits | 7 |
| Total revenue | ₹11,700 |
| Departments | 3 |
| Latest-visit results | 5 |

## Execution evidence
Paste the actual PowerShell output here after successful execution.

## Result
Status: **PASS only if the script prints `HEALTHCARE SQL ANALYTICS VERIFIED SUCCESSFULLY`.**

## Limitations
The exercise uses SQLite. It does not demonstrate a running MySQL or PostgreSQL server. All records are synthetic.