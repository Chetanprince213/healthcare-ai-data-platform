# Step 23 — Dimensional Modeling Evidence

## Objective
Implement a star-schema-style dimensional model and demonstrate Slowly Changing Dimensions (SCD) Types 1, 2, and 3.

## Implementation
- Script: `src/modeling/dimensional_model.py`
- Database: SQLite (in-memory)
- Dataset: Synthetic healthcare patient and visit records
- Cost: ₹0

## Tables
- `dim_patient` — Patient dimension
- `dim_department` — Department dimension
- `fact_visits` — Visit fact table

## SCD Demonstrations
- **Type 1:** Overwrite a patient's existing city.
- **Type 2:** Retain historical and current dimension records using a surrogate key and current flag.
- **Type 3:** Maintain current and previous city values in separate columns.

## Verification
Expected results:
- Three joined fact records
- Type 1 city updated to Hyderabad
- Type 2 retains two versions for patient 102
- Type 3 preserves Chennai as the previous city

## Result
Record PASS only after the script prints:

`STEP 23 DIMENSIONAL MODEL VERIFIED SUCCESSFULLY`

## Limitations
Local SQLite exercise with synthetic records. Not a production healthcare database.