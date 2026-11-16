"""Real local healthcare data quality framework using GX Core."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "config" / "contracts" / "patient_records.json"
REPORT_PATH = ROOT / "data" / "quality_reports" / "gx_contract_reconciliation.json"


def load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8-sig"))


def sample_patients() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("P0001", "North", "completed"),
            ("P0002", "South", "completed"),
            ("P0003", "North", "cancelled"),
            ("P0004", "South", "completed"),
            ("P0005", "North", "completed"),
        ],
        columns=["patient_id", "region", "appointment_status"],
    )


def validate_contract(df: pd.DataFrame, contract: dict) -> dict:
    errors = []
    definitions = contract["columns"]
    expected = set(definitions)
    actual = set(df.columns)

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)

    if missing:
        errors.append(f"Missing columns: {missing}")

    if extra and not contract.get("allow_extra_columns", False):
        errors.append(f"Unexpected columns: {extra}")

    if df.empty:
        errors.append("Dataset must not be empty")

    for column, rules in definitions.items():
        if column not in df.columns:
            continue

        values = df[column]

        if rules["type"] == "string":
            non_null = values.dropna()
            if not non_null.map(lambda value: isinstance(value, str)).all():
                errors.append(f"{column}: non-string values detected")

        if not rules["nullable"]:
            invalid = values.isna()
            if rules["type"] == "string":
                invalid = invalid | values.astype("string").str.strip().eq("").fillna(False)

            if invalid.any():
                errors.append(f"{column}: null or blank values detected")

        if rules.get("unique") and values.duplicated().any():
            errors.append(f"{column}: duplicate values detected")

        allowed = rules.get("allowed_values")
        if allowed is not None:
            invalid = values.notna() & ~values.isin(allowed)
            if invalid.any():
                errors.append(f"{column}: invalid allowed values detected")

    return {
        "passed": not errors,
        "row_count": len(df),
        "errors": errors,
    }


def validate_with_gx(df: pd.DataFrame) -> dict:
    """Execute actual Great Expectations against a pandas batch."""

    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas(
        name="healthcare_pandas_source"
    )

    asset = datasource.add_dataframe_asset(
        name="patient_records"
    )

    batch_definition = asset.add_batch_definition_whole_dataframe(
        "complete_patient_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    contract = load_contract()
    columns = contract["columns"]

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1
        ),
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=list(columns),
            exact_match=True,
        ),
    ]

    for column, rules in columns.items():
        if column not in df.columns:
            continue

        if not rules["nullable"]:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column
                )
            )

        if rules.get("unique"):
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeUnique(
                    column=column
                )
            )

        if rules.get("allowed_values"):
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column=column,
                    value_set=rules["allowed_values"],
                )
            )

    results = []

    for expectation in expectations:
        validation = batch.validate(expectation)

        results.append({
            "expectation": type(expectation).__name__,
            "column": getattr(expectation, "column", None),
            "passed": bool(validation.success),
        })

    return {
        "passed": all(item["passed"] for item in results),
        "expectation_count": len(results),
        "results": results,
    }


def reconcile(source: pd.DataFrame, target: pd.DataFrame) -> dict:
    """Compare source and target as multisets of complete records."""

    contract = load_contract()

    source_check = validate_contract(source, contract)
    target_check = validate_contract(target, contract)

    if not source_check["passed"] or not target_check["passed"]:
        return {
            "passed": False,
            "source_count": len(source),
            "target_count": len(target),
            "reason": "Source or target contract failed",
        }

    columns = list(contract["columns"])

    source_records = Counter(
        source[columns].itertuples(index=False, name=None)
    )

    target_records = Counter(
        target[columns].itertuples(index=False, name=None)
    )

    missing = sum((source_records - target_records).values())
    unexpected = sum((target_records - source_records).values())

    source_by_id = source.set_index("patient_id")
    target_by_id = target.set_index("patient_id")

    common_ids = source_by_id.index.intersection(target_by_id.index)

    changed = sum(
        source_by_id.loc[patient_id].to_dict()
        != target_by_id.loc[patient_id].to_dict()
        for patient_id in common_ids
    )

    return {
        "passed": missing == 0 and unexpected == 0,
        "source_count": len(source),
        "target_count": len(target),
        "missing_records": missing,
        "unexpected_records": unexpected,
        "changed_records": changed,
    }


def run_scenarios() -> dict:
    contract = load_contract()
    valid = sample_patients()

    duplicate = pd.concat(
        [valid, valid.iloc[[0]]],
        ignore_index=True,
    )

    blank = valid.copy()
    blank.loc[0, "patient_id"] = ""

    invalid_region = valid.copy()
    invalid_region.loc[0, "region"] = "Unknown"

    missing_column = valid.drop(columns=["region"])

    missing_target = valid.iloc[:-1].copy()

    changed_target = valid.copy()
    changed_target.loc[0, "appointment_status"] = "cancelled"

    gx_valid = validate_with_gx(valid)
    gx_duplicate = validate_with_gx(duplicate)
    gx_invalid_region = validate_with_gx(invalid_region)

    contract_valid = validate_contract(valid, contract)
    contract_duplicate = validate_contract(duplicate, contract)
    contract_blank = validate_contract(blank, contract)
    contract_missing = validate_contract(missing_column, contract)

    matching = reconcile(valid, valid.copy())
    missing = reconcile(valid, missing_target)
    changed = reconcile(valid, changed_target)

    checks = {
        "gx_valid_data_accepted": gx_valid["passed"],
        "gx_duplicate_rejected": not gx_duplicate["passed"],
        "gx_invalid_region_rejected": not gx_invalid_region["passed"],
        "contract_valid_data_accepted": contract_valid["passed"],
        "contract_duplicate_rejected": not contract_duplicate["passed"],
        "contract_blank_id_rejected": not contract_blank["passed"],
        "contract_missing_column_rejected": not contract_missing["passed"],
        "matching_records_reconciled": matching["passed"],
        "missing_record_detected": not missing["passed"]
            and missing["missing_records"] == 1,
        "changed_record_detected": not changed["passed"]
            and changed["changed_records"] == 1,
    }

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "great_expectations_version": gx.__version__,
        "contract_version": contract["version"],
        "synthetic_data_only": True,
        "source_records": len(valid),
        "checks": checks,
        "passed": all(checks.values()),
        "details": {
            "gx_valid": gx_valid,
            "gx_duplicate": gx_duplicate,
            "gx_invalid_region": gx_invalid_region,
            "contract_valid": contract_valid,
            "contract_duplicate": contract_duplicate,
            "contract_blank": contract_blank,
            "contract_missing_column": contract_missing,
            "matching_reconciliation": matching,
            "missing_reconciliation": missing,
            "changed_reconciliation": changed,
        },
    }


def main() -> None:
    report = run_scenarios()

    if REPORT_PATH.exists():
        raise FileExistsError(
            f"Existing quality report preserved: {REPORT_PATH}"
        )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    REPORT_PATH.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\nGREAT EXPECTATIONS, CONTRACTS AND RECONCILIATION")

    for name, passed in report["checks"].items():
        print(f"{'PASS' if passed else 'FAIL'}: {name}")

    print(f"\nTOTAL SCENARIOS: {len(report['checks'])}")
    print(f"OVERALL RESULT: {'PASS' if report['passed'] else 'FAIL'}")

    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
