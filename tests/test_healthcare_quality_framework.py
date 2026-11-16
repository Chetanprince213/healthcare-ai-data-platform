"""Tests for GX, patient data contracts, and reconciliation."""

import pandas as pd

from src.quality.healthcare_quality_framework import (
    load_contract,
    reconcile,
    run_scenarios,
    sample_patients,
    validate_contract,
    validate_with_gx,
)


def test_valid_gx_expectations():
    assert validate_with_gx(sample_patients())["passed"]


def test_gx_rejects_duplicate():
    df = sample_patients()
    df.loc[1, "patient_id"] = "P0001"
    assert not validate_with_gx(df)["passed"]


def test_gx_rejects_invalid_region():
    df = sample_patients()
    df.loc[0, "region"] = "Unknown"
    assert not validate_with_gx(df)["passed"]


def test_contract_accepts_valid_data():
    assert validate_contract(sample_patients(), load_contract())["passed"]


def test_contract_rejects_blank_id():
    df = sample_patients()
    df.loc[0, "patient_id"] = " "
    assert not validate_contract(df, load_contract())["passed"]


def test_contract_rejects_missing_column():
    df = sample_patients().drop(columns=["region"])
    assert not validate_contract(df, load_contract())["passed"]


def test_contract_rejects_invalid_type():
    df = sample_patients()
    df["patient_id"] = df["patient_id"].astype(object)
    df.loc[0, "patient_id"] = 123
    assert not validate_contract(df, load_contract())["passed"]


def test_reconciliation_accepts_matching_data():
    df = sample_patients()
    assert reconcile(df, df.copy())["passed"]


def test_reconciliation_rejects_missing_record():
    df = sample_patients()
    assert not reconcile(df, df.iloc[:-1])["passed"]


def test_reconciliation_rejects_changed_record():
    source = sample_patients()
    target = source.copy()
    target.loc[0, "appointment_status"] = "cancelled"

    result = reconcile(source, target)

    assert not result["passed"]
    assert result["changed_records"] == 1


def test_complete_quality_scenarios():
    result = run_scenarios()
    assert result["passed"]
    assert len(result["checks"]) == 10
