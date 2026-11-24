"""Healthcare workflow correctness and reliability tests."""

import pytest

from src.orchestration.healthcare_workflow import (
    validate_patients,
    aggregate_patients,
    execute_task
)

from src.orchestration.synthetic_data import (
    generate_patients
)


def patient(identifier="P001", age="35"):

    return {
        "patient_id": identifier,
        "region": "N",
        "age": age,
        "prior_visits": "2",
        "no_show": "0"
    }


def test_valid_patient():

    assert len(
        validate_patients([patient()])
    ) == 1


def test_duplicate_patient():

    with pytest.raises(ValueError):

        validate_patients([
            patient(),
            patient()
        ])


def test_invalid_age():

    with pytest.raises(ValueError):

        validate_patients([
            patient(age="150")
        ])


def test_regional_reconciliation():

    result = aggregate_patients([
        patient("P001"),
        patient("P002")
    ])

    assert result["total_patients"] == 2

    assert result["regional_counts"] == {"N": 2}


def test_transient_failure_retry():

    attempts = []

    history = []

    def operation():

        attempts.append(1)

        if len(attempts) == 1:

            raise OSError("Temporary failure")

        return "SUCCESS"

    result = execute_task(
        "retry_validation",
        operation,
        history,
        max_attempts=3,
        retry_delay=0
    )

    assert result == "SUCCESS"

    assert len(attempts) == 2

    assert history[0]["status"] == "FAILED"

    assert history[1]["status"] == "SUCCESS"


def test_validation_failure_not_retried():

    history = []

    def operation():

        raise ValueError("Invalid data")

    with pytest.raises(ValueError):

        execute_task(
            "validation",
            operation,
            history,
            retry_delay=0
        )

    assert len(history) == 1


def test_retry_exhaustion():

    history = []

    def operation():

        raise OSError("Persistent failure")

    with pytest.raises(OSError):

        execute_task(
            "retry_exhaustion",
            operation,
            history,
            max_attempts=3,
            retry_delay=0
        )

    assert len(history) == 3


def test_reproducible_synthetic_data():

    assert generate_patients(10, 42) == (
        generate_patients(10, 42)
    )


def test_unique_synthetic_identifiers():

    rows = generate_patients(100)

    assert len({
        row["patient_id"] for row in rows
    }) == 100
