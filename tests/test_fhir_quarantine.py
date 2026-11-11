
import json
import tempfile
from pathlib import Path

from importlib.util import (
    spec_from_file_location,
    module_from_spec
)

BASE_DIR = Path(__file__).resolve().parents[1]

VALIDATOR_PATH = (
    BASE_DIR / "src" / "quality" / "validate_fhir.py"
)

spec = spec_from_file_location(
    "validate_fhir",
    VALIDATOR_PATH
)

validator = module_from_spec(spec)
spec.loader.exec_module(validator)


def test_invalid_fhir_record_is_quarantined():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        fhir_dir = temp_path / "fhir"
        quarantine_dir = temp_path / "quarantine"

        fhir_dir.mkdir()
        quarantine_dir.mkdir()

        # Temporarily redirect validator paths
        original_fhir_dir = validator.FHIR_DIR
        original_quarantine_dir = validator.QUARANTINE_DIR

        try:
            validator.FHIR_DIR = fhir_dir
            validator.QUARANTINE_DIR = quarantine_dir

            valid_record = {
                "resourceType": "Patient",
                "id": "P00001"
            }

            invalid_record = {
                "resourceType": "Patient"
            }

            source_path = fhir_dir / "patients.jsonl"

            with source_path.open(
                "w", encoding="utf-8"
            ) as file:
                file.write(json.dumps(valid_record) + "\n")
                file.write(json.dumps(invalid_record) + "\n")

            result = validator.validate_file(
                "patients.jsonl",
                "Patient"
            )

            assert result is False

            quarantine_path = (
                quarantine_dir / "patients.jsonl"
            )

            assert quarantine_path.exists()

            with quarantine_path.open(
                encoding="utf-8"
            ) as file:
                rejected = [
                    json.loads(line)
                    for line in file
                    if line.strip()
                ]

            assert len(rejected) == 1
            assert rejected[0]["line_number"] == 2
            assert rejected[0]["error_reason"] == (
                "Missing resource ID"
            )

        finally:
            validator.FHIR_DIR = original_fhir_dir
            validator.QUARANTINE_DIR = original_quarantine_dir
