
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

FHIR_DIR = BASE_DIR / "data" / "fhir"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine" / "fhir"

QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

RESOURCE_TYPES = {
    "patients.jsonl": "Patient",
    "encounters.jsonl": "Encounter",
    "observations.jsonl": "Observation"
}


def validate_file(filename, expected_type):
    filepath = FHIR_DIR / filename
    quarantine_path = QUARANTINE_DIR / filename

    if not filepath.exists():
        print(f"FAIL: Missing file {filename}")
        return False

    valid_count = 0
    invalid_count = 0

    with filepath.open(encoding="utf-8") as source:
        with quarantine_path.open(
            "w", encoding="utf-8"
        ) as quarantine:

            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)

                    if not isinstance(record, dict):
                        raise ValueError(
                            "Resource must be a JSON object"
                        )

                    if record.get("resourceType") != expected_type:
                        raise ValueError(
                            "Incorrect resourceType"
                        )

                    if not record.get("id"):
                        raise ValueError(
                            "Missing resource ID"
                        )

                    valid_count += 1

                except (
                    json.JSONDecodeError,
                    ValueError
                ) as error:

                    invalid_count += 1

                    rejected_record = {
                        "source_file": filename,
                        "line_number": line_number,
                        "raw_record": line.rstrip("\r\n"),
                        "error_reason": str(error)
                    }

                    quarantine.write(
                        json.dumps(
                            rejected_record,
                            ensure_ascii=False
                        ) + "\n"
                    )

                    print(
                        f"QUARANTINED: {filename}, "
                        f"line {line_number}: {error}"
                    )

    print(
        f"{filename}: "
        f"{valid_count} valid, "
        f"{invalid_count} quarantined"
    )

    return invalid_count == 0


def main():
    results = []

    for filename, resource_type in RESOURCE_TYPES.items():
        results.append(
            validate_file(filename, resource_type)
        )

    if all(results):
        print("SUCCESS: FHIR validation passed!")
    else:
        print("FAILED: Invalid FHIR records detected.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
