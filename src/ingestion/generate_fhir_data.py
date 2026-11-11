
import csv
import json
import random
from datetime import date, datetime, timezone
from pathlib import Path

random.seed(42)

BASE = Path(__file__).resolve().parents[2]
PATIENT_CSV = BASE / "data" / "silver" / "patients.csv"
APPOINTMENT_CSV = BASE / "data" / "silver" / "appointments.csv"
OUTPUT = BASE / "data" / "fhir"
OUTPUT.mkdir(parents=True, exist_ok=True)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_jsonl(filename, records):
    path = OUTPUT / filename
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"Created {filename}: {len(records)} resources")


def make_patients(rows):
    resources = []
    for row in rows:
        resources.append({
            "resourceType": "Patient",
            "id": row["patient_id"],
            "name": [{"text": row["name"]}],
            "gender": {
                "Male": "male",
                "Female": "female",
                "Other": "other"
            }.get(row["gender"], "unknown")
        })
    return resources


def make_encounters(rows):
    resources = []
    for row in rows:
        if row["status"] != "Completed":
            continue

        encounter_id = f"ENC-{row['appointment_id']}"
        start = f"{row['appointment_date']}T09:00:00Z"

        resources.append({
            "resourceType": "Encounter",
            "id": encounter_id,
            "status": "finished",
            "class": {
                "system": (
                    "http://terminology.hl7.org/"
                    "CodeSystem/v3-ActCode"
                ),
                "code": "AMB",
                "display": "ambulatory"
            },
            "subject": {
                "reference": f"Patient/{row['patient_id']}"
            },
            "period": {"start": start}
        })
    return resources


def make_observations(encounters):
    resources = []

    for i, encounter in enumerate(encounters, start=1):
        patient_ref = encounter["subject"]["reference"]

        resources.append({
            "resourceType": "Observation",
            "id": f"OBS-{i:06d}",
            "status": "final",
            "category": [{
                "coding": [{
                    "system": (
                        "http://terminology.hl7.org/"
                        "CodeSystem/observation-category"
                    ),
                    "code": "vital-signs"
                }]
            }],
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": "8867-4",
                    "display": "Heart rate"
                }]
            },
            "subject": {"reference": patient_ref},
            "encounter": {
                "reference": f"Encounter/{encounter['id']}"
            },
            "effectiveDateTime": encounter["period"]["start"],
            "valueQuantity": {
                "value": random.randint(60, 100),
                "unit": "beats/minute",
                "system": "http://unitsofmeasure.org",
                "code": "/min"
            }
        })

    return resources


def main():
    if not PATIENT_CSV.exists() or not APPOINTMENT_CSV.exists():
        raise FileNotFoundError(
            "Silver CSV files missing. Run Step 9 first."
        )

    patients = make_patients(read_csv(PATIENT_CSV))
    encounters = make_encounters(read_csv(APPOINTMENT_CSV))
    observations = make_observations(encounters)

    write_jsonl("patients.jsonl", patients)
    write_jsonl("encounters.jsonl", encounters)
    write_jsonl("observations.jsonl", observations)

    print("FHIR-style data generation completed!")


if __name__ == "__main__":
    main()
