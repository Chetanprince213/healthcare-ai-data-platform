
import csv
import json
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

VALID_STATUSES = {"Completed", "Cancelled", "No-Show"}


def read_bronze(dataset):
    folder = BRONZE_DIR / dataset
    files = sorted(folder.glob("*.jsonl"))

    if not files:
        raise FileNotFoundError(
            f"No Bronze files found for {dataset}"
        )

    records = []

    for path in files:
        with path.open(encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    records.append(json.loads(line))

    return records


def write_csv(path, records, columns):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=columns,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(records)


def validate_patient(record):
    patient_id = record["patient_id"].strip()
    age = int(record["age"])
    registration_date = date.fromisoformat(
        record["registration_date"]
    )

    if not patient_id:
        raise ValueError("Missing patient ID")

    if not 0 <= age <= 120:
        raise ValueError("Invalid patient age")

    if registration_date > date.today():
        raise ValueError("Future registration date")

    return {
        "patient_id": patient_id,
        "name": record["name"].strip(),
        "age": age,
        "gender": record["gender"].strip(),
        "city": record["city"].strip(),
        "registration_date": registration_date.isoformat()
    }


def validate_appointment(record, patient_ids):
    appointment_id = record["appointment_id"].strip()
    patient_id = record["patient_id"].strip()
    appointment_date = date.fromisoformat(
        record["appointment_date"]
    )
    status = record["status"].strip()

    if not appointment_id:
        raise ValueError("Missing appointment ID")

    if patient_id not in patient_ids:
        raise ValueError("Unknown patient ID")

    if status not in VALID_STATUSES:
        raise ValueError("Invalid appointment status")

    return {
        "appointment_id": appointment_id,
        "patient_id": patient_id,
        "appointment_date": appointment_date.isoformat(),
        "department": record["department"].strip(),
        "status": status
    }


def process_records(records, validator, key_field):
    valid = {}
    rejected = []

    for record in records:
        try:
            cleaned = validator(record)
            key = cleaned[key_field]

            if key in valid:
                # Identical replays are safe to skip.
                if cleaned == valid[key]:
                    continue

                raise ValueError(
                    f"Conflicting duplicate {key_field}: {key}"
                )

            valid[key] = cleaned

        except (ValueError, KeyError, TypeError) as error:
            rejected.append({
                "record": record,
                "reason": str(error)
            })

    return list(valid.values()), rejected


def save_quarantine(dataset, rejected):
    path = QUARANTINE_DIR / f"{dataset}_rejected.jsonl"

    with path.open("w", encoding="utf-8") as file:
        for item in rejected:
            file.write(json.dumps(item) + "\n")


def main():
    raw_patients = read_bronze("patients")

    patients, rejected_patients = process_records(
        raw_patients,
        validate_patient,
        "patient_id"
    )

    patient_ids = {p["patient_id"] for p in patients}

    raw_appointments = read_bronze("appointments")

    appointments, rejected_appointments = process_records(
        raw_appointments,
        lambda record: validate_appointment(
            record, patient_ids
        ),
        "appointment_id"
    )

    write_csv(
        SILVER_DIR / "patients.csv",
        patients,
        [
            "patient_id", "name", "age",
            "gender", "city", "registration_date"
        ]
    )

    write_csv(
        SILVER_DIR / "appointments.csv",
        appointments,
        [
            "appointment_id", "patient_id",
            "appointment_date", "department", "status"
        ]
    )

    save_quarantine("patients", rejected_patients)
    save_quarantine("appointments", rejected_appointments)

    print(f"Silver patients: {len(patients)}")
    print(f"Silver appointments: {len(appointments)}")
    print(f"Rejected patients: {len(rejected_patients)}")
    print(f"Rejected appointments: {len(rejected_appointments)}")
    print("Silver transformation completed!")


if __name__ == "__main__":
    main()
