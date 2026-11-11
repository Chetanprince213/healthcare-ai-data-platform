
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"


def read_csv(filename):
    with (RAW_DIR / filename).open(
        newline="",
        encoding="utf-8"
    ) as file:
        return list(csv.DictReader(file))


patients = read_csv("patients.csv")
appointments = read_csv("appointments.csv")

patient_ids = {p["patient_id"] for p in patients}
appointment_ids = [a["appointment_id"] for a in appointments]

assert len(patients) == 1000, "Unexpected patient count"
assert len(appointments) == 3000, "Unexpected appointment count"

assert len(patient_ids) == len(patients), \
    "Duplicate patient IDs found"

assert len(set(appointment_ids)) == len(appointment_ids), \
    "Duplicate appointment IDs found"

assert all(
    a["patient_id"] in patient_ids
    for a in appointments
), "Appointments contain unknown patient IDs"

assert all(
    a["status"] in {"Completed", "Cancelled", "No-Show"}
    for a in appointments
), "Invalid appointment status found"

print("PASS: Patient and appointment counts")
print("PASS: Unique primary keys")
print("PASS: Appointment foreign keys")
print("PASS: Valid appointment statuses")
print("All healthcare data quality checks passed!")
