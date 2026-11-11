
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PATIENT_COUNT = 1000
APPOINTMENT_COUNT = 3000

patients = []
appointments = []

# Generate synthetic patients
for patient_id in range(1, PATIENT_COUNT + 1):
    patients.append({
        "patient_id": f"P{patient_id:05d}",
        "name": fake.name(),
        "age": random.randint(18, 85),
        "gender": random.choice(["Male", "Female", "Other"]),
        "city": fake.city(),
        "registration_date": fake.date_between(
            start_date="-2y",
            end_date="today"
        ).isoformat()
    })

# Generate synthetic appointments
for appointment_id in range(1, APPOINTMENT_COUNT + 1):
    patient = random.choice(patients)
    appointment_date = datetime(2026, 1, 1) + timedelta(
        days=random.randint(0, 300)
    )

    status = random.choices(
        ["Completed", "Cancelled", "No-Show"],
        weights=[75, 15, 10]
    )[0]

    appointments.append({
        "appointment_id": f"A{appointment_id:06d}",
        "patient_id": patient["patient_id"],
        "appointment_date": appointment_date.date().isoformat(),
        "department": random.choice([
            "Cardiology",
            "Neurology",
            "Orthopedics",
            "General Medicine",
            "Dermatology"
        ]),
        "status": status
    })


def save_csv(filename, rows):
    filepath = OUTPUT_DIR / filename

    with filepath.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys())
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Created: {filepath} | Records: {len(rows)}")


save_csv("patients.csv", patients)
save_csv("appointments.csv", appointments)

print("Synthetic healthcare data generation completed!")

save_csv("patients.csv", patients)