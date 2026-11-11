
import csv
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = GOLD_DIR / "healthcare_analytics.db"


def load_csv(connection, filename, table_name, columns):
    filepath = SILVER_DIR / filename

    if not filepath.exists():
        raise FileNotFoundError(f"Missing Silver file: {filepath}")

    with filepath.open(
        newline="",
        encoding="utf-8"
    ) as file:
        records = list(csv.DictReader(file))

    placeholders = ", ".join(["?"] * len(columns))
    column_names = ", ".join(columns)

    connection.executemany(
        f"INSERT INTO {table_name} ({column_names}) "
        f"VALUES ({placeholders})",
        [
            tuple(record[column] for column in columns)
            for record in records
        ]
    )

    return len(records)


def export_query(connection, filename, query):
    cursor = connection.execute(query)
    columns = [column[0] for column in cursor.description]
    records = cursor.fetchall()

    filepath = GOLD_DIR / filename

    with filepath.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(records)

    print(f"Created {filename}: {len(records)} rows")


def main():
    with sqlite3.connect(DB_PATH) as connection:
        connection.executescript("""
            DROP TABLE IF EXISTS appointments;
            DROP TABLE IF EXISTS patients;

            CREATE TABLE patients (
                patient_id TEXT PRIMARY KEY,
                name TEXT,
                age INTEGER,
                gender TEXT,
                city TEXT,
                registration_date TEXT
            );

            CREATE TABLE appointments (
                appointment_id TEXT PRIMARY KEY,
                patient_id TEXT NOT NULL,
                appointment_date TEXT,
                department TEXT,
                status TEXT,
                FOREIGN KEY (patient_id)
                    REFERENCES patients(patient_id)
            );
        """)

        connection.execute("PRAGMA foreign_keys = ON")

        patient_count = load_csv(
            connection,
            "patients.csv",
            "patients",
            [
                "patient_id", "name", "age",
                "gender", "city", "registration_date"
            ]
        )

        appointment_count = load_csv(
            connection,
            "appointments.csv",
            "appointments",
            [
                "appointment_id", "patient_id",
                "appointment_date", "department", "status"
            ]
        )

        export_query(
            connection,
            "department_kpis.csv",
            """
            SELECT
                department,
                COUNT(*) AS total_appointments,
                SUM(CASE WHEN status = 'Completed'
                    THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'Cancelled'
                    THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status = 'No-Show'
                    THEN 1 ELSE 0 END) AS no_shows,
                ROUND(
                    100.0 * SUM(
                        CASE WHEN status = 'No-Show'
                        THEN 1 ELSE 0 END
                    ) / NULLIF(COUNT(*), 0),
                    2
                ) AS no_show_rate_pct
            FROM appointments
            GROUP BY department
            ORDER BY total_appointments DESC
            """
        )

        export_query(
            connection,
            "monthly_appointments.csv",
            """
            SELECT
                SUBSTR(appointment_date, 1, 7) AS month,
                COUNT(*) AS total_appointments,
                SUM(CASE WHEN status = 'Completed'
                    THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'No-Show'
                    THEN 1 ELSE 0 END) AS no_shows
            FROM appointments
            GROUP BY SUBSTR(appointment_date, 1, 7)
            ORDER BY month
            """
        )

        export_query(
            connection,
            "patient_demographics.csv",
            """
            SELECT
                CASE
                    WHEN age < 30 THEN '18-29'
                    WHEN age < 45 THEN '30-44'
                    WHEN age < 60 THEN '45-59'
                    ELSE '60+'
                END AS age_group,
                gender,
                COUNT(*) AS patient_count
            FROM patients
            GROUP BY age_group, gender
            ORDER BY age_group, gender
            """
        )

        connection.commit()

        print(f"Loaded patients: {patient_count}")
        print(f"Loaded appointments: {appointment_count}")
        print("Gold transformation completed!")


if __name__ == "__main__":
    main()
