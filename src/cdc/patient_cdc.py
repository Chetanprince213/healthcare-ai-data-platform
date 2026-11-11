
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parents[2]

SOURCE_FILE = BASE_DIR / "data" / "raw" / "patients.csv"
SNAPSHOT_FILE = (
    BASE_DIR / "data" / "cdc" / "snapshots" / "patients_snapshot.csv"
)
CHANGES_DIR = BASE_DIR / "data" / "cdc" / "changes"

CHANGES_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_patients(path):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    if "patient_id" not in df.columns:
        raise ValueError("Missing patient_id column")

    if df["patient_id"].eq("").any():
        raise ValueError("Empty patient_id detected")

    if df["patient_id"].duplicated().any():
        raise ValueError("Duplicate patient_id detected")

    return df.set_index("patient_id")


def detect_changes(old_df, new_df):
    if set(old_df.columns) != set(new_df.columns):
        raise ValueError("Patient CSV columns have changed")

    columns = list(new_df.columns)
    events = []

    old_ids = set(old_df.index)
    new_ids = set(new_df.index)

    for patient_id in sorted(new_ids - old_ids):
        row = new_df.loc[patient_id]
        events.append({
            "patient_id": patient_id,
            "operation": "INSERT",
            **row.to_dict()
        })

    for patient_id in sorted(old_ids & new_ids):
        old_row = old_df.loc[patient_id, columns]
        new_row = new_df.loc[patient_id, columns]

        if not old_row.equals(new_row):
            events.append({
                "patient_id": patient_id,
                "operation": "UPDATE",
                **new_row.to_dict()
            })

    for patient_id in sorted(old_ids - new_ids):
        row = old_df.loc[patient_id]
        events.append({
            "patient_id": patient_id,
            "operation": "DELETE",
            **row.to_dict()
        })

    return events


def main():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Patient source file missing: {SOURCE_FILE}"
        )

    current_df = load_patients(SOURCE_FILE)

    if not SNAPSHOT_FILE.exists():
        current_df.reset_index().to_csv(
            SNAPSHOT_FILE, index=False
        )
        print("SUCCESS: Initial patient snapshot created")
        print(f"Patients: {len(current_df)}")
        return

    previous_df = load_patients(SNAPSHOT_FILE)
    events = detect_changes(previous_df, current_df)

    if events:
        timestamp = datetime.now(
            timezone.utc
        ).strftime("%Y%m%d_%H%M%S_%f")

        output_file = (
            CHANGES_DIR / f"patient_cdc_{timestamp}.csv"
        )

        changes_df = pd.DataFrame(events)
        changes_df["detected_at_utc"] = (
            datetime.now(timezone.utc).isoformat()
        )

        changes_df.to_csv(output_file, index=False)

        print(f"CDC file created: {output_file.name}")
    else:
        print("No patient changes detected")

    for operation in ["INSERT", "UPDATE", "DELETE"]:
        count = sum(
            event["operation"] == operation
            for event in events
        )
        print(f"{operation}: {count}")

    current_df.reset_index().to_csv(
        SNAPSHOT_FILE, index=False
    )

    print("SUCCESS: CDC processing completed")


if __name__ == "__main__":
    main()
