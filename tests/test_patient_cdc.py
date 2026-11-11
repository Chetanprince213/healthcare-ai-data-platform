
import importlib.util
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
SCRIPT = BASE_DIR / "src" / "cdc" / "patient_cdc.py"

spec = importlib.util.spec_from_file_location(
    "patient_cdc", SCRIPT
)
cdc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cdc)


def test_insert_update_delete():
    old_df = pd.DataFrame([
        {"patient_id": "P001", "name": "Alice"},
        {"patient_id": "P002", "name": "Bob"}
    ]).set_index("patient_id")

    new_df = pd.DataFrame([
        {"patient_id": "P001", "name": "Alice Updated"},
        {"patient_id": "P003", "name": "Charlie"}
    ]).set_index("patient_id")

    events = cdc.detect_changes(old_df, new_df)

    operations = {
        event["patient_id"]: event["operation"]
        for event in events
    }

    assert operations == {
        "P001": "UPDATE",
        "P002": "DELETE",
        "P003": "INSERT"
    }
