
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
FHIR = BASE / "data" / "fhir"


def load_resources(filename):
    with (FHIR / filename).open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_patient_resources():
    patients = load_resources("patients.jsonl")
    assert len(patients) == 1000
    assert all(p["resourceType"] == "Patient" for p in patients)


def test_encounter_patient_references():
    patients = load_resources("patients.jsonl")
    encounters = load_resources("encounters.jsonl")

    patient_refs = {f"Patient/{p['id']}" for p in patients}

    assert encounters
    assert all(
        e["subject"]["reference"] in patient_refs
        for e in encounters
    )


def test_observation_references():
    encounters = load_resources("encounters.jsonl")
    observations = load_resources("observations.jsonl")

    encounter_refs = {
        f"Encounter/{e['id']}" for e in encounters
    }

    assert len(observations) == len(encounters)
    assert all(
        o["encounter"]["reference"] in encounter_refs
        for o in observations
    )


def test_observation_measurements():
    observations = load_resources("observations.jsonl")

    assert all(
        o["resourceType"] == "Observation"
        and o["status"] == "final"
        and 60 <= o["valueQuantity"]["value"] <= 100
        for o in observations
    )
