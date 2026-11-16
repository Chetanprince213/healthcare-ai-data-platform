import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data" / "event_streaming"
EVENT_LOG = DATA_DIR / "healthcare_events.jsonl"
OFFSET_FILE = DATA_DIR / "consumer_offset.json"


def publish_event(patient_id, event_type):
    event = {
        "key": patient_id,
        "event": event_type,
    }

    with EVENT_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event) + "\n")


def read_offset():
    if not OFFSET_FILE.exists():
        return 0

    with OFFSET_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)["offset"]


def save_offset(offset):
    temporary = OFFSET_FILE.with_suffix(".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump({"offset": offset}, file)

    temporary.replace(OFFSET_FILE)


def consume_events():
    offset = read_offset()
    processed = []

    with EVENT_LOG.open("r", encoding="utf-8") as file:
        for index, line in enumerate(file):
            if index < offset:
                continue

            event = json.loads(line)
            processed.append(event)

            print(
                f"Consumed offset={index}: "
                f"{event['key']} -> {event['event']}"
            )

            save_offset(index + 1)

    return processed


def main():
    if DATA_DIR.exists():
        raise FileExistsError(
            "Event streaming data already exists. "
            "Existing data preserved."
        )

    DATA_DIR.mkdir(parents=True)

    publish_event("P0001", "appointment_created")
    publish_event("P0002", "appointment_created")

    print("\nFIRST CONSUMER RUN")
    first_batch = consume_events()
    assert len(first_batch) == 2

    print("\nCONSUMER RESTART")
    second_batch = consume_events()
    assert len(second_batch) == 0

    publish_event("P0003", "appointment_cancelled")

    print("\nNEW EVENT AFTER RESTART")
    third_batch = consume_events()
    assert len(third_batch) == 1

    assert third_batch[0]["key"] == "P0003"
    assert read_offset() == 3

    print("\nFIRST RUN: 2 EVENTS")
    print("RESTART: 0 DUPLICATE EVENTS")
    print("NEW EVENTS: 1")
    print("FINAL CONSUMER OFFSET: 3")

    print("\nLOCAL SIMULATION - NOT DEPLOYED TO CLOUD")
    print("HEALTHCARE EVENT STREAMING VERIFIED SUCCESSFULLY")


if __name__ == "__main__":
    main()
