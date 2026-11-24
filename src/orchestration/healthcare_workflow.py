"""Dependency-aware healthcare workflow orchestration."""

import csv
import json
import logging
import time
import uuid

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

CONFIG = ROOT / "configs/orchestration/healthcare_workflow.json"

LOGGER = logging.getLogger(__name__)


def utc_now():

    return datetime.now(timezone.utc).isoformat()


def write_json(path, payload):

    path = Path(path)

    path.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporary = path.with_suffix(".tmp")

    temporary.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8"
    )

    temporary.replace(path)


def read_patients(path):

    with Path(path).open(
        encoding="utf-8-sig",
        newline=""
    ) as source:

        reader = csv.DictReader(source)

        expected = [
            "patient_id",
            "region",
            "age",
            "prior_visits",
            "no_show"
        ]

        if reader.fieldnames != expected:

            raise ValueError(
                "Unexpected source schema"
            )

        rows = list(reader)

    if not rows:

        raise ValueError("Empty patient dataset")

    return rows


def validate_patients(rows):

    identifiers = set()

    for row in rows:

        patient_id = row["patient_id"].strip()

        region = row["region"].strip()

        age = int(row["age"])

        visits = int(row["prior_visits"])

        no_show = int(row["no_show"])

        if not patient_id or not region:

            raise ValueError(
                "Missing mandatory attributes"
            )

        if patient_id in identifiers:

            raise ValueError(
                "Duplicate patient identifier"
            )

        if not 0 <= age <= 120:

            raise ValueError("Invalid patient age")

        if visits < 0 or no_show not in (0, 1):

            raise ValueError(
                "Invalid utilization values"
            )

        identifiers.add(patient_id)

    return rows


def aggregate_patients(rows):

    counts = Counter(
        row["region"].strip()
        for row in rows
    )

    if sum(counts.values()) != len(rows):

        raise RuntimeError(
            "Regional reconciliation failed"
        )

    return {
        "total_patients": len(rows),
        "regional_counts": dict(sorted(counts.items()))
    }


def execute_task(
    name,
    function,
    history,
    max_attempts=3,
    retry_delay=1
):

    if max_attempts < 1 or retry_delay < 0:

        raise ValueError("Invalid retry configuration")

    for attempt in range(1, max_attempts + 1):

        started = time.perf_counter()

        try:

            result = function()

            history.append({
                "task": name,
                "attempt": attempt,
                "status": "SUCCESS",
                "duration_seconds": round(
                    time.perf_counter() - started, 6
                ),
                "timestamp": utc_now()
            })

            LOGGER.info(
                "Task completed | task=%s | attempt=%s",
                name,
                attempt
            )

            return result

        except Exception as error:

            history.append({
                "task": name,
                "attempt": attempt,
                "status": "FAILED",
                "error": str(error),
                "duration_seconds": round(
                    time.perf_counter() - started, 6
                ),
                "timestamp": utc_now()
            })

            LOGGER.warning(
                "Task failed | task=%s | attempt=%s",
                name,
                attempt
            )

            if (
                not isinstance(error, OSError)
                or attempt == max_attempts
            ):

                raise

            time.sleep(
                retry_delay * (2 ** (attempt - 1))
            )


def run_workflow():

    config = json.loads(
        CONFIG.read_text(encoding="utf-8")
    )

    log_path = ROOT / config["log_path"]

    log_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                log_path, encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )

    output = ROOT / config["output_path"]

    history = []

    run_id = str(uuid.uuid4())

    status = "FAILED"

    started_at = utc_now()

    def task(name, function):

        return execute_task(
            name,
            function,
            history,
            config["max_attempts"],
            config["retry_delay_seconds"]
        )

    try:

        bronze = task(
            "bronze_source_ingestion",
            lambda: read_patients(
                ROOT / config["source_path"]
            )
        )

        silver = task(
            "silver_data_validation",
            lambda: validate_patients(bronze)
        )

        gold = task(
            "gold_regional_aggregation",
            lambda: aggregate_patients(silver)
        )

        write_json(
            output / "regional_metrics.json",
            gold
        )

        status = "SUCCESS"

        return gold

    finally:

        write_json(
            output / "workflow_history.json",
            {
                "run_id": run_id,
                "started_at": started_at,
                "completed_at": utc_now(),
                "status": status,
                "tasks": history
            }
        )


if __name__ == "__main__":

    run_workflow()
