"""Reproducible synthetic healthcare dataset generation."""

import argparse
import csv
import random

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

COLUMNS = [
    "patient_id",
    "region",
    "age",
    "prior_visits",
    "no_show"
]


def generate_patients(count=100, seed=42):

    generator = random.Random(seed)

    return [
        {
            "patient_id": f"P{index:06d}",
            "region": generator.choice(["N", "S", "E"]),
            "age": generator.randint(18, 85),
            "prior_visits": generator.randint(0, 15),
            "no_show": generator.randint(0, 1)
        }
        for index in range(1, count + 1)
    ]


def write_patients(destination, count=100, seed=42):

    destination = Path(destination)

    destination.parent.mkdir(
        parents=True, exist_ok=True
    )

    if destination.exists():

        raise FileExistsError(
            "Existing source dataset will not be overwritten"
        )

    with destination.open(
        "w", encoding="utf-8", newline=""
    ) as output:

        writer = csv.DictWriter(
            output,
            fieldnames=COLUMNS
        )

        writer.writeheader()

        writer.writerows(
            generate_patients(count, seed)
        )

    return destination


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--count", type=int, default=100
    )

    parser.add_argument(
        "--seed", type=int, default=42
    )

    parser.add_argument(
        "--output",
        default=str(
            ROOT / "data/orchestration/patients.csv"
        )
    )

    arguments = parser.parse_args()

    if arguments.count < 1:

        parser.error("Count must be positive")

    write_patients(
        arguments.output,
        arguments.count,
        arguments.seed
    )
