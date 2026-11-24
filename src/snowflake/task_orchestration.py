"""Dependency-aware task execution and bounded scheduling."""

import time

from datetime import datetime, timezone


def execute_dag(
    tasks,
    max_attempts=3,
    retry_delay_seconds=1
):

    if max_attempts < 1:

        raise ValueError("Invalid retry count")

    if retry_delay_seconds < 0:

        raise ValueError("Invalid retry delay")

    completed = set()

    pending = dict(tasks)

    history = []

    results = {}

    while pending:

        ready = [
            name
            for name, task in pending.items()
            if set(
                task["dependencies"]
            ).issubset(completed)
        ]

        if not ready:

            raise ValueError(
                "Circular or unresolved task dependencies"
            )

        for name in ready:

            task = pending[name]

            for attempt in range(
                1, max_attempts + 1
            ):

                started = time.perf_counter()

                timestamp = datetime.now(
                    timezone.utc
                ).isoformat()

                try:

                    results[name] = task["function"]()

                    history.append({
                        "task": name,
                        "attempt": attempt,
                        "status": "SUCCESS",
                        "timestamp": timestamp,
                        "duration_seconds": round(
                            time.perf_counter()
                            - started,
                            6
                        )
                    })

                    completed.add(name)

                    del pending[name]

                    break

                except Exception as error:

                    history.append({
                        "task": name,
                        "attempt": attempt,
                        "status": "FAILED",
                        "timestamp": timestamp,
                        "error": str(error)
                    })

                    if (
                        not isinstance(error, OSError)
                        or attempt == max_attempts
                    ):

                        raise

                    time.sleep(
                        retry_delay_seconds
                        * (2 ** (attempt - 1))
                    )

    return {
        "status": "SUCCESS",
        "history": history,
        "results": results
    }


def run_schedule(
    function,
    cycles,
    interval_seconds
):

    if cycles < 1 or interval_seconds < 0:

        raise ValueError(
            "Invalid schedule configuration"
        )

    executions = []

    for cycle in range(cycles):

        result = function()

        executions.append({
            "cycle": cycle + 1,
            "executed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "result": result
        })

        if cycle + 1 < cycles:

            time.sleep(interval_seconds)

    return executions
