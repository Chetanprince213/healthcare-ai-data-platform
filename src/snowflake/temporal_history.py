"""Event-version history, snapshots and non-destructive recovery."""

from src.snowflake.change_data_capture import (
    PATIENT_COLUMNS
)


def initialize_snapshots(connection):

    connection.execute("""
        CREATE SCHEMA IF NOT EXISTS temporal;

        CREATE TABLE IF NOT EXISTS temporal.snapshot_catalog (
            snapshot_name VARCHAR PRIMARY KEY,
            event_version BIGINT NOT NULL,
            record_count BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS temporal.snapshot_patients (
            snapshot_name VARCHAR NOT NULL,
            patient_id VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL,
            PRIMARY KEY (
                snapshot_name,
                patient_id
            )
        );

        CREATE TABLE IF NOT EXISTS temporal.recovered_patients (
            patient_id VARCHAR PRIMARY KEY,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL
        );
    """)


def query_as_of(connection, event_version):

    maximum = connection.execute("""
        SELECT COALESCE(
            MAX(last_event_id), 0
        )
        FROM cdc.consumer_offsets
        WHERE consumer_name = (
            SELECT metadata_value
            FROM cdc.pipeline_metadata
            WHERE metadata_key = 'analytics_consumer'
        )
    """).fetchone()[0]

    if not 0 <= event_version <= maximum:

        raise ValueError(
            "Historical version is not available"
        )

    rows = connection.execute("""
        SELECT
            patient_id,
            region,
            age,
            prior_visits,
            no_show
        FROM cdc.patient_history
        WHERE valid_from_event <= ?
          AND (
              valid_to_event IS NULL
              OR valid_to_event > ?
          )
        ORDER BY patient_id
    """, [
        event_version,
        event_version
    ]).fetchall()

    return [
        dict(zip(PATIENT_COLUMNS, row))
        for row in rows
    ]


def create_snapshot(
    connection,
    snapshot_name,
    event_version
):

    connection.execute("BEGIN TRANSACTION")

    try:

        existing = connection.execute("""
            SELECT event_version, record_count
            FROM temporal.snapshot_catalog
            WHERE snapshot_name = ?
        """, [snapshot_name]).fetchone()

        if existing:

            if existing[0] != event_version:

                raise ValueError(
                    "Snapshot name already exists"
                )

            connection.execute("COMMIT")

            return existing[1]

        rows = query_as_of(
            connection,
            event_version
        )

        if rows:

            connection.executemany("""
                INSERT INTO temporal.snapshot_patients
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                [snapshot_name] + [
                    row[column]
                    for column in PATIENT_COLUMNS
                ]
                for row in rows
            ])

        connection.execute("""
            INSERT INTO temporal.snapshot_catalog
            VALUES (
                ?, ?, ?,
                CURRENT_TIMESTAMP
            )
        """, [
            snapshot_name,
            event_version,
            len(rows)
        ])

        connection.execute("COMMIT")

        return len(rows)

    except Exception:

        connection.execute("ROLLBACK")

        raise


def restore_snapshot(
    connection,
    snapshot_name
):

    connection.execute("BEGIN TRANSACTION")

    try:

        expected = connection.execute("""
            SELECT record_count
            FROM temporal.snapshot_catalog
            WHERE snapshot_name = ?
        """, [snapshot_name]).fetchone()

        if expected is None:

            raise ValueError(
                "Snapshot does not exist"
            )

        connection.execute("""
            DELETE FROM temporal.recovered_patients
        """)

        connection.execute("""
            INSERT INTO temporal.recovered_patients
            SELECT
                patient_id,
                region,
                age,
                prior_visits,
                no_show
            FROM temporal.snapshot_patients
            WHERE snapshot_name = ?
        """, [snapshot_name])

        restored = connection.execute("""
            SELECT COUNT(*)
            FROM temporal.recovered_patients
        """).fetchone()[0]

        if restored != expected[0]:

            raise RuntimeError(
                "Snapshot recovery reconciliation failed"
            )

        connection.execute("COMMIT")

        return restored

    except Exception:

        connection.execute("ROLLBACK")

        raise
