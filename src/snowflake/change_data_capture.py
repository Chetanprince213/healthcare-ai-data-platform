"""Transactional change tracking and stream consumption."""

import hashlib
import json
import uuid

from datetime import datetime, timezone
from pathlib import Path


PATIENT_COLUMNS = [
    "patient_id",
    "region",
    "age",
    "prior_visits",
    "no_show"
]


def utc_now():

    return datetime.now(timezone.utc)


def initialize_database(connection):

    connection.execute("""
        CREATE SCHEMA IF NOT EXISTS cdc;

        CREATE SEQUENCE IF NOT EXISTS cdc.event_sequence
        START 1;

        CREATE TABLE IF NOT EXISTS cdc.source_patients (
            patient_id VARCHAR PRIMARY KEY,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL,
            CHECK (age BETWEEN 0 AND 120),
            CHECK (prior_visits >= 0),
            CHECK (no_show IN (0, 1))
        );

        CREATE TABLE IF NOT EXISTS cdc.change_events (
            event_id BIGINT PRIMARY KEY
                DEFAULT nextval('cdc.event_sequence'),
            event_key VARCHAR UNIQUE NOT NULL,
            patient_id VARCHAR NOT NULL,
            operation VARCHAR NOT NULL,
            before_payload VARCHAR,
            after_payload VARCHAR,
            changed_at TIMESTAMPTZ NOT NULL,
            CHECK (
                operation IN (
                    'INSERT', 'UPDATE', 'DELETE'
                )
            )
        );

        CREATE TABLE IF NOT EXISTS cdc.consumer_offsets (
            consumer_name VARCHAR PRIMARY KEY,
            last_event_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cdc.current_patients (
            patient_id VARCHAR PRIMARY KEY,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cdc.patient_history (
            patient_id VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            prior_visits INTEGER NOT NULL,
            no_show INTEGER NOT NULL,
            valid_from_event BIGINT NOT NULL,
            valid_to_event BIGINT,
            is_current BOOLEAN NOT NULL,
            PRIMARY KEY (
                patient_id,
                valid_from_event
            )
        );

        CREATE TABLE IF NOT EXISTS cdc.processed_events (
            consumer_name VARCHAR NOT NULL,
            event_id BIGINT NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (
                consumer_name,
                event_id
            )
        );

        CREATE TABLE IF NOT EXISTS cdc.pipeline_metadata (
            metadata_key VARCHAR PRIMARY KEY,
            metadata_value VARCHAR NOT NULL
        );
    """)


def validate_patient(payload):

    patient = {
        "patient_id": str(
            payload["patient_id"]
        ).strip(),
        "region": str(
            payload["region"]
        ).strip(),
        "age": int(payload["age"]),
        "prior_visits": int(
            payload["prior_visits"]
        ),
        "no_show": int(payload["no_show"])
    }

    if not patient["patient_id"]:

        raise ValueError("Missing patient identifier")

    if not patient["region"]:

        raise ValueError("Missing region")

    if not 0 <= patient["age"] <= 120:

        raise ValueError("Invalid age")

    if patient["prior_visits"] < 0:

        raise ValueError("Invalid visit count")

    if patient["no_show"] not in (0, 1):

        raise ValueError("Invalid appointment status")

    return patient


def get_patient(connection, patient_id):

    row = connection.execute("""
        SELECT
            patient_id,
            region,
            age,
            prior_visits,
            no_show
        FROM cdc.source_patients
        WHERE patient_id = ?
    """, [patient_id]).fetchone()

    if row is None:

        return None

    return dict(zip(PATIENT_COLUMNS, row))


def apply_change(
    connection,
    operation,
    patient_id,
    payload=None,
    event_key=None
):

    operation = operation.upper()

    patient_id = str(patient_id).strip()

    event_key = event_key or str(uuid.uuid4())

    if operation not in (
        "INSERT", "UPDATE", "DELETE"
    ):

        raise ValueError("Unsupported operation")

    if not patient_id:

        raise ValueError("Missing patient identifier")

    after = None

    if operation != "DELETE":

        if payload is None:

            raise ValueError("Payload required")

        after = validate_patient(payload)

        if after["patient_id"] != patient_id:

            raise ValueError(
                "Patient identifier mismatch"
            )

    connection.execute("BEGIN TRANSACTION")

    try:

        previous = connection.execute("""
            SELECT
                event_id,
                operation,
                patient_id,
                after_payload
            FROM cdc.change_events
            WHERE event_key = ?
        """, [event_key]).fetchone()

        if previous:

            previous_payload = (
                json.loads(previous[3])
                if previous[3]
                else None
            )

            if (
                previous[1] != operation
                or previous[2] != patient_id
                or previous_payload != after
            ):

                raise ValueError(
                    "Conflicting event key reuse"
                )

            connection.execute("COMMIT")

            return previous[0]

        before = get_patient(
            connection,
            patient_id
        )

        if operation == "INSERT":

            if before is not None:

                raise ValueError(
                    "Patient already exists"
                )

            connection.execute("""
                INSERT INTO cdc.source_patients
                VALUES (?, ?, ?, ?, ?)
            """, [
                after[column]
                for column in PATIENT_COLUMNS
            ])

        elif operation == "UPDATE":

            if before is None:

                raise ValueError(
                    "Patient does not exist"
                )

            connection.execute("""
                UPDATE cdc.source_patients
                SET region = ?,
                    age = ?,
                    prior_visits = ?,
                    no_show = ?
                WHERE patient_id = ?
            """, [
                after["region"],
                after["age"],
                after["prior_visits"],
                after["no_show"],
                patient_id
            ])

        else:

            if before is None:

                raise ValueError(
                    "Patient does not exist"
                )

            connection.execute("""
                DELETE FROM cdc.source_patients
                WHERE patient_id = ?
            """, [patient_id])

        event_id = connection.execute("""
            INSERT INTO cdc.change_events (
                event_key,
                patient_id,
                operation,
                before_payload,
                after_payload,
                changed_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING event_id
        """, [
            event_key,
            patient_id,
            operation,
            json.dumps(before) if before else None,
            json.dumps(after) if after else None,
            utc_now()
        ]).fetchone()[0]

        connection.execute("COMMIT")

        return event_id

    except Exception:

        connection.execute("ROLLBACK")

        raise


def bootstrap_source(connection, source_path):

    source_path = Path(source_path)

    fingerprint = hashlib.sha256(
        source_path.read_bytes()
    ).hexdigest()

    existing = connection.execute("""
        SELECT metadata_value
        FROM cdc.pipeline_metadata
        WHERE metadata_key = 'source_fingerprint'
    """).fetchone()

    if existing:

        if existing[0] != fingerprint:

            raise ValueError(
                "Source fingerprint changed. "
                "Use explicit CDC events for changes."
            )

        return 0

    connection.execute("BEGIN TRANSACTION")

    try:

        connection.execute("""
            CREATE OR REPLACE TEMP TABLE bootstrap_patients AS
            SELECT
                TRIM(patient_id) AS patient_id,
                TRIM(region) AS region,
                CAST(age AS INTEGER) AS age,
                CAST(prior_visits AS INTEGER) AS prior_visits,
                CAST(no_show AS INTEGER) AS no_show
            FROM read_csv_auto(
                ?,
                all_varchar = true
            )
        """, [str(source_path)])

        rows, unique, invalid = connection.execute("""
            SELECT
                COUNT(*),
                COUNT(DISTINCT patient_id),
                COUNT(*) FILTER (
                    WHERE patient_id IS NULL
                       OR patient_id = ''
                       OR region IS NULL
                       OR region = ''
                       OR age IS NULL
                       OR age NOT BETWEEN 0 AND 120
                       OR prior_visits IS NULL
                       OR prior_visits < 0
                       OR no_show IS NULL
                       OR no_show NOT IN (0, 1)
                )
            FROM bootstrap_patients
        """).fetchone()

        if rows == 0 or rows != unique or invalid:

            raise ValueError(
                "Bootstrap data validation failed"
            )

        connection.execute("""
            INSERT INTO cdc.source_patients
            SELECT *
            FROM bootstrap_patients
        """)

        connection.execute("""
            INSERT INTO cdc.change_events (
                event_key,
                patient_id,
                operation,
                before_payload,
                after_payload,
                changed_at
            )
            SELECT
                'bootstrap:' || patient_id,
                patient_id,
                'INSERT',
                NULL,
                CAST(to_json(
                    struct_pack(
                        patient_id := patient_id,
                        region := region,
                        age := age,
                        prior_visits := prior_visits,
                        no_show := no_show
                    )
                ) AS VARCHAR),
                CURRENT_TIMESTAMP
            FROM bootstrap_patients
            ORDER BY patient_id
        """)

        connection.execute("""
            INSERT INTO cdc.pipeline_metadata
            VALUES ('source_fingerprint', ?)
        """, [fingerprint])

        connection.execute("COMMIT")

        return rows

    except Exception:

        connection.execute("ROLLBACK")

        raise


def get_offset(connection, consumer_name):

    result = connection.execute("""
        SELECT last_event_id
        FROM cdc.consumer_offsets
        WHERE consumer_name = ?
    """, [consumer_name]).fetchone()

    return result[0] if result else 0


def read_stream(
    connection,
    consumer_name,
    append_only=False
):

    offset = get_offset(
        connection,
        consumer_name
    )

    query = """
        SELECT
            event_id,
            patient_id,
            operation,
            before_payload,
            after_payload
        FROM cdc.change_events
        WHERE event_id > ?
    """

    if append_only:

        query += " AND operation = 'INSERT'"

    query += " ORDER BY event_id"

    return connection.execute(
        query, [offset]
    ).fetchall()


def consume_append_only(
    connection,
    consumer_name
):

    connection.execute("BEGIN TRANSACTION")

    try:

        events = read_stream(
            connection,
            consumer_name,
            append_only=True
        )

        high_watermark = connection.execute("""
            SELECT COALESCE(MAX(event_id), 0)
            FROM cdc.change_events
        """).fetchone()[0]

        connection.execute("""
            INSERT INTO cdc.consumer_offsets
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (consumer_name)
            DO UPDATE SET
                last_event_id = excluded.last_event_id,
                updated_at = excluded.updated_at
        """, [
            consumer_name,
            high_watermark
        ])

        connection.execute("COMMIT")

        return events

    except Exception:

        connection.execute("ROLLBACK")

        raise
