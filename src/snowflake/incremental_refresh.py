"""Incremental target synchronization and derived analytics."""

import json

from src.snowflake.change_data_capture import (
    PATIENT_COLUMNS,
    get_offset,
    read_stream
)


def initialize_analytics(connection):

    connection.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.regional_metrics (
            region VARCHAR PRIMARY KEY,
            total_patients BIGINT NOT NULL,
            total_visits BIGINT NOT NULL,
            missed_appointments BIGINT NOT NULL,
            average_age DOUBLE NOT NULL,
            no_show_rate_pct DOUBLE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS analytics.refresh_history (
            refresh_id VARCHAR PRIMARY KEY,
            processed_events BIGINT NOT NULL,
            affected_regions BIGINT NOT NULL,
            last_event_id BIGINT NOT NULL,
            refreshed_at TIMESTAMPTZ NOT NULL
        );
    """)


def synchronize_patient(
    connection,
    event_id,
    patient_id,
    operation,
    after
):

    connection.execute("""
        UPDATE cdc.patient_history
        SET
            valid_to_event = ?,
            is_current = FALSE
        WHERE patient_id = ?
          AND is_current = TRUE
    """, [
        event_id,
        patient_id
    ])

    if operation == "DELETE":

        connection.execute("""
            DELETE FROM cdc.current_patients
            WHERE patient_id = ?
        """, [patient_id])

        return

    values = [
        after[column]
        for column in PATIENT_COLUMNS
    ]

    connection.execute("""
        MERGE INTO cdc.current_patients AS target
        USING (
            SELECT
                CAST(? AS VARCHAR) AS patient_id,
                CAST(? AS VARCHAR) AS region,
                CAST(? AS INTEGER) AS age,
                CAST(? AS INTEGER) AS prior_visits,
                CAST(? AS INTEGER) AS no_show
        ) AS source
        ON target.patient_id = source.patient_id

        WHEN MATCHED THEN
            UPDATE SET
                region = source.region,
                age = source.age,
                prior_visits = source.prior_visits,
                no_show = source.no_show

        WHEN NOT MATCHED THEN
            INSERT (
                patient_id,
                region,
                age,
                prior_visits,
                no_show
            )
            VALUES (
                source.patient_id,
                source.region,
                source.age,
                source.prior_visits,
                source.no_show
            )
    """, values)

    connection.execute("""
        INSERT INTO cdc.patient_history
        VALUES (
            ?, ?, ?, ?, ?, ?, NULL, TRUE
        )
    """, values + [event_id])


def refresh_regions(connection, regions):

    for region in sorted(regions):

        connection.execute("""
            DELETE FROM analytics.regional_metrics
            WHERE region = ?
        """, [region])

        connection.execute("""
            INSERT INTO analytics.regional_metrics
            SELECT
                region,
                COUNT(*),
                SUM(prior_visits),
                SUM(no_show),
                ROUND(AVG(age), 2),
                ROUND(
                    100.0 * SUM(no_show) / COUNT(*),
                    2
                )
            FROM cdc.current_patients
            WHERE region = ?
            GROUP BY region
        """, [region])


def reconcile_analytics(connection):

    target_count = connection.execute("""
        SELECT COUNT(*)
        FROM cdc.current_patients
    """).fetchone()[0]

    analytics_count = connection.execute("""
        SELECT COALESCE(
            SUM(total_patients), 0
        )
        FROM analytics.regional_metrics
    """).fetchone()[0]

    current_history = connection.execute("""
        SELECT COUNT(*)
        FROM cdc.patient_history
        WHERE is_current = TRUE
    """).fetchone()[0]

    if not (
        target_count
        == analytics_count
        == current_history
    ):

        raise RuntimeError(
            "Incremental analytics reconciliation failed"
        )

    mismatch = connection.execute("""
        SELECT COUNT(*)
        FROM (
            (
                SELECT *
                FROM cdc.current_patients

                EXCEPT

                SELECT
                    patient_id,
                    region,
                    age,
                    prior_visits,
                    no_show
                FROM cdc.patient_history
                WHERE is_current = TRUE
            )

            UNION ALL

            (
                SELECT
                    patient_id,
                    region,
                    age,
                    prior_visits,
                    no_show
                FROM cdc.patient_history
                WHERE is_current = TRUE

                EXCEPT

                SELECT *
                FROM cdc.current_patients
            )
        )
    """).fetchone()[0]

    if mismatch:

        raise RuntimeError(
            "Historical state reconciliation failed"
        )

    return target_count


def consume_changes(
    connection,
    consumer_name,
    refresh_id,
    inject_failure=False
):

    connection.execute("BEGIN TRANSACTION")

    try:

        owner = connection.execute("""
            SELECT metadata_value
            FROM cdc.pipeline_metadata
            WHERE metadata_key = 'analytics_consumer'
        """).fetchone()

        if owner and owner[0] != consumer_name:

            raise ValueError(
                "Analytics target already has a consumer"
            )

        if not owner:

            connection.execute("""
                INSERT INTO cdc.pipeline_metadata
                VALUES ('analytics_consumer', ?)
            """, [consumer_name])

        events = read_stream(
            connection,
            consumer_name
        )

        regions = set()

        offset = get_offset(
            connection,
            consumer_name
        )

        for event in events:

            event_id = event[0]

            patient_id = event[1]

            operation = event[2]

            before = (
                json.loads(event[3])
                if event[3]
                else None
            )

            after = (
                json.loads(event[4])
                if event[4]
                else None
            )

            if before:

                regions.add(before["region"])

            if after:

                regions.add(after["region"])

            synchronize_patient(
                connection,
                event_id,
                patient_id,
                operation,
                after
            )

            connection.execute("""
                INSERT INTO cdc.processed_events
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, [
                consumer_name,
                event_id
            ])

            offset = event_id

        if inject_failure:

            raise RuntimeError(
                "Injected transactional failure"
            )

        refresh_regions(
            connection,
            regions
        )

        target_count = reconcile_analytics(
            connection
        )

        connection.execute("""
            INSERT INTO cdc.consumer_offsets
            VALUES (?, ?, CURRENT_TIMESTAMP)

            ON CONFLICT (consumer_name)
            DO UPDATE SET
                last_event_id = excluded.last_event_id,
                updated_at = excluded.updated_at
        """, [
            consumer_name,
            offset
        ])

        connection.execute("""
            INSERT INTO analytics.refresh_history
            VALUES (
                ?, ?, ?, ?,
                CURRENT_TIMESTAMP
            )
        """, [
            refresh_id,
            len(events),
            len(regions),
            offset
        ])

        connection.execute("COMMIT")

        return {
            "processed_events": len(events),
            "affected_regions": len(regions),
            "last_event_id": offset,
            "target_records": target_count
        }

    except Exception:

        connection.execute("ROLLBACK")

        raise


def freshness_status(
    connection,
    consumer_name,
    target_lag_seconds
):

    if target_lag_seconds < 0:

        raise ValueError("Invalid target lag")

    offset = get_offset(
        connection,
        consumer_name
    )

    pending, lag = connection.execute("""
        SELECT
            COUNT(*),
            COALESCE(
                MAX(
                    EXTRACT(
                        EPOCH FROM (
                            CURRENT_TIMESTAMP
                            - changed_at
                        )
                    )
                ),
                0
            )
        FROM cdc.change_events
        WHERE event_id > ?
    """, [offset]).fetchone()

    lag = max(0.0, float(lag))

    return {
        "pending_events": pending,
        "oldest_pending_event_age_seconds": round(
            lag, 4
        ),
        "target_lag_seconds": target_lag_seconds,
        "status": (
            "WITHIN_TARGET"
            if lag <= target_lag_seconds
            else "TARGET_EXCEEDED"
        )
    }
