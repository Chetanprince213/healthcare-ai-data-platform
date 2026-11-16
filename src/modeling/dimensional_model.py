import sqlite3

conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row

try:
    conn.executescript("""
        CREATE TABLE dim_patient (
            patient_key INTEGER PRIMARY KEY,
            patient_id INTEGER,
            patient_name TEXT,
            city TEXT,
            is_current INTEGER
        );

        CREATE TABLE dim_department (
            department_key INTEGER PRIMARY KEY,
            department_name TEXT
        );

        CREATE TABLE fact_visits (
            visit_id INTEGER PRIMARY KEY,
            patient_key INTEGER,
            department_key INTEGER,
            bill_amount REAL
        );

        INSERT INTO dim_patient VALUES
        (1, 101, 'Alice', 'Bengaluru', 1),
        (2, 102, 'Bob', 'Mumbai', 1),
        (3, 103, 'Carol', 'Chennai', 1);

        INSERT INTO dim_department VALUES
        (1, 'Cardiology'),
        (2, 'Neurology');

        INSERT INTO fact_visits VALUES
        (1001, 1, 1, 1200),
        (1002, 2, 2, 1500),
        (1003, 3, 1, 2000);
    """)

    print("\nFACT AND DIMENSION TABLES")

    rows = conn.execute("""
        SELECT
            p.patient_name,
            d.department_name,
            f.bill_amount
        FROM fact_visits f
        JOIN dim_patient p
            ON f.patient_key = p.patient_key
        JOIN dim_department d
            ON f.department_key = d.department_key
        ORDER BY f.visit_id
    """).fetchall()

    for row in rows:
        print(dict(row))

    assert len(rows) == 3

    # SCD Type 1: overwrite an existing attribute
    conn.execute("""
        UPDATE dim_patient
        SET city = 'Hyderabad'
        WHERE patient_id = 101
    """)

    print("\nSCD TYPE 1: OVERWRITE")
    print(dict(conn.execute("""
        SELECT patient_id, city
        FROM dim_patient
        WHERE patient_id = 101
    """).fetchone()))

    # SCD Type 2: preserve historical versions
    conn.execute("""
        UPDATE dim_patient
        SET is_current = 0
        WHERE patient_id = 102
    """)

    conn.execute("""
        INSERT INTO dim_patient
        VALUES (4, 102, 'Bob', 'Pune', 1)
    """)

    print("\nSCD TYPE 2: HISTORY")
    history = conn.execute("""
        SELECT patient_key, patient_id, city, is_current
        FROM dim_patient
        WHERE patient_id = 102
        ORDER BY patient_key
    """).fetchall()

    for row in history:
        print(dict(row))

    assert len(history) == 2

    # SCD Type 3: store current and previous values
    conn.execute("""
        ALTER TABLE dim_patient
        ADD COLUMN previous_city TEXT
    """)

    conn.execute("""
        UPDATE dim_patient
        SET previous_city = city,
            city = 'Delhi'
        WHERE patient_id = 103
    """)

    print("\nSCD TYPE 3: PREVIOUS VALUE")
    result = conn.execute("""
        SELECT patient_id, city, previous_city
        FROM dim_patient
        WHERE patient_id = 103
    """).fetchone()

    print(dict(result))

    assert result["previous_city"] == "Chennai"
    assert result["city"] == "Delhi"

    print("\nSTEP 23 DIMENSIONAL MODEL VERIFIED SUCCESSFULLY")

finally:
    conn.close()
