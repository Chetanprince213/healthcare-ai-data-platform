import sqlite3

# Create a temporary database in memory.
# No existing database files will be modified.
conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row

try:
    conn.executescript("""
        CREATE TABLE patients (
            patient_id INTEGER PRIMARY KEY,
            patient_name TEXT NOT NULL,
            department TEXT NOT NULL
        );

        CREATE TABLE visits (
            visit_id INTEGER PRIMARY KEY,
            patient_id INTEGER NOT NULL,
            visit_date TEXT NOT NULL,
            bill_amount REAL NOT NULL,
            FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
        );

        INSERT INTO patients VALUES
            (1, 'Alice', 'Cardiology'),
            (2, 'Bob', 'Neurology'),
            (3, 'Carol', 'Cardiology'),
            (4, 'David', 'Orthopedics'),
            (5, 'Eva', 'Neurology');

        INSERT INTO visits VALUES
            (101, 1, '2026-01-10', 1200),
            (102, 1, '2026-02-15', 1800),
            (103, 2, '2026-01-20', 1500),
            (104, 3, '2026-02-05', 2000),
            (105, 3, '2026-03-01', 2500),
            (106, 4, '2026-01-25', 1000),
            (107, 5, '2026-02-18', 1700);
    """)

    def run_query(title, query):
        print(f"\n{'=' * 55}")
        print(title)
        print("=" * 55)

        rows = conn.execute(query).fetchall()

        for row in rows:
            print(dict(row))

        print(f"Rows returned: {len(rows)}")
        return rows

    # QUERY 1: GROUP BY aggregation
    department_metrics = run_query(
        "QUERY 1: Department-level metrics",
        """
        SELECT
            p.department,
            COUNT(v.visit_id) AS total_visits,
            ROUND(SUM(v.bill_amount), 2) AS total_revenue
        FROM patients p
        JOIN visits v
            ON p.patient_id = v.patient_id
        GROUP BY p.department
        ORDER BY p.department;
        """
    )

    # QUERY 2: Common Table Expression (CTE)
    patient_spending = run_query(
        "QUERY 2: CTE - Total spending per patient",
        """
        WITH patient_totals AS (
            SELECT
                patient_id,
                SUM(bill_amount) AS total_spending
            FROM visits
            GROUP BY patient_id
        )
        SELECT
            p.patient_name,
            pt.total_spending
        FROM patient_totals pt
        JOIN patients p
            ON pt.patient_id = p.patient_id
        ORDER BY pt.total_spending DESC;
        """
    )

    # QUERY 3: ROW_NUMBER window function
    latest_visits = run_query(
        "QUERY 3: ROW_NUMBER - Latest visit per patient",
        """
        WITH ranked_visits AS (
            SELECT
                patient_id,
                visit_date,
                bill_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY patient_id
                    ORDER BY visit_date DESC, visit_id DESC
                ) AS row_num
            FROM visits
        )
        SELECT
            patient_id,
            visit_date,
            bill_amount
        FROM ranked_visits
        WHERE row_num = 1
        ORDER BY patient_id;
        """
    )

    # QUERY 4: RANK window function
    ranked_spending = run_query(
        "QUERY 4: RANK - Patient spending ranking",
        """
        WITH patient_totals AS (
            SELECT
                patient_id,
                SUM(bill_amount) AS total_spending
            FROM visits
            GROUP BY patient_id
        )
        SELECT
            patient_id,
            total_spending,
            RANK() OVER (
                ORDER BY total_spending DESC
            ) AS spending_rank
        FROM patient_totals
        ORDER BY spending_rank, patient_id;
        """
    )

    # Automated verification
    assert len(department_metrics) == 3
    assert len(patient_spending) == 5
    assert len(latest_visits) == 5
    assert len(ranked_spending) == 5

    assert sum(
        row["total_visits"] for row in department_metrics
    ) == 7

    assert sum(
        row["total_revenue"] for row in department_metrics
    ) == 11700

    print("\nSTEP 22 SQL ANALYTICS VERIFIED SUCCESSFULLY")

finally:
    conn.close()