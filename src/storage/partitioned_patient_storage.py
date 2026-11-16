from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = PROJECT_ROOT / "data" / "hive_style" / "patients"

if OUTPUT.exists():
    raise FileExistsError(
        "Partitioned output already exists. Existing data preserved."
    )

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

try:
    con.execute("""
        CREATE TABLE patients AS
        SELECT * FROM (
            VALUES
                (1, 'Alice', 'North'),
                (2, 'Bob', 'South'),
                (3, 'Carol', 'North'),
                (4, 'David', 'South'),
                (5, 'Eva', 'North')
        ) AS t(patient_id, patient_name, region)
    """)

    con.execute(
        """
        COPY patients TO ?
        (FORMAT PARQUET, PARTITION_BY (region))
        """,
        [str(OUTPUT)]
    )

    north = OUTPUT / "region=North"
    south = OUTPUT / "region=South"

    assert north.is_dir()
    assert south.is_dir()

    parquet_path = str(OUTPUT / "**" / "*.parquet").replace("\\", "/")

    results = con.execute(
        """
        SELECT region, COUNT(*) AS patient_count
        FROM read_parquet(?, hive_partitioning=true)
        GROUP BY region
        ORDER BY region
        """,
        [parquet_path]
    ).fetchall()

    print("\nPARTITIONED PATIENT COUNTS")
    for region, count in results:
        print(f"{region}: {count}")

    assert results == [("North", 3), ("South", 2)]

    filtered = con.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?, hive_partitioning=true)
        WHERE region = 'North'
        """,
        [parquet_path]
    ).fetchone()[0]

    assert filtered == 3

    print("\nPARTITION DIRECTORIES VERIFIED")
    print("SQL COUNTS VERIFIED")
    print("LOCAL SIMULATION - NOT DEPLOYED TO CLOUD")
    print("PARTITIONED HEALTHCARE STORAGE VERIFIED SUCCESSFULLY")

finally:
    con.close()
