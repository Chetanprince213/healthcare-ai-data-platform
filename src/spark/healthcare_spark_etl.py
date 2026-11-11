
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parents[2]

PATIENTS_FILE = BASE_DIR / "data" / "raw" / "patients.csv"
APPOINTMENTS_FILE = (
    BASE_DIR / "data" / "raw" / "appointments.csv"
)

OUTPUT_DIR = BASE_DIR / "data" / "spark" / "output"


def create_spark():
    # Use the project's Python environment
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("HealthcareSparkETL")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def run_pipeline(spark):
    if not PATIENTS_FILE.exists():
        raise FileNotFoundError(PATIENTS_FILE)

    if not APPOINTMENTS_FILE.exists():
        raise FileNotFoundError(APPOINTMENTS_FILE)

    patients = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(PATIENTS_FILE))
    )

    appointments = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(APPOINTMENTS_FILE))
    )

    print("Patient records:", patients.count())
    print("Appointment records:", appointments.count())

    if "patient_id" not in patients.columns:
        raise ValueError("Missing patient_id in patients")

    if "patient_id" not in appointments.columns:
        raise ValueError("Missing patient_id in appointments")

    # Remove records without patient IDs
    clean_patients = patients.filter(
        F.col("patient_id").isNotNull()
    ).dropDuplicates(["patient_id"])

    clean_appointments = appointments.filter(
        F.col("patient_id").isNotNull()
    )

    # Aggregate appointments before joining.
    # This avoids duplicating patient rows.
    appointment_counts = (
        clean_appointments
        .groupBy("patient_id")
        .agg(
            F.count("*").alias("appointment_count")
        )
    )

    # Broadcast the smaller aggregated dataset
    patient_analytics = (
        clean_patients
        .join(
            F.broadcast(appointment_counts),
            on="patient_id",
            how="left"
        )
        .fillna({"appointment_count": 0})
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_path = OUTPUT_DIR / "patient_analytics"

    (
        patient_analytics
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_path))
    )

    print("Analytics records:", patient_analytics.count())
    print("Output location:", output_path)

    patient_analytics.select(
        "patient_id", "appointment_count"
    ).show(10, truncate=False)

    print("SUCCESS: PySpark ETL completed!")


def main():
    spark = create_spark()

    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
