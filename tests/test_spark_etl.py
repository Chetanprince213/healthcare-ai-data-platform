
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("HealthcareSparkTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_patient_appointment_aggregation(spark):
    patients = spark.createDataFrame(
        [
            ("P001", "Alice"),
            ("P002", "Bob")
        ],
        ["patient_id", "name"]
    )

    appointments = spark.createDataFrame(
        [
            ("A001", "P001"),
            ("A002", "P001"),
            ("A003", "P002")
        ],
        ["appointment_id", "patient_id"]
    )

    counts = (
        appointments
        .groupBy("patient_id")
        .agg(F.count("*").alias("appointment_count"))
    )

    result = (
        patients
        .join(
            F.broadcast(counts),
            "patient_id",
            "left"
        )
        .collect()
    )

    actual = {
        row["patient_id"]: row["appointment_count"]
        for row in result
    }

    assert actual == {
        "P001": 2,
        "P002": 1
    }
