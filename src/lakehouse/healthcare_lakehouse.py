from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    trim,
)

BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE = BASE_DIR / "data" / "lakehouse" / "bronze" / "patients"
SILVER = BASE_DIR / "data" / "lakehouse" / "silver" / "patients"
GOLD = BASE_DIR / "data" / "lakehouse" / "gold" / "patient_analytics"

for path in [BRONZE, SILVER, GOLD]:
    if path.exists():
        raise FileExistsError(
            f"Output already exists: {path}. "
            "No files were overwritten."
        )

builder = (
    SparkSession.builder
    .appName("HealthcareDeltaLakehouse")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    # BRONZE: Raw synthetic patient records
    records = [
        ("P001", "Alice", 35, "North"),
        ("P002", "Bob", 42, "South"),
        ("P003", "Carol", 29, "North"),
        ("P004", "David", 51, "East"),
        ("P005", "Eva", 38, "South"),
    ]

    columns = ["patient_id", "name", "age", "region"]

    bronze_df = spark.createDataFrame(records, columns)

    bronze_df.write.format("delta").mode("errorifexists").save(
        str(BRONZE)
    )

    print("BRONZE LAYER CREATED")

    # SILVER: Clean and validate records
    silver_df = (
        spark.read.format("delta").load(str(BRONZE))
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn("region", trim(col("region")))
        .filter(col("patient_id").isNotNull())
        .filter(col("age").between(0, 120))
        .dropDuplicates(["patient_id"])
        .withColumn("processed_at", current_timestamp())
    )

    silver_df.write.format("delta").mode("errorifexists").save(
        str(SILVER)
    )

    print("SILVER LAYER CREATED")

    # GOLD: Aggregated patient analytics
    gold_df = (
        spark.read.format("delta").load(str(SILVER))
        .groupBy("region")
        .agg(count("*").alias("patient_count"))
    )

    gold_df.write.format("delta").mode("errorifexists").save(
        str(GOLD)
    )

    print("GOLD LAYER CREATED")

    # Verify the actual Delta table
    result = spark.read.format("delta").load(str(GOLD))
    result.orderBy("region").show()

    for path in [BRONZE, SILVER, GOLD]:
        assert (path / "_delta_log").is_dir(), (
            f"Missing Delta transaction log: {path}"
        )

    print("DELTA LAKEHOUSE COMPLETED SUCCESSFULLY")

finally:
    spark.stop()