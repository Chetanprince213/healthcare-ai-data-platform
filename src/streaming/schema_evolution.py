import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)

base = Path(__file__).resolve().parents[2]
lab = base / "data" / "step25_schema_evolution"

input_dir = lab / "input"
output_dir = lab / "output"
checkpoint_dir = lab / "checkpoint"

# Explicitly define the expanded schema.
schema = StructType([
    StructField("patient_id", IntegerType(), False),
    StructField("patient_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("visit_type", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("Step25SchemaEvolution")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

try:
    if lab.exists():
        raise FileExistsError(
            "Schema evolution lab already exists. "
            "Existing files have been preserved."
        )

    input_dir.mkdir(parents=True)

    records = [
        {
            "patient_id": 5,
            "patient_name": "Eva",
            "department": "Neurology",
        },
        {
            "patient_id": 6,
            "patient_name": "Frank",
            "department": "Cardiology",
            "visit_type": "Follow-up",
        },
    ]

    with (input_dir / "batch_003.json").open(
        "x", encoding="utf-8"
    ) as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

    stream = (
        spark.readStream
        .schema(schema)
        .json(str(input_dir))
        .writeStream
        .format("parquet")
        .option("path", str(output_dir))
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )

    stream.awaitTermination()

    result = spark.read.parquet(str(output_dir))
    result.orderBy("patient_id").show(truncate=False)

    assert result.count() == 2

    old_record = result.filter(
        "patient_id = 5"
    ).first()

    new_record = result.filter(
        "patient_id = 6"
    ).first()

    assert old_record["visit_type"] is None
    assert new_record["visit_type"] == "Follow-up"

    print("\nSTEP 25 SCHEMA EVOLUTION VERIFIED SUCCESSFULLY")

finally:
    spark.stop()