import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)

base = Path(__file__).resolve().parents[2]
lab = base / "data" / "patient_streaming"
input_dir = lab / "input"
output_dir = lab / "output"
checkpoint_dir = lab / "checkpoint"

spark = (
    SparkSession.builder
    .appName("component_25HealthcareStreaming")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

schema = StructType([
    StructField("patient_id", IntegerType(), False),
    StructField("patient_name", StringType(), True),
    StructField("department", StringType(), True),
])

def create_batch(filename, records):
    path = input_dir / filename
    if path.exists():
        print(f"Existing input preserved: {path.name}")
        return

    with path.open("x", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

def process_stream():
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

    return spark.read.parquet(str(output_dir)).count()

try:
    input_dir.mkdir(parents=True, exist_ok=True)

    if output_dir.exists() or checkpoint_dir.exists():
        raise FileExistsError(
            "Streaming output or checkpoint already exists. "
            "Inspect existing data before starting a new run."
        )

    create_batch("batch_001.json", [
        {"patient_id": 1, "patient_name": "Alice",
         "department": "Cardiology"},
        {"patient_id": 2, "patient_name": "Bob",
         "department": "Neurology"},
    ])

    first_count = process_stream()
    print("First batch total:", first_count)

    assert first_count == 2

    create_batch("batch_002.json", [
        {"patient_id": 3, "patient_name": "Carol",
         "department": "Cardiology"},
        {"patient_id": 4, "patient_name": "David",
         "department": "Orthopedics"},
    ])

    second_count = process_stream()
    print("Second batch cumulative total:", second_count)

    assert second_count == 4
    assert checkpoint_dir.exists()
    assert output_dir.exists()

    print("\nHEALTHCARE STREAMING VERIFIED SUCCESSFULLY")

finally:
    spark.stop()
