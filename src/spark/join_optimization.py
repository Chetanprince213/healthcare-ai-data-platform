from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count

spark = (
    SparkSession.builder
    .appName("Step24PySparkOptimization")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

try:
    patients = spark.createDataFrame(
        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carol"),
            (4, "David"),
            (5, "Eva"),
        ],
        ["patient_id", "patient_name"],
    )

    appointments = spark.createDataFrame(
        [
            (101, 1),
            (102, 1),
            (103, 2),
            (104, 3),
            (105, 3),
            (106, 5),
        ],
        ["appointment_id", "patient_id"],
    )

    appointment_counts = appointments.groupBy(
        "patient_id"
    ).agg(
        count("*").alias("appointment_count")
    )

    print("\n=== NORMAL JOIN EXECUTION PLAN ===")

    normal_join = patients.join(
        appointment_counts,
        "patient_id",
        "left",
    )

    normal_join.explain(True)

    print("\n=== BROADCAST JOIN EXECUTION PLAN ===")

    optimized_join = patients.join(
        broadcast(appointment_counts),
        "patient_id",
        "left",
    )

    optimized_join.explain(True)

    print("\n=== OPTIMIZED RESULTS ===")
    optimized_join.orderBy("patient_id").show()

    plan = optimized_join._jdf.queryExecution().executedPlan().toString()

    assert "BroadcastHashJoin" in plan, (
        "BroadcastHashJoin not found in physical plan"
    )

    assert optimized_join.count() == 5

    print("\nSTEP 24 PYSPARK OPTIMIZATION VERIFIED SUCCESSFULLY")

finally:
    spark.stop()