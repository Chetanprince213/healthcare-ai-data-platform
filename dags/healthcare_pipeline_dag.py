
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = Path(
    "/opt/airflow/healthcare-ai-platform"
)

DEFAULT_ARGS = {
    "owner": "healthcare-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_script(script_path):
    full_path = PROJECT_DIR / script_path

    subprocess.run(
        [sys.executable, str(full_path)],
        cwd=str(PROJECT_DIR),
        check=True
    )


with DAG(
    dag_id="healthcare_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Healthcare Data Lakehouse ETL Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "etl", "fhir", "cdc", "scd2"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_synthetic_data",
        python_callable=run_script,
        op_args=[
            "src/ingestion/generate_healthcare_data.py"
        ],
    )

    validate_data = PythonOperator(
        task_id="validate_source_data",
        python_callable=run_script,
        op_args=[
            "src/quality/validate_healthcare_data.py"
        ],
    )

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_script,
        op_args=[
            "src/ingestion/bronze_ingestion.py"
        ],
    )

    silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_script,
        op_args=[
            "src/transformation/silver_transformation.py"
        ],
    )

    gold = PythonOperator(
        task_id="gold_transformation",
        python_callable=run_script,
        op_args=[
            "src/transformation/gold_transformation.py"
        ],
    )

    generate_fhir = PythonOperator(
        task_id="generate_fhir",
        python_callable=run_script,
        op_args=[
            "src/ingestion/generate_fhir_data.py"
        ],
    )

    validate_fhir = PythonOperator(
        task_id="validate_fhir",
        python_callable=run_script,
        op_args=[
            "src/quality/validate_fhir.py"
        ],
    )

    cdc = PythonOperator(
        task_id="patient_cdc",
        python_callable=run_script,
        op_args=[
            "src/cdc/patient_cdc.py"
        ],
    )

    scd2 = PythonOperator(
        task_id="patient_scd2",
        python_callable=run_script,
        op_args=[
            "src/scd/patient_scd2.py"
        ],
    )

    spark = PythonOperator(
        task_id="spark_analytics",
        python_callable=run_script,
        op_args=[
            "src/spark/healthcare_spark_etl.py"
        ],
    )

    (
        generate_data
        >> validate_data
        >> bronze
        >> silver
        >> gold
        >> generate_fhir
        >> validate_fhir
        >> cdc
        >> scd2
        >> spark
    )
