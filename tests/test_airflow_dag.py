
import ast
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DAG_FILE = (
    BASE_DIR / "dags" / "healthcare_pipeline_dag.py"
)


def test_airflow_dag_exists():
    assert DAG_FILE.exists()


def test_airflow_dag_syntax():
    source = DAG_FILE.read_text(encoding="utf-8")

    tree = ast.parse(source)

    assert isinstance(tree, ast.Module)


def test_airflow_dag_contains_required_tasks():
    source = DAG_FILE.read_text(encoding="utf-8")

    expected_tasks = [
        "generate_synthetic_data",
        "validate_source_data",
        "bronze_ingestion",
        "silver_transformation",
        "gold_transformation",
        "generate_fhir",
        "validate_fhir",
        "patient_cdc",
        "patient_scd2",
        "spark_analytics",
    ]

    for task_id in expected_tasks:
        assert task_id in source
