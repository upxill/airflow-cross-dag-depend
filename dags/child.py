from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="data_processing_pipeline",
    description="Process runtime data inputs received from the upstream DAG.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"depends_on_past": False, "retries": 0},
    tags=["cross_dag_dependency"],
) as downstream_dag:
    # Process the configuration passed from the triggering DAG
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=(
            "echo 'Processing file: {{ dag_run.conf[\"source_file\"] }}' "
            "&& echo 'File status: {{ dag_run.conf[\"status\"] }}'"
        ),
    )

    # Finalize the downstream workflow
    finalize = BashOperator(
        task_id="finalize",
        bash_command="echo 'Data processing complete.'",
    )

    transform_data >> finalize
