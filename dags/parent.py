from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# ==========================================
# 1. DATA GENERATION DAG
# ==========================================
with DAG(
    dag_id="data_generation_pipeline",
    description="Generate daily data assets and trigger downstream processing.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"depends_on_past": False, "retries": 0},
    tags=["cross_dag_dependency"],
) as upstream_dag:
    # Generate the daily source data asset
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="echo 'Generating daily data feed...' && sleep 5",
    )

    # Trigger the downstream processing DAG with runtime configuration
    trigger_downstream = TriggerDagRunOperator(
        task_id="trigger_data_processing",
        trigger_dag_id="data_processing_pipeline",
        conf={"source_file": "daily_report.csv", "status": "fresh"},
        wait_for_completion=False,
    )

    generate_data >> trigger_downstream
