"""Airflow DAG tests for the cross-DAG dependency pipeline."""

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def load_dag_bag():
    dag_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "dags")
    )
    with suppress_logging("airflow"):
        return DagBag(dag_folder=dag_folder, include_examples=False)


def test_dags_import_cleanly():
    dag_bag = load_dag_bag()
    assert not dag_bag.import_errors, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )


def test_expected_dag_ids_present():
    dag_bag = load_dag_bag()
    expected = {"data_generation_pipeline", "data_processing_pipeline"}
    assert expected == set(dag_bag.dags.keys())


def test_dag_tags_and_default_args():
    dag_bag = load_dag_bag()

    approved_tags = {"cross_dag_dependency"}
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG {dag_id} must have tags"
        assert approved_tags.issuperset(dag.tags), (
            f"DAG {dag_id} contains unexpected tags: {dag.tags}"
        )
        assert dag.default_args.get("depends_on_past", False) is False
        assert dag.default_args.get("retries", 0) >= 0


def test_parent_dag_trigger_operator_config():
    dag_bag = load_dag_bag()
    parent_dag = dag_bag.get_dag("data_generation_pipeline")
    trigger_task = parent_dag.task_dict.get("trigger_data_processing")

    assert trigger_task is not None, (
        "Parent DAG must include trigger_data_processing task"
    )
    assert isinstance(trigger_task, TriggerDagRunOperator)
    assert trigger_task.trigger_dag_id == "data_processing_pipeline"
    assert trigger_task.conf.get("source_file") == "daily_report.csv"
    assert trigger_task.conf.get("status") == "fresh"


def test_downstream_dag_is_trigger_only():
    dag_bag = load_dag_bag()
    downstream_dag = dag_bag.get_dag("data_processing_pipeline")

    assert downstream_dag is not None
    assert downstream_dag.schedule_interval is None
    assert downstream_dag.task_dict.get("transform_data") is not None
    assert downstream_dag.task_dict.get("finalize") is not None
    assert (
        'dag_run.conf["source_file"]'
        in downstream_dag.task_dict["transform_data"].bash_command
    )
