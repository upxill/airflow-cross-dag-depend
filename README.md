# Airflow Cross-DAG Dependency Pipeline

This repository contains an Apache Airflow project demonstrating a parent/child DAG dependency pattern using `TriggerDagRunOperator`.

## Project overview

- `dags/parent.py` defines the upstream DAG: `data_generation_pipeline`
- `dags/child.py` defines the triggered downstream DAG: `data_processing_pipeline`
- The upstream DAG generates a dataset artifact and triggers downstream processing with runtime configuration
- The downstream DAG consumes `dag_run.conf` parameters and executes a simple transform/finalize flow

## DAGs

### `data_generation_pipeline`

- Scheduled daily (`@daily`)
- Executes a source data generation task
- Triggers `data_processing_pipeline` via `TriggerDagRunOperator`
- Passes runtime configuration values such as `source_file` and `status`

### `data_processing_pipeline`

- Trigger-only DAG (`schedule=None`)
- Reads runtime parameters from `dag_run.conf`
- Runs processing and finalize tasks

## Local development

### Requirements

- Python 3.11+ (or the interpreter configured for this project)
- Apache Airflow compatible with the installed provider packages
- Astronomer CLI if using `astro dev` locally

### Run locally

If you are using Astronomer:

```bash
astro dev start
```

Then open the Airflow UI at:

```text
http://localhost:8080
```

If you are running Airflow directly, ensure the environment has the required dependencies and the `dags/` folder is included in `AIRFLOW__CORE__DAGS_FOLDER`.

## Review and validation

- Confirm DAG IDs in the Airflow UI: `data_generation_pipeline`, `data_processing_pipeline`
- Verify `data_generation_pipeline` can trigger `data_processing_pipeline`
- Check the rendered task logs for `dag_run.conf` values in the downstream DAG

## Notes

- Use descriptive DAG IDs, task IDs, and tags for clarity
- Keep downstream DAG schedule set to `None` when it should only run from a trigger
- Store runtime values in `conf` only for lightweight configuration data; use XCom, Variables, or external storage for larger payloads

## File structure

- `dags/parent.py`
- `dags/child.py`
- `Dockerfile`
- `packages.txt`
- `requirements.txt`
- `airflow_settings.yaml`
- `include/`
- `plugins/`


