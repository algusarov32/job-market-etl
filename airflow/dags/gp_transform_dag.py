"""
Transform DAG — triggered by extraction_dag on success.

Responsibility: normalise raw staging data into core tables.
No I/O with MinIO, ClickHouse, or marts.

Stages
------
1. transform_core — staging.* → core.*  (gp_transform_vacancies_raw.sql)

On success triggers load_clickhouse_dag via TriggerDagRunOperator.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Task callable
# ---------------------------------------------------------------------------


def transform_core() -> None:
    """Run SQL: staging.* → core.*"""
    from src.loaders.gp_loader import GreenplumLoader

    GreenplumLoader().execute_script("/opt/airflow/scripts/gp_transform_vacancies_raw.sql")
    log.info("transform_core completed")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="gp_transform_core",
    description="Transform: Greenplum staging → core",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # triggered by extraction_dag
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "transform", "job-market"],
    max_active_runs=1,
) as dag:

    t_gp_transform_core = PythonOperator(
        task_id="gp_transform_core",
        python_callable=transform_core,
    )

    t_trigger_gp_marts = TriggerDagRunOperator(
        task_id="trigger_gp_build_marts",
        trigger_dag_id="gp_build_marts_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_gp_transform_core >> t_trigger_gp_marts

