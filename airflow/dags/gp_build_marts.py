"""
Build Marts DAG — triggered by gp_transform_dag on success.

Responsibility: rebuild Greenplum marts.* tables from core.* for today.
Idempotent: DELETE + INSERT.

Stages
------
1. build_marts — core.* → marts.*  (gp_rebuild_marts.sql)

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


def build_marts() -> None:
    """Run SQL: core.* → marts.*"""
    from src.loaders.gp_loader import GreenplumLoader

    GreenplumLoader().execute_script("/opt/airflow/scripts/gp_rebuild_marts.sql")
    log.info("build_marts completed")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="gp_build_marts_dag",
    description="Build: Greenplum core → marts",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # triggered by gp_transform_dag
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "marts", "job-market"],
    max_active_runs=1,
) as dag:

    t_gp_build_marts = PythonOperator(
        task_id="gp_build_marts",
        python_callable=build_marts,
    )

    t_trigger_clickhouse = TriggerDagRunOperator(
        task_id="trigger_load_clickhouse_dag",
        trigger_dag_id="load_clickhouse_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_gp_build_marts >> t_trigger_clickhouse