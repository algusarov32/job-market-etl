"""
Sensor DAG — triggered by BOTH source DAGs.

Waits for:
    - theirstack_extraction_dag
    - synthetic_hh_dag

Then triggers gp_transform_dag.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins.custom_sensors.external_task_window_sensor import ExternalTaskWindowSensor

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="trigger_transform_sensor",
    description="Wait for both sources, then trigger transform",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Triggered by source DAGs
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "trigger"],
    max_active_runs=1,
) as dag:

    wait_theirstack = ExternalTaskWindowSensor(
        task_id="wait_theirstack",
        external_dag_id="theirstack_extraction_dag",
        external_task_id="trigger_sensor",
        time_window=timedelta(minutes=15), 
        allowed_states=["success"],
        failed_states=["failed", "skipped", "running"],
        timeout=30,
        poke_interval=15,
    )

    wait_synthetic = ExternalTaskWindowSensor(
        task_id="wait_synthetic",
        external_dag_id="hh_synth_load_dag",
        external_task_id="hh_synth_load",
        time_window=timedelta(minutes=15),
        allowed_states=["success"],
        failed_states=["failed", "skipped", "running"],
        timeout=120,
        poke_interval=30,
    )
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_gp_transform",
        trigger_dag_id="gp_transform_core",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [wait_theirstack, wait_synthetic] >> trigger_transform