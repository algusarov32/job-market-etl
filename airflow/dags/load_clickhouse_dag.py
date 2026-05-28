"""
Load ClickHouse DAG — triggered by gp_transform_dag on success.

Responsibility: read pre-aggregated data from Greenplum marts.* and write
into ClickHouse facts.* tables. Grafana aggregates on the fly.

Stages
------
1. load_clickhouse — marts.skills_stats     → facts.skills_stats
                     marts.company_stats    → facts.company_stats
                     marts.market_dynamics  → facts.market_dynamics
"""

import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context


log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

# Таблицы для загрузки: Greenplum → ClickHouse
_TABLES = [
    {
        "gp_table": "marts.skills_stats",
        "ch_table": "facts.skills_stats",
    },
    {
        "gp_table": "marts.company_stats",
        "ch_table": "facts.company_stats",
    },
    {
        "gp_table": "marts.market_dynamics",
        "ch_table": "facts.market_dynamics",
    },
]


# ---------------------------------------------------------------------------
# Task callable
# ---------------------------------------------------------------------------


def load_clickhouse(**context: Context) -> None:
    """
    Read pre-aggregated data from Greenplum marts.* and load into ClickHouse.

    Each mart table is loaded into a corresponding facts.* table.
    Partitions are dropped before reload to avoid duplicates.
    """
    import psycopg2.extras

    from src.loaders.ch_loader import ClickHouseLoader
    from src.utils.gp_connection import gp_connection

    ds: str = context["ds"]
    partition_date = date.fromisoformat(ds)
    ch = ClickHouseLoader()
    total_written = 0

    for table_conf in _TABLES:
        gp_table = table_conf["gp_table"]
        ch_table = table_conf["ch_table"]

        with gp_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM {gp_table} WHERE stat_date = %s",  # noqa: S608
                    (ds,),
                )
                rows = [dict(r) for r in cur.fetchall()]

        if not rows:
            log.warning("No data in %s for date %s — skipping", gp_table, ds)
            continue

        # Drop partition for today before reload to avoid duplicates
        ch.drop_partition_for_date(ch_table, partition_date)

        result = ch.load(records=rows, table=ch_table)
        total_written += result.rows_written

        log.info(
            "Loaded %s → %s | date=%s written=%d failed=%d",
            gp_table, ch_table, ds, result.rows_written, result.rows_failed,
        )

    log.info("ClickHouse load complete | date=%s total_written=%d", ds, total_written)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="load_clickhouse_dag",
    description="Load: Greenplum marts.* → ClickHouse facts.* (pre-aggregated)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # triggered by gp_transform_dag
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "clickhouse", "job-market"],
    max_active_runs=1,
) as dag:

    t_load_clickhouse = PythonOperator(
        task_id="load_clickhouse",
        python_callable=load_clickhouse,
    )