"""
HH Extraction DAG — runs at 02:00 every day.

Responsibility: collect raw vacancy data from hh.ru and land it in
Greenplum staging. Nothing else.

Stages
------
1. extract_hh   — hh.ru API → MinIO (raw NDJSON per query)
2. load_staging — MinIO NDJSON → Greenplum staging.vacancies_raw

On success triggers gp_transform_dag via TriggerDagRunOperator.
"""


import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

_QUERIES = [
    "Data Engineer",
    "Analytics Engineer",
    "Data Analyst",
    "ML Engineer",
]


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def extract_hh(**context: Context) -> None:
    """Pull vacancies from hh.ru for each query and upload to MinIO as NDJSON."""
    from src.extractors.hh_extractor import HHExtractor
    from src.loaders.s3_loader import S3Loader
    from src.utils.config import minio as minio_cfg

    ds: str = context["ds"]
    extractor = HHExtractor()
    loader = S3Loader(bucket=minio_cfg.bucket_raw, key_prefix=f"hh_ru/{ds}")
    loader.ensure_bucket()

    for query in _QUERIES:
        vacancies = extractor.run(search_query=query, limit=500, area=1)
        safe_query = query.lower().replace(" ", "_")
        key_suffix = f"{safe_query}.json"

        result = loader.load_batch(
            records=vacancies,
            key_suffix=key_suffix,
            metadata={
                "source": "hh.ru",
                "query": query,
                "count": str(len(vacancies)),
                "date": ds,
            },
        )
        log.info(
            "Uploaded %d vacancies | query='%s' key=%s/%s",
            result.rows_written, query, f"hh_ru/{ds}", key_suffix,
        )


def load_to_staging(**context: Context) -> None:
    """Download all NDJSON files for today from MinIO and insert into Greenplum staging."""
    from src.loaders.gp_loader import GreenplumLoader
    from src.loaders.s3_loader import S3Loader
    from src.utils.config import minio as minio_cfg

    ds: str = context["ds"]
    s3 = S3Loader(bucket=minio_cfg.bucket_raw)
    gp = GreenplumLoader()

    keys = s3.list_objects(prefix=f"hh_ru/{ds}/")
    if not keys:
        raise ValueError(f"No files found in MinIO for date {ds} — extraction may have failed")

    total_written = 0
    total_failed = 0

    for key in keys:
        records = s3.download_ndjson(key) 
        
        result = gp.load(
            records=records,
            table="staging.vacancies_raw",
            source="hh.ru",
        )
        total_written += result.rows_written
        total_failed += result.rows_failed
        log.info(
            "Staging load | key=%s written=%d failed=%d",
            key, result.rows_written, result.rows_failed,
        )

    if total_failed > 0:
        log.error(
            "Staging load completed with errors | written=%d failed=%d",
            total_written, total_failed,
        )
        raise ValueError(f"Failed to load {total_failed} rows into staging — check logs")

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="hh_extraction_dag",
    description="Daily: hh.ru → MinIO → Greenplum staging",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "extraction", "job-market"],
    max_active_runs=1,
) as dag:

    t_extract_hh = PythonOperator(
        task_id="extract_hh",
        python_callable=extract_hh,
    )

    t_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_to_staging,
    )

    t_trigger_transform = TriggerDagRunOperator(
        task_id="trigger_gp_transform_dag",
        trigger_dag_id="gp_transform_dag",
        execution_date="{{ ds }}",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_extract_hh >> t_load_staging >> t_trigger_transform