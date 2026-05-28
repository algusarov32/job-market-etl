"""
TheirStack Extraction DAG — runs at 02:00 every day.

Responsibility: collect raw vacancy data from TheirStack API and land
it in Greenplum staging. Nothing else.

Incremental strategy
--------------------
TheirStack charges 1 credit per returned job. To avoid paying for
duplicates we pass ``posted_at_gte`` = start of the previous day
so each run fetches only genuinely new postings.

Stages
------
1. extract_theirstack — TheirStack API → MinIO (raw JSON per query)
2. load_staging       — MinIO JSON → Greenplum staging.hh_vacancies_raw

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

# Country codes to search in — adjust as needed
_COUNTRY_CODES = ["RU", "DE", "NL", "PL", "CZ"]


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def extract_theirstack(**context: Context) -> None:
    """
    Pull vacancies from TheirStack for each query and upload to MinIO.

    Uses posted_at_gte = execution_date - 1 day for incremental loads
    so we only fetch new jobs and don't waste API credits on duplicates.
    """
    from src.extractors.theirstack_extractor import TheirStackExtractor
    from src.loaders.s3_loader import S3Loader
    from src.utils.config import minio as minio_cfg

    ds: str = context["ds"]
    # Watermark: fetch only jobs discovered since the previous day
    posted_at_gte = (
        datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=10)
    ).strftime("%Y-%m-%dT00:00:00Z")

    extractor = TheirStackExtractor()
    loader = S3Loader(bucket=minio_cfg.bucket_raw, key_prefix=f"theirstack/{ds}")
    loader.ensure_bucket()

    for query in _QUERIES:
        vacancies = extractor.run(
            search_query=query,
            limit=10,
            country_codes=_COUNTRY_CODES,
            posted_at_gte=posted_at_gte,
        )

        if not vacancies:
            log.info("No new vacancies for query='%s' since %s", query, posted_at_gte)
            continue

        safe_query = query.lower().replace(" ", "_")
        key_suffix = f"{safe_query}.json"

        result = loader.load_batch(
            records=vacancies,
            key_suffix=key_suffix,
            metadata={
                "source": "theirstack",
                "query": query,
                "count": str(len(vacancies)),
                "date": ds,
                "posted_at_gte": posted_at_gte,
            },
        )
        log.info(
            "Uploaded %d vacancies | query='%s' key=%s/%s",
            result.rows_written, query, f"theirstack/{ds}", key_suffix,
        )


def load_to_staging(**context: Context) -> None:
    """Download all NDJSON files for today from MinIO and insert into Greenplum staging."""
    from src.loaders.gp_loader import GreenplumLoader
    from src.loaders.s3_loader import S3Loader
    from src.utils.config import minio as minio_cfg

    ds: str = context["ds"]
    s3 = S3Loader(bucket=minio_cfg.bucket_raw)
    gp = GreenplumLoader()

    keys = s3.list_objects(prefix=f"theirstack/{ds}/")
    if not keys:
        log.warning("No files found in MinIO for date %s", ds)
        return

    for key in keys:
        records = s3.download_ndjson(key)
        if not isinstance(records, list):
            records = [records]
        result = gp.load(
            records=records,
            table="staging.vacancies_raw",
            source="theirstack",
        )
        log.info(
            "Staging load | key=%s written=%d failed=%d",
            key, result.rows_written, result.rows_failed,
        )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="theirstack_extraction_dag",
    description="Daily: TheirStack → MinIO → Greenplum staging",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "extraction", "job-market"],
    max_active_runs=1,
) as dag:

    t_extract_theirstack = PythonOperator(
        task_id="extract_theirstack",
        python_callable=extract_theirstack,
    )

    t_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_to_staging,
    )

    t_trigger_sensor = TriggerDagRunOperator(
        task_id="trigger_sensor",
        trigger_dag_id="trigger_transform_sensor",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_extract_theirstack >> t_load_staging >> t_trigger_sensor
