"""
Synthetic HH Extraction DAG — manual trigger.

Responsibility: load synthetic hh.ru data from Excel in S3 directly
into Greenplum staging, mimicking the hh.ru extraction flow.
"""

import io
import logging
from datetime import datetime, timedelta

import pandas as pd
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

_S3_KEY = "HH/hh_data.xlsx"


def hh_synth_load(**context: Context) -> None:
    """
    Download Excel from S3, parse, and load directly into Greenplum staging.
    """
    from src.loaders.s3_loader import S3Loader
    from src.loaders.gp_loader import GreenplumLoader
    from src.transformers.excel_to_hh import transform_excel_to_hh
    from src.utils.config import minio as minio_cfg

    s3 = S3Loader(bucket=minio_cfg.bucket_raw)
    gp = GreenplumLoader()

    # 1. Download Excel from S3
    log.info("Downloading Excel from s3://%s/%s", minio_cfg.bucket_raw, _S3_KEY)
    excel_bytes = s3._client.get_object(
        Bucket=minio_cfg.bucket_raw, Key=_S3_KEY
    )["Body"].read()

    # 2. Parse Excel → DataFrame
    df = pd.read_excel(io.BytesIO(excel_bytes))
    log.info("Parsed %d rows from Excel", len(df))

    # 3. Transform to hh.ru JSON format
    records = transform_excel_to_hh(df)
    log.info("Transformed %d records to hh.ru format", len(records))

    # Log sample for debugging
    if records:
        import json
        log.info("Sample record: %s", json.dumps(records[0], ensure_ascii=False, indent=2))

    # 4. Load directly into Greenplum staging
    result = gp.load(
        records=records,
        table="staging.vacancies_raw",
        source="synthetic_hh",
    )
    log.info(
        "Staging load complete | written=%d failed=%d",
        result.rows_written, result.rows_failed,
    )


with DAG(
    dag_id="hh_synth_load_dag",
    description="Load synthetic hh.ru data from Excel in S3 → Greenplum staging",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "synthetic", "hh"],
    max_active_runs=1,
) as dag:

    t_load_synthetic = PythonOperator(
        task_id="hh_synth_load",
        python_callable=hh_synth_load,
    )