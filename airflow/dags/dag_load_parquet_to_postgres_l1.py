####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- DAG Load Parquet (staging) to Postgres L1
####

from datetime import timedelta
from typing import Optional

from airflow import DAG
from airflow.sdk import timezone as tz
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts.load_parquet_to_postgres_l1 import run as load_to_l1

DAG_ID = "etl_open_meteo_parquet_to_postgres_l1"

def _minio_cfg_for_container() -> dict:
    cfg = Variable.get("MINIO_CONFIG", deserialize_json=True)
    if str(cfg.get("endpoint", "")).startswith("localhost"):
        cfg["endpoint"] = "minio:9000"
    return cfg

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id=DAG_ID,
    description="Load staging Parquet (MinIO) --> Postgres L1 (weather.l1_weather_hourly)",
    start_date=tz.datetime(2025, 10, 1, 0, 0, 0),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "open-meteo", "staging", "l1", "postgres"],
    template_searchpath=["/opt/project/scripts/sql"],
) as dag:

    def _task_load(ds, dag_run=None, **_):
        # Conf from trigger
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        ds_conf: Optional[str] = conf.get("ds")
        object_key_staging: Optional[str] = conf.get("object_key_staging")

        if not object_key_staging:
            raise ValueError("object_key_staging tidak ditemukan (harap kirim via dag_run.conf).")

        minio_cfg = _minio_cfg_for_container()
        pg_cfg    = Variable.get("POSTGRES_CONFIG", deserialize_json=True)

        return load_to_l1(
            minio_cfg=minio_cfg,
            pg_cfg=pg_cfg,
            run_date=ds_conf or ds,
            object_key_staging=object_key_staging,
        )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_default",
        sql="create_schema.sql",
    )

    load_parquet_to_l1 = PythonOperator(
        task_id="load_parquet_to_postgres_l1",
        python_callable=_task_load,
        execution_timeout=timedelta(minutes=10),
    )

    load_parquet_to_l1 >> create_schema