####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- DAG Normalize Open-Meteo RAW JSON → Parquet (staging)
####

from datetime import timedelta
from typing import Optional

from airflow import DAG
from airflow.sdk import timezone as tz
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import TaskInstance
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.normalize_open_meteo_to_parquet import run as normalize_to_parquet


DAG_ID = "etl_open_meteo_json_to_parquet"

def _minio_cfg_for_container() -> dict:
    cfg = Variable.get("MINIO_CONFIG", deserialize_json=True)
    if str(cfg.get("endpoint", "")).startswith("localhost"):
        cfg["endpoint"] = "minio:9000"
    return cfg

# --------- DAG ---------
default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id=DAG_ID,
    description="RAW JSON → Parquet (staging)",
    start_date=tz.datetime(2025, 10, 1, 0, 0, 0),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "open-meteo", "raw", "staging"],
) as dag:

    def _task_normalize(ds, ti: TaskInstance, dag_run=None, **_):
         # Take from trigger conf
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        ds_conf: Optional[str] = conf.get("ds")
        object_key_raw: Optional[str] = conf.get("object_key_raw")

        # Fail fast
        if not object_key_raw:
            raise ValueError("object_key_raw not found (dag_run.conf atau XCom).")

        minio_cfg = _minio_cfg_for_container()
        om_cfg    = Variable.get("OPEN_METEO_CONFIG", deserialize_json=True)

        return normalize_to_parquet(
            minio_cfg=minio_cfg,
            open_meteo_cfg=om_cfg,
            object_key_raw=object_key_raw,
            run_date=ds_conf or ds,
        )

    normalize = PythonOperator(
        task_id="normalize_to_parquet",
        python_callable=_task_normalize,
        execution_timeout=timedelta(minutes=5),
    )

    trigger_load_l1 = TriggerDagRunOperator(
        task_id="trigger_load_parquet_to_postgres_l1",
        trigger_dag_id="etl_open_meteo_parquet_to_postgres_l1",
        reset_dag_run=True,
        wait_for_completion=False,
        conf={
            "ds": "{{ ds }}",
            "object_key_staging": "{{ ti.xcom_pull(task_ids='normalize_to_parquet') }}",
        },
        doc_md="Trigger DAG Loader L1 dengan conf: ds & object_key_staging (hasil normalize).",
    )

    normalize >> trigger_load_l1