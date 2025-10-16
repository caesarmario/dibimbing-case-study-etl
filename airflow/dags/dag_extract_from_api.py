####
## dibimbing.id - Case Study ETL
## Mario Caesar // caesarmario87@gmail.com
## -- DAG to extract open meteo data to MinIO
####

from datetime import timedelta
from airflow import DAG
from airflow.utils import timezone as tz
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

from scripts.extract_open_meteo_to_minio import run as extract_to_minio

# ---------------------------
# Default args & DAG config
# ---------------------------
default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="etl_open_meteo_raw_to_minio",
    description="Extract Open-Meteo hourly → store RAW JSON to MinIO (by ds)",
    start_date=tz.datetime(2025, 10, 1, 0, 0, 0),
    schedule="0 0 * * *",
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "raw", "open-meteo"],
) as dag:

    def _task_extract(ds, ti, **_):
        """
        Python callable executed by PythonOperator.
        """
        minio_cfg = Variable.get("MINIO_CONFIG", deserialize_json=True)
        om_cfg    = Variable.get("OPEN_METEO_CONFIG", deserialize_json=True)

        object_key = extract_to_minio(
            minio_cfg=minio_cfg,
            open_meteo_cfg=om_cfg,
            run_date=ds,  # format YYYY-MM-DD
        )

        # pushed to XCom (useful for debug)
        ti.xcom_push(key="object_key_raw", value=object_key)
        return object_key 

    extract = PythonOperator(
        task_id="extract_open_meteo_to_minio_raw",
        python_callable=_task_extract,
        execution_timeout=timedelta(minutes=5),
    )

    trigger_normalize = TriggerDagRunOperator(
        task_id="trigger_normalize_dag",
        trigger_dag_id="etl_open_meteo_json_to_parquet",
        reset_dag_run=True,
        wait_for_completion=False,
        conf={
            "ds": "{{ ds }}",
            "object_key_raw": "{{ ti.xcom_pull(task_ids='extract_open_meteo_to_minio_raw') }}",
        },
        doc_md="Trigger DAG 01 dengan conf: ds & object_key_raw",
    )

    extract >> trigger_normalize