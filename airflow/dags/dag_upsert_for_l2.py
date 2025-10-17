####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- DAG Upsert weather.l2_weather_hourly from L1
####

from datetime import timedelta

from airflow import DAG
from airflow.sdk import timezone as tz
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


DAG_ID = "etl_open_meteo_l1_to_l2_sql"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    description="Build/Upsert L2 (weather.l2_weather_hourly) dari L1 via SQL (idempotent)",
    start_date=tz.datetime(2025, 10, 1, 0, 0, 0),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "open-meteo", "l2", "sql"],
    template_searchpath=["/opt/project/scripts/sql"],
) as dag:

    create_l2 = SQLExecuteQueryOperator(
        task_id="create_l2",
        conn_id="postgres_default",
        sql="create_l2.sql",
    )

    upsert_l2_for_ds = SQLExecuteQueryOperator(
        task_id="upsert_l2_for_ds",
        conn_id="postgres_default",
        sql="upsert_l2_for_ds.sql",
        params={"ds": "{{ ds }}"},
    )

    create_l2 >> upsert_l2_for_ds