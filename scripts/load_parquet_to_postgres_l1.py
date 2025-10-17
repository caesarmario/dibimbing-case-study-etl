####
## dibimbing.id - Case Study ETL
## Mario Caesar // caesarmario87@gmail.com
## -- Load staging Parquet --> Postgres L1
####

import json
import argparse
import psycopg2
import pandas as pd
import numpy as np

from io import BytesIO
from typing import Dict, Any, Optional
from minio import Minio
from psycopg2.extras import execute_values

from helper_logging import logger


# ---------- MinIO helpers ----------
def _get_parquet_from_minio(client: Minio, bucket: str, object_key: str) -> pd.DataFrame:
    logger.info("Downloading Parquet from s3://%s/%s ...", bucket, object_key)
    obj = client.get_object(bucket, object_key)
    try:
        data = obj.read()
    finally:
        obj.close()
        obj.release_conn()
    buf = BytesIO(data)
    df = pd.read_parquet(buf)
    logger.info("Parquet loaded: %d rows, %d cols", len(df), len(df.columns))
    return df


# ---------- Postgres helpers ----------
DDL_L1 = """
CREATE TABLE IF NOT EXISTS {table} (
  ts             TIMESTAMPTZ      NOT NULL,
  date           DATE             NOT NULL,
  hour           SMALLINT,
  latitude       DOUBLE PRECISION,
  longitude      DOUBLE PRECISION,
  timezone       TEXT,
  temperature_c  DOUBLE PRECISION,
  load_ds        DATE             NOT NULL,
  source         TEXT,
  CONSTRAINT pk_l1_weather_hourly PRIMARY KEY (ts, latitude, longitude, source)
);
"""

INSERT_SQL = """
INSERT INTO {table} (
  ts, date, hour, latitude, longitude, timezone, temperature_c, load_ds, source
) VALUES %s
ON CONFLICT (ts, latitude, longitude, source) DO UPDATE
SET
  temperature_c = EXCLUDED.temperature_c,
  timezone      = EXCLUDED.timezone,
  load_ds       = EXCLUDED.load_ds;
"""


def _connect_pg(pg: Dict[str, Any]):
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["user"], password=pg["password"]
    )
    conn.autocommit = False
    return conn


def _qual_table(pg: Dict[str, Any]) -> str:
    """Return fully-qualified table name: schema.table (default schema 'public')."""
    schema = pg.get("schema", "public")
    table  = pg.get("table_l1", "l1_weather_hourly")
    return f"{schema}.{table}"


def _ensure_table(conn, table_fqdn: str, schema: str):
    with conn.cursor() as cur:
        # ensure schema exists
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        # ensure table exists (qualified)
        cur.execute(DDL_L1.format(table=table_fqdn))
    conn.commit()


def _normalize_df_for_pg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure dtype & format "secure" to INSERT to Postgres.
    """
    # Ensure column exists
    expected = ["ts","date","hour","latitude","longitude","timezone","temperature_c","load_ds","source"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in staging DF: {missing}")

    # ts: pandas tz-aware ok; convert to ISO string
    if isinstance(df["ts"].dtype, pd.DatetimeTZDtype):
        df["ts"] = df["ts"].dt.tz_convert("UTC")
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # date & load_ds to YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["load_ds"] = pd.to_datetime(df["load_ds"]).dt.strftime("%Y-%m-%d")

    # hour smallint
    df["hour"] = df["hour"].astype("Int64").fillna(pd.NA)
    
    # float
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")

    # text
    df["timezone"] = df["timezone"].astype("string")
    df["source"] = df["source"].astype("string")

    # Ordering column
    return df[["ts","date","hour","latitude","longitude","timezone","temperature_c","load_ds","source"]]


def _upsert_dataframe(conn, table_fqdn: str, df: pd.DataFrame, batch_size: int = 5000) -> int:
    # 1) Change <NA>/NaN to None
    df = df.where(pd.notna(df), None)

    # 2) helper convert numpy into python native
    def _to_native(v):
        if v is None:
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, pd.Timestamp):
            return v.tz_convert("UTC").to_pydatetime()
        return v

    rows = [tuple(_to_native(x) for x in r) for r in df.itertuples(index=False, name=None)]

    affected = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]
            execute_values(cur, INSERT_SQL.format(table=table_fqdn), chunk, template=None)
            affected += len(chunk)
    conn.commit()
    return affected


# ---------- main ----------
def run(
    minio_cfg: Dict[str, Any],
    pg_cfg: Dict[str, Any],
    run_date: str,
    object_key_staging: Optional[str] = None,
) -> int:
    """
    Load staging Parquet (MinIO) to Postgres L1.
    """
    logger.info("=== START load_parquet_to_postgres_l1 (ds=%s) ===", run_date)

    # minio client
    endpoint = minio_cfg["endpoint"]
    client = Minio(
        endpoint=endpoint,
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=bool(minio_cfg.get("secure", False)),
    )

    # key parquet
    if not object_key_staging:
        object_key_staging = minio_cfg["staging_key_template"].format(ds=run_date)

    # 1) read Parquet
    df = _get_parquet_from_minio(client, minio_cfg["bucket_staging"], object_key_staging)

    # 2) normalize dtypes
    df = _normalize_df_for_pg(df)

    # 3) load to Postgres (schema-qualified)
    table_fqdn = _qual_table(pg_cfg)                # e.g., weather.l1_weather_hourly
    schema     = pg_cfg.get("schema", "public")

    conn = _connect_pg(pg_cfg)
    try:
        _ensure_table(conn, table_fqdn, schema)
        n = _upsert_dataframe(conn, table_fqdn, df)
    finally:
        conn.close()

    logger.info("SUCCESS upsert %d rows into %s", n, table_fqdn)
    logger.info("=== END load_parquet_to_postgres_l1 (ds=%s) ===", run_date)
    return n


# -------------- CLI for local / operator --------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load staging Parquet → Postgres L1.")
    parser.add_argument("--config-file", required=True, help="JSON with MINIO_CONFIG & POSTGRES_CONFIG")
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--object-key-staging", default=None, help="Optional explicit staging object key")
    args = parser.parse_args()

    with open(args.config_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cnt = run(
        minio_cfg=cfg["MINIO_CONFIG"],
        pg_cfg=cfg["POSTGRES_CONFIG"],
        run_date=args.run_date,
        object_key_staging=args.object_key_staging,
    )
    print("OK -> inserted/updated rows:", cnt)
