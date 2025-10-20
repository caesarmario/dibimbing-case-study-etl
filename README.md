<h1 align="center">🌦️ [dibimbing.id] UrbanMart Weather ETL</h1> <p align="center">Daily ETL that pulls hourly weather from <b>Open-Meteo</b>, lands raw JSON in <b>MinIO</b>, normalizes to <b>Parquet</b>, loads to <b>Postgres L1</b>, and builds a curated <b>L2</b> with SQL — orchestrated by <b>Apache Airflow</b>.</p><br> <p align="center"> <img src="https://img.shields.io/static/v1?label=%F0%9F%8C%9F&message=If%20Useful&style=flat&color=BC4E99" alt="Star Badge"/> <a href="https://github.com/caesarmario"> <img src="https://img.shields.io/github/followers/caesarmario?style=social" alt="GitHub"/> </a> <a href="https://beacons.ai/caesarmario_"> <img src="https://img.shields.io/badge/Follow%20My%20Other%20Works-019875?style=flat&labelColor=019875" alt="Beacons"/> </a> </p> <br>

---

## 📃 Table of Contents

* [Tech Stack](#tech-stack)
* [Features](#features)
* [Architecture](#architecture)
* [Repository Structure](#repository-structure)
* [Quickstart](#quickstart)
  * [1) Prerequisites](#1-prerequisites)
  * [2) Environment](#2-environment)
  * [3) Bring up the stack](#3-bring-up-the-stack)
  * [4) Airflow Variables (UI → Admin → Variables)](#4-airflow-variables-ui--admin--variables)
  * [5) Run DAGs (order)](#5-run-dags-order)
* [Main Modules](#-main-modules)
* [Schema-Aware Config (example)](#-schema-aware-config-example)
* [Data & DQ Schemas](#-data--dq-schemas)
* [Monitoring & Dashboards](#monitoring--dashboards)
* [Acknowledgements](#acknowledgements)
* [Support me!](#support-me)

---

## Tech Stack

* **Apache Airflow 3.1** (Docker, LocalExecutor)
* **MinIO** (S3-compatible object storage)
* **PostgreSQL 13**
* **Python 3.11**: `pandas`, `pyarrow`, `requests`, `minio`, `psycopg2-binary`
* **SQL**: idempotent DDL + upsert DML (Postgres)

## Features

* **Layered data architecture**: `raw(JSON)` → `staging(Parquet)` → `L1(Bronze, Postgres)` → `L2(Silver, Postgres)`.
* **Config-driven** via **Airflow Variables** & `.env` (no hardcoded creds).
* **Robustness**: HTTP **retry/backoff**, **idempotent** bucket/table creation, **UPSERT** with clear keys.
* **Backfill-friendly**: partitioned by `ds` in object keys; trigger DAGs with JSON conf.
* **Lightweight DQ** at normalize step (array length check, non-empty, not all NaN).
* **Separation of concerns**: 1 DAG per stage; optional **TriggerDagRun** between DAGs.
* **SQL-only L2** using external `.sql` files (clean & reusable).

## Architecture

**Pipeline flow**

```
Open-Meteo API
   └─(Extract)→  MinIO /raw (JSON, partitioned by ds)
                 └─(Normalize)→ MinIO /staging (Parquet, fixed schema)
                                   └─(Load)→ Postgres L1 (weather.l1_weather_hourly)
                                                └─(SQL)→ Postgres L2 (weather.l2_weather_hourly)
```

**DAGs**

1. `etl_open_meteo_raw_to_minio` (Extract)
2. `etl_open_meteo_json_to_parquet` (Normalize; optionally triggered from 1)
3. `etl_open_meteo_parquet_to_postgres_l1` (Load L1; creates schema/table if needed)
4. `etl_open_meteo_l1_to_l2_sql` (SQL-only Upsert L2 using `.sql` files)

## Repository Structure

```
.
├── airflow/
│   └── dags/
│       ├── dag_extract_from_api.py
│       ├── dag_normalize_to_parquet.py
│       ├── dag_load_parquet_to_postgres_l1.py
│       └── dag_upsert_for_l2.py
├── scripts/
│   ├── extract_open_meteo_to_minio.py
│   ├── normalize_open_meteo_to_parquet.py
│   ├── load_parquet_to_postgres_l1.py
│   └── helper_logging.py
├── sql/
│   ├── create_schema.sql
│   ├── create_l2.sql
│   └── upsert_l2_for_ds.sql
├── docker-compose.yml
├── requirements.txt
├── .env                # your local env (not committed)
└── variables.json      # optional: local test config for scripts
```

## Quickstart

### 1) Prerequisites

* Docker & Docker Compose
* (Optional) Python 3.11 if you want to run scripts locally

### 2) Environment

Create a `.env` file (example values):

```dotenv
# Postgres
POSTGRES_USER=airflow_dbbg
POSTGRES_PASSWORD=airflow_dbbg
POSTGRES_DB=dibimbing
POSTGRES_HOST_PORT=5449

# MinIO
MINIO_ROOT_USER=admin_dbbg
MINIO_ROOT_PASSWORD=admin_dbbg
MINIO_API_HOST_PORT=9100
MINIO_CONSOLE_HOST_PORT=9101
MINIO_CONSOLE_PORT=9101
MINIO_BUCKET_RAW=dibimbing-etl-raw
MINIO_BUCKET_STAGING=dibimbing-etl-staging

# Airflow
AIRFLOW_HOST_PORT=8080
```

### 3) Bring up the stack

```bash
# Build & run
docker compose up -d

# See logs (optional)
docker compose logs -f airflow
```

### 4) Airflow Variables (UI → Admin → Variables)

Create **three** JSON variables:

**`MINIO_CONFIG`**

```json
{
  "endpoint": "minio:9000",
  "access_key": "admin_dbbg",
  "secret_key": "admin_dbbg",
  "secure": false,
  "bucket_raw": "dibimbing-etl-raw",
  "bucket_staging": "dibimbing-etl-staging",
  "raw_key_template": "source=open-meteo/ds={ds}/weather_raw.json",
  "staging_key_template": "source=open-meteo/ds={ds}/weather_hourly.parquet"
}
```

**`OPEN_METEO_CONFIG`**

```json
{
  "base_url": "https://api.open-meteo.com/v1/forecast",
  "latitude": -6.2,
  "longitude": 106.8,
  "hourly": ["temperature_2m"],
  "timezone": "Asia/Jakarta",
  "timeout_sec": 30,
  "retries": 3,
  "backoff_sec": 2
}
```

**`POSTGRES_CONFIG`**

```json
{
  "host": "postgres",
  "port": 5432,
  "database": "dibimbing",
  "user": "airflow_dbbg",
  "password": "airflow_dbbg",
  "schema": "weather",
  "table_l1": "l1_weather_hourly"
}
```

> **Connection**: Create `postgres_default` in **Admin → Connections**
>
> * Conn Type: *Postgres*
> * Host: `postgres`, Port: `5432`, Schema: `dibimbing`, Login: `airflow_dbbg`, Password: `airflow_dbbg`

### 5) Run DAGs (order)

**Option A – orchestrated (recommended):**

1. Trigger `etl_open_meteo_raw_to_minio` (cron `0 0 * * *`).
   It can **TriggerDagRun** → `etl_open_meteo_json_to_parquet` with `{"ds": "{{ ds }}", "object_key_raw": "..."}`.
2. `etl_open_meteo_json_to_parquet` (manual or cron) → writes Parquet to staging, then **TriggerDagRun** → `etl_open_meteo_parquet_to_postgres_l1`.
3. `etl_open_meteo_parquet_to_postgres_l1` → creates schema/table (idempotent), loads to L1.
4. `etl_open_meteo_l1_to_l2_sql` (cron `0 1 * * *`) → creates L2 table (idempotent) and upserts **for `{{ ds }}`**.

**Backfill manually?** Use **Trigger DAG** and pass:

```json
{ "ds": "2025-10-16", "object_key_raw": "source=open-meteo/ds=2025-10-16/weather_raw.json" }
```

---

## 🔧 Main Modules

* `extract_open_meteo_to_minio.py` — calls Open-Meteo with retry/backoff; writes raw JSON to MinIO (`raw_key_template`).
* `normalize_open_meteo_to_parquet.py` — validates arrays, builds fixed schema (tz-aware `ts`, `date`, `hour`, `lat`, `lon`, etc.); writes Snappy Parquet to staging.
* `load_parquet_to_postgres_l1.py` — reads Parquet from MinIO, casts dtypes, creates schema/table if missing, **UPSERT** to `weather.l1_weather_hourly`.
* `sql/create_l2.sql` & `sql/upsert_l2_for_ds.sql` — SQL-only curated L2 using `ROW_NUMBER()` and `ON CONFLICT DO UPDATE`.

## 🧩 Schema-Aware Config (example)

Local script testing with `variables.json`:

```json
{
  "MINIO_CONFIG": { "...": "same as Airflow Variable" },
  "OPEN_METEO_CONFIG": { "...": "same as Airflow Variable" },
  "POSTGRES_CONFIG": {
    "host": "localhost",
    "port": 5449,
    "database": "dibimbing",
    "user": "airflow_dbbg",
    "password": "airflow_dbbg",
    "schema": "weather",
    "table_l1": "l1_weather_hourly"
  }
}
```

Run locally (Windows PowerShell example):

```powershell
# Extract
py .\scripts\extract_open_meteo_to_minio.py --config-file .\variables.json --run-date 2025-10-16

# Normalize
py .\scripts\normalize_open_meteo_to_parquet.py --config-file .\variables.json --object-key-raw "source=open-meteo/ds=2025-10-16/weather_raw.json" --run-date 2025-10-16

# Load L1
py .\scripts\load_parquet_to_postgres_l1.py --config-file .\variables.json --object-key-staging "source=open-meteo/ds=2025-10-16/weather_hourly.parquet" --run-date 2025-10-16
```

## 🗃 Data & DQ Schemas

**Staging Parquet columns**

```
ts (tz-aware), date, hour, latitude, longitude, timezone, temperature_c, load_ds, source
```

**L1 (weather.l1_weather_hourly)**

* PK: `(ts, latitude, longitude, source)`
* Types: `ts TIMESTAMPTZ`, `date DATE`, `hour SMALLINT`, `latitude DOUBLE PRECISION`, `longitude DOUBLE PRECISION`, `timezone TEXT`, `temperature_c DOUBLE PRECISION`, `load_ds DATE`, `source TEXT`

**L2 (weather.l2_weather_hourly)**

* Same business columns, curated by SQL:

  * `ROW_NUMBER()` chooses latest `load_ds` per key.
  * `INSERT … ON CONFLICT … DO UPDATE` for idempotency.

**Light DQ at normalize**

* time/temp array length equality
* not empty
* not all `NaN` in `temperature_c`

## Monitoring & Dashboards

* Airflow UI: retries, durations, logs (the scripts log key parameters: `ds`, object keys, row counts).
* Optional: build a simple Metabase dashboard on top of L2 or extend to Gold layer.

## Acknowledgements

* [Open-Meteo](https://open-meteo.com/)
* Apache Airflow, MinIO, PostgreSQL communities

## Support me!

👉 If you find this project useful, **please ⭐ this repository 😆**!
---

👉 _More about myself: <a href="https://linktr.ee/caesarmario_"> here </a>_