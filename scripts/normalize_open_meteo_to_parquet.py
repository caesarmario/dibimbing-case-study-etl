####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- Normalize Open-Meteo RAW JSON → Parquet (staging)
####

# Importing Libraries
import argparse
import os
import json
import pandas as pd

from io import BytesIO
from typing import Dict, Any, Optional
from minio import Minio
from minio.error import S3Error

from helper_logging import logger

# ---------------------------
# Helpers
# ---------------------------
def _ensure_bucket(client: Minio, bucket: str) -> None:
    """Create bucket if not exists (idempotent)."""
    if not client.bucket_exists(bucket):
        logger.info("Bucket '%s' not found. Creating...", bucket)
        client.make_bucket(bucket)
        logger.info("Bucket '%s' created.", bucket)
    else:
        logger.info("Bucket '%s' already exists.", bucket)


def _read_raw_json_from_minio(client: Minio, bucket: str, object_key: str) -> Dict[str, Any]:
    """Download RAW JSON object and deserialize."""
    logger.info("Downloading RAW from s3://%s/%s ...", bucket, object_key)
    resp = client.get_object(bucket, object_key)
    try:
        raw_bytes = resp.read()
        logger.info("Downloaded RAW size ~%s bytes", len(raw_bytes))
    finally:
        resp.close()
        resp.release_conn()
    return json.loads(raw_bytes.decode("utf-8"))


def _build_hourly_df(payload: Dict[str, Any], timezone: str, latitude: float, longitude: float, ds: str) -> pd.DataFrame:
    """Explode hourly arrays into DataFrame with fixed schema."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])

    logger.info("Payload hourly lengths: time=%d, temperature_2m=%d", len(times), len(temps))

    # Basic DQ: equal length
    if len(times) != len(temps):
        raise ValueError(f"Length mismatch hourly arrays: time={len(times)} vs temp={len(temps)}")

    # Build DataFrame
    df = pd.DataFrame({"ts": pd.to_datetime(times), "temperature_c": pd.to_numeric(temps, errors="coerce")})

    # Make ts timezone-aware (based on API timezone)
    logger.info("Localizing timestamps to tz='%s'", timezone)
    df["ts"] = df["ts"].dt.tz_localize(timezone, nonexistent="shift_forward", ambiguous="NaT")

    # Additional columns
    df["date"] = df["ts"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["ts"].dt.hour.astype("Int64")
    df["latitude"] = float(latitude)
    df["longitude"] = float(longitude)
    df["timezone"] = timezone
    df["load_ds"] = ds
    df["source"] = "open-meteo"

    # Enforce schema/dtypes (fixed schema)
    dtypes = {
        "ts": "datetime64[ns, {}]".format(timezone),
        "date": "string",
        "hour": "Int64",
        "latitude": "float64",
        "longitude": "float64",
        "timezone": "string",
        "temperature_c": "float64",
        "load_ds": "string",
        "source": "string",
    }
    for col, typ in dtypes.items():
        if col in ("ts",):
            continue
        if col in df.columns:
            df[col] = df[col].astype(typ)

    # Minimal DQ checks
    if df.empty:
        raise ValueError("No hourly rows produced from payload.")
    if df["temperature_c"].isna().all():
        raise ValueError("All temperature_c values are NaN.")

    logger.info("DataFrame built with %d rows; columns=%s", len(df), list(df.columns))
    return df[
        ["ts", "date", "hour", "latitude", "longitude", "timezone", "temperature_c", "load_ds", "source"]
    ]

# ---------------------------
# Main entry
# ---------------------------
def run(
    minio_cfg: Dict[str, Any],
    open_meteo_cfg: Dict[str, Any],
    object_key_raw: str,
    run_date: Optional[str] = None,
) -> str:
    """
    Normalize RAW JSON from MinIO into Parquet (snappy) and upload to staging.
    """
    ds = run_date
    logger.info("=== START normalize_open_meteo_to_parquet (ds=%s) ===", ds)

    # MinIO client (do not log secrets)
    logger.info("Connecting MinIO endpoint=%s secure=%s", minio_cfg["endpoint"], bool(minio_cfg.get("secure", False)))
    client = Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=bool(minio_cfg.get("secure", False)),
    )

    # Read RAW json
    raw = _read_raw_json_from_minio(client, minio_cfg["bucket_raw"], object_key_raw)

    # Extract meta for columns
    timezone = raw.get("timezone", open_meteo_cfg.get("timezone", "UTC"))
    latitude = float(raw.get("latitude", open_meteo_cfg.get("latitude")))
    longitude = float(raw.get("longitude", open_meteo_cfg.get("longitude")))
    logger.info("Meta: tz=%s lat=%.4f lon=%.4f", timezone, latitude, longitude)

    # Build hourly DF
    df = _build_hourly_df(raw, timezone=timezone, latitude=latitude, longitude=longitude, ds=ds)

    # Serialize to Parquet into memory
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    logger.info("Parquet buffer prepared: %s bytes", buf.getbuffer().nbytes)

    # Ensure staging bucket & upload
    bucket_staging = minio_cfg["bucket_staging"]
    _ensure_bucket(client, bucket_staging)
    object_key_staging = minio_cfg["staging_key_template"].format(ds=ds)

    logger.info("Uploading Parquet → s3://%s/%s", bucket_staging, object_key_staging)
    client.put_object(
        bucket_name=bucket_staging,
        object_name=object_key_staging,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    # Keep original print for compatibility; Airflow captures both print & logging
    print(f"[normalize_open_meteo_to_parquet] stored: s3://{bucket_staging}/{object_key_staging} rows={len(df)}")
    logger.info("SUCCESS store Parquet rows=%d => s3://%s/%s", len(df), bucket_staging, object_key_staging)
    logger.info("=== END normalize_open_meteo_to_parquet (ds=%s) ===", ds)
    return object_key_staging

# -------------- CLI for local testing --------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize RAW Open-Meteo JSON → Parquet (staging).")
    parser.add_argument("--config-file", required=True, help="Path to JSON with MINIO_CONFIG & OPEN_METEO_CONFIG")
    parser.add_argument("--object-key-raw", required=True, help="Object key of RAW JSON (from extract output)")
    parser.add_argument("--run-date", required=True, help="ds=YYYY-MM-DD used for staging key")
    args = parser.parse_args()

    if not os.path.exists(args.config_file):
        raise SystemExit(f"Config file not found: {args.config_file}")

    with open(args.config_file, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    out_key = run(
        minio_cfg=cfg["MINIO_CONFIG"],
        open_meteo_cfg=cfg["OPEN_METEO_CONFIG"],
        object_key_raw=args.object_key_raw,
        run_date=args.run_date,
    )
    print("OK ->", out_key)