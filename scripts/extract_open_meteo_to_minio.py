####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- Extract Open-Meteo hourly weather to MinIO (raw JSON)
## 
####

# Importing Libraries
import json
import time
import requests
import argparse
import os

from io import BytesIO
from datetime import datetime, date
from typing import Dict, Any, Optional
from minio import Minio
from minio.error import S3Error

from helper_logging import logger

# ---------------------------
# Helpers
# ---------------------------
def _ensure_bucket(client: Minio, bucket: str) -> None:
    """Create the bucket if not exists (idempotent)."""
    found = client.bucket_exists(bucket)
    if not found:
        logger.info(f"Bucket '{bucket}' not found. Creating...")
        client.make_bucket(bucket)
        logger.info(f"Bucket '{bucket}' created.")
    else:
        logger.info(f"Bucket '{bucket}' already exists. Continue.")

def _build_open_meteo_params(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Map config keys to query parameters supported by Open-Meteo."""
    params = {
        "latitude": cfg["latitude"],
        "longitude": cfg["longitude"],
        "hourly": ",".join(cfg.get("hourly", ["temperature_2m"])),
        "timezone": cfg.get("timezone", "UTC"),
    }
    # LOG: safe to log (no secrets)
    logger.info(
        "Built Open-Meteo params: lat=%s lon=%s hourly=%s tz=%s",
        params["latitude"], params["longitude"], params["hourly"], params["timezone"]
    )
    return params

def _http_get_json(url: str, params: Dict[str, Any], timeout_sec: int, retries: int, backoff_sec: int) -> Dict[str, Any]:
    """
    Simple GET with retry & linear backoff.
    - url: API base URL
    - params: query parameters
    - timeout_sec: HTTP timeout in seconds
    - retries: number of retries on failure
    - backoff_sec: seconds to wait between retries
    """
    last_err = None
    for attempt in range(1, retries + 2):  # retries + 1 attempts
        try:
            logger.info("HTTP GET attempt %s to %s (timeout=%ss)", attempt, url, timeout_sec)
            resp = requests.get(url, params=params, timeout=timeout_sec)
            logger.info("HTTP status: %s", resp.status_code)
            resp.raise_for_status()
            payload = resp.json()
            # LOG: basic size info
            try:
                size_bytes = len(json.dumps(payload))
                logger.info("Received JSON ~%s bytes", size_bytes)
            except Exception:
                logger.info("Received JSON (size unknown)")
            return payload
        except Exception as e:
            last_err = e
            logger.warning("HTTP attempt %s failed: %s", attempt, repr(e))
            if attempt <= retries:
                logger.info("Retrying in %s sec...", backoff_sec)
                time.sleep(backoff_sec)
            else:
                logger.error("All HTTP retries exhausted.")
                raise
    # Should not reach here
    raise last_err  # type: ignore

def _object_key_from_template(template: str, ds: str) -> str:
    """Fill the MinIO object key template with ds: e.g., 'source=open-meteo/ds=2025-10-15/weather_raw.json'."""
    key = template.format(ds=ds)
    logger.info("Resolved object key: %s", key)
    return key

def _to_bytesio_json(data: Dict[str, Any]) -> BytesIO:
    """Serialize dict to BytesIO so MinIO client can upload it."""
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    logger.info("Serialized JSON to bytes: %s bytes", len(payload))
    return BytesIO(payload)

# ---------------------------
# Main entry
# ---------------------------
def run(
    minio_cfg: Dict[str, Any],
    open_meteo_cfg: Dict[str, Any],
    run_date: Optional[str] = None,
) -> str:
    """
    Orchestrate extract -> store to MinIO.
    """
    start_ts = time.time()
    ds = run_date or date.today().strftime("%Y-%m-%d")
    logger.info("=== START extract_open_meteo_to_minio (ds=%s) ===", ds)

    # 1) Build request
    url = open_meteo_cfg["base_url"]
    logger.info("Using Open-Meteo base_url: %s", url)
    params = _build_open_meteo_params(open_meteo_cfg)

    # 2) Fetch JSON with small retry/backoff
    data = _http_get_json(
        url=url,
        params=params,
        timeout_sec=int(open_meteo_cfg.get("timeout_sec", 30)),
        retries=int(open_meteo_cfg.get("retries", 3)),
        backoff_sec=int(open_meteo_cfg.get("backoff_sec", 2)),
    )

    # 3) Prepare MinIO client and bucket (do NOT log secrets)
    logger.info("Connecting to MinIO endpoint=%s secure=%s bucket=%s",
                minio_cfg["endpoint"], bool(minio_cfg.get("secure", False)), minio_cfg["bucket_raw"])
    client = Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=bool(minio_cfg.get("secure", False)),
    )
    bucket = minio_cfg["bucket_raw"]
    _ensure_bucket(client, bucket)

    # 4) Build object key from template
    object_key = _object_key_from_template(minio_cfg["raw_key_template"], ds)

    # 5) Upload JSON as bytes
    buf = _to_bytesio_json(data)
    size = buf.getbuffer().nbytes
    logger.info("Uploading object to MinIO: s3://%s/%s (size=%s bytes)", bucket, object_key, size)
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=buf,
        length=size,
        content_type="application/json",
    )

    # 6) Basic logging to stdout (retain your original print)
    elapsed = round(time.time() - start_ts, 2)
    logger.info("SUCCESS store raw JSON → s3://%s/%s (elapsed=%ss)", bucket, object_key, elapsed)
    print(
        f"[extract_open_meteo_to_minio] stored object: s3://{bucket}/{object_key} "
        f"(lat={params['latitude']}, lon={params['longitude']}, ds={ds})"
    )
    logger.info("=== END extract_open_meteo_to_minio (ds=%s) ===", ds)
    return object_key

# -------------- CLI for local testing --------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Open-Meteo JSON and store to MinIO (raw).")
    parser.add_argument("--config-file", type=str, help="Path to combined JSON config file.")
    parser.add_argument("--run-date", type=str, default=None, help="Execution date (YYYY-MM-DD).")
    args = parser.parse_args()

    if not args.config_file or not os.path.exists(args.config_file):
        raise SystemExit("Please provide --config-file with keys: MINIO_CONFIG, OPEN_METEO_CONFIG, PROJECT_CONFIG")

    with open(args.config_file, "r", encoding="utf-8") as f:
        cfg_all = json.load(f)

    obj_key = run(
        minio_cfg=cfg_all["MINIO_CONFIG"],
        open_meteo_cfg=cfg_all["OPEN_METEO_CONFIG"],
        run_date=args.run_date,
    )
    print("OK ->", obj_key)