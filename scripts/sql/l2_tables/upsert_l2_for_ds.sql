----
-- dibimbing.id - Case Study ETL
-- Mario Caesar // linkedin.com/in/caesarmario
-- -- Upsert l2 for ds
----

WITH ranked AS (
  SELECT
    ts, date, hour, latitude, longitude, timezone, temperature_c, load_ds, source,
    ROW_NUMBER() OVER (
      PARTITION BY ts, latitude, longitude, source
      ORDER BY load_ds DESC
    ) AS rn
  FROM weather.l1_weather_hourly
  WHERE load_ds = '{{ ds }}'::date
),
latest AS (
  SELECT ts, date, hour, latitude, longitude, timezone, temperature_c, load_ds, source
  FROM ranked
  WHERE rn = 1
)
INSERT INTO weather.l2_weather_hourly (
  ts, date, hour, latitude, longitude, timezone, temperature_c, load_ds, source
)
SELECT
  ts, date, hour, latitude, longitude, timezone, temperature_c, load_ds, source
FROM latest
ON CONFLICT (ts, latitude, longitude, source) DO UPDATE SET
  date          = EXCLUDED.date,
  hour          = EXCLUDED.hour,
  timezone      = EXCLUDED.timezone,
  temperature_c = EXCLUDED.temperature_c,
  load_ds       = EXCLUDED.load_ds;
