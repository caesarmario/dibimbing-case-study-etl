----
-- dibimbing.id - Case Study ETL
-- Mario Caesar // linkedin.com/in/caesarmario
-- -- Create l2 table sql
----

CREATE TABLE IF NOT EXISTS weather.l2_weather_hourly (
  ts TIMESTAMPTZ NOT NULL,
  date DATE NOT NULL,
  hour SMALLINT,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  timezone TEXT,
  temperature_c DOUBLE PRECISION,
  load_ds DATE NOT NULL,
  source TEXT NOT NULL,
  CONSTRAINT pk_l2_weather_hourly PRIMARY KEY (ts, latitude, longitude, source)
);

CREATE INDEX IF NOT EXISTS idx_l2_weather_hourly_date    ON weather.l2_weather_hourly(date);
CREATE INDEX IF NOT EXISTS idx_l2_weather_hourly_lat_lon ON weather.l2_weather_hourly(latitude, longitude);