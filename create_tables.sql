CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_hourly (
    city String,
    datetime DateTime,
    temperature Float32,
    precipitation Float32,
    wind_speed Float32,
    wind_direction String
) ENGINE = MergeTree()
ORDER BY (city, datetime);

CREATE TABLE IF NOT EXISTS weather.weather_daily (
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    total_precipitation Float32,
    wind_alert String
) ENGINE = MergeTree()
ORDER BY (city, date);