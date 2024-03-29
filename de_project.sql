CREATE KEYSPACE iotsolution
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

USE iotsolution;

CREATE TABLE bronze_turbine_sensor (
    id UUID PRIMARY KEY,
    recorded_date DATE,
    device_id TEXT,
    window TIMESTAMP,
    rpm FLOAT,
    angle FLOAT
);

CREATE TABLE bronze_weather_sensor (
    id UUID PRIMARY KEY,
    recorded_date DATE,
    window TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT
);

CREATE TABLE silver_aggregate_turbine (
    id UUID PRIMARY KEY,
    recorded_date DATE,
    device_id TEXT,
    time_interval TIME,
    rpm FLOAT,
    angle FLOAT
);

CREATE TABLE silver_aggregate_weather (
    id UUID PRIMARY KEY,
    recorded_date DATE,
    time_interval TIME,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT
);

CREATE TABLE gold_enriched_turbine (
    id UUID PRIMARY KEY,
    recorded_date DATE,
    time_interval TIME,
    rpm FLOAT,
    angle FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT
);

ALTER TABLE iotsolution.bronze_weather_sensor ADD DEVICE_ID text;
