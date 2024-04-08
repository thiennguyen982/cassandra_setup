DROP KEYSPACE IF EXISTS iotsolution;

CREATE KEYSPACE iotsolution
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

USE iotsolution;

CREATE TABLE bronze_turbine_sensor (
    id UUID,
    recorded_date DATE,
    device_id TEXT,
    window TIMESTAMP,
    rpm FLOAT,
    angle FLOAT,
    PRIMARY KEY(device_id, recorded_date, id)
);

CREATE TABLE bronze_weather_sensor (
    id UUID,
    device_id TEXT,
    recorded_date DATE,
    window TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT,
    PRIMARY KEY (device_id, recorded_date, window)
) WITH CLUSTERING ORDER BY (recorded_date DESC, window DESC);

CREATE TABLE silver_aggregate_turbine (
    id UUID,
    recorded_date TEXT,
    device_id TEXT,
    rpm FLOAT,
    angle FLOAT,
    PRIMARY KEY (device_id, recorded_date, id)
);

CREATE TABLE silver_aggregate_weather (
    id UUID,
    device_id TEXT,
    recorded_date TEXT,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT,
    PRIMARY KEY (device_id, recorded_date, id)
);

CREATE TABLE gold_enriched_turbine (
    id UUID,
    device_id TEXT,
    recorded_date TEXT,
    rpm FLOAT,
    angle FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    windspeed FLOAT,
    winddirection TEXT,
    PRIMARY KEY (device_id, recorded_date)
);