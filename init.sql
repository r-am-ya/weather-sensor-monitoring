
CREATE DATABASE IF NOT EXISTS weather_streaming;
USE weather_streaming;

CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id CHAR(36) NOT NULL UNIQUE,
    station_id VARCHAR(50) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    event_timestamp DATETIME(3) NOT NULL,
    temperature_c DECIMAL(6,2) NOT NULL,
    humidity_pct DECIMAL(5,2) NOT NULL,
    wind_speed_kmh DECIMAL(7,2) NOT NULL,
    heat_index_c DECIMAL(6,2) NULL,
    rolling_avg_temp_c DECIMAL(6,2) NULL,
    rolling_avg_humidity_pct DECIMAL(5,2) NULL,
    rolling_avg_wind_kmh DECIMAL(7,2) NULL,
    peak_wind_kmh DECIMAL(7,2) NULL,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_score FLOAT NULL,
    anomaly_scenario VARCHAR(40) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_station_timestamp (station_id, event_timestamp),
    INDEX idx_anomaly_timestamp (is_anomaly, event_timestamp)
);

CREATE TABLE IF NOT EXISTS station_metrics (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    latest_timestamp DATETIME(3) NOT NULL,
    temperature_c DECIMAL(6,2) NOT NULL,
    humidity_pct DECIMAL(5,2) NOT NULL,
    wind_speed_kmh DECIMAL(7,2) NOT NULL,
    heat_index_c DECIMAL(6,2) NULL,
    rolling_avg_temp_c DECIMAL(6,2) NULL,
    rolling_avg_humidity_pct DECIMAL(5,2) NULL,
    rolling_avg_wind_kmh DECIMAL(7,2) NULL,
    peak_temp_c DECIMAL(6,2) NULL,
    peak_wind_kmh DECIMAL(7,2) NULL,
    total_readings BIGINT NOT NULL DEFAULT 0,
    anomaly_count BIGINT NOT NULL DEFAULT 0,
    last_is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);
