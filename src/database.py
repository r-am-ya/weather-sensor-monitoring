
"""MySQL persistence for processed weather events."""

import logging
from datetime import datetime, timezone

import mysql.connector
from mysql.connector import Error

from src.config import (
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)

logger = logging.getLogger(__name__)


def get_connection():
    """Open a connection to the weather database."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connection_timeout=10,
    )


def parse_timestamp(value: str) -> datetime:
    """Convert an ISO timestamp into a naive UTC datetime for MySQL."""
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if timestamp.utcoffset() is not None:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)

    return timestamp


def save_reading(reading: dict) -> None:
    """
    Store a weather event and update station metrics only for newly
    inserted, valid sensor readings.
    """
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        event_id = reading["event_id"]
        event_timestamp = parse_timestamp(reading["event_timestamp"])

        # Store every event, including faulty sensor readings.
        # Duplicate event IDs must not create duplicate database rows.
        insert_sql = """
            INSERT INTO weather_readings (
                event_id, station_id, station_name, event_timestamp,
                temperature_c, humidity_pct, wind_speed_kmh, heat_index_c,
                rolling_avg_temp_c, rolling_avg_humidity_pct,
                rolling_avg_wind_kmh, peak_wind_kmh,
                is_anomaly, anomaly_score, anomaly_scenario
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)
        """

        cursor.execute(
            insert_sql,
            (
                event_id,
                reading["station_id"],
                reading["station_name"],
                event_timestamp,
                reading["temperature_c"],
                reading["humidity_pct"],
                reading["wind_speed_kmh"],
                reading.get("heat_index_c"),
                reading.get("rolling_avg_temp_c"),
                reading.get("rolling_avg_humidity_pct"),
                reading.get("rolling_avg_wind_kmh"),
                reading.get("peak_wind_kmh"),
                bool(reading.get("is_anomaly", False)),
                reading.get("anomaly_score"),
                reading.get("scenario")
                or reading.get("anomaly_scenario"),
            ),
        )

        # With the no-op duplicate-key update above, a newly inserted
        # row normally reports rowcount=1; an unchanged duplicate
        # normally reports rowcount=0. Do not count duplicate events twice.
        if cursor.rowcount == 0:
            connection.commit()
            logger.info("Duplicate event ignored: %s", event_id)
            return

        is_anomaly = bool(reading.get("is_anomaly", False))
        sensor_fault = bool(reading.get("sensor_fault", False))

        # Faulty sensor readings remain in weather_readings but must not
        # affect station metrics or replace the latest valid station data.
        if not sensor_fault:
            metrics_sql = """
                INSERT INTO station_metrics (
                    station_id, station_name, latest_timestamp,
                    temperature_c, humidity_pct, wind_speed_kmh, heat_index_c,
                    rolling_avg_temp_c, rolling_avg_humidity_pct,
                    rolling_avg_wind_kmh, peak_temp_c, peak_wind_kmh,
                    total_readings, anomaly_count, last_is_anomaly
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, 1, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    station_name = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(station_name), station_name
                    ),
                    temperature_c = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(temperature_c), temperature_c
                    ),
                    humidity_pct = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(humidity_pct), humidity_pct
                    ),
                    wind_speed_kmh = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(wind_speed_kmh), wind_speed_kmh
                    ),
                    heat_index_c = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(heat_index_c), heat_index_c
                    ),
                    rolling_avg_temp_c = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(rolling_avg_temp_c),
                        rolling_avg_temp_c
                    ),
                    rolling_avg_humidity_pct = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(rolling_avg_humidity_pct),
                        rolling_avg_humidity_pct
                    ),
                    rolling_avg_wind_kmh = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(rolling_avg_wind_kmh),
                        rolling_avg_wind_kmh
                    ),
                    peak_temp_c = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(peak_temp_c), peak_temp_c
                    ),
                    peak_wind_kmh = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(peak_wind_kmh), peak_wind_kmh
                    ),
                    total_readings = total_readings + 1,
                    anomaly_count = anomaly_count + VALUES(anomaly_count),
                    last_is_anomaly = IF(
                        VALUES(latest_timestamp) >= latest_timestamp,
                        VALUES(last_is_anomaly), last_is_anomaly
                    ),
                    latest_timestamp = GREATEST(
                        latest_timestamp, VALUES(latest_timestamp)
                    )
            """

            cursor.execute(
                metrics_sql,
                (
                    reading["station_id"],
                    reading["station_name"],
                    event_timestamp,
                    reading["temperature_c"],
                    reading["humidity_pct"],
                    reading["wind_speed_kmh"],
                    reading.get("heat_index_c"),
                    reading.get("rolling_avg_temp_c"),
                    reading.get("rolling_avg_humidity_pct"),
                    reading.get("rolling_avg_wind_kmh"),
                    reading.get("peak_temp_c"),
                    reading.get("peak_wind_kmh"),
                    int(is_anomaly),
                    int(is_anomaly),
                ),
            )

        connection.commit()

    except Error:
        if connection is not None:
            connection.rollback()

        logger.exception(
            "Failed to save event %s to MySQL",
            reading.get("event_id"),
        )
        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None and connection.is_connected():
            connection.close()
