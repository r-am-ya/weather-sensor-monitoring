
"""Safely test whether an existing event increments metrics twice."""

from src.database import get_connection, save_reading


def main():
    connection = get_connection()
    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute("""
            SELECT *
            FROM weather_readings
            WHERE humidity_pct BETWEEN 0 AND 100
            ORDER BY event_timestamp DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row is None:
            print("No valid reading found. Test stopped.")
            return

        station_id = row["station_id"]

        cursor.execute("""
            SELECT total_readings, anomaly_count
            FROM station_metrics
            WHERE station_id = %s
        """, (station_id,))
        before = cursor.fetchone()

        reading = {
            "event_id": row["event_id"],
            "station_id": row["station_id"],
            "station_name": row["station_name"],
            "event_timestamp": row["event_timestamp"].isoformat(),
            "temperature_c": row["temperature_c"],
            "humidity_pct": row["humidity_pct"],
            "wind_speed_kmh": row["wind_speed_kmh"],
            "heat_index_c": row["heat_index_c"],
            "rolling_avg_temp_c": row["rolling_avg_temp_c"],
            "rolling_avg_humidity_pct": row["rolling_avg_humidity_pct"],
            "rolling_avg_wind_kmh": row["rolling_avg_wind_kmh"],
            "peak_wind_kmh": row["peak_wind_kmh"],
            "is_anomaly": bool(row["is_anomaly"]),
            "anomaly_score": row["anomaly_score"],
            "anomaly_scenario": row["anomaly_scenario"],
            "sensor_fault": False,
        }

        cursor.close()
        connection.close()
        connection = None

        print("Testing existing event:", reading["event_id"])
        print("Station:", station_id)
        print("Metrics before:", before)

        # Save the exact same existing event.
        save_reading(reading)

        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT total_readings, anomaly_count
            FROM station_metrics
            WHERE station_id = %s
        """, (station_id,))
        after = cursor.fetchone()

        print("Metrics after: ", after)

        if before == after:
            print("PASS: Duplicate event did not change station metrics.")
        else:
            print("FAIL: Station metrics changed during duplicate test.")

        cursor.close()

    finally:
        if connection is not None and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    main()
