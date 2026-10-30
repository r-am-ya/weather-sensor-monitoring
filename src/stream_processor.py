
"""Consume Kafka weather events and calculate per-station sliding windows."""

import json
import logging
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from statistics import mean

import joblib
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from mysql.connector import Error as MySQLError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
    MODEL_PATH,
    WINDOW_SIZE,
)
from src.database import save_reading


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# Load the trained model and its feature names once at startup.
saved_model = joblib.load(MODEL_PATH)
model = (
    saved_model["model"]
    if isinstance(saved_model, dict)
    else saved_model
)
MODEL_FEATURES = [
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
]
logger.info("Isolation Forest model loaded from %s", MODEL_PATH)
logger.info("Model features: %s", MODEL_FEATURES)


# Maintain an independent sliding window for each weather station.
station_windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


def validate_reading(reading: dict) -> None:
    """Validate event structure and numeric values."""
    if not isinstance(reading, dict):
        raise ValueError("Reading must be a dictionary")

    required = (
        "event_id",
        "station_id",
        "station_name",
        "event_timestamp",
        "temperature_c",
        "humidity_pct",
        "wind_speed_kmh",
    )

    missing = [field for field in required if field not in reading]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    for field in (
        "temperature_c",
        "humidity_pct",
        "wind_speed_kmh",
    ):
        value = reading[field]

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field} must be numeric")

        if not math.isfinite(value):
            raise ValueError(f"{field} must be finite")

    if not isinstance(reading["station_id"], str) or not reading["station_id"].strip():
        raise ValueError("station_id cannot be empty")

    if not isinstance(reading["station_name"], str) or not reading["station_name"].strip():
        raise ValueError("station_name cannot be empty")

    if not isinstance(reading["event_id"], str) or not reading["event_id"].strip():
        raise ValueError("event_id cannot be empty")

    if not isinstance(reading["event_timestamp"], str):
        raise ValueError("event_timestamp must be a string")

    try:
        datetime.fromisoformat(
            reading["event_timestamp"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("event_timestamp must be a valid ISO timestamp") from exc

    if reading["wind_speed_kmh"] < 0:
        raise ValueError("wind_speed_kmh cannot be negative")

    # Deliberately do not reject humidity outside 0–100 here.
    # Those events must be retained and marked as sensor faults.


def calculate_heat_index_c(
    temperature_c: float,
    humidity_pct: float,
):
    """Estimate heat index in Celsius using the NOAA heat-index algorithm."""

    if not math.isfinite(temperature_c) or not math.isfinite(humidity_pct):
        return None

    if not 0 <= humidity_pct <= 100:
        return None

    if temperature_c < 26.7 or temperature_c > 40.0:
        return None

    # Convert Celsius to Fahrenheit.
    temperature_f = temperature_c * 9 / 5 + 32
    relative_humidity = humidity_pct

    # NOAA preliminary heat-index estimate.
    heat_index_f = 0.5 * (
        temperature_f
        + 61.0
        + ((temperature_f - 68.0) * 1.2)
        + (relative_humidity * 0.094)
    )

    heat_index_f = (heat_index_f + temperature_f) / 2

    # Apply Rothfusz regression when the preliminary estimate is >= 80°F.
    if heat_index_f >= 80:
        heat_index_f = (
            -42.379
            + 2.04901523 * temperature_f
            + 10.14333127 * relative_humidity
            - 0.22475541 * temperature_f * relative_humidity
            - 0.00683783 * temperature_f**2
            - 0.05481717 * relative_humidity**2
            + 0.00122874 * temperature_f**2 * relative_humidity
            + 0.00085282 * temperature_f * relative_humidity**2
            - 0.00000199 * temperature_f**2 * relative_humidity**2
        )

        # Low-humidity adjustment.
        if relative_humidity < 13 and 80 <= temperature_f <= 112:
            heat_index_f -= (
                (13 - relative_humidity) / 4
            ) * math.sqrt((17 - abs(temperature_f - 95)) / 17)

        # High-humidity adjustment.
        elif relative_humidity > 85 and 80 <= temperature_f <= 87:
            heat_index_f += (
                (relative_humidity - 85) / 10
            ) * ((87 - temperature_f) / 5)

    # Convert Fahrenheit back to Celsius.
    heat_index_c = (heat_index_f - 32) * 5 / 9

    if not math.isfinite(heat_index_c):
        return None

    return round(heat_index_c, 2)


def calculate_metrics(readings: list[dict]) -> dict:
    """Calculate rolling averages, peaks, and heat index."""
    if not readings:
        raise ValueError("Cannot calculate metrics for an empty window")

    latest = readings[-1]

    return {
        "window_count": len(readings),
        "rolling_avg_temp_c": round(
            mean(item["temperature_c"] for item in readings), 2
        ),
        "rolling_avg_humidity_pct": round(
            mean(item["humidity_pct"] for item in readings), 2
        ),
        "rolling_avg_wind_kmh": round(
            mean(item["wind_speed_kmh"] for item in readings), 2
        ),
        "peak_temp_c": round(
            max(item["temperature_c"] for item in readings), 2
        ),
        "peak_wind_kmh": round(
            max(item["wind_speed_kmh"] for item in readings), 2
        ),
        "heat_index_c": calculate_heat_index_c(
            latest["temperature_c"],
            latest["humidity_pct"],
        ),
    }


def predict_anomaly(reading: dict) -> dict:
    """Score a valid weather reading using the trained Isolation Forest."""
    features = [[reading[name] for name in MODEL_FEATURES]]

    prediction = int(model.predict(features)[0])
    score = float(model.decision_function(features)[0])

    return {
        "is_anomaly": prediction == -1,
        "anomaly_score": score,
    }


def process_reading(reading: dict) -> dict:
    """Process readings and retain faulty humidity events."""
    validate_reading(reading)

    station_id = reading["station_id"]
    humidity = reading["humidity_pct"]
    scenario = reading.get("scenario")

    # Identify sensor faults before calculating metrics or predictions.
    humidity_fault = not 0 <= humidity <= 100

    if humidity_fault:
        scenario = scenario or "humidity_fault"

        result = {
            **reading,
            "window_count": len(station_windows[station_id]),
            "rolling_avg_temp_c": None,
            "rolling_avg_humidity_pct": None,
            "rolling_avg_wind_kmh": None,
            "peak_temp_c": None,
            "peak_wind_kmh": None,
            "heat_index_c": None,
            "is_anomaly": True,
            "anomaly_score": None,
            "scenario": scenario,
            "anomaly_scenario": scenario,
            "sensor_fault": True,
        }

        logger.warning(
            "Sensor fault detected: station=%s humidity=%.2f%%; "
            "excluded from model and rolling metrics",
            station_id,
            humidity,
        )

        return result

    # Only readings with valid humidity enter the station window.
    window = station_windows[station_id]
    window.append(reading)

    metrics = calculate_metrics(list(window))
    anomaly = predict_anomaly(reading)

    result = {
        **reading,
        **metrics,
        **anomaly,
        "scenario": scenario,
        "anomaly_scenario": scenario,
        "sensor_fault": False,
    }

    logger.debug(
        "Processed station=%s window=%d scenario=%s "
        "avg_temp=%.2fC avg_humidity=%.2f%% "
        "avg_wind=%.2fkm/h heat_index=%s "
        "anomaly=%s score=%.4f",
        station_id,
        metrics["window_count"],
        scenario,
        metrics["rolling_avg_temp_c"],
        metrics["rolling_avg_humidity_pct"],
        metrics["rolling_avg_wind_kmh"],
        metrics["heat_index_c"],
        anomaly["is_anomaly"],
        anomaly["anomaly_score"],
    )

    return result


def run_processor() -> None:
    """Consume Kafka events continuously until interrupted."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    logger.info(
        "Listening to topic '%s' at %s",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    pending_commit = 0
    total_processed = 0
    commit_batch_size = 50
    try:
        for message in consumer:
            try:
                reading = message.value
                reading["received_timestamp"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                processed_reading = process_reading(reading)
                save_reading(processed_reading)
                total_processed += 1

            except (ValueError, TypeError, KeyError, json.JSONDecodeError, MySQLError):
                logger.exception(
                    "Skipping invalid event at partition=%s offset=%s",
                    message.partition,
                    message.offset,
                )
            pending_commit += 1
            if pending_commit >= commit_batch_size:
                consumer.commit()
                logger.info("Committed %d events; %d processed this session", pending_commit, total_processed)
                pending_commit = 0

    except KeyboardInterrupt:
        logger.info("Stopping stream processor.")
        if pending_commit:
            consumer.commit()

    except KafkaError:
        logger.exception("Kafka consumer encountered an error.")
        raise

    finally:
        consumer.close()


if __name__ == "__main__":
    run_processor()
