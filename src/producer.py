"""Publish simulated or Open-Meteo weather readings to Kafka."""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SENSOR_INTERVAL_SECONDS,
    STATIONS,
    WEATHER_SOURCE,
    OPEN_METEO_INTERVAL_SECONDS,
)
from src.sensor_simulator import generate_reading
from src.weather_visuals import fetch_bengaluru_current

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def build_open_meteo_reading(current: dict, received_at: datetime | None = None) -> dict:
    """Map a current API sample to the common Kafka event schema."""
    observation = current["observation_timestamp"]
    received_at = received_at or datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"open-meteo:blr:{observation}")),
        "station_id": "BLR", "station_name": "Bengaluru",
        "event_timestamp": observation,
        "observation_timestamp": observation,
        "received_timestamp": received_at.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
        "temperature_c": current["temperature_c"],
        "humidity_pct": current["humidity_pct"],
        "wind_speed_kmh": current["wind_speed_kmh"],
        "wind_direction_deg": current["wind_direction_deg"],
        "source_type": "open_meteo", "scenario": None, "is_simulated": False,
    }


def create_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def create_producer_with_retry() -> KafkaProducer:
    """Reconnect with capped backoff while Kafka is temporarily unavailable."""
    delay = 2
    while True:
        try:
            return create_producer()
        except KafkaError:
            logger.exception("Kafka is unavailable; retrying producer connection in %ss", delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def run_producer() -> None:
    """Publish readings until interrupted with Ctrl+C."""
    producer = create_producer_with_retry()
    station_ids = list(STATIONS)
    if WEATHER_SOURCE not in {"simulated", "open_meteo"}:
        producer.close()
        raise ValueError("WEATHER_SOURCE must be simulated or open_meteo")

    # One scenario can be injected using the SCENARIO environment variable.
    scenario = os.getenv("SCENARIO", "").strip().lower() or None
    if scenario not in {None, "heatwave", "storm_gust", "humidity_fault"}:
        producer.close()
        raise ValueError(
            "SCENARIO must be heatwave, storm_gust, humidity_fault, or unset."
        )

    count = 0
    logger.info(
        "Publishing %s readings to topic '%s' at %s",
        WEATHER_SOURCE,
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    try:
        while True:
            if WEATHER_SOURCE == "open_meteo":
                try:
                    current = fetch_bengaluru_current()
                    reading = build_open_meteo_reading(current)
                    readings_to_send = [reading]
                except Exception:
                    logger.exception("Open-Meteo request failed; retrying after backoff")
                    time.sleep(min(OPEN_METEO_INTERVAL_SECONDS, 60))
                    continue
            else:
                readings_to_send = []
                for station_id in station_ids:
                    reading = generate_reading(station_id=station_id, scenario=scenario)
                    reading.update({"source_type": "simulated", "observation_timestamp": reading["event_timestamp"],
                                    "received_timestamp": reading["event_timestamp"]})
                    readings_to_send.append(reading)

            for reading in readings_to_send:
                station_id = reading["station_id"]

                try:
                    metadata = producer.send(
                        KAFKA_TOPIC,
                        key=station_id,
                        value=reading,
                    ).get(timeout=15)
                    count += 1
                    logger.info(
                        "Published #%d station=%s temp=%.2fC "
                        "humidity=%.2f%% wind=%.2fkm/h partition=%d offset=%d",
                        count,
                        station_id,
                        reading["temperature_c"],
                        reading["humidity_pct"],
                        reading["wind_speed_kmh"],
                        metadata.partition,
                        metadata.offset,
                    )
                except (KafkaError, TimeoutError):
                    logger.exception(
                        "Failed to publish reading for station %s",
                        station_id,
                    )

            time.sleep(OPEN_METEO_INTERVAL_SECONDS if WEATHER_SOURCE == "open_meteo" else SENSOR_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping producer after %d published readings.", count)
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    run_producer()
