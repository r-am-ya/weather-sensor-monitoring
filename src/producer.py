"""Publish simulated weather readings to Kafka."""

import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SENSOR_INTERVAL_SECONDS,
    STATIONS,
)
from src.sensor_simulator import generate_reading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


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


def run_producer() -> None:
    """Publish readings until interrupted with Ctrl+C."""
    producer = create_producer()
    station_ids = list(STATIONS)

    # One scenario can be injected using the SCENARIO environment variable.
    import os

    scenario = os.getenv("SCENARIO", "").strip().lower() or None
    if scenario not in {None, "heatwave", "storm_gust", "humidity_fault"}:
        producer.close()
        raise ValueError(
            "SCENARIO must be heatwave, storm_gust, humidity_fault, or unset."
        )

    count = 0
    logger.info(
        "Publishing simulated readings to topic '%s' at %s",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    try:
        while True:
            for station_id in station_ids:
                reading = generate_reading(
                    station_id=station_id,
                    scenario=scenario,
                )

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

            time.sleep(SENSOR_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping producer after %d published readings.", count)
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    run_producer()
