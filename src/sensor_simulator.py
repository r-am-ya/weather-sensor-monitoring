
"""Generate simulated weather readings for the streaming pipeline."""

import random
import uuid
from datetime import datetime, timezone
from typing import Any

from src.config import STATIONS


# Keep a persistent weather state for each station.
# This makes ordinary weather change gradually between readings.
_WEATHER_STATE: dict[str, dict[str, float]] = {}


def _get_weather_state(station_id: str) -> dict[str, float]:
    """Get or initialize the ordinary weather state for a station."""
    if station_id not in _WEATHER_STATE:
        _WEATHER_STATE[station_id] = {
            "temperature_c": random.uniform(25.0, 31.0),
            "humidity_pct": random.uniform(45.0, 75.0),
            "wind_speed_kmh": random.uniform(5.0, 15.0),
        }

    state = _WEATHER_STATE[station_id]

    # Small changes between consecutive readings.
    state["temperature_c"] += random.uniform(-0.35, 0.35)
    state["humidity_pct"] += random.uniform(-1.5, 1.5)
    state["wind_speed_kmh"] += random.uniform(-1.5, 1.5)

    # Keep ordinary weather within plausible simulation bounds.
    state["temperature_c"] = max(
        22.0, min(36.0, state["temperature_c"])
    )
    state["humidity_pct"] = max(
        30.0, min(90.0, state["humidity_pct"])
    )
    state["wind_speed_kmh"] = max(
        0.0, min(30.0, state["wind_speed_kmh"])
    )

    return state.copy()


def generate_reading(
    station_id: str | None = None,
    scenario: str | None = None,
) -> dict[str, Any]:
    """Generate one simulated weather reading.

    Supported scenarios:
        None: gradually changing ordinary weather
        heatwave: unusually high temperature
        storm_gust: unusually high wind speed
        humidity_fault: implausible humidity sensor value
    """
    if station_id is None:
        station_id = random.choice(list(STATIONS))

    if station_id not in STATIONS:
        raise ValueError(
            f"Unknown station_id {station_id!r}. "
            f"Valid IDs: {', '.join(STATIONS)}"
        )

    valid_scenarios = {
        None,
        "heatwave",
        "storm_gust",
        "humidity_fault",
    }

    if scenario not in valid_scenarios:
        raise ValueError(
            f"Unknown scenario {scenario!r}. "
            "Use heatwave, storm_gust, humidity_fault, or None."
        )

    state = _get_weather_state(station_id)

    temperature_c = state["temperature_c"]
    humidity_pct = state["humidity_pct"]
    wind_speed_kmh = state["wind_speed_kmh"]

    # Apply deliberate anomalies without changing the normal state.
    if scenario == "heatwave":
        temperature_c = random.uniform(40.0, 46.0)

    elif scenario == "storm_gust":
        wind_speed_kmh = random.uniform(75.0, 120.0)

    elif scenario == "humidity_fault":
        humidity_pct = random.choice(
            [
                random.uniform(-20.0, -1.0),
                random.uniform(101.0, 140.0),
            ]
        )

    return {
        "event_id": str(uuid.uuid4()),
        "station_id": station_id,
        "station_name": STATIONS[station_id],
        "event_timestamp": datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
        "temperature_c": round(temperature_c, 2),
        "humidity_pct": round(humidity_pct, 2),
        "wind_speed_kmh": round(wind_speed_kmh, 2),
        "scenario": scenario,
        "is_simulated": True,
    }


if __name__ == "__main__":
    print("Ordinary simulated readings for Bengaluru:")
    for _ in range(5):
        print(generate_reading(station_id="BLR"))

    print("\nSimulated heatwave:")
    print(generate_reading(station_id="BLR", scenario="heatwave"))

    print("\nSimulated storm gust:")
    print(generate_reading(station_id="MUM", scenario="storm_gust"))

    print("\nSimulated humidity sensor fault:")
    print(generate_reading(station_id="DEL", scenario="humidity_fault"))
