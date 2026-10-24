from datetime import datetime

import joblib
import pytest

from src.config import MODEL_PATH
from src.stream_processor import MODEL_FEATURES, predict_anomaly, validate_reading
from src.producer import build_open_meteo_reading
from src.weather_visuals import build_animated_wind_figure, parse_current_response


def test_open_meteo_parser_normalizes_bengaluru_offset():
    parsed = parse_current_response({
        "timezone": "Asia/Kolkata",
        "current": {
            "time": "2026-10-24T12:00",
            "temperature_2m": 28.5,
            "relative_humidity_2m": 64,
            "wind_speed_10m": 9.2,
            "wind_direction_10m": 240,
        },
    })
    assert parsed["temperature_c"] == 28.5
    assert parsed["wind_direction_deg"] == 240
    assert datetime.fromisoformat(parsed["observation_timestamp"]).utcoffset().total_seconds() == 19800


def test_open_meteo_parser_rejects_missing_fields():
    with pytest.raises(ValueError, match="missing or invalid"):
        parse_current_response({"current": {"time": "2026-10-24T12:00"}})


def test_simulated_and_api_readings_share_processor_schema():
    from src.sensor_simulator import generate_reading

    sample = generate_reading("BLR")
    sample.update(source_type="simulated", observation_timestamp=sample["event_timestamp"])
    validate_reading(sample)
    assert 0 <= sample["wind_direction_deg"] < 360
    api_current = {
        "temperature_c": 28.5, "humidity_pct": 64,
        "wind_speed_kmh": 9.2, "wind_direction_deg": 240,
        "observation_timestamp": "2026-10-24T12:00:00+05:30",
    }
    api_sample = build_open_meteo_reading(api_current)
    validate_reading(api_sample)
    assert api_sample["source_type"] == "open_meteo"
    assert build_open_meteo_reading(api_current)["event_id"] == api_sample["event_id"]
    assert tuple(MODEL_FEATURES) == ("temperature_c", "humidity_pct", "wind_speed_kmh")


def test_saved_model_loads_and_scores_expected_feature_order():
    saved = joblib.load(MODEL_PATH)
    model = saved.get("model", saved) if isinstance(saved, dict) else saved
    assert model.n_features_in_ == 3
    normal = {"temperature_c": 27.0, "humidity_pct": 60.0, "wind_speed_kmh": 10.0}
    extreme = {"temperature_c": 45.0, "humidity_pct": 60.0, "wind_speed_kmh": 95.0}
    assert isinstance(predict_anomaly(normal)["is_anomaly"], bool)
    assert predict_anomaly(extreme)["is_anomaly"] is True


def test_non_finite_model_input_is_rejected():
    from src.sensor_simulator import generate_reading

    sample = generate_reading("BLR")
    sample["temperature_c"] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        validate_reading(sample)


def test_animated_wind_map_uses_sample_data_and_contains_playable_frames():
    reading = {
        "wind_speed_kmh": 18.5, "wind_direction_deg": 245,
        "temperature_c": 28.0, "humidity_pct": 63.0,
        "event_timestamp": "2026-10-24T12:00:00+00:00",
    }
    figure = build_animated_wind_figure(reading, "Simulated Bengaluru sensor")
    assert len(figure.frames) == 18
    assert any(trace.name == "Animated particles" for trace in figure.data)
    particles = next(trace for trace in figure.data if trace.name == "Animated particles")
    assert set(particles.marker.color) == {18.5}
    assert figure.layout.updatemenus[0].buttons[0].label == "▶ Play flow"
