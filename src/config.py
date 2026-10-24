"""Central configuration for the weather-streaming project."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Project root is the parent directory of src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load environment variables from the project-root .env file.
load_dotenv(PROJECT_ROOT / ".env")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-events")
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP", "weather-processor-group"
)

# MySQL configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3308"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "weather_streaming")
MYSQL_USER = os.getenv("MYSQL_USER", "weather_app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

# Weather simulation configuration
STATIONS = {
    "BLR": "Bengaluru",
    "MUM": "Mumbai",
    "DEL": "Delhi",
    "CHE": "Chennai",
}

# Number of recent readings maintained for each station.
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))

# Sensor simulator interval in seconds.
SENSOR_INTERVAL_SECONDS = float(
    os.getenv("SENSOR_INTERVAL_SECONDS", "2")
)

# Model path
MODEL_PATH = PROJECT_ROOT / "models" / "isolation_forest.joblib"
