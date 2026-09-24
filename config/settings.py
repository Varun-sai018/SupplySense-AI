import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def _get_env(key: str, required: bool = True) -> str:
    """Retrieve an environment variable, raising an error if it's required and missing."""
    value = os.getenv(key)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {key}")
    return value

# ==============================================================================
# Database Configuration
# ==============================================================================
DB_HOST = _get_env("DB_HOST")
try:
    DB_PORT = int(_get_env("DB_PORT"))
except ValueError:
    raise ValueError("DB_PORT environment variable must be a valid integer.")
DB_USER = _get_env("DB_USER")
DB_PASSWORD = _get_env("DB_PASSWORD")
DB_NAME = _get_env("DB_NAME")

# ==============================================================================
# Kafka Configuration
# ==============================================================================
KAFKA_BROKER = _get_env("KAFKA_BROKER")
KAFKA_TOPIC_EVENTS = _get_env("KAFKA_TOPIC_EVENTS")
KAFKA_CONSUMER_GROUP = _get_env("KAFKA_CONSUMER_GROUP")

# ==============================================================================
# Data Configuration
# ==============================================================================
OLIST_DATA_DIR = _get_env("OLIST_DATA_DIR")

# ==============================================================================
# Square Sandbox Configuration
# ==============================================================================
SQUARE_ENVIRONMENT = _get_env("SQUARE_ENVIRONMENT", required=False)
SQUARE_BASE_URL = _get_env("SQUARE_BASE_URL", required=False)
SQUARE_APPLICATION_ID = _get_env("SQUARE_APPLICATION_ID", required=False)
SQUARE_ACCESS_TOKEN = _get_env("SQUARE_ACCESS_TOKEN", required=False)
SQUARE_LOCATION_ID = _get_env("SQUARE_LOCATION_ID", required=False)

