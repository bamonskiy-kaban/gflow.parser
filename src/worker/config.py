import os

from core.config import positive_float

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "")
TOPIC = os.getenv("TOPIC", "")
# Limits connect/terminal cleanup; delivery and reconnect drain wait out outages.
KAFKA_TIMEOUT = positive_float("KAFKA_TIMEOUT", 20)
KAFKA_RETRY_DELAY = positive_float("KAFKA_RETRY_DELAY", 5)
KAFKA_LOG_INTERVAL = positive_float("KAFKA_LOG_INTERVAL", 30)

MAX_BATCH_EVENTS = 256
MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_EVENT_BYTES = 1024 * 1024