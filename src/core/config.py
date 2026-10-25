import os
from math import isfinite
from pathlib import Path


def positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if not isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


REDIS_URL = os.environ["REDIS_URL"]
DATABASE_URL = os.environ["DB_URL"]
TARGET_ROOT = Path(os.getenv("TARGET_ROOT", "/targets"))
DISPATCH_TIMEOUT = positive_float("DISPATCH_TIMEOUT", 10)
DISPATCH_RECHECK = positive_float("DISPATCH_RECHECK", 600)
HEARTBEAT_INTERVAL = positive_float("HEARTBEAT_INTERVAL", 10)
HEARTBEAT_TIMEOUT = positive_float("HEARTBEAT_TIMEOUT", 600)
MAX_UPLOADS = int(os.getenv("MAX_UPLOADS", "4"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(100 * 1024**3)))
if MAX_UPLOADS < 1 or MAX_UPLOAD_BYTES < 1:
    raise ValueError("Upload limits must be positive integers")
if HEARTBEAT_TIMEOUT < 3 * HEARTBEAT_INTERVAL:
    raise ValueError("HEARTBEAT_TIMEOUT must be at least 3 * HEARTBEAT_INTERVAL")
