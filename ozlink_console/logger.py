from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from .paths import logs_root

_LOGGER: logging.Logger | None = None


def _make_json_safe(value, seen: set[int] | None = None):
    if seen is None:
        seen = set()

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    obj_id = id(value)
    if obj_id in seen:
        return "<recursive>"

    if isinstance(value, dict):
        seen.add(obj_id)
        try:
            return {str(key): _make_json_safe(item, seen) for key, item in value.items()}
        finally:
            seen.discard(obj_id)

    if isinstance(value, (list, tuple, set)):
        seen.add(obj_id)
        try:
            return [_make_json_safe(item, seen) for item in value]
        finally:
            seen.discard(obj_id)

    if isinstance(value, Path):
        return str(value)

    return repr(value)

class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        extra_data = getattr(record, "data", None)
        if extra_data is not None:
            payload["data"] = _make_json_safe(extra_data)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=repr)

def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    logger = logging.getLogger("ozlink_console")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_path = logs_root() / f"OzlinkConsole_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(JsonLineFormatter())
    logger.addHandler(handler)

    _LOGGER = logger
    logger.info("Diagnostics initialized.", extra={"data": {"log_path": str(log_path)}})
    return logger

def log_info(message: str, **data) -> None:
    get_logger().info(message, extra={"data": data or None})

def log_warn(message: str, **data) -> None:
    get_logger().warning(message, extra={"data": data or None})

def log_error(message: str, **data) -> None:
    get_logger().error(message, extra={"data": data or None})
