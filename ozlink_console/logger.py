from __future__ import annotations

"""JSON line logs under ``logs_root()/<session>/``.

SharePoint Graph sync uses structured ``message`` keys such as ``graph_resolve_*`` and
``graph_refresh_*`` (fields: ``phase``, ``reason``, ``move_index``, ``candidates_tried``, etc.).
Set environment variable ``OZLINK_FULL_TRACE=1`` for per-candidate path-miss traces during
resolution (can be large).
"""

import json
import logging
import os
import re
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional

from ozlink_console.paths import logs_root


def _make_json_safe(value: Any, seen: set[int] | None = None) -> Any:
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

# ~8 MB per file, 4 backups (~40 MB max per stream) for rotating JSON logs.
_ROTATE_BYTES = 8 * 1024 * 1024
_ROTATE_BACKUPS = 4

_SESSION_DIR: Optional[Path] = None
_SESSION_LOCK = threading.Lock()

_STREAM_APP = "app"
_STREAM_DEST_PREVIEW = "destination_preview"
_STREAM_DEST_FINALIZE = "destination_finalize"
_STREAM_DEST_MERGE_TICKS = "destination_merge_ticks"
_STREAM_DEST_RECONCILE = "destination_reconcile"
_STREAM_GRAPH = "graph_resolution"
_STREAM_SOURCE = "source_tree"
_STREAM_WORKERS = "workers"
_STREAM_CRASH = "crash"

# Substrings of log message (lowercase match) → destination_merge_ticks.log (before generic incremental merge → preview).
_MERGE_TICK_LOG_MARKERS = (
    "destination_incremental_merge_tick",
    "destination_incremental_merge_progress",
    "destination_incremental_merge_parent_sort_cost",
    "destination_incremental_merge_root_attach_resume",
)

# Merge message key prefix, then any chars, then "finalize" (avoids unrelated "finalize" elsewhere in the line).
_DESTINATION_INCREMENTAL_MERGE_FINALIZE_PHASE_RE = re.compile(r"destination_incremental_merge_.*finalize")

# crash.log is shared with faulthandler/Qt (binary append, one fd) — no in-process rotation.
_STREAM_FILES: Dict[str, str] = {
    _STREAM_APP: "app.log",
    _STREAM_DEST_PREVIEW: "destination_preview.log",
    _STREAM_DEST_FINALIZE: "destination_finalize.log",
    _STREAM_DEST_MERGE_TICKS: "destination_merge_ticks.log",
    _STREAM_DEST_RECONCILE: "destination_reconcile.log",
    _STREAM_GRAPH: "graph_resolution.log",
    _STREAM_SOURCE: "source_tree.log",
    _STREAM_WORKERS: "workers.log",
}

_ALL_STREAM_NAMES = frozenset(_STREAM_FILES) | {_STREAM_CRASH}


class CrashLogSink:
    """Single crash.log (UTF-8) for native dumps and JSON lines; one lock for all writers."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._fp: Optional[BinaryIO] = None

    def _ensure_open(self) -> BinaryIO:
        if self._fp is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fp = open(self.path, "ab", buffering=0)
        return self._fp

    def binary_file(self) -> BinaryIO:
        """File object for faulthandler.enable(file=...). Do not close from outside."""
        with self._lock:
            return self._ensure_open()

    def write_native_text(self, text: str) -> None:
        data = text.encode("utf-8", errors="replace")
        with self._lock:
            fp = self._ensure_open()
            fp.write(data)
            fp.flush()

    def write_json_line(self, line: str) -> None:
        data = line.encode("utf-8", errors="replace") + b"\n"
        with self._lock:
            fp = self._ensure_open()
            fp.write(data)
            fp.flush()

    def flush(self) -> None:
        with self._lock:
            if self._fp is not None:
                self._fp.flush()

    def close(self) -> None:
        with self._lock:
            if self._fp is not None:
                self._fp.close()
                self._fp = None


_CRASH_SINK: Optional[CrashLogSink] = None


def _get_crash_sink(session_dir: Path) -> CrashLogSink:
    global _CRASH_SINK
    if _CRASH_SINK is None:
        _CRASH_SINK = CrashLogSink(session_dir / "crash.log")
    return _CRASH_SINK


def get_crash_log_path() -> Path:
    """Path to this session's crash.log (native + JSON crash stream)."""
    return get_session_logs_dir() / "crash.log"


def get_crash_binary_log_file() -> BinaryIO:
    """Binary append file for faulthandler; same file as JSON crash routing."""
    return _get_crash_sink(get_session_logs_dir()).binary_file()


def init_session_logging() -> Path:
    """Create per-run log folder (idempotent). Call early from app startup."""
    global _SESSION_DIR, _CRASH_SINK
    with _SESSION_LOCK:
        if _SESSION_DIR is not None:
            return _SESSION_DIR
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        _SESSION_DIR = logs_root() / stamp
        _SESSION_DIR.mkdir(parents=True, exist_ok=True)
        _CRASH_SINK = CrashLogSink(_SESSION_DIR / "crash.log")
        return _SESSION_DIR


def get_session_logs_dir() -> Path:
    """Return the active session log directory, creating it if needed."""
    return init_session_logging()


def _record_data(record: logging.LogRecord) -> dict:
    raw = getattr(record, "data", None)
    if isinstance(raw, dict):
        return raw
    return {}


def resolve_log_stream(record: logging.LogRecord) -> str:
    """Pick target log file from explicit hint, message text, and structured data."""
    data = _record_data(record)
    hint = data.get("_ozlink_stream")
    if hint and str(hint) in _ALL_STREAM_NAMES:
        return str(hint)

    tc = data.get("trace_category")
    if isinstance(tc, str):
        tcl = tc.lower()
        if tcl == "worker":
            return _STREAM_WORKERS
        if tcl == "graph":
            return _STREAM_GRAPH
        if tcl in ("app", "memory"):
            return _STREAM_APP

    msg = record.getMessage()
    ml = msg.lower()
    ds = json.dumps(data, default=str).lower() if data else ""

    if "qt message captured" in ml or "unhandled exception" in ml or "fatal python error" in ml:
        return _STREAM_CRASH
    if "exception" in ml and ("traceback" in ds or "exc_type" in ds):
        return _STREAM_CRASH

    if ml.startswith("graph_") or "graph_linkage" in ml or "graph_refresh" in ml:
        return _STREAM_GRAPH
    if "graph resolve" in ml or "graph_resolve" in ml or "graph projection" in ml:
        return _STREAM_GRAPH

    if "reconcile" in ml and (
        "destination" in ml
        or "semantic" in ml
        or "sibling_folder" in ml
        or "projection" in ml
        or "memory restore reconcile" in ml
    ):
        return _STREAM_DEST_RECONCILE
    if "destination_projection_reconcile" in ml or "destination_semantic" in ml:
        return _STREAM_DEST_RECONCILE

    # Incremental merge finalize phase (after reconcile, before merge-tick + generic preview routing).
    if "destination_incremental_merge_session_complete" in ml:
        return _STREAM_DEST_FINALIZE
    if "destination_incremental_merge_finalize_tick" in ml:
        return _STREAM_DEST_FINALIZE
    if "destination_incremental_merge_finalize_complete" in ml:
        return _STREAM_DEST_FINALIZE
    if _DESTINATION_INCREMENTAL_MERGE_FINALIZE_PHASE_RE.search(ml):
        return _STREAM_DEST_FINALIZE

    if any(marker in ml for marker in _MERGE_TICK_LOG_MARKERS):
        return _STREAM_DEST_MERGE_TICKS

    if "destination_incremental_merge" in ml or "destination_future" in ml:
        return _STREAM_DEST_PREVIEW
    if "destination_bind" in ml or "destination_materialize" in ml:
        return _STREAM_DEST_PREVIEW
    if "destination_visible_future" in ml or "apply_visible_destination" in ml:
        return _STREAM_DEST_PREVIEW
    if "destination_double_render" in ml or "destination_shallow" in ml:
        return _STREAM_DEST_PREVIEW
    if "destination_planning" in ml or "planning_tree" in ml:
        return _STREAM_DEST_PREVIEW
    if "destination_projection" in ml and "reconcile" not in ml:
        return _STREAM_DEST_PREVIEW

    if "source_replace_children" in ml or "find_visible_source_item" in ml:
        return _STREAM_SOURCE
    if "library_restore" in ml and ("source" in ds or '"selector_group": "source"' in ds):
        return _STREAM_SOURCE
    if "perf_explorer" in ml and "source" in ds and "destination" not in ml:
        return _STREAM_SOURCE

    if "worker lifecycle" in ml or "root_load" in ml or "folder-" in ml:
        return _STREAM_WORKERS
    if "root_worker" in ml or "discovery_worker" in ml or "full_count_deferred" in ml:
        return _STREAM_WORKERS

    return _STREAM_APP


class JsonLineFormatter(logging.Formatter):
    """Emit one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(timespec="seconds"),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        extra_data = getattr(record, "data", None)
        if extra_data is not None:
            safe = _make_json_safe(extra_data)
            if isinstance(safe, dict):
                payload = {k: v for k, v in safe.items() if not str(k).startswith("_ozlink")}
            else:
                payload = safe
            log_entry["data"] = payload

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False, default=repr)


class RoutingStreamHandler(logging.Handler):
    """Dispatch records to per-subsystem rotating files or crash.log (JSON lines)."""

    def __init__(self, session_dir: Path) -> None:
        super().__init__()
        self.session_dir = session_dir
        self._targets: Dict[str, RotatingFileHandler] = {}
        self._formatter = JsonLineFormatter()
        self._crash_sink = _get_crash_sink(session_dir)
        for stream, filename in _STREAM_FILES.items():
            path = session_dir / filename
            h = RotatingFileHandler(
                str(path),
                maxBytes=_ROTATE_BYTES,
                backupCount=_ROTATE_BACKUPS,
                encoding="utf-8",
            )
            h.setLevel(logging.DEBUG)
            h.setFormatter(self._formatter)
            self._targets[stream] = h

    def emit(self, record: logging.LogRecord) -> None:
        stream = resolve_log_stream(record)
        if stream == _STREAM_CRASH:
            try:
                line = self._formatter.format(record)
                self._crash_sink.write_json_line(line)
            except Exception:
                self.handleError(record)
            return
        target = self._targets.get(stream) or self._targets[_STREAM_APP]
        target.emit(record)

    def flush(self) -> None:
        self._crash_sink.flush()
        for h in self._targets.values():
            h.flush()

    def close(self) -> None:
        for h in self._targets.values():
            h.close()
        super().close()


_LOGGER: Optional[logging.Logger] = None
_LOGGER_LOCK = threading.Lock()


def get_logger() -> logging.Logger:
    global _LOGGER
    with _LOGGER_LOCK:
        if _LOGGER is not None:
            return _LOGGER
        session_dir = get_session_logs_dir()
        logger = logging.getLogger("ozlink_console")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        router = RoutingStreamHandler(session_dir)
        router.setLevel(logging.DEBUG)
        logger.addHandler(router)

        _LOGGER = logger
        return _LOGGER


def log_info(message: str, **data: Any) -> None:
    stream = data.pop("_ozlink_stream", None)
    if stream:
        data = dict(data)
        data["_ozlink_stream"] = stream
    get_logger().info(message, extra={"data": data or None})


def log_warn(message: str, **data: Any) -> None:
    stream = data.pop("_ozlink_stream", None)
    if stream:
        data = dict(data)
        data["_ozlink_stream"] = stream
    get_logger().warning(message, extra={"data": data or None})


def log_error(message: str, **data: Any) -> None:
    stream = data.pop("_ozlink_stream", None)
    if stream:
        data = dict(data)
        data["_ozlink_stream"] = stream
    get_logger().error(message, extra={"data": data or None})


def trace_enabled() -> bool:
    """True when full trace is enabled (OZLINK_FULL_TRACE is 1/true/yes). App defaults this on at startup."""
    return os.environ.get("OZLINK_FULL_TRACE", "").strip().lower() in ("1", "true", "yes")


def log_trace(category: str, action: str, **data: Any) -> None:
    """Structured UI/worker trace when trace_enabled(); disable with OZLINK_FULL_TRACE=0 if logs are too large."""
    if not trace_enabled():
        return
    stream = data.pop("_ozlink_stream", None)
    payload: Dict[str, Any] = {"trace_category": category, "trace_action": action}
    if data:
        payload.update(data)
    if stream:
        payload["_ozlink_stream"] = stream
    get_logger().info(f"UI trace [{category}] {action}.", extra={"data": payload})


def flush_logger() -> None:
    """Flush all log handlers."""
    global _LOGGER
    with _LOGGER_LOCK:
        if _LOGGER is None:
            return
        for handler in _LOGGER.handlers:
            try:
                handler.flush()
            except Exception:
                pass


def reset_logging_for_tests() -> None:
    """Tear down module logging state (unit tests only)."""
    global _LOGGER, _SESSION_DIR, _CRASH_SINK
    if _LOGGER is not None:
        for handler in list(_LOGGER.handlers):
            try:
                handler.close()
            except Exception:
                pass
            _LOGGER.removeHandler(handler)
        _LOGGER = None
    if _CRASH_SINK is not None:
        try:
            _CRASH_SINK.close()
        except Exception:
            pass
        _CRASH_SINK = None
    _SESSION_DIR = None


def log_session_diagnostics_initialized() -> None:
    """Log session paths once handlers exist (call after get_logger first use)."""
    sd = get_session_logs_dir()
    files = sorted([*_STREAM_FILES.values(), "crash.log"])
    log_info(
        "Diagnostics initialized.",
        session_log_dir=str(sd),
        log_files=files,
        rotating_max_mb=round(_ROTATE_BYTES / (1024 * 1024), 1),
        rotating_backups=_ROTATE_BACKUPS,
        crash_log_note="crash.log shares one file with native/Qt capture; rotation applies to other JSON logs only",
    )


__all__ = [
    "flush_logger",
    "get_crash_binary_log_file",
    "get_crash_log_path",
    "get_logger",
    "get_session_logs_dir",
    "init_session_logging",
    "log_error",
    "log_info",
    "log_session_diagnostics_initialized",
    "log_trace",
    "log_warn",
    "reset_logging_for_tests",
    "resolve_log_stream",
    "trace_enabled",
]
