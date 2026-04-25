"""Append-only JSONL audit events for transfer jobs (local MVP; future: ship to SaaS API)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ozlink_console.logger import log_error


def append_audit_event(
    path: str | Path | None,
    *,
    job_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    if not path:
        return
    p = Path(path)
    line_obj: dict[str, Any] = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "job_id": str(job_id or "").strip(),
        "event": str(event_type or "").strip(),
    }
    if payload:
        line_obj["data"] = payload
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line_obj, ensure_ascii=False) + "\n")
    except OSError as exc:
        log_error("audit_append_failed", path=str(p), error=str(exc))
