"""Structured internal validation run records for the snapshot path (no execution semantics)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from ozlink_console.logger import log_warn

from ozlink_console.execution.snapshot_summary import SnapshotRunResultSummary

INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION = "1"

TriStateYesNo = Literal["yes", "no"]

_ENV_CAPTURE_PATH = "OZLINK_VALIDATION_CAPTURE_JSONL"
_ENV_SCENARIO = "OZLINK_VALIDATION_SCENARIO_NAME"
_ENV_NOTES = "OZLINK_VALIDATION_OPERATOR_NOTES"
_ENV_MATCHED = "OZLINK_VALIDATION_MATCHED_INTENT"
_ENV_DIFFERED = "OZLINK_VALIDATION_DIFFERED_FROM_LEGACY"

_MAX_NOTES_LEN = 4000


def _utc_capture_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_yes_no(value: str | None) -> TriStateYesNo | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if not v:
        return None
    if v in ("yes", "y", "true", "1", "on"):
        return "yes"
    if v in ("no", "n", "false", "0", "off"):
        return "no"
    return None


def _operator_fields_from_environ() -> dict[str, Any]:
    notes = str(os.environ.get(_ENV_NOTES, "") or "")[:_MAX_NOTES_LEN]
    return {
        "scenario_name": str(os.environ.get(_ENV_SCENARIO, "") or "").strip(),
        "operator_notes": notes,
        "matched_intent": _parse_yes_no(os.environ.get(_ENV_MATCHED)),
        "differed_from_legacy": _parse_yes_no(os.environ.get(_ENV_DIFFERED)),
    }


@dataclass(frozen=True)
class InternalValidationRunRecord:
    """Stable structured record for one manual/internal validation run (snapshot path)."""

    scenario_name: str
    execution_path: str
    run_id: str
    snapshot_id: str | None
    plan_id: str | None
    final_status: str
    failure_boundary: str | None
    boundary_detail: str | None
    stop_reason: str | None
    operator_notes: str
    matched_intent: TriStateYesNo | None
    differed_from_legacy: TriStateYesNo | None
    schema_version: str = INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION
    captured_at_utc: str = field(default_factory=_utc_capture_timestamp)
    stopped_at: str | None = None
    exception_type: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        """JSON-serializable dict with a fixed key set for tooling."""
        return {
            "schema_version": self.schema_version,
            "captured_at_utc": self.captured_at_utc,
            "scenario_name": self.scenario_name,
            "execution_path": self.execution_path,
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "plan_id": self.plan_id,
            "final_status": self.final_status,
            "failure_boundary": self.failure_boundary,
            "boundary_detail": self.boundary_detail,
            "stopped_at": self.stopped_at,
            "stop_reason": self.stop_reason,
            "exception_type": self.exception_type,
            "operator_notes": self.operator_notes,
            "matched_intent": self.matched_intent,
            "differed_from_legacy": self.differed_from_legacy,
        }


def build_internal_validation_run_record_from_snapshot_summary(
    summary: SnapshotRunResultSummary,
    *,
    scenario_name: str = "",
    execution_path: str = "snapshot_pipeline",
    operator_notes: str = "",
    matched_intent: TriStateYesNo | None = None,
    differed_from_legacy: TriStateYesNo | None = None,
) -> InternalValidationRunRecord:
    snap = summary.snapshot_id.strip() or None
    fb = summary.failure_boundary.strip() or None
    bd = summary.boundary_detail.strip() or None
    st = summary.stopped_at.strip() or None
    return InternalValidationRunRecord(
        scenario_name=str(scenario_name or "").strip(),
        execution_path=str(execution_path or "snapshot_pipeline").strip(),
        run_id=summary.run_id,
        snapshot_id=snap,
        plan_id=summary.plan_id,
        final_status=summary.final_status,
        failure_boundary=fb,
        boundary_detail=bd,
        stop_reason=summary.stop_reason,
        stopped_at=st,
        exception_type=summary.exception_type,
        operator_notes=str(operator_notes or "")[:_MAX_NOTES_LEN],
        matched_intent=matched_intent,
        differed_from_legacy=differed_from_legacy,
    )


def append_internal_validation_jsonl(path: Path | str, record: InternalValidationRunRecord | dict[str, Any]) -> None:
    """Append one JSON object per line. Raises OSError only to caller; orchestrator uses try/warn."""
    p = Path(path)
    line = json.dumps(
        record.to_json_dict() if isinstance(record, InternalValidationRunRecord) else dict(record),
        ensure_ascii=False,
    )
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def maybe_append_snapshot_validation_capture(summary: SnapshotRunResultSummary) -> None:
    """
    If ``OZLINK_VALIDATION_CAPTURE_JSONL`` is set, append a validation record for this snapshot run.

    Operator fields default from env (see module constants). Never affects pipeline outcome.
    """
    raw_path = str(os.environ.get(_ENV_CAPTURE_PATH, "") or "").strip()
    if not raw_path:
        return
    env_ops = _operator_fields_from_environ()
    record = build_internal_validation_run_record_from_snapshot_summary(
        summary,
        scenario_name=env_ops["scenario_name"],
        operator_notes=env_ops["operator_notes"],
        matched_intent=env_ops["matched_intent"],
        differed_from_legacy=env_ops["differed_from_legacy"],
    )
    try:
        append_internal_validation_jsonl(raw_path, record)
    except OSError as exc:
        log_warn(
            "internal_validation_capture_write_failed",
            path=raw_path,
            error=str(exc)[:500],
            run_id=summary.run_id,
        )
