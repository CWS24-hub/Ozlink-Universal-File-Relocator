from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.execution.snapshot_summary import SnapshotRunResultSummary
from ozlink_console.execution.validation_capture import (
    INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION,
    append_internal_validation_jsonl,
    build_internal_validation_run_record_from_snapshot_summary,
    maybe_append_snapshot_validation_capture,
)


def _summary_ok() -> SnapshotRunResultSummary:
    return SnapshotRunResultSummary(
        run_id="r1",
        snapshot_id="s1",
        plan_id="p1",
        final_status="completed",
        failure_boundary="",
        stopped_at="",
        stop_reason=None,
        boundary_detail="",
    )


def test_build_record_shape_and_schema():
    rec = build_internal_validation_run_record_from_snapshot_summary(
        _summary_ok(),
        scenario_name="smoke-a2",
        operator_notes="note",
        matched_intent="yes",
        differed_from_legacy="no",
    )
    d = rec.to_json_dict()
    assert d["schema_version"] == INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION
    assert set(d.keys()) == {
        "schema_version",
        "captured_at_utc",
        "scenario_name",
        "execution_path",
        "run_id",
        "snapshot_id",
        "plan_id",
        "final_status",
        "failure_boundary",
        "boundary_detail",
        "stopped_at",
        "stop_reason",
        "exception_type",
        "operator_notes",
        "matched_intent",
        "differed_from_legacy",
    }
    assert d["scenario_name"] == "smoke-a2"
    assert d["execution_path"] == "snapshot_pipeline"
    assert d["matched_intent"] == "yes"
    assert d["differed_from_legacy"] == "no"
    assert d["failure_boundary"] is None
    assert d["exception_type"] is None


def test_append_jsonl_roundtrip(tmp_path: Path):
    rec = build_internal_validation_run_record_from_snapshot_summary(_summary_ok())
    p = tmp_path / "cap.jsonl"
    append_internal_validation_jsonl(p, rec)
    line = p.read_text(encoding="utf-8").strip()
    parsed = json.loads(line)
    assert parsed["run_id"] == "r1"
    assert parsed["schema_version"] == INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION


def test_maybe_append_respects_env(monkeypatch, tmp_path: Path):
    p = tmp_path / "out.jsonl"
    monkeypatch.setenv("OZLINK_VALIDATION_CAPTURE_JSONL", str(p))
    monkeypatch.setenv("OZLINK_VALIDATION_SCENARIO_NAME", "step-a2")
    monkeypatch.setenv("OZLINK_VALIDATION_MATCHED_INTENT", "yes")
    monkeypatch.setenv("OZLINK_VALIDATION_DIFFERED_FROM_LEGACY", "false")
    maybe_append_snapshot_validation_capture(_summary_ok())
    data = json.loads(p.read_text(encoding="utf-8").strip())
    assert data["scenario_name"] == "step-a2"
    assert data["matched_intent"] == "yes"
    assert data["differed_from_legacy"] == "no"


def test_maybe_append_skips_when_env_unset(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("OZLINK_VALIDATION_CAPTURE_JSONL", raising=False)
    p = tmp_path / "missing.jsonl"
    maybe_append_snapshot_validation_capture(_summary_ok())
    assert not p.exists()


def test_invalid_yes_no_env_becomes_null(monkeypatch, tmp_path: Path):
    p = tmp_path / "x.jsonl"
    monkeypatch.setenv("OZLINK_VALIDATION_CAPTURE_JSONL", str(p))
    monkeypatch.setenv("OZLINK_VALIDATION_MATCHED_INTENT", "maybe")
    maybe_append_snapshot_validation_capture(_summary_ok())
    data = json.loads(p.read_text(encoding="utf-8").strip())
    assert data["matched_intent"] is None
