"""UI helper: first failed bridge step diagnostic block."""

from __future__ import annotations

from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineRunResult
from ozlink_console.execution.boundary_vocabulary import BRIDGE_STEP_FAILED
from ozlink_console.execution.snapshot_summary import format_first_failed_bridge_step_diagnostic


def test_bridge_diagnostic_empty_when_not_bridge_failed():
    r = DraftPipelineRunResult(snapshot_id="s", run_id="r", plan_id="p", import_kind="x")
    r.failure_boundary = "resolution"
    r.boundary_detail = "unresolved_or_ambiguous"
    assert format_first_failed_bridge_step_diagnostic(r) == ""


def test_bridge_diagnostic_first_failed_in_plan_order():
    r = DraftPipelineRunResult(snapshot_id="s", run_id="r", plan_id="p", import_kind="x")
    r.failure_boundary = "bridge"
    r.boundary_detail = BRIDGE_STEP_FAILED
    r.plan_step_ids = ["a", "b", "c"]
    r.bridge_step_outcomes = {
        "a": {"outcome": "succeeded", "mapping_id": "m0", "step_type": "create_folder"},
        "b": {
            "outcome": "failed",
            "mapping_id": "m1",
            "step_type": "copy_item",
            "source_path": "src/x.txt",
            "destination_path": "dst/x.txt",
            "detail": "Graph said no",
            "backend_status": "failed",
        },
        "c": {"outcome": "failed", "mapping_id": "m2", "step_type": "copy_item"},
    }
    text = format_first_failed_bridge_step_diagnostic(r)
    assert "step_id: b" in text
    assert "mapping_id: m1" in text
    assert "copy_item" in text
    assert "src/x.txt" in text
    assert "dst/x.txt" in text
    assert "Graph said no" in text
    assert "backend_status: failed" in text
    assert "m2" not in text
