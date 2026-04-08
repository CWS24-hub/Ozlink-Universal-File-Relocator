"""
REGRESSION LOCK — stable log correlation strings for Graph async copy + monitor 401 recovery.

Operators grep console logs for these event names; renames break runbooks without code failure.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_OZ = _REPO_ROOT / "ozlink_console"


def test_graph_async_copy_and_monitor_401_log_markers_present():
    graph_py = (_OZ / "graph.py").read_text(encoding="utf-8")
    assert "graph_async_copy_submitted" in graph_py
    assert "graph_async_monitor_poll_401" in graph_py


def test_transfer_job_graph_copy_recovered_log_marker_present():
    tjr = (_OZ / "transfer_job_runner.py").read_text(encoding="utf-8")
    assert "transfer_job_graph_copy_recovered_after_monitor_401" in tjr


def test_draft_snapshot_harness_summary_pipeline_marker_present():
    """Final harness phase emits draft_snapshot.harness.summary (via log_harness_phase subphase=summary)."""
    run_log = (_OZ / "draft_snapshot" / "run_log.py").read_text(encoding="utf-8")
    assert "draft_snapshot.harness." in run_log
    assert "harness_subphase" in run_log
    harness = (_OZ / "draft_snapshot" / "pipeline_harness.py").read_text(encoding="utf-8")
    assert 'subphase="summary"' in harness
