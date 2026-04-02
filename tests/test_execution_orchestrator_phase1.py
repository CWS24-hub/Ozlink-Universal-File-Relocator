from __future__ import annotations

from datetime import datetime, timezone

from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineRunResult
from ozlink_console.execution import ExecutionOrchestrator, ExecutionStopReason, RunContext
from ozlink_console.execution.snapshot_summary import (
    build_snapshot_run_result_summary,
    snapshot_internal_comparison_record,
)


def test_run_manifest_emits_started_only(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    orch = ExecutionOrchestrator()
    ctx = orch.run_manifest(manifest_path="/x.json", dry_run=True, environment_context={"tenant_id": "t1"})

    assert ctx.execution_kind == "manifest"
    assert ctx.source_of_truth == "manifest"
    assert len(calls) == 1
    assert calls[0][0] == "execution_run_started"
    assert calls[0][1].get("execution_event") == "execution_run_started"
    assert calls[0][1].get("run_id") == ctx.run_id
    assert calls[0][1].get("execution_kind") == "manifest"
    assert calls[0][1].get("source_of_truth") == "manifest"


def test_run_snapshot_emits_started_then_completed(monkeypatch):
    calls: list[tuple[str, dict]] = []
    captured: list[dict] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    def _fake_pipeline(_path, **kwargs):
        captured.append(dict(kwargs))
        rid = kwargs.get("run_id")
        return DraftPipelineRunResult(
            snapshot_id="snap-1",
            run_id=str(rid or ""),
            plan_id="plan-1",
            import_kind="bundle",
            success=True,
            phases_completed=["summary"],
        )

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    monkeypatch.setattr(
        "ozlink_console.execution.orchestrator.run_pipeline_from_bundle_folder",
        _fake_pipeline,
    )
    orch = ExecutionOrchestrator()
    ctx, result = orch.run_snapshot(
        manifest_path="/y.json",
        bundle_folder="/bundle",
        dry_run=True,
        environment_context={"tenant_id": "t1", "mode_source": "env"},
        graph_client=object(),
    )

    assert ctx.execution_kind == "snapshot_pipeline"
    assert ctx.source_of_truth == "execution_plan"
    assert result.success is True
    assert len(calls) == 3
    assert calls[0][0] == "execution_run_started"
    assert calls[1][0] == "execution_run_completed"
    assert calls[1][1].get("execution_kind") == "snapshot_pipeline"
    assert calls[1][1].get("source_of_truth") == "execution_plan"
    assert calls[1][1].get("final_status") == "completed"
    assert calls[2][0] == "snapshot_run_internal_summary"
    assert calls[2][1].get("final_status") == "completed"
    assert calls[2][1].get("internal_comparison", {}).get("execution_path") == "snapshot_pipeline"
    assert captured and captured[0].get("block_on_resolution_gaps") is True
    assert captured[0].get("run_id") == ctx.run_id
    orch_ctx = captured[0].get("orchestration_context") or {}
    assert orch_ctx.get("execution_kind") == "snapshot_pipeline"
    assert orch_ctx.get("source_of_truth") == "execution_plan"


def test_run_snapshot_stopped_logs_stopped(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    def _fake_pipeline(_path, **kwargs):
        return DraftPipelineRunResult(
            snapshot_id="snap-x",
            run_id=str(kwargs.get("run_id") or ""),
            plan_id=None,
            import_kind="bundle",
            success=False,
            stopped_at="environment_validation",
            errors=["environment_validation_failed"],
        )

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    monkeypatch.setattr(
        "ozlink_console.execution.orchestrator.run_pipeline_from_bundle_folder",
        _fake_pipeline,
    )
    orch = ExecutionOrchestrator()
    ctx, result = orch.run_snapshot(
        manifest_path="/y.json",
        bundle_folder="/b",
        dry_run=False,
        environment_context={},
        graph_client=None,
    )
    assert result.stopped_at == "environment_validation"
    assert [c[0] for c in calls] == [
        "execution_run_started",
        "execution_run_stopped",
        "snapshot_run_internal_summary",
    ]
    assert calls[1][1].get("stop_reason") == ExecutionStopReason.ENVIRONMENT_VALIDATION_FAILED.value
    assert calls[1][1].get("failure_boundary") == "environment"
    assert calls[1][1].get("final_status") == "stopped"
    assert calls[1][1].get("run_id") == ctx.run_id
    assert calls[2][1].get("failure_boundary") == "environment"
    assert calls[2][1].get("boundary_detail") == "environment_validation"


def test_run_snapshot_pipeline_exception_logs_runner_summary(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    def _boom(_path, **_kwargs):
        raise RuntimeError("harness boom")

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    monkeypatch.setattr(
        "ozlink_console.execution.orchestrator.run_pipeline_from_bundle_folder",
        _boom,
    )
    orch = ExecutionOrchestrator()
    try:
        orch.run_snapshot(
            manifest_path="/y.json",
            bundle_folder="/b",
            dry_run=True,
            environment_context={},
            graph_client=object(),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected re-raise")
    assert [c[0] for c in calls] == ["execution_run_started", "execution_run_failed", "snapshot_run_internal_summary"]
    assert calls[1][1].get("failure_boundary") == "runner"
    assert calls[1][1].get("boundary_detail") == "runner_uncaught_exception"
    assert calls[1][1].get("exception_type") == "RuntimeError"
    assert calls[2][1].get("final_status") == "runner_failed"
    assert calls[2][1].get("internal_comparison", {}).get("failure_boundary") == "runner"
    assert calls[2][1].get("exception_type") == "RuntimeError"


def test_run_snapshot_bridge_failed_logs_boundary(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    def _fake_pipeline(_path, **kwargs):
        rid = str(kwargs.get("run_id") or "")
        return DraftPipelineRunResult(
            snapshot_id="snap-b",
            run_id=rid,
            plan_id="plan-b",
            import_kind="bundle",
            success=False,
            phases_completed=["execution_bridge", "summary"],
            failure_boundary="bridge",
            boundary_detail="bridge_step_failed",
        )

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    monkeypatch.setattr(
        "ozlink_console.execution.orchestrator.run_pipeline_from_bundle_folder",
        _fake_pipeline,
    )
    orch = ExecutionOrchestrator()
    _ctx, _result = orch.run_snapshot(
        manifest_path="/y.json",
        bundle_folder="/b",
        dry_run=True,
        environment_context={},
        graph_client=object(),
    )
    assert [c[0] for c in calls][1] == "execution_run_failed"
    assert calls[1][1].get("failure_boundary") == "bridge"
    assert calls[1][1].get("final_status") == "failed"
    assert calls[2][1].get("stop_reason") == ExecutionStopReason.BRIDGE_STEP_FAILED.value
    assert calls[1][1].get("stop_reason") == ExecutionStopReason.BRIDGE_STEP_FAILED.value


def test_run_snapshot_bridge_compatibility_maps_stop_reason(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def _capture(msg, **data):
        calls.append((msg, dict(data)))

    def _fake_pipeline(_path, **kwargs):
        rid = str(kwargs.get("run_id") or "")
        return DraftPipelineRunResult(
            snapshot_id="snap-c",
            run_id=rid,
            plan_id="plan-c",
            import_kind="bundle",
            success=False,
            phases_completed=["execution_bridge", "summary"],
            failure_boundary="bridge",
            boundary_detail="bridge_compatibility_blocked",
        )

    monkeypatch.setattr("ozlink_console.execution.orchestrator.log_info", _capture)
    monkeypatch.setattr(
        "ozlink_console.execution.orchestrator.run_pipeline_from_bundle_folder",
        _fake_pipeline,
    )
    orch = ExecutionOrchestrator()
    orch.run_snapshot(
        manifest_path="/y.json",
        bundle_folder="/b",
        dry_run=True,
        environment_context={},
        graph_client=object(),
    )
    assert calls[1][1].get("stop_reason") == ExecutionStopReason.BRIDGE_COMPATIBILITY_BLOCKED.value


def test_build_snapshot_summary_and_comparison_record():
    ctx = RunContext(
        run_id="r1",
        execution_kind="snapshot_pipeline",
        source_of_truth="execution_plan",
        snapshot_id=None,
        plan_id=None,
        environment_context={},
        dry_run=True,
        correlation={},
        current_phase="x",
        started_at=datetime.now(timezone.utc),
    )
    result = DraftPipelineRunResult(
        snapshot_id="s1",
        run_id="r1",
        plan_id="p1",
        import_kind="bundle",
        success=True,
        phases_completed=["summary"],
        failure_boundary="",
        boundary_detail="",
    )
    s = build_snapshot_run_result_summary(ctx, result)
    assert s.final_status == "completed"
    rec = snapshot_internal_comparison_record(s)
    assert rec["execution_path"] == "snapshot_pipeline"
    assert rec["plan_id"] == "p1"
    assert rec["failure_boundary"] is None
