"""Internal snapshot run summaries and comparison-shaped records (validation / observability only)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from ozlink_console.execution.boundary_vocabulary import (
    BRIDGE_COMPATIBILITY_BLOCKED,
    BRIDGE_OUTCOME_UNSPECIFIED,
    BRIDGE_STEP_FAILED,
    RUNNER_UNCAUGHT_EXCEPTION,
    internal_outcome_phrase,
    normalize_boundary_detail,
)
from ozlink_console.execution.run_context import RunContext
from ozlink_console.execution.stop_reason import ExecutionStopReason

if TYPE_CHECKING:
    from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineRunResult

FinalSnapshotStatus = Literal["completed", "stopped", "failed", "runner_failed"]


def _fallback_failure_boundary(result: DraftPipelineRunResult) -> str:
    """If harness did not set ``failure_boundary`` (older callers), derive from ``stopped_at`` / phases."""
    if result.stopped_at in ("environment_validation", "graph_client_required"):
        return "environment"
    if result.stopped_at == "resolution":
        return "resolution"
    if result.stopped_at in ("plan_build", "plan_override_mismatch"):
        return "plan_build"
    if not result.success and "execution_bridge" in result.phases_completed:
        return "bridge"
    return ""


def _fallback_boundary_detail(result: DraftPipelineRunResult) -> str:
    if getattr(result, "boundary_detail", ""):
        return str(result.boundary_detail)
    if result.stopped_at:
        return str(result.stopped_at)
    if not result.success and "execution_bridge" in result.phases_completed:
        return BRIDGE_OUTCOME_UNSPECIFIED
    return ""


def pipeline_stopped_at_to_stop_reason(stopped_at: str) -> ExecutionStopReason:
    """Map harness ``stopped_at`` to a controlled orchestrator stop reason."""
    return {
        "environment_validation": ExecutionStopReason.ENVIRONMENT_VALIDATION_FAILED,
        "graph_client_required": ExecutionStopReason.GRAPH_CLIENT_REQUIRED,
        "resolution": ExecutionStopReason.RESOLUTION_BLOCKERS,
        "plan_build": ExecutionStopReason.PLAN_BUILD_FAILED,
        "plan_override_mismatch": ExecutionStopReason.PLAN_OVERRIDE_MISMATCH,
    }.get(stopped_at, ExecutionStopReason.SNAPSHOT_PIPELINE_EXCEPTION)


@dataclass(frozen=True)
class SnapshotRunResultSummary:
    """Concise internal-use outcome for one snapshot pipeline run (not end-user copy)."""

    run_id: str
    snapshot_id: str
    plan_id: str | None
    final_status: FinalSnapshotStatus
    failure_boundary: str
    stopped_at: str
    stop_reason: str | None
    boundary_detail: str
    exception_type: str | None = None
    """Populated for ``runner_failed`` only (exception class name)."""

    def as_log_dict(self) -> dict[str, Any]:
        """Flat dict for structured logs (stable keys for grep / internal tooling)."""
        d: dict[str, Any] = {
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id or "",
            "plan_id": self.plan_id,
            "final_status": self.final_status,
            "failure_boundary": self.failure_boundary,
            "boundary_detail": self.boundary_detail,
            "stopped_at": self.stopped_at,
            "stop_reason": self.stop_reason,
        }
        if self.exception_type:
            d["exception_type"] = self.exception_type
        return d

    def one_line_internal(self) -> str:
        """Short line for status labels / compact dialogs."""
        base = f"status={self.final_status} run_id={self.run_id} snapshot_id={self.snapshot_id or 'n/a'} plan_id={self.plan_id or 'n/a'}"
        phrase = internal_outcome_phrase(
            final_status=self.final_status,
            failure_boundary=self.failure_boundary,
            boundary_detail=self.boundary_detail,
            stopped_at=self.stopped_at,
        )
        if self.failure_boundary or self.final_status != "completed":
            return f"{base} | {phrase} | detail={self.boundary_detail or 'n/a'}"
        return f"{base} | {phrase}"


def format_first_failed_bridge_step_diagnostic(result: DraftPipelineRunResult) -> str:
    """Multi-line addendum for UI when ``failure_boundary=bridge`` and ``bridge_step_failed``."""
    boundary = str(getattr(result, "failure_boundary", "") or "").strip()
    detail = str(getattr(result, "boundary_detail", "") or "").strip()
    if boundary != "bridge" or detail != BRIDGE_STEP_FAILED:
        return ""
    outcomes = getattr(result, "bridge_step_outcomes", None) or {}
    if not outcomes:
        return (
            "\n\nFirst failed bridge step:\n"
            "  (bridge_step_outcomes empty — see execution_bridge logs)"
        )
    order = list(getattr(result, "plan_step_ids", None) or [])
    if not order:
        order = sorted(outcomes.keys())
    for sid in order:
        row = outcomes.get(sid)
        if not isinstance(row, dict):
            continue
        if row.get("outcome") == "failed":
            return _format_failed_bridge_step_block(sid, row)
    return (
        "\n\nFirst failed bridge step:\n"
        "  (no step with outcome=failed in bridge_step_outcomes)"
    )


def _format_failed_bridge_step_block(step_id: str, row: dict[str, Any]) -> str:
    def _line(label: str, val: Any) -> str:
        s = str(val).strip() if val is not None else ""
        disp = s if s else "—"
        return f"  {label}: {disp}"

    lines = [
        "",
        "First failed bridge step (plan order):",
        _line("step_id", step_id),
        _line("mapping_id", row.get("mapping_id")),
        _line("step_type", row.get("step_type")),
        _line("source_path", row.get("source_path")),
        _line("destination_path", row.get("destination_path")),
        _line("detail", row.get("detail")),
        _line("backend_status", row.get("backend_status")),
    ]
    return "\n".join(lines)


def build_snapshot_run_result_summary(ctx: RunContext, result: DraftPipelineRunResult) -> SnapshotRunResultSummary:
    boundary = str(getattr(result, "failure_boundary", "") or "").strip() or _fallback_failure_boundary(result)
    detail_raw = str(getattr(result, "boundary_detail", "") or "").strip() or _fallback_boundary_detail(result)
    stopped_at = str(result.stopped_at or "")
    detail = normalize_boundary_detail(detail_raw, stopped_at=stopped_at, failure_boundary=boundary)

    if result.stopped_at:
        final_status: FinalSnapshotStatus = "stopped"
        stop_reason = pipeline_stopped_at_to_stop_reason(stopped_at).value
    elif not result.success:
        final_status = "failed"
        if boundary == "bridge":
            if detail == BRIDGE_COMPATIBILITY_BLOCKED:
                stop_reason = ExecutionStopReason.BRIDGE_COMPATIBILITY_BLOCKED.value
            elif detail == BRIDGE_STEP_FAILED:
                stop_reason = ExecutionStopReason.BRIDGE_STEP_FAILED.value
            else:
                stop_reason = ExecutionStopReason.BRIDGE_EXECUTION_FAILED.value
        else:
            stop_reason = ExecutionStopReason.BRIDGE_EXECUTION_FAILED.value
    else:
        final_status = "completed"
        stop_reason = None

    return SnapshotRunResultSummary(
        run_id=ctx.run_id,
        snapshot_id=str(result.snapshot_id or ""),
        plan_id=result.plan_id,
        final_status=final_status,
        failure_boundary=boundary,
        stopped_at=stopped_at,
        stop_reason=stop_reason,
        boundary_detail=detail,
    )


def build_runner_failed_summary(ctx: RunContext, exc: BaseException) -> SnapshotRunResultSummary:
    return SnapshotRunResultSummary(
        run_id=ctx.run_id,
        snapshot_id="",
        plan_id=None,
        final_status="runner_failed",
        failure_boundary="runner",
        stopped_at="",
        stop_reason=ExecutionStopReason.SNAPSHOT_PIPELINE_EXCEPTION.value,
        boundary_detail=RUNNER_UNCAUGHT_EXCEPTION,
        exception_type=type(exc).__name__,
    )


def snapshot_internal_comparison_record(summary: SnapshotRunResultSummary) -> dict[str, Any]:
    """Stable shape for side-by-side internal comparison with legacy manifest runs (see playbook)."""
    rec: dict[str, Any] = {
        "execution_path": "snapshot_pipeline",
        "run_id": summary.run_id,
        "snapshot_id": summary.snapshot_id or None,
        "plan_id": summary.plan_id,
        "final_status": summary.final_status,
        "failure_boundary": summary.failure_boundary or None,
        "boundary_detail": summary.boundary_detail or None,
        "stopped_at": summary.stopped_at or None,
        "stop_reason": summary.stop_reason,
    }
    if summary.exception_type:
        rec["exception_type"] = summary.exception_type
    return rec
