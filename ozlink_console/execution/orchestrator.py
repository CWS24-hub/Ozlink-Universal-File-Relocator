"""Execution orchestrator: manifest shell + snapshot pipeline wiring to draft harness."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ozlink_console.logger import log_info

from ozlink_console.draft_snapshot.environment import ConnectedEnvironmentContext
from ozlink_console.execution.run_context import RunContext

if TYPE_CHECKING:
    from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineRunResult
from ozlink_console.execution.snapshot_summary import (
    build_runner_failed_summary,
    build_snapshot_run_result_summary,
    pipeline_stopped_at_to_stop_reason,
    snapshot_internal_comparison_record,
)
from ozlink_console.execution.stop_reason import ExecutionStopReason
from ozlink_console.execution.validation_capture import maybe_append_snapshot_validation_capture


def _connected_from_execution_env(env: dict[str, Any]) -> ConnectedEnvironmentContext:
    """Strip orchestration-only keys; map dict to ``ConnectedEnvironmentContext`` fields."""
    fields = ConnectedEnvironmentContext.__dataclass_fields__
    return ConnectedEnvironmentContext(**{name: str(env.get(name, "") or "") for name in fields})


def run_pipeline_from_bundle_folder(*args: Any, **kwargs: Any):
    """Thin delegate for :func:`draft_snapshot.pipeline_harness.run_pipeline_from_bundle_folder` (tests monkeypatch this)."""
    from ozlink_console.draft_snapshot.pipeline_harness import run_pipeline_from_bundle_folder as _impl

    return _impl(*args, **kwargs)


class ExecutionOrchestrator:
    """Creates RunContext, emits top-level execution logs, routes snapshot runs through the harness."""

    def run_manifest(
        self,
        *,
        manifest_path: str,
        dry_run: bool,
        environment_context: dict[str, Any],
    ) -> RunContext:
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        ctx = RunContext(
            run_id=run_id,
            execution_kind="manifest",
            source_of_truth="manifest",
            snapshot_id=None,
            plan_id=None,
            environment_context=dict(environment_context or {}),
            dry_run=bool(dry_run),
            correlation={"manifest_path": str(manifest_path or "")},
            current_phase="initialized",
            started_at=started_at,
        )
        log_info(
            "execution_run_started",
            execution_event="execution_run_started",
            run_id=ctx.run_id,
            execution_kind=ctx.execution_kind,
            source_of_truth=ctx.source_of_truth,
            dry_run=ctx.dry_run,
            manifest_path=str(manifest_path or ""),
            current_phase=ctx.current_phase,
            started_at_iso=ctx.started_at.isoformat(),
            environment_context=ctx.environment_context,
            correlation=ctx.correlation,
        )
        return ctx

    def run_snapshot(
        self,
        *,
        manifest_path: str,
        bundle_folder: str,
        dry_run: bool,
        environment_context: dict[str, Any],
        graph_client: Any | None = None,
        snapshot_scoped_mode: str = "",
        scoped_seed_mapping_ids: frozenset[str] | None = None,
    ) -> tuple[RunContext, DraftPipelineRunResult]:
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        env = dict(environment_context or {})
        ctx = RunContext(
            run_id=run_id,
            execution_kind="snapshot_pipeline",
            source_of_truth="execution_plan",
            snapshot_id=None,
            plan_id=None,
            environment_context=env,
            dry_run=bool(dry_run),
            correlation={
                "manifest_path": str(manifest_path or ""),
                "bundle_folder": str(bundle_folder or ""),
            },
            current_phase="initialized",
            started_at=started_at,
        )
        log_info(
            "execution_run_started",
            execution_event="execution_run_started",
            run_id=ctx.run_id,
            execution_kind=ctx.execution_kind,
            source_of_truth=ctx.source_of_truth,
            dry_run=ctx.dry_run,
            manifest_path=str(manifest_path or ""),
            bundle_folder=str(bundle_folder or ""),
            current_phase=ctx.current_phase,
            started_at_iso=ctx.started_at.isoformat(),
            environment_context=ctx.environment_context,
            correlation=ctx.correlation,
        )

        connected = _connected_from_execution_env(env)
        orch_extra = {
            "execution_kind": ctx.execution_kind,
            "source_of_truth": ctx.source_of_truth,
            "started_at_iso": ctx.started_at.isoformat(),
            "correlation": ctx.correlation,
            "dry_run": ctx.dry_run,
        }

        try:
            result = run_pipeline_from_bundle_folder(
                bundle_folder,
                connected=connected,
                graph_client=graph_client,
                bridge_dry_run=ctx.dry_run,
                strict_canonical_tenant=True,
                allow_move_as_copy=False,
                run_id=ctx.run_id,
                block_on_resolution_gaps=True,
                orchestration_context=orch_extra,
                snapshot_scoped_mode=snapshot_scoped_mode,
                scoped_seed_mapping_ids=scoped_seed_mapping_ids,
            )
        except Exception as exc:
            rsum = build_runner_failed_summary(ctx, exc)
            log_info(
                "execution_run_failed",
                execution_event="execution_run_failed",
                run_id=ctx.run_id,
                execution_kind=ctx.execution_kind,
                source_of_truth=ctx.source_of_truth,
                dry_run=ctx.dry_run,
                manifest_path=str(manifest_path or ""),
                bundle_folder=str(bundle_folder or ""),
                current_phase=ctx.current_phase,
                started_at_iso=ctx.started_at.isoformat(),
                environment_context=ctx.environment_context,
                correlation=ctx.correlation,
                stop_reason=ExecutionStopReason.SNAPSHOT_PIPELINE_EXCEPTION.value,
                failure_boundary=rsum.failure_boundary,
                boundary_detail=rsum.boundary_detail,
                exception_type=rsum.exception_type,
                error=str(exc)[:500],
            )
            log_info(
                "snapshot_run_internal_summary",
                execution_event="snapshot_run_internal_summary",
                internal_comparison=snapshot_internal_comparison_record(rsum),
                **rsum.as_log_dict(),
            )
            raise

        ctx.snapshot_id = result.snapshot_id or None
        ctx.plan_id = result.plan_id
        ctx.current_phase = "snapshot_pipeline_complete"

        summary = build_snapshot_run_result_summary(ctx, result)

        base_terminal = {
            "run_id": ctx.run_id,
            "execution_kind": ctx.execution_kind,
            "source_of_truth": ctx.source_of_truth,
            "dry_run": ctx.dry_run,
            "manifest_path": str(manifest_path or ""),
            "bundle_folder": str(bundle_folder or ""),
            "current_phase": ctx.current_phase,
            "started_at_iso": ctx.started_at.isoformat(),
            "environment_context": ctx.environment_context,
            "correlation": ctx.correlation,
            "snapshot_id": result.snapshot_id,
            "plan_id": result.plan_id,
            "stopped_at": result.stopped_at or None,
            "pipeline_success": result.success,
            "failure_boundary": summary.failure_boundary or None,
            "boundary_detail": summary.boundary_detail or None,
            "final_status": summary.final_status,
        }

        if result.stopped_at:
            log_info(
                "execution_run_stopped",
                execution_event="execution_run_stopped",
                stop_reason=pipeline_stopped_at_to_stop_reason(result.stopped_at).value,
                errors=list(result.errors) or None,
                **base_terminal,
            )
        elif not result.success:
            log_info(
                "execution_run_failed",
                execution_event="execution_run_failed",
                stop_reason=summary.stop_reason or ExecutionStopReason.BRIDGE_EXECUTION_FAILED.value,
                bridge_summary=result.bridge_summary,
                compatibility_blocks=list(result.compatibility_blocks) or None,
                **base_terminal,
            )
        else:
            log_info(
                "execution_run_completed",
                execution_event="execution_run_completed",
                **base_terminal,
            )

        log_info(
            "snapshot_run_internal_summary",
            execution_event="snapshot_run_internal_summary",
            internal_comparison=snapshot_internal_comparison_record(summary),
            **summary.as_log_dict(),
        )
        maybe_append_snapshot_validation_capture(summary)

        return ctx, result
