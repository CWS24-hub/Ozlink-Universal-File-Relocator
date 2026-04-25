"""Non-live dry-run / integration harness for the draft snapshot → plan → bridge pipeline.

Does not wire into ``main_window`` or replace manifest-based execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from ozlink_console.draft_snapshot.adapters import apply_library_relative_paths
from ozlink_console.draft_snapshot.detached import (
    DetachedSubmittedSnapshot,
    load_detached_from_bundle_folder,
    load_detached_from_canonical_json_bytes,
    load_detached_from_req_json_bytes,
)
from ozlink_console.draft_snapshot.environment import ConnectedEnvironmentContext, validate_environment_against_snapshot
from ozlink_console.draft_snapshot.execution_bridge import BridgeStepState, ExecutionBridgeRuntimeState, ExecutionPlanBridge
from ozlink_console.draft_snapshot.execution_plan_contracts import ExecutionPlan, ExecutionStep
from ozlink_console.draft_snapshot.plan_materialization import (
    build_execution_plan_from_resolved,
    subset_execution_plan_by_dependency_closure,
    subset_execution_plan_by_mapping_ids,
)
from ozlink_console.draft_snapshot.resolution_contracts import ResolvedSnapshot
from ozlink_console.draft_snapshot.resolver_service import GraphResolveIdsService
from ozlink_console.execution.boundary_vocabulary import (
    BRIDGE_COMPATIBILITY_BLOCKED,
    BRIDGE_STEP_FAILED,
    ENVIRONMENT_VALIDATION,
    GRAPH_CLIENT_REQUIRED,
    PLAN_BUILD_EXCEPTION,
    PLAN_OVERRIDE_MISMATCH,
    RESOLUTION_UNRESOLVED_OR_AMBIGUOUS,
)
from ozlink_console.draft_snapshot.run_log import (
    event_from_detached_import,
    log_environment_validation_summary,
    log_harness_phase,
    log_pipeline_info,
    validation_report_as_dict,
)


@dataclass
class DraftPipelineHarnessRequest:
    """Detached snapshot + connected context; optional Graph client for resolve/plan/bridge."""

    detached: DetachedSubmittedSnapshot
    connected: ConnectedEnvironmentContext
    graph_client: Any | None = None
    bridge_dry_run: bool = True
    strict_canonical_tenant: bool = True
    allow_move_as_copy: bool = False
    execution_plan_override: ExecutionPlan | None = None
    """
    When set, environment validation still runs, then resolution and plan materialization are skipped
    and this plan is executed via the bridge (same ``snapshot_id`` as ``detached.snapshot`` required).
    For integration tests where full Graph resolution cannot yet produce a given plan shape.
    """
    block_on_resolution_gaps: bool = False
    """When True, stop before plan build if any mapping is unresolved or ambiguous (orchestrated snapshot runs)."""
    orchestration_context: dict[str, Any] | None = None
    """Optional top-level execution fields (e.g. execution_kind) merged into harness start logs."""
    snapshot_scoped_mode: str = ""
    """``strict``: seed mapping_ids only; ``dependency_closure``: minimal structural closure."""
    scoped_seed_mapping_ids: frozenset[str] | None = None
    """Planned-move / allocation mapping ids selected for scoped snapshot runs."""


@dataclass
class DraftPipelineRunResult:
    snapshot_id: str
    run_id: str
    plan_id: str | None
    import_kind: str
    phases_completed: list[str] = field(default_factory=list)
    stopped_at: Literal["", "environment_validation", "graph_client_required", "plan_override_mismatch"] | str = ""
    success: bool = False
    import_warnings: list[str] = field(default_factory=list)
    normalization_notes: list[str] = field(default_factory=list)
    environment_passed: bool | None = None
    environment_report: dict[str, Any] | None = None
    resolution_summary: dict[str, Any] | None = None
    resolution_items_brief: list[dict[str, Any]] = field(default_factory=list)
    unresolved_mapping_ids: list[str] = field(default_factory=list)
    ambiguous_mapping_ids: list[str] = field(default_factory=list)
    plan_summary: dict[str, Any] | None = None
    plan_materialization: dict[str, Any] | None = None
    plan_hash: str | None = None
    bridge_summary: dict[str, Any] | None = None
    bridge_step_outcomes: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Per ``step_id`` bridge state; values include ``step_id``, paths when known."""
    plan_step_ids: list[str] = field(default_factory=list)
    """Execution order of plan steps (for first-failure diagnostics)."""
    compatibility_blocks: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    resolution_skipped: bool = False
    plan_build_skipped: bool = False
    failure_boundary: str = ""
    """Internal validation: environment | resolution | plan_build | bridge | (orchestrator: runner)."""
    boundary_detail: str = ""
    """Stable sub-code (e.g. graph_client_required, bridge_step_failed); not raw exception text."""


def _resolution_brief(resolved: ResolvedSnapshot) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for r in (*resolved.mapping_results, *resolved.proposed_folder_results):
        row: dict[str, Any] = {
            "mapping_id": r.mapping_id,
            "item_kind": r.item_kind,
            "item_type": r.item_type,
            "status": r.status,
            "message": r.message,
        }
        if r.raw.get("deferred_destination_parent_to_mkdir_chain"):
            row["deferred_destination_parent_to_mkdir_chain"] = True
        rows.append(row)
    return rows


def _bridge_step_state_dict(ss: BridgeStepState, step: ExecutionStep | None = None) -> dict[str, Any]:
    dest_path = ""
    src_path = ""
    if step is not None:
        dest_path = str(step.destination_path or "")
        meta = step.metadata if isinstance(step.metadata, dict) else {}
        src_path = str(meta.get("source_path") or "")
    return {
        "step_id": ss.step_id,
        "status": ss.status,
        "outcome": ss.outcome,
        "detail": ss.detail,
        "step_type": ss.step_type,
        "mapping_id": ss.mapping_id,
        "source_path": src_path,
        "destination_path": dest_path,
        "resolved_destination_parent_item_id": ss.resolved_destination_parent_item_id,
        "parent_resolution_source": ss.parent_resolution_source,
        "output_item_id": ss.output_item_id,
        "compatibility_decision": ss.compatibility_decision,
        "backend_status": ss.backend_status,
    }


def bridge_runtime_state_summary(state: ExecutionBridgeRuntimeState) -> dict[str, Any]:
    outcomes: dict[str, int] = {}
    for ss in state.step_states.values():
        outcomes[ss.outcome] = outcomes.get(ss.outcome, 0) + 1
    blocks = [
        f"{sid}:{ss.detail}"
        for sid, ss in state.step_states.items()
        if ss.outcome == "blocked_compatibility" or (ss.status == "blocked" and ss.outcome == "blocked_compatibility")
    ]
    return {
        "plan_id": state.plan_id,
        "completed_step_ids": list(state.completed_step_ids),
        "outcome_counts": outcomes,
        "compatibility_notes": list(state.compatibility_notes),
        "compatibility_block_messages": blocks,
    }


def run_draft_snapshot_pipeline(req: DraftPipelineHarnessRequest) -> DraftPipelineRunResult:
    det = req.detached
    snap = det.snapshot
    rid = det.run_id
    adapter = str(snap.adapter_source or det.import_kind or "")
    result = DraftPipelineRunResult(
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        plan_id=None,
        import_kind=det.import_kind,
        import_warnings=list(det.import_warnings),
        normalization_notes=list(det.normalization_notes),
    )

    log_harness_phase(
        subphase="start",
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        adapter_source=adapter,
        import_kind=det.import_kind,
    )
    log_pipeline_info(
        event_from_detached_import(
            "snapshot_import",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            adapter_source=adapter,
            import_kind=det.import_kind,
            message="draft_snapshot.harness.detached_loaded",
            extra={"import_warnings_count": len(det.import_warnings)},
        )
    )
    result.phases_completed.append("import")

    apply_library_relative_paths(snap)
    log_pipeline_info(
        event_from_detached_import(
            "normalization",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            adapter_source=adapter,
            import_kind=det.import_kind,
            message="draft_snapshot.harness.normalization_complete",
            extra={"library_relative_paths_applied": True},
        )
    )
    result.phases_completed.append("normalization")

    env = validate_environment_against_snapshot(
        snap,
        req.connected,
        run_id=rid,
        strict_canonical_tenant=req.strict_canonical_tenant,
    )
    result.environment_passed = env.passed
    result.environment_report = validation_report_as_dict(env)
    log_environment_validation_summary(
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        passed=env.passed,
        error_count=len(env.errors()),
        warning_count=len(env.warnings()),
        adapter_source=adapter,
    )
    log_harness_phase(
        subphase="environment_validation",
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        adapter_source=adapter,
        import_kind=det.import_kind,
        extra={"passed": env.passed},
    )
    result.phases_completed.append("environment_validation")

    if not env.passed:
        result.stopped_at = "environment_validation"
        result.failure_boundary = "environment"
        result.boundary_detail = ENVIRONMENT_VALIDATION
        result.errors.append("environment_validation_failed")
        log_harness_phase(
            subphase="summary",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            adapter_source=adapter,
            import_kind=det.import_kind,
            extra={
                "success": False,
                "stopped_at": result.stopped_at,
                "failure_boundary": result.failure_boundary,
                "boundary_detail": result.boundary_detail,
            },
        )
        return result

    if req.graph_client is None:
        result.stopped_at = "graph_client_required"
        result.failure_boundary = "environment"
        result.boundary_detail = GRAPH_CLIENT_REQUIRED
        result.errors.append("graph_client_required_after_environment")
        log_harness_phase(
            subphase="summary",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            adapter_source=adapter,
            import_kind=det.import_kind,
            extra={
                "success": False,
                "stopped_at": result.stopped_at,
                "failure_boundary": result.failure_boundary,
                "boundary_detail": result.boundary_detail,
            },
        )
        return result

    plan: ExecutionPlan
    if req.execution_plan_override is not None:
        plan = req.execution_plan_override
        if plan.snapshot_id != snap.snapshot_id:
            result.stopped_at = "plan_override_mismatch"
            result.failure_boundary = "plan_build"
            result.boundary_detail = PLAN_OVERRIDE_MISMATCH
            result.errors.append(
                f"execution_plan_override.snapshot_id {plan.snapshot_id!r} != detached.snapshot.snapshot_id {snap.snapshot_id!r}"
            )
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                plan_id=plan.plan_id,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                },
            )
            return result
        result.resolution_skipped = True
        result.plan_build_skipped = True
        result.plan_id = plan.plan_id
        log_harness_phase(
            subphase="plan_build_skipped",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            plan_id=plan.plan_id,
            adapter_source=adapter,
            import_kind=det.import_kind,
            extra={"reason": "execution_plan_override"},
        )
    else:
        resolver = GraphResolveIdsService(graph_client=req.graph_client)
        resolved = resolver.resolve(snap, run_id=rid)
        result.resolution_summary = asdict(resolved.summary)
        result.resolution_items_brief = _resolution_brief(resolved)
        result.unresolved_mapping_ids = list(resolved.summary.unresolved_mapping_ids)
        result.ambiguous_mapping_ids = list(resolved.summary.ambiguous_mapping_ids)
        log_harness_phase(
            subphase="resolution_complete",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            adapter_source=adapter,
            import_kind=det.import_kind,
            extra={
                "resolved_count": resolved.summary.resolved_count,
                "unresolved_count": resolved.summary.unresolved_count,
                "ambiguous_count": resolved.summary.ambiguous_count,
            },
        )
        result.phases_completed.append("resolution")

        if req.block_on_resolution_gaps and (
            resolved.summary.unresolved_count > 0 or resolved.summary.ambiguous_count > 0
        ):
            result.stopped_at = "resolution"
            result.failure_boundary = "resolution"
            result.boundary_detail = RESOLUTION_UNRESOLVED_OR_AMBIGUOUS
            result.errors.append("resolution_unresolved_or_ambiguous")
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                    "unresolved_count": resolved.summary.unresolved_count,
                    "ambiguous_count": resolved.summary.ambiguous_count,
                },
            )
            return result

        try:
            plan = build_execution_plan_from_resolved(resolved, run_id=rid, graph_client=req.graph_client)
        except Exception as exc:
            result.stopped_at = "plan_build"
            result.failure_boundary = "plan_build"
            result.boundary_detail = PLAN_BUILD_EXCEPTION
            result.errors.append(f"plan_build_exception:{exc!s}")
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                },
            )
            return result
        result.plan_id = plan.plan_id
        result.phases_completed.append("plan_build")

    scoped_mode = str(req.snapshot_scoped_mode or "").strip()
    seeds = req.scoped_seed_mapping_ids
    if scoped_mode and seeds is not None:
        if scoped_mode not in ("strict", "dependency_closure"):
            result.stopped_at = "plan_build"
            result.failure_boundary = "plan_build"
            result.boundary_detail = "scoped_execution_invalid_mode"
            result.errors.append(f"scoped_execution_invalid_mode:{scoped_mode!r}")
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                },
            )
            return result
        if not seeds:
            result.stopped_at = "plan_build"
            result.failure_boundary = "plan_build"
            result.boundary_detail = "scoped_execution_empty_seeds"
            result.errors.append("scoped_execution_empty_seeds")
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                },
            )
            return result
        if scoped_mode == "strict":
            subset_plan = subset_execution_plan_by_mapping_ids(plan, seeds)
        else:
            subset_plan = subset_execution_plan_by_dependency_closure(plan, seeds)
        if subset_plan is None:
            result.stopped_at = "plan_build"
            result.failure_boundary = "plan_build"
            result.boundary_detail = "scoped_execution_no_matching_steps"
            result.errors.append("scoped_execution_no_matching_steps")
            log_harness_phase(
                subphase="summary",
                snapshot_id=snap.snapshot_id,
                run_id=rid,
                adapter_source=adapter,
                import_kind=det.import_kind,
                extra={
                    "success": False,
                    "stopped_at": result.stopped_at,
                    "failure_boundary": result.failure_boundary,
                    "boundary_detail": result.boundary_detail,
                },
            )
            return result
        plan = subset_plan
        log_harness_phase(
            subphase="plan_scoped_subset",
            snapshot_id=snap.snapshot_id,
            run_id=rid,
            plan_id=plan.plan_id,
            adapter_source=adapter,
            import_kind=det.import_kind,
            extra={
                "snapshot_scoped_mode": scoped_mode,
                "scoped_seed_mapping_id_count": len(seeds),
                "subset_total_steps": plan.summary.total_steps,
            },
        )

    result.plan_summary = asdict(plan.summary)
    result.plan_materialization = asdict(plan.materialization)
    result.plan_hash = plan.plan_hash
    log_harness_phase(
        subphase="plan_ready",
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        plan_id=plan.plan_id,
        adapter_source=adapter,
        import_kind=det.import_kind,
        extra={"total_steps": plan.summary.total_steps, "plan_hash": plan.plan_hash},
    )

    result.plan_step_ids = [s.step_id for s in plan.steps]
    step_by_id = {s.step_id: s for s in plan.steps}
    bridge = ExecutionPlanBridge(allow_move_as_copy=req.allow_move_as_copy)
    bridge_state = bridge.execute_plan(plan, graph_client=req.graph_client, dry_run=req.bridge_dry_run)
    result.bridge_summary = bridge_runtime_state_summary(bridge_state)
    result.bridge_step_outcomes = {
        sid: _bridge_step_state_dict(ss, step_by_id.get(sid)) for sid, ss in bridge_state.step_states.items()
    }
    result.compatibility_blocks = list(result.bridge_summary.get("compatibility_block_messages") or [])

    failed = any(ss.outcome == "failed" for ss in bridge_state.step_states.values())
    blocked = any(ss.outcome == "blocked_compatibility" for ss in bridge_state.step_states.values())
    result.success = not failed and not blocked
    result.phases_completed.append("execution_bridge")
    if failed:
        result.failure_boundary = "bridge"
        result.boundary_detail = BRIDGE_STEP_FAILED
    elif blocked:
        result.failure_boundary = "bridge"
        result.boundary_detail = BRIDGE_COMPATIBILITY_BLOCKED
    else:
        result.failure_boundary = ""
        result.boundary_detail = ""

    log_harness_phase(
        subphase="summary",
        snapshot_id=snap.snapshot_id,
        run_id=rid,
        plan_id=plan.plan_id,
        adapter_source=adapter,
        import_kind=det.import_kind,
        extra={
            "success": result.success,
            "bridge_dry_run": req.bridge_dry_run,
            "outcome_counts": result.bridge_summary.get("outcome_counts"),
            "failure_boundary": result.failure_boundary or None,
            "boundary_detail": result.boundary_detail or None,
        },
    )
    result.phases_completed.append("summary")
    return result


def run_pipeline_from_canonical_json_bytes(
    data: bytes,
    *,
    connected: ConnectedEnvironmentContext,
    graph_client: Any | None,
    run_id: str | None = None,
    bridge_dry_run: bool = True,
    strict_canonical_tenant: bool = True,
    allow_move_as_copy: bool = False,
    execution_plan_override: ExecutionPlan | None = None,
) -> DraftPipelineRunResult:
    det = load_detached_from_canonical_json_bytes(data, run_id=run_id)
    return run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=connected,
            graph_client=graph_client,
            bridge_dry_run=bridge_dry_run,
            strict_canonical_tenant=strict_canonical_tenant,
            allow_move_as_copy=allow_move_as_copy,
            execution_plan_override=execution_plan_override,
        )
    )


def run_pipeline_from_req_json_bytes(
    data: bytes,
    *,
    connected: ConnectedEnvironmentContext,
    graph_client: Any | None,
    run_id: str | None = None,
    bridge_dry_run: bool = True,
    strict_canonical_tenant: bool = True,
    allow_move_as_copy: bool = False,
    execution_plan_override: ExecutionPlan | None = None,
) -> DraftPipelineRunResult:
    det = load_detached_from_req_json_bytes(data, run_id=run_id)
    return run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=connected,
            graph_client=graph_client,
            bridge_dry_run=bridge_dry_run,
            strict_canonical_tenant=strict_canonical_tenant,
            allow_move_as_copy=allow_move_as_copy,
            execution_plan_override=execution_plan_override,
        )
    )


def run_pipeline_from_bundle_folder(
    path: Path | str,
    *,
    connected: ConnectedEnvironmentContext,
    graph_client: Any | None,
    run_id: str | None = None,
    bridge_dry_run: bool = True,
    strict_canonical_tenant: bool = True,
    allow_move_as_copy: bool = False,
    execution_plan_override: ExecutionPlan | None = None,
    block_on_resolution_gaps: bool = False,
    orchestration_context: dict[str, Any] | None = None,
    snapshot_scoped_mode: str = "",
    scoped_seed_mapping_ids: frozenset[str] | None = None,
) -> DraftPipelineRunResult:
    det = load_detached_from_bundle_folder(Path(path), run_id=run_id)
    return run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=connected,
            graph_client=graph_client,
            bridge_dry_run=bridge_dry_run,
            strict_canonical_tenant=strict_canonical_tenant,
            allow_move_as_copy=allow_move_as_copy,
            execution_plan_override=execution_plan_override,
            block_on_resolution_gaps=block_on_resolution_gaps,
            orchestration_context=orchestration_context,
            snapshot_scoped_mode=snapshot_scoped_mode,
            scoped_seed_mapping_ids=scoped_seed_mapping_ids,
        )
    )


def draft_pipeline_run_result_to_dict(r: DraftPipelineRunResult) -> dict[str, Any]:
    return asdict(r)
