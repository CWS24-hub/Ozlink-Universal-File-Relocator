"""Execution bridge for running ExecutionPlan steps via existing manifest runner.

This module intentionally does NOT wire into ``main_window`` or replace live execution paths.
It is an adapter layer that can be invoked by future integration points.
"""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.execution_plan_contracts import ExecutionPlan, ExecutionStep
from ozlink_console.draft_snapshot.run_log import log_execution_bridge_step
from ozlink_console.transfer_job_runner import (
    RunManifestResult,
    is_absolute_local_path,
    run_manifest_local_filesystem,
)

BridgeStepStatus = Literal["pending", "running", "ok", "failed", "skipped", "dry_run", "blocked"]
ParentResolutionSource = Literal["direct", "parent_step_output", "parent_step_dry_run", "missing"]
BridgeStepOutcome = Literal["succeeded", "skipped_existing", "blocked_compatibility", "failed", "pending"]


@dataclass
class BridgeStepState:
    step_id: str
    mapping_id: str
    step_type: str
    status: BridgeStepStatus = "pending"
    detail: str = ""
    resolved_destination_parent_item_id: str = ""
    parent_resolution_source: ParentResolutionSource = "missing"
    output_item_id: str = ""
    backend_status: str = ""
    backend_records: list[dict[str, Any]] = field(default_factory=list)
    outcome: BridgeStepOutcome = "pending"
    compatibility_decision: str = ""


@dataclass
class ExecutionBridgeRuntimeState:
    snapshot_id: str
    run_id: str
    plan_id: str
    step_states: dict[str, BridgeStepState] = field(default_factory=dict)
    completed_step_ids: list[str] = field(default_factory=list)
    compatibility_notes: list[str] = field(default_factory=list)


class _GraphClientRuntimeProxy:
    """Captures Graph mkdir outputs per active bridge step."""

    def __init__(self, graph_client: Any | None) -> None:
        self._inner = graph_client
        self._active_step_id: str = ""
        self._created_item_ids_by_step: dict[str, str] = {}

    def set_active_step(self, step_id: str) -> None:
        self._active_step_id = str(step_id or "")

    def consume_created_item_id(self, step_id: str) -> str:
        return str(self._created_item_ids_by_step.pop(step_id, "") or "")

    def create_child_folder(
        self,
        drive_id: str,
        parent_item_id: str,
        name: str,
        *,
        conflict_behavior: str = "fail",
    ) -> dict[str, Any]:
        if self._inner is None:
            raise RuntimeError("Graph client not available for create_child_folder")
        payload = self._inner.create_child_folder(
            drive_id,
            parent_item_id,
            name,
            conflict_behavior=conflict_behavior,
        )
        created_id = str(payload.get("id") or "") if isinstance(payload, dict) else ""
        if self._active_step_id and created_id:
            self._created_item_ids_by_step[self._active_step_id] = created_id
        return payload

    def __getattr__(self, name: str) -> Any:
        if self._inner is None:
            raise AttributeError(name)
        return getattr(self._inner, name)


class ExecutionPlanBridge:
    """Adapter that executes ``ExecutionPlan`` steps via ``run_manifest_local_filesystem``."""

    COMPATIBILITY_NOTES: tuple[str, ...] = (
        "ExecutionPlan step_type=move_item is blocked by default in bridge mode; this phase does not implement source deletion semantics.",
        "ExecutionPolicy.file_conflict_policy=skip is handled by bridge preflight destination-exists checks and returns skipped_existing.",
        "ExecutionPolicy.folder_conflict_policy=merge is handled by bridge preflight destination-exists checks and returns succeeded without mutation.",
    )

    def __init__(self, runner: Any = run_manifest_local_filesystem, *, allow_move_as_copy: bool = False) -> None:
        self._runner = runner
        self._allow_move_as_copy = bool(allow_move_as_copy)

    def execute_plan(
        self,
        plan: ExecutionPlan,
        *,
        graph_client: Any | None = None,
        dry_run: bool = False,
    ) -> ExecutionBridgeRuntimeState:
        state = ExecutionBridgeRuntimeState(
            snapshot_id=plan.snapshot_id,
            run_id=plan.run_id,
            plan_id=plan.plan_id,
            compatibility_notes=list(self.COMPATIBILITY_NOTES),
        )
        for s in plan.steps:
            state.step_states[s.step_id] = BridgeStepState(
                step_id=s.step_id,
                mapping_id=s.mapping_id,
                step_type=s.step_type,
            )
        proxy = _GraphClientRuntimeProxy(graph_client)

        for step in plan.steps:
            ss = state.step_states[step.step_id]
            compat_action, compat_msg = self._compatibility_gate(step)
            if compat_action == "block":
                ss.status = "blocked"
                ss.outcome = "blocked_compatibility"
                ss.detail = compat_msg
                ss.compatibility_decision = compat_msg
                log_execution_bridge_step(
                    snapshot_id=plan.snapshot_id,
                    run_id=plan.run_id,
                    plan_id=plan.plan_id,
                    step_id=step.step_id,
                    mapping_id=step.mapping_id,
                    event="compatibility_gate",
                    status=ss.status,
                    extra={"decision": compat_msg, "outcome": ss.outcome},
                    warn=True,
                )
                if plan.policy.stop_on_error:
                    break
                continue
            if compat_action == "downgrade":
                ss.compatibility_decision = compat_msg
                log_execution_bridge_step(
                    snapshot_id=plan.snapshot_id,
                    run_id=plan.run_id,
                    plan_id=plan.plan_id,
                    step_id=step.step_id,
                    mapping_id=step.mapping_id,
                    event="policy_compatibility",
                    status=ss.status,
                    extra={"decision": compat_msg, "outcome": "succeeded"},
                )
            parent_id, source, dep_detail = self._resolve_destination_parent(step, state, dry_run=dry_run)
            ss.resolved_destination_parent_item_id = parent_id
            ss.parent_resolution_source = source
            log_execution_bridge_step(
                snapshot_id=plan.snapshot_id,
                run_id=plan.run_id,
                plan_id=plan.plan_id,
                step_id=step.step_id,
                mapping_id=step.mapping_id,
                event="dependency_resolved",
                status=ss.status,
                extra={
                    "parent_resolution_source": source,
                    "parent_resolution_detail": dep_detail,
                    "resolved_destination_parent_item_id": parent_id,
                    "step_type": step.step_type,
                },
                warn=(source == "missing"),
            )
            if source == "missing":
                ss.status = "blocked"
                ss.outcome = "failed"
                ss.detail = dep_detail
                if plan.policy.stop_on_error:
                    break
                continue
            exists, reason = self._preflight_existing_destination(
                step,
                graph_client=graph_client,
            )
            if exists and step.step_type in ("copy_item", "move_item") and plan.policy.file_conflict_policy == "skip":
                ss.status = "skipped"
                ss.outcome = "skipped_existing"
                ss.detail = f"destination already exists; skipped by file_conflict_policy=skip ({reason})"
                ss.compatibility_decision = "file_conflict_skip_existing"
                log_execution_bridge_step(
                    snapshot_id=plan.snapshot_id,
                    run_id=plan.run_id,
                    plan_id=plan.plan_id,
                    step_id=step.step_id,
                    mapping_id=step.mapping_id,
                    event="policy_compatibility",
                    status=ss.status,
                    extra={"outcome": ss.outcome, "decision": ss.compatibility_decision, "reason": reason},
                )
                state.completed_step_ids.append(step.step_id)
                continue
            if exists and step.step_type == "create_folder" and plan.policy.folder_conflict_policy == "merge":
                ss.status = "ok"
                ss.outcome = "succeeded"
                ss.detail = f"destination folder already exists; merge-compatible success ({reason})"
                ss.compatibility_decision = "folder_conflict_merge_existing"
                log_execution_bridge_step(
                    snapshot_id=plan.snapshot_id,
                    run_id=plan.run_id,
                    plan_id=plan.plan_id,
                    step_id=step.step_id,
                    mapping_id=step.mapping_id,
                    event="policy_compatibility",
                    status=ss.status,
                    extra={"outcome": ss.outcome, "decision": ss.compatibility_decision, "reason": reason},
                )
                state.completed_step_ids.append(step.step_id)
                continue

            manifest = self._manifest_for_step(plan, step, resolved_parent_id=parent_id)
            proxy.set_active_step(step.step_id)
            ss.status = "running"
            result: RunManifestResult = self._runner(
                manifest,
                dry_run=dry_run,
                verify_integrity=bool(plan.policy.integrity_verify),
                graph_client=proxy if graph_client is not None else None,
            )
            self._update_step_state_from_result(ss, result)
            if step.step_type == "create_folder" and ss.status == "ok" and not dry_run:
                ss.output_item_id = proxy.consume_created_item_id(step.step_id)
                if not ss.output_item_id:
                    ss.status = "failed"
                    ss.outcome = "failed"
                    ss.detail = "create_folder completed but no created folder id was captured"
            elif step.step_type == "create_folder" and ss.status == "dry_run":
                ss.output_item_id = f"dryrun-created:{step.step_id}"

            log_execution_bridge_step(
                snapshot_id=plan.snapshot_id,
                run_id=plan.run_id,
                plan_id=plan.plan_id,
                step_id=step.step_id,
                mapping_id=step.mapping_id,
                event="step_finished",
                status=ss.status,
                extra={
                    "backend_status": ss.backend_status,
                    "output_item_id": ss.output_item_id,
                    "outcome": ss.outcome,
                    "detail": ss.detail[:300],
                },
                warn=(ss.status in ("failed", "blocked")),
            )
            state.completed_step_ids.append(step.step_id)
            if ss.status == "failed" and bool(plan.policy.stop_on_error):
                break
        return state

    def _compatibility_gate(self, step: ExecutionStep) -> tuple[str, str]:
        if step.step_type != "move_item":
            return "none", ""
        if self._allow_move_as_copy:
            return "downgrade", "move_item downgraded to copy behavior by allow_move_as_copy=true"
        return "block", "move_item blocked: source deletion semantics are not implemented in bridge phase"

    def _resolve_destination_parent(
        self,
        step: ExecutionStep,
        state: ExecutionBridgeRuntimeState,
        *,
        dry_run: bool,
    ) -> tuple[str, ParentResolutionSource, str]:
        direct = str(step.destination_parent_item_id or "").strip()
        if direct:
            return direct, "direct", "destination_parent_item_id present on step"
        parent_step_id = str(step.parent_step_id or "").strip()
        if not parent_step_id:
            return "", "missing", "step has neither destination_parent_item_id nor parent_step_id"
        parent_state = state.step_states.get(parent_step_id)
        if parent_state is None:
            return "", "missing", f"parent_step_id {parent_step_id!r} not found in runtime state"
        if parent_state.status == "ok" and parent_state.output_item_id:
            return parent_state.output_item_id, "parent_step_output", f"resolved from parent step {parent_step_id}"
        if dry_run and parent_state.status in ("dry_run", "ok"):
            return f"dryrun-parent:{parent_step_id}", "parent_step_dry_run", "synthetic parent id for dry-run dependency"
        return "", "missing", (
            f"parent step {parent_step_id} has no output id (status={parent_state.status!r}); "
            "cannot resolve dependent destination parent"
        )

    def _manifest_for_step(
        self,
        plan: ExecutionPlan,
        step: ExecutionStep,
        *,
        resolved_parent_id: str,
    ) -> dict[str, Any]:
        opts = {
            "verify_integrity": bool(plan.policy.integrity_verify),
            "graph_copy_conflict_behavior": "fail",
            "graph_mkdir_conflict_behavior": "fail",
        }
        if plan.policy.file_conflict_policy == "replace":
            opts["graph_copy_conflict_behavior"] = "replace"
        if plan.policy.rename_on_conflict:
            opts["graph_copy_conflict_behavior"] = "rename"
        if step.step_type == "create_folder":
            return {
                "manifest_version": 2,
                "execution_options": opts,
                "proposed_folder_steps": [
                    {
                        "index": 0,
                        "operation": "ensure_folder",
                        "folder_name": step.destination_name,
                        "destination_path": step.destination_path,
                        "destination_drive_id": step.destination_drive_id,
                        "destination_parent_item_id": resolved_parent_id,
                    }
                ],
                "transfer_steps": [],
            }
        # move_item / copy_item / verify_only use transfer step shape.
        return {
            "manifest_version": 2,
            "execution_options": opts,
            "proposed_folder_steps": [],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "is_source_folder": bool(step.item_type == "folder"),
                    "source_name": step.destination_name,
                    "destination_name": step.destination_name,
                    "source_path": "",
                    "destination_path": step.destination_path,
                    "source_drive_id": step.source_drive_id,
                    "source_item_id": step.source_item_id,
                    "destination_drive_id": step.destination_drive_id,
                    "destination_item_id": resolved_parent_id,
                }
            ],
        }

    @staticmethod
    def _preflight_existing_destination(step: ExecutionStep, *, graph_client: Any | None) -> tuple[bool, str]:
        """Best-effort destination-exists probe for policy compatibility."""
        dst = str(step.destination_path or "").strip()
        if not dst:
            return False, "missing_destination_path"
        if is_absolute_local_path(dst):
            return Path(dst).exists(), "local_path_exists_check"
        if graph_client is None:
            return False, "graph_client_not_available"
        get_by_path = getattr(graph_client, "get_drive_item_by_path", None)
        if callable(get_by_path):
            try:
                hit = get_by_path(str(step.destination_drive_id or ""), dst)
                if isinstance(hit, dict) and str(hit.get("id") or "").strip():
                    return True, "graph_get_drive_item_by_path_hit"
            except Exception:
                return False, "graph_get_drive_item_by_path_error"
        return False, "graph_path_probe_unavailable"

    @staticmethod
    def _update_step_state_from_result(step_state: BridgeStepState, result: RunManifestResult) -> None:
        records = list(result.records or [])
        step_state.backend_records = [
            {
                "phase": r.phase,
                "step_index": r.step_index,
                "status": r.status,
                "detail": r.detail,
                "attempts": r.attempts,
                "integrity_verified": r.integrity_verified,
            }
            for r in records
        ]
        if not records:
            step_state.status = "failed"
            step_state.backend_status = "no_record"
            step_state.detail = "backend returned no step records"
            step_state.outcome = "failed"
            return
        rec = records[-1]
        step_state.backend_status = rec.status
        step_state.detail = rec.detail
        if rec.status in ("ok", "failed", "skipped", "dry_run"):
            step_state.status = rec.status  # type: ignore[assignment]
        else:
            step_state.status = "failed"
        if step_state.status in ("ok", "dry_run"):
            step_state.outcome = "succeeded"
        elif step_state.status == "failed":
            step_state.outcome = "failed"
        elif step_state.status == "blocked":
            step_state.outcome = "blocked_compatibility"
        elif step_state.status == "skipped":
            step_state.outcome = "failed"

