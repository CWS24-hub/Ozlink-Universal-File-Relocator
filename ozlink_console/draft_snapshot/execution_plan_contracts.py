"""ExecutionPlan contracts and deterministic ordering helpers (no runner wiring)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.contracts import AssignmentMode, ItemType, SourceRetentionPolicy, new_snapshot_id

ExecutionStepState = Literal["pending", "ready", "blocked"]
ExecutionStepType = Literal["move_item", "copy_item", "create_folder", "verify_only"]

FileConflictPolicy = Literal["skip", "fail", "replace"]
FolderConflictPolicy = Literal["merge", "fail"]


@dataclass
class ExecutionPolicy:
    """Explicit defaults for materialized plans; runner may override later."""

    conflict_behavior: Literal["rename", "fail", "replace"] = "fail"
    file_conflict_policy: FileConflictPolicy = "skip"
    folder_conflict_policy: FolderConflictPolicy = "merge"
    dry_run: bool = False
    continue_on_error: bool = True
    max_parallel_steps: int = 1
    allow_overwrite: bool = False
    rename_on_conflict: bool = False
    stop_on_error: bool = False
    integrity_verify: bool = True
    source_retention_policy: SourceRetentionPolicy = "retain"
    notes: list[str] = field(default_factory=list)


@dataclass
class ExecutionStep:
    step_id: str
    step_type: ExecutionStepType
    mapping_id: str
    item_type: ItemType
    assignment_mode: AssignmentMode
    source_drive_id: str
    source_item_id: str
    destination_drive_id: str
    destination_parent_item_id: str
    destination_name: str
    destination_path: str
    depth: int = 0
    state: ExecutionStepState = "pending"
    parent_step_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionPlanSummary:
    total_steps: int = 0
    folder_steps: int = 0
    file_steps: int = 0
    blocked_steps: int = 0
    move_steps: int = 0
    copy_steps: int = 0


@dataclass
class PlanMaterializationSummary:
    """Counts for audit; unresolved/ambiguous remain visible here (not as executable steps)."""

    mapping_items_total: int = 0
    proposed_folder_items_total: int = 0
    mapping_eligible_resolved_or_skipped: int = 0
    mapping_steps_emitted: int = 0
    proposed_eligible_resolved_or_skipped: int = 0
    proposed_folder_steps_emitted: int = 0
    unresolved_mapping_ids: list[str] = field(default_factory=list)
    ambiguous_mapping_ids: list[str] = field(default_factory=list)
    skipped_mapping_ids: list[str] = field(default_factory=list)
    excluded_mapping_not_file: list[str] = field(default_factory=list)
    excluded_mapping_missing_ids: list[str] = field(default_factory=list)
    unresolved_proposed_folder_ids: list[str] = field(default_factory=list)
    ambiguous_proposed_folder_ids: list[str] = field(default_factory=list)
    skipped_proposed_folder_ids: list[str] = field(default_factory=list)
    excluded_proposed_missing_ids: list[str] = field(default_factory=list)
    destination_folder_chain_paths: list[str] = field(default_factory=list)
    destination_chain_existing_folder_count: int = 0
    destination_chain_mkdir_steps: int = 0
    destination_chain_skipped_duplicate_proposed: int = 0
    chain_resolution_errors: list[str] = field(default_factory=list)


@dataclass
class ExecutionPlan:
    plan_id: str
    snapshot_id: str
    run_id: str
    resolver_name: str
    policy: ExecutionPolicy = field(default_factory=lambda: default_materialization_policy())
    steps: list[ExecutionStep] = field(default_factory=list)
    summary: ExecutionPlanSummary = field(default_factory=ExecutionPlanSummary)
    materialization: PlanMaterializationSummary = field(default_factory=PlanMaterializationSummary)
    plan_hash: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def new_execution_plan_id() -> str:
    return new_snapshot_id()


def default_materialization_policy() -> ExecutionPolicy:
    """Policy defaults attached at plan materialization (execution unchanged until wired)."""
    return ExecutionPolicy(
        conflict_behavior="fail",
        file_conflict_policy="skip",
        folder_conflict_policy="merge",
        dry_run=False,
        continue_on_error=True,
        max_parallel_steps=1,
        allow_overwrite=False,
        rename_on_conflict=False,
        stop_on_error=False,
        integrity_verify=True,
        source_retention_policy="retain",
    )


def compute_execution_plan_hash(payload: dict[str, Any]) -> str:
    """Deterministic SHA-256 over canonical JSON (sorted keys, stable separators)."""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def execution_step_sort_key(step: ExecutionStep) -> tuple[int, int, str, str, str]:
    """
    Deterministic order:
    1) folders before files
    2) shallow before deep
    3) parent path/name before child path/name (lexicographic as stable fallback)
    4) mapping_id / step_id as final tie-breakers
    """
    is_file = 1 if step.item_type == "file" else 0
    depth = int(step.depth or 0)
    path = str(step.destination_path or "").replace("\\", "/")
    return (is_file, depth, path, str(step.mapping_id or ""), str(step.step_id or ""))


def sort_execution_steps(steps: list[ExecutionStep]) -> list[ExecutionStep]:
    return sorted(steps, key=execution_step_sort_key)


def summarize_execution_plan_steps(steps: list[ExecutionStep]) -> ExecutionPlanSummary:
    s = ExecutionPlanSummary(total_steps=len(steps))
    for step in steps:
        if step.item_type == "folder":
            s.folder_steps += 1
        else:
            s.file_steps += 1
        if step.state == "blocked":
            s.blocked_steps += 1
        if step.step_type == "move_item":
            s.move_steps += 1
        elif step.step_type == "copy_item":
            s.copy_steps += 1
    return s

