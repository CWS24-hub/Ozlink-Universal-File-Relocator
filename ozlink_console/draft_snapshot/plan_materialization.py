"""Materialize ExecutionPlan from ResolvedSnapshot (no runner / transfer execution)."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Any

from ozlink_console.draft_snapshot.contracts import AssignmentMode, CanonicalMappingItem, CanonicalSubmittedSnapshot, utc_now_iso
from ozlink_console.draft_snapshot.execution_plan_contracts import (
    ExecutionPlan,
    ExecutionPlanSummary,
    ExecutionPolicy,
    ExecutionStep,
    PlanMaterializationSummary,
    compute_execution_plan_hash,
    default_materialization_policy,
    execution_step_sort_key,
    new_execution_plan_id,
    sort_execution_steps,
    summarize_execution_plan_steps,
)
from ozlink_console.draft_snapshot.plan_builder_interface import BuildExecutionPlanRequest
from ozlink_console.draft_snapshot.resolution_contracts import ResolvedSnapshot
from ozlink_console.draft_snapshot.resolver_service import GraphResolutionClient
from ozlink_console.draft_snapshot.run_log import log_plan_build_phase
from ozlink_console.planned_move_graph_resolve import is_internal_proposed_destination_item_id


def _norm_rel_path(p: str) -> str:
    s = str(p or "").replace("\\", "/").strip()
    while "//" in s:
        s = s.replace("//", "/")
    return s.strip("/")


def _parent_dir_of_file_destination(destination_path: str) -> str:
    n = _norm_rel_path(destination_path)
    if not n or "/" not in n:
        return ""
    return n.rsplit("/", 1)[0]


def _dirname_rel(folder_path: str) -> str:
    n = _norm_rel_path(folder_path)
    if not n or "/" not in n:
        return ""
    return n.rsplit("/", 1)[0]


def _chain_mapping_id(prefix_path: str) -> str:
    return f"dst_chain:{_norm_rel_path(prefix_path).replace('/', '|')}"


def _stable_step_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:32]


def _destination_drive_id_for_chain_materialization(snapshot: CanonicalSubmittedSnapshot) -> str:
    """
    ``library_drive_id`` is often empty on legacy bundle imports even when rows include
    ``DestinationDriveId``. The resolver already falls back to per-item drives; destination mkdir
    chain materialization must do the same or deferred-parent file mappings never get a
    ``parent_step_id`` and are dropped from the executable plan (breaking scoped seeds).
    """
    d = str(snapshot.destination.site_library.library_drive_id or "").strip()
    if d:
        return d
    for m in snapshot.mapping_items:
        cand = str(m.destination_graph_drive_id or "").strip()
        if cand:
            return cand
    for p in snapshot.proposed_folder_items:
        cand = str(p.destination_drive_id or "").strip()
        if cand:
            return cand
    return ""


def build_destination_chain_steps(
    graph_client: GraphResolutionClient,
    snapshot: CanonicalSubmittedSnapshot,
    plan_id: str,
    sorted_prefixes: list[str],
    mat: PlanMaterializationSummary,
) -> tuple[list[ExecutionStep], dict[str, str], dict[str, str], PlanMaterializationSummary]:
    """
    Resolve each library-relative folder prefix on the destination drive.

    Existing folders get Graph ids recorded. Missing segments become executable ``create_folder``
    steps ordered shallow → deep, using ``destination_parent_item_id`` when the parent already
    exists on Graph, or ``parent_step_id`` when the parent is created earlier in the same plan.
    """
    drive = _destination_drive_id_for_chain_materialization(snapshot)
    if not drive:
        mat.chain_resolution_errors.append("missing_destination_drive_id")
        return [], {}, {}, mat

    root = graph_client.get_drive_root_item(drive)
    root_id = str(root.get("id") or "").strip() if isinstance(root, dict) else ""
    if not root_id:
        mat.chain_resolution_errors.append("missing_destination_root_item")
        return [], {}, {}, mat

    path_to_item_id: dict[str, str] = {}
    path_to_planned_step_id: dict[str, str] = {}
    steps: list[ExecutionStep] = []

    for prefix in sorted_prefixes:
        hit = graph_client.get_drive_item_by_path(drive, prefix)
        hid = str(hit.get("id") or "").strip() if isinstance(hit, dict) else ""
        if hid:
            path_to_item_id[prefix] = hid
            mat.destination_chain_existing_folder_count += 1
            continue

        parent_path = _dirname_rel(prefix)
        seg = prefix.rsplit("/", 1)[-1] if "/" in prefix else prefix
        seg = str(seg or "").strip()
        if not seg:
            mat.chain_resolution_errors.append(f"empty_chain_segment:{prefix}")
            continue

        parent_graph = ""
        parent_step_id = ""
        if not parent_path:
            parent_graph = root_id
        else:
            parent_graph = path_to_item_id.get(parent_path, "")
            parent_step_id = path_to_planned_step_id.get(parent_path, "")
            if not parent_graph and not parent_step_id:
                mat.chain_resolution_errors.append(f"chain_parent_unresolved:{prefix}")
                continue

        step_id = _stable_step_id(plan_id, "dst_chain_mkdir", prefix, drive, parent_graph, parent_step_id, seg)
        path_to_planned_step_id[prefix] = step_id
        steps.append(
            ExecutionStep(
                step_id=step_id,
                step_type="create_folder",
                mapping_id=_chain_mapping_id(prefix),
                item_type="folder",
                assignment_mode="copy_recursive",
                source_drive_id="",
                source_item_id="",
                destination_drive_id=drive,
                destination_parent_item_id=parent_graph,
                destination_name=seg,
                destination_path=prefix,
                depth=prefix.count("/"),
                state="pending",
                parent_step_id=parent_step_id,
                metadata={
                    "item_kind": "destination_chain",
                    "chain_prefix": prefix,
                },
            )
        )
        mat.destination_chain_mkdir_steps += 1

    return steps, path_to_item_id, path_to_planned_step_id, mat


def _folder_chain_prefixes(full_folder_path: str) -> list[str]:
    """Non-empty path prefixes for a folder path 'a/b/c' -> ['a','a/b','a/b/c']."""
    n = _norm_rel_path(full_folder_path)
    if not n:
        return []
    parts = [x for x in n.split("/") if x]
    out: list[str] = []
    acc: list[str] = []
    for part in parts:
        acc.append(part)
        out.append("/".join(acc))
    return out


def _mapping_by_id(snapshot: CanonicalSubmittedSnapshot) -> dict[str, CanonicalMappingItem]:
    return {m.mapping_id: m for m in snapshot.mapping_items}


def _assignment_for_step(mode: AssignmentMode) -> tuple[str, str]:
    if mode in ("move", "move_recursive"):
        return ("move_item", mode)
    if mode in ("copy", "copy_recursive"):
        return ("copy_item", mode)
    if mode == "unknown":
        return ("copy_item", "copy")
    return ("copy_item", mode)


def _normalize_materialization_policy(p: ExecutionPolicy) -> ExecutionPolicy:
    d = asdict(p)
    if d.get("stop_on_error"):
        d["continue_on_error"] = False
    else:
        d["continue_on_error"] = True
    return ExecutionPolicy(**d)


def _proposed_destination_path(rs: ResolvedSnapshot, mapping_id: str) -> str:
    for p in rs.snapshot.proposed_folder_items:
        if str(p.proposed_id or "") == str(mapping_id or ""):
            return _norm_rel_path(p.destination_path)
    return ""


class ResolvedSnapshotExecutionPlanBuilder:
    """
    Builds an ExecutionPlan from a ResolvedSnapshot using only execution-eligible items.

    - Unresolved / ambiguous items never become executable steps (tracked in materialization summary).
    - With ``graph_client`` on the request, every library-relative destination folder prefix is
      resolved on the destination drive; missing segments become ``create_folder`` steps with
      concrete Graph parent ids or ``parent_step_id`` when the parent is created earlier in-plan.
    - Without ``graph_client``, proposed-folder steps and resolver parent ids are used (legacy).
    - Proposed-folder steps are skipped when the same path is already on Graph or already planned
      via the destination chain walk.
    """

    def build_plan(self, request: BuildExecutionPlanRequest) -> ExecutionPlan:
        rs = request.resolved_snapshot
        run_id = str(request.run_id or rs.run_id or "").strip()
        policy = _normalize_materialization_policy(request.policy)
        plan_id = new_execution_plan_id()
        log_plan_build_phase(
            phase="start",
            snapshot_id=rs.snapshot_id,
            run_id=run_id,
            plan_id=plan_id,
            extra={"resolver_name": rs.resolver_name},
        )

        mat = PlanMaterializationSummary(
            mapping_items_total=len(rs.mapping_results),
            proposed_folder_items_total=len(rs.proposed_folder_results),
            unresolved_mapping_ids=list(rs.summary.unresolved_mapping_ids),
            ambiguous_mapping_ids=list(rs.summary.ambiguous_mapping_ids),
            unresolved_proposed_folder_ids=[
                r.mapping_id for r in rs.proposed_folder_results if r.status == "unresolved"
            ],
            ambiguous_proposed_folder_ids=[
                r.mapping_id for r in rs.proposed_folder_results if r.status == "ambiguous"
            ],
        )

        mapping_by_id = _mapping_by_id(rs.snapshot)
        steps: list[ExecutionStep] = []

        chain_paths = self._collect_destination_chain_paths(rs)
        mat.destination_folder_chain_paths = chain_paths

        path_to_item_id: dict[str, str] = {}
        path_to_planned_step_id: dict[str, str] = {}
        if request.graph_client is not None:
            chain_steps, path_to_item_id, path_to_planned_step_id, mat = build_destination_chain_steps(
                request.graph_client,
                rs.snapshot,
                plan_id,
                chain_paths,
                mat,
            )
            steps.extend(chain_steps)

        folder_steps, mat = self._build_folder_steps(rs, plan_id, mat, path_to_item_id, path_to_planned_step_id)
        steps.extend(folder_steps)
        mat.skipped_proposed_folder_ids = sorted(
            {r.mapping_id for r in rs.proposed_folder_results if r.status == "skipped"}
        )

        file_steps, mat = self._build_file_steps(
            rs, mapping_by_id, plan_id, mat, path_to_item_id, path_to_planned_step_id
        )
        steps.extend(file_steps)
        mat.skipped_mapping_ids = sorted({r.mapping_id for r in rs.mapping_results if r.status == "skipped"})

        ordered = sort_execution_steps(steps)
        summary = summarize_execution_plan_steps(ordered)

        meta = self._plan_metadata(
            plan_id=plan_id,
            snapshot_id=rs.snapshot_id,
            run_id=run_id,
            resolver_name=rs.resolver_name,
            policy=policy,
            summary=summary,
            materialization=mat,
            steps=ordered,
        )
        plan_hash = compute_execution_plan_hash(meta["plan_hash_payload"])

        plan = ExecutionPlan(
            plan_id=plan_id,
            snapshot_id=rs.snapshot_id,
            run_id=run_id,
            resolver_name=rs.resolver_name,
            policy=policy,
            steps=ordered,
            summary=summary,
            materialization=mat,
            plan_hash=plan_hash,
            metadata=meta,
            notes=list(rs.notes),
        )

        log_plan_build_phase(
            phase="end",
            snapshot_id=rs.snapshot_id,
            run_id=run_id,
            plan_id=plan_id,
            extra={
                "total_steps": summary.total_steps,
                "folder_steps": summary.folder_steps,
                "file_steps": summary.file_steps,
                "mapping_steps_emitted": mat.mapping_steps_emitted,
                "proposed_folder_steps_emitted": mat.proposed_folder_steps_emitted,
                "mapping_resolution_ok_count": mat.mapping_eligible_resolved_or_skipped,
                "proposed_resolution_ok_count": mat.proposed_eligible_resolved_or_skipped,
                "resolved_count": rs.summary.resolved_count,
                "unresolved_count": rs.summary.unresolved_count,
                "ambiguous_count": rs.summary.ambiguous_count,
                "skipped_count": rs.summary.skipped_count,
                "destination_chain_mkdir_steps": mat.destination_chain_mkdir_steps,
                "destination_chain_existing_folders": mat.destination_chain_existing_folder_count,
            },
        )
        return plan

    def _build_folder_steps(
        self,
        rs: ResolvedSnapshot,
        plan_id: str,
        mat: PlanMaterializationSummary,
        path_to_item_id: dict[str, str],
        path_to_planned_step_id: dict[str, str],
    ) -> tuple[list[ExecutionStep], PlanMaterializationSummary]:
        steps: list[ExecutionStep] = []
        for r in rs.proposed_folder_results:
            if r.status == "unresolved":
                continue
            if r.status == "ambiguous":
                continue
            if r.status not in ("resolved", "skipped"):
                continue
            mat.proposed_eligible_resolved_or_skipped += 1
            drive = str(r.destination_drive_id or "").strip()
            parent = str(r.destination_parent_item_id or "").strip()
            if parent and is_internal_proposed_destination_item_id(parent):
                parent = ""
            name = str(r.destination_name or "").strip()
            if not drive or not parent or not name:
                mat.excluded_proposed_missing_ids.append(r.mapping_id)
                continue
            dest_path = _norm_rel_path(str(r.raw.get("destination_path") or "")) if isinstance(r.raw, dict) else ""
            if not dest_path:
                dest_path = _proposed_destination_path(rs, r.mapping_id)
            if not dest_path:
                dest_path = name
            if dest_path in path_to_item_id or dest_path in path_to_planned_step_id:
                mat.destination_chain_skipped_duplicate_proposed += 1
                continue
            depth = dest_path.count("/") if dest_path else 0
            sid = _stable_step_id(plan_id, "create_folder", r.mapping_id, drive, parent, name, dest_path)
            steps.append(
                ExecutionStep(
                    step_id=sid,
                    step_type="create_folder",
                    mapping_id=r.mapping_id,
                    item_type="folder",
                    assignment_mode="copy_recursive",
                    source_drive_id="",
                    source_item_id="",
                    destination_drive_id=drive,
                    destination_parent_item_id=parent,
                    destination_name=name,
                    destination_path=dest_path or name,
                    depth=depth,
                    state="pending",
                    metadata={
                        "resolution_status": r.status,
                        "item_kind": "proposed_folder",
                    },
                )
            )
            mat.proposed_folder_steps_emitted += 1

        return steps, mat

    def _build_file_steps(
        self,
        rs: ResolvedSnapshot,
        mapping_by_id: dict[str, CanonicalMappingItem],
        plan_id: str,
        mat: PlanMaterializationSummary,
        path_to_item_id: dict[str, str],
        path_to_planned_step_id: dict[str, str],
    ) -> tuple[list[ExecutionStep], PlanMaterializationSummary]:
        steps: list[ExecutionStep] = []
        for r in rs.mapping_results:
            if r.status == "unresolved":
                continue
            if r.status == "ambiguous":
                continue
            if r.status not in ("resolved", "skipped"):
                continue
            mat.mapping_eligible_resolved_or_skipped += 1
            if r.item_type != "file":
                mat.excluded_mapping_not_file.append(r.mapping_id)
                continue
            src_drive = str(r.source_drive_id or "").strip()
            src_item = str(r.source_item_id or "").strip()
            dst_drive = str(r.destination_drive_id or "").strip()
            dst_parent = str(r.destination_parent_item_id or "").strip()
            if dst_parent and is_internal_proposed_destination_item_id(dst_parent):
                dst_parent = ""
            dst_name = str(r.destination_name or "").strip()
            if not src_drive or not src_item or not dst_drive or not dst_name:
                mat.excluded_mapping_missing_ids.append(r.mapping_id)
                continue
            item = mapping_by_id.get(r.mapping_id)
            dest_path = _norm_rel_path(item.destination_path if item else "")
            if not dest_path:
                dest_path = _norm_rel_path(f"{dst_name}")
            parent_path = _parent_dir_of_file_destination(dest_path)
            file_parent_step_id = ""
            if parent_path:
                cid = path_to_item_id.get(parent_path, "")
                psid = path_to_planned_step_id.get(parent_path, "")
                if cid:
                    dst_parent = cid
                elif psid:
                    dst_parent = ""
                    file_parent_step_id = psid
            if parent_path:
                if not dst_parent and not file_parent_step_id:
                    mat.excluded_mapping_missing_ids.append(r.mapping_id)
                    continue
            elif not dst_parent:
                mat.excluded_mapping_missing_ids.append(r.mapping_id)
                continue
            depth = dest_path.count("/") if dest_path else 0
            mode: AssignmentMode = item.assignment_mode if item else "copy"
            step_type, _ = _assignment_for_step(mode)
            sid = _stable_step_id(
                plan_id, step_type, r.mapping_id, src_item, dst_parent, file_parent_step_id, dst_name, dest_path
            )
            steps.append(
                ExecutionStep(
                    step_id=sid,
                    step_type=step_type,  # type: ignore[arg-type]
                    mapping_id=r.mapping_id,
                    item_type="file",
                    assignment_mode=mode,
                    source_drive_id=src_drive,
                    source_item_id=src_item,
                    destination_drive_id=dst_drive,
                    destination_parent_item_id=dst_parent,
                    destination_name=dst_name,
                    destination_path=dest_path,
                    depth=depth,
                    state="pending",
                    parent_step_id=file_parent_step_id,
                    metadata={
                        "resolution_status": r.status,
                        "item_kind": "mapping_item",
                        "destination_parent_path": parent_path,
                        "source_path": str(item.source_path or "") if item else "",
                    },
                )
            )
            mat.mapping_steps_emitted += 1
        return steps, mat

    def _collect_destination_chain_paths(self, rs: ResolvedSnapshot) -> list[str]:
        """Sorted unique folder path prefixes from canonical snapshot paths (audit / future mkdir)."""
        paths: set[str] = set()
        for p in rs.snapshot.proposed_folder_items:
            pth = _norm_rel_path(p.destination_path)
            if pth:
                paths.update(_folder_chain_prefixes(pth))
        for m in rs.snapshot.mapping_items:
            if m.item_type != "file":
                continue
            pd = _parent_dir_of_file_destination(m.destination_path)
            if pd:
                paths.update(_folder_chain_prefixes(pd))
        return sorted(paths, key=lambda x: (x.count("/"), x))

    def _plan_metadata(
        self,
        *,
        plan_id: str,
        snapshot_id: str,
        run_id: str,
        resolver_name: str,
        policy: ExecutionPolicy,
        summary: ExecutionPlanSummary,
        materialization: PlanMaterializationSummary,
        steps: list[ExecutionStep],
    ) -> dict[str, Any]:
        policy_d = asdict(policy)
        step_rows = [
            {
                "step_id": s.step_id,
                "step_type": s.step_type,
                "mapping_id": s.mapping_id,
                "item_type": s.item_type,
                "assignment_mode": s.assignment_mode,
                "source_drive_id": s.source_drive_id,
                "source_item_id": s.source_item_id,
                "destination_drive_id": s.destination_drive_id,
                "destination_parent_item_id": s.destination_parent_item_id,
                "destination_name": s.destination_name,
                "destination_path": s.destination_path,
                "depth": s.depth,
                "state": s.state,
                "parent_step_id": s.parent_step_id,
                "metadata": dict(sorted((s.metadata or {}).items())),
            }
            for s in sorted(steps, key=execution_step_sort_key)
        ]
        payload = {
            "plan_id": plan_id,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "resolver_name": resolver_name,
            "policy": dict(sorted(policy_d.items())),
            "summary": asdict(summary),
            "materialization": {
                **asdict(materialization),
                "unresolved_mapping_ids": sorted(materialization.unresolved_mapping_ids),
                "ambiguous_mapping_ids": sorted(materialization.ambiguous_mapping_ids),
                "skipped_mapping_ids": sorted(materialization.skipped_mapping_ids),
                "excluded_mapping_not_file": sorted(materialization.excluded_mapping_not_file),
                "excluded_mapping_missing_ids": sorted(materialization.excluded_mapping_missing_ids),
                "unresolved_proposed_folder_ids": sorted(materialization.unresolved_proposed_folder_ids),
                "ambiguous_proposed_folder_ids": sorted(materialization.ambiguous_proposed_folder_ids),
                "skipped_proposed_folder_ids": sorted(materialization.skipped_proposed_folder_ids),
                "excluded_proposed_missing_ids": sorted(materialization.excluded_proposed_missing_ids),
                "destination_folder_chain_paths": list(materialization.destination_folder_chain_paths),
                "chain_resolution_errors": sorted(materialization.chain_resolution_errors),
            },
            "steps": step_rows,
        }
        return {
            "plan_id": plan_id,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "resolver_name": resolver_name,
            "schema": "ozlink.execution_plan_materialization/v1",
            "built_at_utc": utc_now_iso(),
            "plan_hash_payload": payload,
        }


def _subset_plan_from_steps(plan: ExecutionPlan, filtered: list[ExecutionStep], audit_note: str) -> ExecutionPlan | None:
    """Rebuild hash/metadata/notes for a step subset (shared by strict and dependency closure)."""
    if not filtered:
        return None
    ordered = sort_execution_steps(filtered)
    summary = summarize_execution_plan_steps(ordered)
    builder = ResolvedSnapshotExecutionPlanBuilder()
    meta = builder._plan_metadata(
        plan_id=plan.plan_id,
        snapshot_id=plan.snapshot_id,
        run_id=plan.run_id,
        resolver_name=plan.resolver_name,
        policy=plan.policy,
        summary=summary,
        materialization=plan.materialization,
        steps=ordered,
    )
    plan_hash = compute_execution_plan_hash(meta["plan_hash_payload"])
    notes = list(plan.notes)
    if audit_note and audit_note not in notes:
        notes.append(audit_note)
    return ExecutionPlan(
        plan_id=plan.plan_id,
        snapshot_id=plan.snapshot_id,
        run_id=plan.run_id,
        resolver_name=plan.resolver_name,
        policy=plan.policy,
        steps=ordered,
        summary=summary,
        materialization=plan.materialization,
        plan_hash=plan_hash,
        metadata=meta,
        notes=notes,
    )


def subset_execution_plan_by_mapping_ids(
    plan: ExecutionPlan,
    allowed_mapping_ids: frozenset[str],
) -> ExecutionPlan | None:
    """
    Keep only steps whose ``mapping_id`` is in ``allowed_mapping_ids``.

    Does not add parent chain or mkdir steps — callers rely on explicit selection only.
    Returns ``None`` when no steps match.
    """
    if not allowed_mapping_ids:
        return None
    filtered = [s for s in plan.steps if str(s.mapping_id or "") in allowed_mapping_ids]
    note = "scoped_execution_mapping_ids:" + ",".join(sorted(allowed_mapping_ids))
    return _subset_plan_from_steps(plan, filtered, note)


def subset_execution_plan_by_dependency_closure(
    plan: ExecutionPlan,
    seed_mapping_ids: frozenset[str],
) -> ExecutionPlan | None:
    """
    Subset plan to seed mapping_ids plus structural ancestors (``parent_step_id``) and
    required destination-path ``create_folder`` steps for seeded file transfers.

    For each required directory prefix, prefers ``dst_chain:*`` steps over proposed-folder
    steps for the same normalized path. Sibling mappings outside the closure are excluded.
    """
    if not seed_mapping_ids:
        return None
    steps_by_id: dict[str, ExecutionStep] = {str(s.step_id): s for s in plan.steps}
    closed: set[str] = set()
    stack: list[str] = [
        str(s.step_id) for s in plan.steps if str(s.mapping_id or "") in seed_mapping_ids
    ]
    if not stack:
        return None

    while stack:
        sid = stack.pop()
        if sid not in steps_by_id or sid in closed:
            continue
        closed.add(sid)
        p = str(steps_by_id[sid].parent_step_id or "").strip()
        if p:
            stack.append(p)

    changed = True
    while changed:
        changed = False
        for sid in list(closed):
            st = steps_by_id.get(sid)
            if not st:
                continue
            if str(st.item_type or "") != "file":
                continue
            if str(st.step_type or "") not in ("copy_item", "move_item"):
                continue
            parent_path = _parent_dir_of_file_destination(str(st.destination_path or ""))
            if not parent_path:
                continue
            for pdir in _folder_chain_prefixes(parent_path):
                mid_chain = _chain_mapping_id(pdir)
                chain_steps = [x for x in plan.steps if str(x.mapping_id or "") == mid_chain]
                proposed_steps = [
                    x
                    for x in plan.steps
                    if str(x.step_type or "") == "create_folder"
                    and not str(x.mapping_id or "").startswith("dst_chain:")
                    and _norm_rel_path(str(x.destination_path or "")) == pdir
                ]
                to_add = chain_steps if chain_steps else proposed_steps
                for c in to_add:
                    cid = str(c.step_id)
                    if cid not in closed:
                        closed.add(cid)
                        changed = True
        for sid in list(closed):
            st = steps_by_id.get(sid)
            if not st:
                continue
            p = str(st.parent_step_id or "").strip()
            if p and p not in closed:
                closed.add(p)
                changed = True

    filtered = [s for s in plan.steps if str(s.step_id) in closed]
    note = "scoped_execution_dependency_closure:" + ",".join(sorted(seed_mapping_ids))
    return _subset_plan_from_steps(plan, filtered, note)


def build_execution_plan_from_resolved(
    resolved: ResolvedSnapshot,
    *,
    run_id: str = "",
    policy: ExecutionPolicy | None = None,
    graph_client: GraphResolutionClient | None = None,
) -> ExecutionPlan:
    """Convenience wrapper for :class:`ResolvedSnapshotExecutionPlanBuilder`."""
    req = BuildExecutionPlanRequest(
        resolved_snapshot=resolved,
        run_id=run_id or resolved.run_id,
        policy=policy or default_materialization_policy(),
        graph_client=graph_client,
    )
    return ResolvedSnapshotExecutionPlanBuilder().build_plan(req)
