"""
Sandboxed legacy backup migration: read bundle → stamp identity / Graph ids → write Migrated_* output.

Does not import MainWindow or invoke live planning persist hooks.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ozlink_console.destination_path_bridge import (
    remap_under_visible_library_anchor,
    strip_legacy_internal_root_prefix,
)
from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path
from ozlink_console.sharepoint_destination_overlay_attach import WORKSPACE_ROW_STATE_PLANNED_ONLY

from .library_container_strip import (
    OUTCOME_AMBIGUOUS as LIB_WRAP_AMBIGUOUS,
    OUTCOME_IDENTITY_MISSING as LIB_WRAP_IDENTITY_MISSING,
    OUTCOME_STRIPPED as LIB_WRAP_STRIPPED,
    strip_legacy_library_container_segment,
)
from .live_path_reanchor import (
    GRAPH_ROOT_EMPTY,
    GRAPH_ROOT_NON_EMPTY,
    GRAPH_ROOT_NO_LISTING,
    legacy_anchor_classification_from_reanchor,
    probe_live_destination_graph_root,
    reanchor_manifest_destination_path_against_live_skeleton,
    strip_internal_root_with_log_legacy_only,
)
from .planned_parent_index import (
    align_legacy_top_segment_to_anchor,
    build_planned_parent_path_index,
    lookup_planned_parent,
)
from .shape import is_legacy_shaped_bundle
from .types import (
    ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT,
    ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_ANCESTOR,
    ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_EXACT,
    ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY,
    ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH,
    MigrationConflictRecord,
    MigrationIdentityPreflight,
    MigrationResult,
    migration_identity_complete,
)


def _live_graph_root_probe(
    graph: Any | None, destination_drive_id: str, anchor: str
) -> tuple[list[str], str]:
    """Return live top-level folder names and probe kind (see :func:`probe_live_destination_graph_root`)."""
    if graph is None:
        return [], GRAPH_ROOT_NO_LISTING
    names, kind = probe_live_destination_graph_root(graph, str(destination_drive_id or "").strip())
    if kind == GRAPH_ROOT_NON_EMPTY:
        return names, kind
    if kind == GRAPH_ROOT_EMPTY:
        return [], kind
    an = normalize_manifest_path(str(anchor or "").strip())
    parts = [x for x in an.split("\\") if x]
    if parts:
        return [parts[0]], kind
    return [], kind


def planned_scaffold_live_folder_collision_hint(
    row: dict[str, Any], *, live_resolved_folder: bool
) -> str | None:
    """
    When Graph later confirms a folder at the same path as a prior planned-only scaffold row,
    callers may record a conflict / reconcile branch (duplicate policy — not silent merge).
    """
    if not isinstance(row, dict) or not bool(row.get("LegacyMigrationPlannedScaffoldOnly")):
        return None
    if not live_resolved_folder:
        return None
    return "planned_scaffold_vs_live_folder_same_path"


def _stamp_planned_scaffold_empty_library_row(
    r: dict[str, Any],
    *,
    row_index: int,
    row_kind: str,
    path_excerpt: str,
) -> None:
    """Mark destination path rows when the live Graph library root has no folder children (planning scaffold only)."""
    r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY
    r["LegacyMigrationPlannedScaffoldOnly"] = True
    r["LegacyMigrationRootNotLiveConfirmed"] = True
    r["LegacyMigrationUnresolvedGraphAnchor"] = True
    r["workspace_row_state"] = WORKSPACE_ROW_STATE_PLANNED_ONLY
    r["verification_state"] = "planned_only"
    log_info("legacy_backup_migration_planned_scaffold_created", row_index=row_index, kind=row_kind, path_excerpt=path_excerpt[:160])
    log_info(
        "legacy_backup_migration_root_preserved_as_planned_scaffold",
        row_index=row_index,
        kind=row_kind,
        path_excerpt=path_excerpt[:160],
    )
    log_info("legacy_backup_migration_root_not_live_confirmed", row_index=row_index, kind=row_kind)
    log_info(
        "legacy_backup_migration_live_anchor_absent",
        row_index=row_index,
        kind=row_kind,
        reason="empty_destination_graph_root",
    )


def _apply_status_suffix(status: str, suffix: str, *, default_base: str = "Pending") -> str:
    s = str(status or "").strip()
    base = s if s else default_base
    return base if suffix in base else f"{base}_{suffix}"


def _stamp_allocation_planned_destination_parent(
    r: dict[str, Any],
    *,
    parent_planned_norm: str,
    match_kind: str,
) -> None:
    """Mark allocation row as covered by migrated proposed/planned paths (no fake Graph ids)."""
    r["DestinationParentItemId"] = ""
    r["LegacyMigrationPlannedParentResolved"] = True
    r["LegacyMigrationDestinationParentResolution"] = "planned_parent"
    r["DestinationParentPlannedPath"] = parent_planned_norm
    r["LegacyMigrationPlannedParentMatchKind"] = match_kind
    if match_kind == "exact":
        r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_EXACT
        r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "PlannedParentResolved")
    elif match_kind == "ancestor":
        r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_ANCESTOR
        r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "PlannedParentResolved")
    else:
        r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT
        r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "PlannedParentPartial")


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _rel_graph_path(path: str, anchor: str, *, apply_anchor_remap: bool = True) -> str:
    p = normalize_manifest_path(str(path or "").strip())
    an = normalize_manifest_path(str(anchor or "").strip())
    if apply_anchor_remap and an:
        p = remap_under_visible_library_anchor(p, an)
    p = strip_legacy_internal_root_prefix(p)
    return p.replace("\\", "/")


def _parent_rel(path: str) -> str:
    p = normalize_manifest_path(str(path or "").strip())
    if not p:
        return ""
    parts = [x for x in p.split("\\") if x]
    if len(parts) <= 1:
        return ""
    return normalize_manifest_path("\\".join(parts[:-1])).replace("\\", "/")


def _leaf_name(path: str) -> str:
    p = normalize_manifest_path(str(path or "").strip())
    if not p:
        return ""
    parts = [x for x in p.split("\\") if x]
    return parts[-1] if parts else ""


def _legacy_foreign_hub_before_anchor_remap(stripped_rel_path: str, anchor: str) -> bool:
    """
    Detect a wrong top-level hub *before* remap_under_visible_library_anchor prepends the anchor.

    A multi-segment path whose first segment is not the anchor root but names a foreign hub
    (segment name ending in 'Hub', case-insensitive) is rejected — prepend remap would hide it
    under the selected anchor (e.g. OtherSiteHub\\X → Root3\\OtherSiteHub\\X).
    """
    if not str(anchor or "").strip() or not str(stripped_rel_path or "").strip():
        return False
    parts = [x for x in normalize_manifest_path(stripped_rel_path).split("\\") if x]
    if len(parts) < 2:
        return False
    a_parts = [x for x in normalize_manifest_path(str(anchor).strip()).split("\\") if x]
    if not a_parts:
        return False
    a0 = a_parts[0].lower()
    top = parts[0].lower()
    if top == a0:
        return False
    # Match segment names that end in "hub" (SharePoint hub sites), not arbitrary 2-letter codes like "hr".
    return len(top) > 3 and top.endswith("hub")


def load_bundle_jsons(source_folder: Path) -> tuple[dict[str, Any], list[dict], list[dict], dict[str, Any] | None]:
    sf = Path(source_folder)
    sess_path = sf / "Draft-SessionState.json"
    alloc_path = sf / "Draft-AllocationQueue.json"
    prop_path = sf / "Draft-ProposedFolders.json"
    ws_path = sf / "WorkspaceSnapshot.json"

    session = json.loads(sess_path.read_text(encoding="utf-8"))
    if not isinstance(session, dict):
        raise ValueError("Draft-SessionState.json must be an object")
    raw_a = json.loads(alloc_path.read_text(encoding="utf-8"))
    raw_p = json.loads(prop_path.read_text(encoding="utf-8"))
    allocations = [dict(x) for x in raw_a] if isinstance(raw_a, list) else []
    proposed = [dict(x) for x in raw_p] if isinstance(raw_p, list) else []
    ws: dict[str, Any] | None = None
    if ws_path.is_file():
        try:
            w = json.loads(ws_path.read_text(encoding="utf-8"))
            ws = w if isinstance(w, dict) else None
        except Exception:
            ws = None
    return session, allocations, proposed, ws


def migrate_legacy_backup_folder(
    source_folder: Path,
    output_parent: Path,
    *,
    identity: MigrationIdentityPreflight | None,
    graph: Any | None = None,
    skip_graph_resolution: bool = False,
    live_duplicate_check: bool = True,
) -> MigrationResult:
    """
    Copy legacy bundle to output_parent/Migrated_<timestamp>/ with stamped JSON and a report.

    When identity is incomplete, returns needs_identity_confirmation=True without writing files.
    """
    source_folder = Path(source_folder)
    output_parent = Path(output_parent)
    log_info(
        "legacy_backup_migration_started",
        source=str(source_folder),
        output_parent=str(output_parent),
    )

    try:
        session, allocations, proposed, ws = load_bundle_jsons(source_folder)
    except Exception as exc:
        log_info("legacy_backup_migration_failed", error=str(exc)[:400], phase="load_bundle")
        return MigrationResult(ok=False, error_message=str(exc))

    legacy, legacy_reasons = is_legacy_shaped_bundle(session, allocations, proposed)

    if not migration_identity_complete(identity):
        log_info(
            "legacy_backup_migration_identity_ambiguous",
            missing="source_or_destination_drive_or_site_key",
        )
        log_info("legacy_backup_migration_failed", error="identity_incomplete", phase="preflight")
        return MigrationResult(
            ok=False,
            needs_identity_confirmation=True,
            error_message="Migration requires confirmed source and destination site/library Graph ids.",
            report={"legacy_reasons": legacy_reasons, "identity_required": True},
        )

    assert identity is not None
    mi = identity
    log_info(
        "legacy_backup_migration_identity_user_confirmed",
        destination_drive_suffix=str(mi.destination_drive_id)[-16:] if mi.destination_drive_id else "",
        source_drive_suffix=str(mi.source_drive_id)[-16:] if mi.source_drive_id else "",
    )
    log_info("legacy_backup_migration_identity_resolved", phase="preflight_ok")

    anchor = str(mi.visible_destination_anchor or "").strip()
    live_top_names, graph_root_probe_kind = _live_graph_root_probe(graph, str(mi.destination_drive_id or ""), anchor)
    empty_destination_graph = graph_root_probe_kind == GRAPH_ROOT_EMPTY
    if empty_destination_graph:
        log_info(
            "legacy_backup_migration_empty_destination_graph",
            destination_drive_suffix=str(mi.destination_drive_id)[-16:] if mi.destination_drive_id else "",
        )

    stamp_dir = output_parent / f"Migrated_{_now_stamp()}"
    try:
        stamp_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log_info("legacy_backup_migration_failed", error=str(exc), phase="mkdir")
        return MigrationResult(ok=False, error_message=str(exc))

    # --- Stamp session selectors and snapshot envelope ---
    session = dict(session)
    session["SelectedSourceSiteKey"] = str(mi.source_site_key or session.get("SelectedSourceSiteKey") or "")
    session["SelectedDestinationSiteKey"] = str(
        mi.destination_site_key or session.get("SelectedDestinationSiteKey") or ""
    )
    session["SelectedSourceLibraryId"] = str(mi.source_drive_id or session.get("SelectedSourceLibraryId") or "")
    session["SelectedDestinationLibraryId"] = str(
        mi.destination_drive_id or session.get("SelectedDestinationLibraryId") or ""
    )
    session["DestinationTreeSnapshotIdentityDriveId"] = str(
        mi.destination_drive_id or session.get("DestinationTreeSnapshotIdentityDriveId") or ""
    )
    session["DestinationTreeSnapshotIdentitySiteId"] = str(
        mi.destination_site_id or session.get("DestinationTreeSnapshotIdentitySiteId") or ""
    )
    session["DestinationTreeSnapshotIdentityLibraryId"] = str(
        mi.destination_drive_id or session.get("DestinationTreeSnapshotIdentityLibraryId") or ""
    )
    session["DestinationTreeSnapshotIdentityInferredFromLegacy"] = False
    session["LegacyMigrationEmptyDestinationGraph"] = bool(empty_destination_graph)
    session["LegacyMigrationGraphRootProbeKind"] = str(graph_root_probe_kind or "")

    dst_lib_disp = str(
        mi.destination_library_display_name or session.get("SelectedDestinationLibrary") or ""
    ).strip()
    src_lib_disp = str(mi.source_library_display_name or session.get("SelectedSourceLibrary") or "").strip()

    conflicts: list[MigrationConflictRecord] = []
    counts = {
        "allocations_input": len(allocations),
        "proposed_input": len(proposed),
        "graph_resolved_alloc": 0,
        "graph_unresolved_alloc": 0,
        "graph_resolved_prop": 0,
        "graph_unresolved_prop": 0,
        "path_normalized": 0,
        "path_anchor_ambiguous": 0,
        "foreign_hub_rejected": 0,
        "live_duplicate_detected": 0,
        "rows_rejected": 0,
        "synthetic_root_stripped": 0,
        "path_reanchored": 0,
        "path_reanchor_ambiguous": 0,
        "path_reanchor_failed": 0,
        "foreign_root_blocked": 0,
        "planned_scaffold_empty_library_rows": 0,
        "library_container_destination_wrappers_stripped": 0,
        "library_container_source_wrappers_stripped": 0,
        "library_wrapper_identity_missing_destination_rows": 0,
        "library_wrapper_identity_missing_source_rows": 0,
        "library_wrapper_ambiguous_destination_rows": 0,
        "library_wrapper_ambiguous_source_rows": 0,
        "allocation_reanchor_attempted": 0,
        "allocation_reanchored": 0,
        "allocation_reanchor_failed": 0,
        "allocation_foreign_root_after_unresolved_documents_token": 0,
        "allocation_parent_graph_resolved": 0,
        "allocation_parent_graph_unresolved_but_planned": 0,
        "allocation_planned_parent_resolved": 0,
        "allocation_planned_parent_exact": 0,
        "allocation_planned_parent_ancestor": 0,
        "allocation_planned_parent_missing_descendant": 0,
        "allocation_parent_unresolved_after_planned_lookup": 0,
    }
    row_notes: list[dict[str, Any]] = []

    can_skeleton = bool(
        live_top_names
        and graph is not None
        and str(mi.destination_drive_id or "").strip()
        and not empty_destination_graph
    )
    live_top_names_cf = frozenset(str(n or "").casefold() for n in (live_top_names or []))

    out_prop: list[dict[str, Any]] = []
    for j, row in enumerate(proposed):
        r = dict(row)
        if not str(r.get("StableKey") or "").strip():
            r["StableKey"] = uuid.uuid4().hex
            log_info("legacy_backup_migration_row_stamped", row_index=j, kind="proposed", field="StableKey")

        dp_in = str(r.get("DestinationPath") or "").strip()
        pp_in = str(r.get("ParentPath") or "").strip()
        dp, dp_lw = strip_legacy_library_container_segment(
            dp_in,
            dst_lib_disp or None,
            row_index=j,
            row_kind="proposed_dest",
            role="destination",
        )
        pp, pp_lw = strip_legacy_library_container_segment(
            pp_in,
            dst_lib_disp or None,
            row_index=j,
            row_kind="proposed_parent",
            role="destination",
        )
        if dp_lw == LIB_WRAP_STRIPPED:
            counts["library_container_destination_wrappers_stripped"] += 1
        if pp_lw == LIB_WRAP_STRIPPED:
            counts["library_container_destination_wrappers_stripped"] += 1
        if dp_lw == LIB_WRAP_IDENTITY_MISSING and dp_in:
            counts["library_wrapper_identity_missing_destination_rows"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "LegacyLibraryWrapperIdentityMissing" not in stp:
                r["Status"] = _apply_status_suffix(stp, "LegacyLibraryWrapperIdentityMissing", default_base="Proposed")
        elif dp_lw == LIB_WRAP_AMBIGUOUS and dp_in:
            counts["library_wrapper_ambiguous_destination_rows"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "LegacyLibraryWrapperAmbiguous" not in stp:
                r["Status"] = _apply_status_suffix(stp, "LegacyLibraryWrapperAmbiguous", default_base="Proposed")
        if pp_lw == LIB_WRAP_IDENTITY_MISSING and pp_in:
            counts["library_wrapper_identity_missing_destination_rows"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "LegacyLibraryWrapperIdentityMissing" not in stp:
                r["Status"] = _apply_status_suffix(stp, "LegacyLibraryWrapperIdentityMissing", default_base="Proposed")
        elif pp_lw == LIB_WRAP_AMBIGUOUS and pp_in:
            counts["library_wrapper_ambiguous_destination_rows"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "LegacyLibraryWrapperAmbiguous" not in stp:
                r["Status"] = _apply_status_suffix(stp, "LegacyLibraryWrapperAmbiguous", default_base="Proposed")

        if can_skeleton:
            stripped_dp = normalize_manifest_path(dp)
            synth_d = False
        elif empty_destination_graph:
            stripped_dp = normalize_manifest_path(dp)
            synth_d = False
        else:
            stripped_dp, synth_d = strip_internal_root_with_log_legacy_only(
                dp,
                row_index=j,
                row_kind="proposed_dest",
            )
        if can_skeleton:
            stripped_pp = normalize_manifest_path(pp)
            synth_p = False
        elif empty_destination_graph:
            stripped_pp = normalize_manifest_path(pp)
            synth_p = False
        else:
            stripped_pp, synth_p = strip_internal_root_with_log_legacy_only(
                pp,
                row_index=j,
                row_kind="proposed_parent",
            )
        if synth_d:
            counts["synthetic_root_stripped"] += 1
        if synth_p:
            counts["synthetic_root_stripped"] += 1

        if empty_destination_graph and (dp or pp):
            if dp:
                r["DestinationPath"] = normalize_manifest_path(stripped_dp)
            if pp:
                r["ParentPath"] = normalize_manifest_path(stripped_pp)
            _stamp_planned_scaffold_empty_library_row(
                r,
                row_index=j,
                row_kind="proposed",
                path_excerpt=(dp or pp or "")[:220],
            )
            counts["planned_scaffold_empty_library_rows"] += 1
            if not str(r.get("DestinationDriveId") or "").strip():
                r["DestinationDriveId"] = str(mi.destination_drive_id)
                log_info("legacy_backup_migration_row_stamped", row_index=j, kind="proposed", field="DestinationDriveId")
            out_prop.append(r)
            continue

        if dp and anchor and not empty_destination_graph and _legacy_foreign_hub_before_anchor_remap(stripped_dp, anchor):
            counts["foreign_hub_rejected"] += 1
            counts["rows_rejected"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "_ForeignHubRejected" not in stp:
                r["Status"] = f"{stp}_ForeignHubRejected"
            log_info(
                "legacy_backup_migration_foreign_hub_rejected",
                row_index=j,
                kind="proposed",
                top_segment=(stripped_dp.split("\\")[0] if stripped_dp else "")[:80],
                anchor_segment=normalize_manifest_path(anchor).split("\\")[0][:80],
            )
            if not str(r.get("DestinationDriveId") or "").strip():
                r["DestinationDriveId"] = str(mi.destination_drive_id)
            out_prop.append(r)
            continue

        work_dp = stripped_dp
        if can_skeleton and dp:
            ra_d = reanchor_manifest_destination_path_against_live_skeleton(
                work_dp,
                destination_drive_id=str(mi.destination_drive_id),
                graph=graph,
                live_top_level_names=live_top_names,
                row_index=j,
                row_kind="proposed_dest",
            )
            work_dp = ra_d.path_manifest or stripped_dp
            if getattr(ra_d, "ignored_legacy_internal_root_segment", False):
                counts["synthetic_root_stripped"] += 1
            if ra_d.kind == "reanchored":
                counts["path_reanchored"] += 1
            elif ra_d.kind == "ambiguous":
                counts["path_reanchor_ambiguous"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorAmbiguous", default_base="Proposed")
            elif ra_d.kind == "failed":
                counts["path_reanchor_failed"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorFailed", default_base="Proposed")
            elif ra_d.kind == "foreign_root_blocked":
                counts["foreign_root_blocked"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyForeignRootBlocked", default_base="Proposed")
            r["DestinationPath"] = work_dp
            r["LegacyMigrationAnchorClassification"] = legacy_anchor_classification_from_reanchor(ra_d)
            if ra_d.kind in ("ambiguous", "failed", "foreign_root_blocked"):
                if not str(r.get("DestinationDriveId") or "").strip():
                    r["DestinationDriveId"] = str(mi.destination_drive_id)
                counts["graph_unresolved_prop"] += 1
                out_prop.append(r)
                continue
        elif dp and anchor:
            new_dp = remap_under_visible_library_anchor(
                stripped_dp,
                normalize_manifest_path(anchor),
            )
            r["DestinationPath"] = new_dp
            if new_dp != dp:
                counts["path_normalized"] += 1
                r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH
                log_info(
                    "legacy_backup_migration_path_normalized",
                    row_index=j,
                    kind="proposed",
                    before_excerpt=dp[:120],
                    after_excerpt=new_dp[:120],
                )

        if pp and anchor and not empty_destination_graph and _legacy_foreign_hub_before_anchor_remap(stripped_pp, anchor):
            counts["foreign_hub_rejected"] += 1
            counts["rows_rejected"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "_ForeignHubRejected" not in stp:
                r["Status"] = f"{stp}_ForeignHubRejected"
            log_info(
                "legacy_backup_migration_foreign_hub_rejected",
                row_index=j,
                kind="proposed_parent",
                top_segment=(stripped_pp.split("\\")[0] if stripped_pp else "")[:80],
                anchor_segment=normalize_manifest_path(anchor).split("\\")[0][:80],
            )
            if not str(r.get("DestinationDriveId") or "").strip():
                r["DestinationDriveId"] = str(mi.destination_drive_id)
            out_prop.append(r)
            continue

        work_pp = stripped_pp
        if can_skeleton and pp:
            ra_p = reanchor_manifest_destination_path_against_live_skeleton(
                work_pp,
                destination_drive_id=str(mi.destination_drive_id),
                graph=graph,
                live_top_level_names=live_top_names,
                row_index=j,
                row_kind="proposed_parent",
            )
            work_pp = ra_p.path_manifest or stripped_pp
            if getattr(ra_p, "ignored_legacy_internal_root_segment", False):
                counts["synthetic_root_stripped"] += 1
            if ra_p.kind == "reanchored":
                counts["path_reanchored"] += 1
            elif ra_p.kind == "ambiguous":
                counts["path_reanchor_ambiguous"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorAmbiguous", default_base="Proposed")
            elif ra_p.kind == "failed":
                counts["path_reanchor_failed"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorFailed", default_base="Proposed")
            elif ra_p.kind == "foreign_root_blocked":
                counts["foreign_root_blocked"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyForeignRootBlocked", default_base="Proposed")
            r["ParentPath"] = work_pp
            r["LegacyMigrationAnchorClassification"] = legacy_anchor_classification_from_reanchor(ra_p)
            if ra_p.kind in ("ambiguous", "failed", "foreign_root_blocked"):
                if not str(r.get("DestinationDriveId") or "").strip():
                    r["DestinationDriveId"] = str(mi.destination_drive_id)
                counts["graph_unresolved_prop"] += 1
                out_prop.append(r)
                continue
        elif pp and anchor:
            new_pp = remap_under_visible_library_anchor(
                stripped_pp,
                normalize_manifest_path(anchor),
            )
            r["ParentPath"] = new_pp
            if new_pp != pp:
                counts["path_normalized"] += 1
                r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH
                log_info(
                    "legacy_backup_migration_path_normalized",
                    row_index=j,
                    kind="proposed_parent",
                    before_excerpt=pp[:120],
                    after_excerpt=new_pp[:120],
                )
        elif pp:
            r["ParentPath"] = work_pp

        if not str(r.get("DestinationDriveId") or "").strip():
            r["DestinationDriveId"] = str(mi.destination_drive_id)
            log_info("legacy_backup_migration_row_stamped", row_index=j, kind="proposed", field="DestinationDriveId")

        # Canonical full folder path: legacy exports often omit DestinationPath; planning uses ParentPath + FolderName.
        folder_name_prop = str(r.get("FolderName") or "").strip()
        parent_path_prop = str(r.get("ParentPath") or "").strip()
        dest_path_prop = str(r.get("DestinationPath") or "").strip()
        if folder_name_prop and parent_path_prop and not dest_path_prop:
            dest_path_prop = normalize_manifest_path(f"{parent_path_prop}\\{folder_name_prop}")
            r["DestinationPath"] = dest_path_prop

        unresolved = False
        rel_parent = _rel_graph_path(str(r.get("ParentPath") or ""), anchor) if str(r.get("ParentPath") or "").strip() else ""
        graph_has_path = bool(
            not skip_graph_resolution
            and graph is not None
            and str(mi.destination_drive_id or "").strip()
            and getattr(graph, "get_drive_item_by_path", None)
        )
        if graph_has_path:
            try:
                if rel_parent:
                    parent_item = graph.get_drive_item_by_path(str(mi.destination_drive_id), rel_parent)
                    if parent_item and str(parent_item.get("id") or "").strip():
                        r["DestinationParentItemId"] = str(parent_item.get("id") or "").strip()
                        counts["graph_resolved_prop"] += 1
                        log_info(
                            "legacy_backup_migration_graph_id_resolved",
                            row_index=j,
                            kind="proposed_parent",
                            field="DestinationParentItemId",
                        )
                    else:
                        unresolved = True
                elif str(r.get("ParentPath") or "").strip():
                    unresolved = True

                # Duplicate = live **folder** at exact full proposed path (not parent-only, not anchor-only).
                if live_duplicate_check:
                    log_info(
                        "legacy_backup_migration_proposed_duplicate_check_started",
                        row_index=j,
                        stable_key=str(r.get("StableKey") or "")[:80],
                        folder_name_excerpt=folder_name_prop[:120],
                    )
                    fold_rel = ""
                    ex = None
                    if dest_path_prop and folder_name_prop:
                        fold_rel = _rel_graph_path(dest_path_prop, anchor)
                        log_info(
                            "legacy_backup_migration_proposed_duplicate_check_path",
                            row_index=j,
                            checked_graph_path=fold_rel,
                            proposed_full_path_excerpt=dest_path_prop[:220],
                        )
                        if fold_rel:
                            ex = graph.get_drive_item_by_path(str(mi.destination_drive_id), fold_rel)
                        else:
                            log_info(
                                "legacy_backup_migration_proposed_duplicate_check_no_match",
                                row_index=j,
                                reason="empty_graph_relative_path_after_normalize",
                            )
                    else:
                        log_info(
                            "legacy_backup_migration_proposed_duplicate_check_no_match",
                            row_index=j,
                            reason="missing_folder_name_or_destination_path",
                        )

                    if dest_path_prop and folder_name_prop and fold_rel:
                        if not ex:
                            log_info(
                                "legacy_backup_migration_proposed_duplicate_check_no_match",
                                row_index=j,
                                reason="no_graph_item_at_full_proposed_path",
                                checked_graph_path_excerpt=fold_rel[:200],
                            )
                        else:
                            fld = ex.get("folder")
                            is_folder = fld is not None and isinstance(fld, dict)
                            if not is_folder:
                                log_info(
                                    "legacy_backup_migration_proposed_duplicate_check_no_match",
                                    row_index=j,
                                    reason="path_exists_but_not_a_folder",
                                    checked_graph_path_excerpt=fold_rel[:200],
                                )
                            else:
                                web_u = str(ex.get("webUrl") or "")
                                rec = MigrationConflictRecord(
                                    kind="live_duplicate_proposed_folder",
                                    path=fold_rel,
                                    detail="live_folder_exists_at_proposed_full_path",
                                    proposed_row_index=j,
                                    proposed_stable_key=str(r.get("StableKey") or ""),
                                    proposed_folder_name=folder_name_prop,
                                    proposed_parent_path=parent_path_prop,
                                    proposed_full_path=dest_path_prop,
                                    checked_graph_path=fold_rel,
                                    live_item_id=str(ex.get("id") or ""),
                                    live_item_name=str(ex.get("name") or ""),
                                    live_item_type="folder",
                                    live_item_web_url=web_u,
                                    destination_drive_id=str(mi.destination_drive_id or ""),
                                )
                                conflicts.append(rec)
                                counts["live_duplicate_detected"] += 1
                                log_info(
                                    "legacy_backup_migration_live_duplicate_proposed_folder_detected",
                                    row_index=j,
                                    checked_graph_path_excerpt=fold_rel[:180],
                                    live_item_id_suffix=str(ex.get("id") or "")[-16:],
                                    proposed_full_path_excerpt=dest_path_prop[:200],
                                )
                                log_info(
                                    "legacy_backup_migration_conflict_recorded",
                                    conflict_kind="live_duplicate_proposed_folder",
                                    path_excerpt=fold_rel[:160],
                                )
            except Exception:
                unresolved = True
        elif not skip_graph_resolution and graph is None and pp:
            unresolved = not str(r.get("DestinationParentItemId") or "").strip()

        if unresolved:
            counts["graph_unresolved_prop"] += 1
            stp = str(r.get("Status") or "Proposed")
            if "_LegacyMigrationUnresolved" not in stp:
                r["Status"] = f"{stp}_LegacyMigrationUnresolved"
            log_info("legacy_backup_migration_graph_id_unresolved", row_index=j, kind="proposed")

        out_prop.append(r)

    planned_parent_index_cf, planned_scaffold_index_ct, planned_indexed_path_ct = build_planned_parent_path_index(
        out_prop,
        anchor=str(anchor or ""),
        live_top_level_names_cf=live_top_names_cf,
    )
    log_info(
        "legacy_backup_migration_planned_parent_index_built",
        proposed_count=len(out_prop),
        scaffold_count=planned_scaffold_index_ct,
        indexed_path_count=planned_indexed_path_ct,
    )

    out_alloc: list[dict[str, Any]] = []
    for i, row in enumerate(allocations):
        r = dict(row)
        dest_in = str(r.get("RequestedDestinationPath") or "").strip()
        src_in = str(r.get("SourcePath") or "").strip()
        dest_path, dst_lw = strip_legacy_library_container_segment(
            dest_in,
            dst_lib_disp or None,
            row_index=i,
            row_kind="allocation",
            role="destination",
        )
        src_path, src_lw = strip_legacy_library_container_segment(
            src_in,
            src_lib_disp or None,
            row_index=i,
            row_kind="allocation",
            role="source",
        )
        r["SourcePath"] = src_path
        if dst_lw == LIB_WRAP_STRIPPED:
            counts["library_container_destination_wrappers_stripped"] += 1
        if src_lw == LIB_WRAP_STRIPPED:
            counts["library_container_source_wrappers_stripped"] += 1
        if dst_lw == LIB_WRAP_IDENTITY_MISSING and dest_in:
            counts["library_wrapper_identity_missing_destination_rows"] += 1
            stx = str(r.get("Status") or "Pending")
            if "LegacyLibraryWrapperIdentityMissing" not in stx:
                r["Status"] = _apply_status_suffix(stx, "LegacyLibraryWrapperIdentityMissing")
        elif dst_lw == LIB_WRAP_AMBIGUOUS and dest_in:
            counts["library_wrapper_ambiguous_destination_rows"] += 1
            stx = str(r.get("Status") or "Pending")
            if "LegacyLibraryWrapperAmbiguous" not in stx:
                r["Status"] = _apply_status_suffix(stx, "LegacyLibraryWrapperAmbiguous")
        if src_lw == LIB_WRAP_IDENTITY_MISSING and src_in:
            counts["library_wrapper_identity_missing_source_rows"] += 1
            stx = str(r.get("Status") or "Pending")
            if "LegacyLibraryWrapperSourceIdentityMissing" not in stx:
                r["Status"] = _apply_status_suffix(stx, "LegacyLibraryWrapperSourceIdentityMissing")
        elif src_lw == LIB_WRAP_AMBIGUOUS and src_in:
            counts["library_wrapper_ambiguous_source_rows"] += 1
            stx = str(r.get("Status") or "Pending")
            if "LegacyLibraryWrapperSourceAmbiguous" not in stx:
                r["Status"] = _apply_status_suffix(stx, "LegacyLibraryWrapperSourceAmbiguous")

        if can_skeleton:
            stripped_dest = normalize_manifest_path(dest_path)
            synth = False
        elif empty_destination_graph:
            # No live top-level anchors: preserve legacy segments (including Root / Root3) as planning scaffold.
            stripped_dest = normalize_manifest_path(dest_path)
            synth = False
        else:
            stripped_dest, synth = strip_internal_root_with_log_legacy_only(
                dest_path,
                row_index=i,
                row_kind="allocation",
            )
        if synth:
            counts["synthetic_root_stripped"] += 1

        if empty_destination_graph and dest_path:
            r["RequestedDestinationPath"] = normalize_manifest_path(stripped_dest)
            _stamp_planned_scaffold_empty_library_row(
                r,
                row_index=i,
                row_kind="allocation",
                path_excerpt=str(r.get("RequestedDestinationPath") or ""),
            )
            counts["planned_scaffold_empty_library_rows"] += 1
            if not str(r.get("DestinationDriveId") or "").strip():
                r["DestinationDriveId"] = str(mi.destination_drive_id)
                log_info(
                    "legacy_backup_migration_row_stamped",
                    row_index=i,
                    kind="allocation",
                    field="DestinationDriveId",
                )
            unresolved = False
            if not skip_graph_resolution and graph is not None and str(mi.source_drive_id):
                try:
                    src_path = str(r.get("SourcePath") or "").strip()
                    src_rel = _rel_graph_path(src_path, "")
                    if src_rel and getattr(graph, "get_drive_item_by_path", None):
                        src_item = graph.get_drive_item_by_path(str(mi.source_drive_id), src_rel)
                        if src_item and str(src_item.get("id") or "").strip():
                            r["SourceItemId"] = str(src_item.get("id") or "").strip()
                            r["SourceDriveId"] = str(mi.source_drive_id)
                            log_info(
                                "legacy_backup_migration_graph_id_resolved",
                                row_index=i,
                                kind="allocation_source",
                                field="SourceItemId",
                            )
                        else:
                            unresolved = True
                except Exception as exc:
                    unresolved = True
                    log_info("legacy_backup_migration_failed", error=str(exc)[:200], phase="allocation_scaffold_source")
            elif not skip_graph_resolution and graph is None:
                unresolved = not str(r.get("SourceItemId") or "").strip()

            if unresolved:
                counts["graph_unresolved_alloc"] += 1
                sta = str(r.get("Status") or "Pending")
                if "_LegacyMigrationUnresolved" not in sta:
                    r["Status"] = f"{sta}_LegacyMigrationUnresolved"
                log_info(
                    "legacy_backup_migration_graph_id_unresolved",
                    row_index=i,
                    kind="allocation",
                )
            row_notes.append(
                {
                    "index": i,
                    "kind": "allocation",
                    "unresolved": unresolved,
                    "empty_library_scaffold": True,
                }
            )
            out_alloc.append(r)
            continue

        if dest_path and anchor and not empty_destination_graph and _legacy_foreign_hub_before_anchor_remap(stripped_dest, anchor):
            counts["foreign_hub_rejected"] += 1
            counts["rows_rejected"] += 1
            st = str(r.get("Status") or "Pending")
            if "_ForeignHubRejected" not in st:
                r["Status"] = f"{st}_ForeignHubRejected"
            log_info(
                "legacy_backup_migration_foreign_hub_rejected",
                row_index=i,
                kind="allocation",
                top_segment=(stripped_dest.split("\\")[0] if stripped_dest else "")[:80],
                anchor_segment=normalize_manifest_path(anchor).split("\\")[0][:80],
            )
            if not str(r.get("DestinationDriveId") or "").strip():
                r["DestinationDriveId"] = str(mi.destination_drive_id)
                log_info(
                    "legacy_backup_migration_row_stamped",
                    row_index=i,
                    kind="allocation",
                    field="DestinationDriveId",
                )
            row_notes.append({"index": i, "kind": "allocation", "unresolved": False, "foreign_hub": True})
            out_alloc.append(r)
            continue

        work_path = stripped_dest
        reanchor_unresolved = False
        if can_skeleton:
            counts["allocation_reanchor_attempted"] += 1
            log_info(
                "legacy_backup_migration_alloc_reanchor_attempt",
                row_index=i,
                path_excerpt=work_path[:220],
            )
            ra = reanchor_manifest_destination_path_against_live_skeleton(
                work_path,
                destination_drive_id=str(mi.destination_drive_id),
                graph=graph,
                live_top_level_names=live_top_names,
                row_index=i,
                row_kind="allocation",
            )
            work_path = ra.path_manifest or stripped_dest
            if getattr(ra, "ignored_legacy_internal_root_segment", False):
                counts["synthetic_root_stripped"] += 1
            if ra.kind == "reanchored":
                counts["path_reanchored"] += 1
                counts["allocation_reanchored"] += 1
                log_info(
                    "legacy_backup_migration_alloc_reanchored",
                    row_index=i,
                    after_excerpt=work_path[:220],
                )
            elif ra.kind == "ambiguous":
                counts["path_reanchor_ambiguous"] += 1
                reanchor_unresolved = True
                counts["allocation_reanchor_failed"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorAmbiguous")
                log_info(
                    "legacy_backup_migration_alloc_reanchor_failed",
                    row_index=i,
                    reason="ambiguous",
                    path_excerpt=work_path[:220],
                )
            elif ra.kind == "failed":
                counts["path_reanchor_failed"] += 1
                reanchor_unresolved = True
                counts["allocation_reanchor_failed"] += 1
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorFailed")
                log_info(
                    "legacy_backup_migration_alloc_reanchor_failed",
                    row_index=i,
                    reason="no_live_match",
                    path_excerpt=work_path[:220],
                )
            elif ra.kind == "foreign_root_blocked":
                counts["foreign_root_blocked"] += 1
                counts["allocation_foreign_root_after_unresolved_documents_token"] += 1
                reanchor_unresolved = True
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyForeignRootBlocked")
                log_info(
                    "legacy_backup_migration_alloc_foreign_block_after_reanchor_failed",
                    row_index=i,
                    path_excerpt=work_path[:220],
                    note="namespace_token_documents_not_live_proven",
                )
            r["RequestedDestinationPath"] = work_path
            r["LegacyMigrationAnchorClassification"] = legacy_anchor_classification_from_reanchor(ra)
            if reanchor_unresolved:
                if not str(r.get("DestinationDriveId") or "").strip():
                    r["DestinationDriveId"] = str(mi.destination_drive_id)
                    log_info(
                        "legacy_backup_migration_row_stamped",
                        row_index=i,
                        kind="allocation",
                        field="DestinationDriveId",
                    )
                # Fall through: try live Graph parent resolution, then proposed/planned path index.
        elif dest_path and anchor:
            new_dp = remap_under_visible_library_anchor(
                stripped_dest,
                normalize_manifest_path(anchor),
            )
            r["RequestedDestinationPath"] = new_dp
            if new_dp != dest_path:
                counts["path_normalized"] += 1
                r["LegacyMigrationAnchorClassification"] = ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH
                log_info(
                    "legacy_backup_migration_path_normalized",
                    row_index=i,
                    kind="allocation",
                    before_excerpt=dest_path[:120],
                    after_excerpt=new_dp[:120],
                )
        elif dest_path and not str(r.get("RequestedDestinationPath") or "").strip():
            r["RequestedDestinationPath"] = normalize_manifest_path(stripped_dest)

        if anchor and str(r.get("RequestedDestinationPath") or "").strip():
            al = align_legacy_top_segment_to_anchor(
                str(r.get("RequestedDestinationPath") or ""),
                anchor,
                live_top_level_names_cf=live_top_names_cf,
            )
            if al != str(r.get("RequestedDestinationPath") or "").strip():
                r["RequestedDestinationPath"] = al

        if not str(r.get("DestinationDriveId") or "").strip():
            r["DestinationDriveId"] = str(mi.destination_drive_id)
            log_info(
                "legacy_backup_migration_row_stamped",
                row_index=i,
                kind="allocation",
                field="DestinationDriveId",
            )

        parent_rel_display = _parent_rel(str(r.get("RequestedDestinationPath") or ""))
        planned_parent_norm = normalize_manifest_path(parent_rel_display) if parent_rel_display else ""

        dest_graph_unresolved = False
        src_unresolved = False
        if not skip_graph_resolution and graph is not None and str(mi.destination_drive_id):
            rel_full = _rel_graph_path(str(r.get("RequestedDestinationPath") or ""), anchor)
            parent_drive = _rel_graph_path(parent_rel_display, anchor) if parent_rel_display else ""

            try:
                if rel_full and live_duplicate_check and getattr(graph, "get_drive_item_by_path", None):
                    live_item = graph.get_drive_item_by_path(str(mi.destination_drive_id), rel_full)
                    if live_item and isinstance(live_item, dict):
                        fld = live_item.get("folder")
                        is_folder = fld is not None and isinstance(fld, dict)
                        if is_folder:
                            conflicts.append(
                                MigrationConflictRecord(
                                    kind="live_duplicate_allocation_target",
                                    path=rel_full,
                                    detail="live_folder_exists_at_planned_destination_path",
                                )
                            )
                            counts["live_duplicate_detected"] += 1
                            log_info(
                                "legacy_backup_migration_live_duplicate_detected",
                                row_index=i,
                                path_excerpt=rel_full[:160],
                                kind="allocation",
                            )
                            log_info(
                                "legacy_backup_migration_conflict_recorded",
                                conflict_kind="live_duplicate_allocation_target",
                                path_excerpt=rel_full[:160],
                            )
                if parent_drive and getattr(graph, "get_drive_item_by_path", None):
                    parent_item = graph.get_drive_item_by_path(str(mi.destination_drive_id), parent_drive)
                    if parent_item and str(parent_item.get("id") or "").strip():
                        r["DestinationParentItemId"] = str(parent_item.get("id") or "").strip()
                        r["LegacyMigrationDestinationParentResolution"] = "graph"
                        r["LegacyMigrationPlannedParentResolved"] = False
                        r["DestinationParentPlannedPath"] = ""
                        r["LegacyMigrationPlannedParentMatchKind"] = ""
                        counts["graph_resolved_alloc"] += 1
                        counts["allocation_parent_graph_resolved"] += 1
                        log_info(
                            "legacy_backup_migration_graph_id_resolved",
                            row_index=i,
                            kind="allocation_destination_parent",
                            field="DestinationParentItemId",
                        )
                    else:
                        dest_graph_unresolved = True
                elif parent_rel_display:
                    dest_graph_unresolved = True
                src_path = str(r.get("SourcePath") or "").strip()
                src_rel = _rel_graph_path(src_path, "")
                if src_rel and getattr(graph, "get_drive_item_by_path", None):
                    src_item = graph.get_drive_item_by_path(str(mi.source_drive_id), src_rel)
                    if src_item and str(src_item.get("id") or "").strip():
                        r["SourceItemId"] = str(src_item.get("id") or "").strip()
                        r["SourceDriveId"] = str(mi.source_drive_id)
                        log_info(
                            "legacy_backup_migration_graph_id_resolved",
                            row_index=i,
                            kind="allocation_source",
                            field="SourceItemId",
                        )
                    else:
                        src_unresolved = True
            except Exception as exc:
                dest_graph_unresolved = bool(parent_rel_display)
                src_unresolved = True
                log_info("legacy_backup_migration_failed", error=str(exc)[:200], phase="allocation_graph_row")
        elif not skip_graph_resolution and graph is None:
            dest_graph_unresolved = bool(not str(r.get("DestinationParentItemId") or "").strip()) and bool(
                planned_parent_norm
            )
            src_unresolved = bool(not str(r.get("SourceItemId") or "").strip())
        else:
            # skip_graph_resolution=True: still evaluate migrated proposed/planned parent support.
            dest_graph_unresolved = bool(not str(r.get("DestinationParentItemId") or "").strip()) and bool(
                planned_parent_norm
            )
            src_unresolved = bool(not str(r.get("SourceItemId") or "").strip())

        planned_lookup = None
        if dest_graph_unresolved and planned_parent_index_cf and planned_parent_norm:
            planned_lookup = lookup_planned_parent(planned_parent_norm, planned_parent_index_cf)

        dest_planned_ok = bool(planned_lookup is not None and (planned_lookup.kind or "") not in ("", "none"))

        if dest_graph_unresolved and dest_planned_ok and planned_lookup is not None:
            _stamp_allocation_planned_destination_parent(
                r,
                parent_planned_norm=planned_parent_norm,
                match_kind=planned_lookup.kind,
            )
            counts["allocation_planned_parent_resolved"] += 1
            counts["allocation_parent_graph_unresolved_but_planned"] += 1
            if planned_lookup.kind == "exact":
                counts["allocation_planned_parent_exact"] += 1
            elif planned_lookup.kind == "ancestor":
                counts["allocation_planned_parent_ancestor"] += 1
            else:
                counts["allocation_planned_parent_missing_descendant"] += 1
            log_info(
                "legacy_backup_migration_allocation_planned_parent_resolved",
                row_index=i,
                match_kind=planned_lookup.kind,
                parent_excerpt=planned_parent_norm[:220],
            )
            log_info(
                "legacy_backup_migration_allocation_parent_graph_unresolved_but_planned",
                row_index=i,
                parent_excerpt=planned_parent_norm[:220],
            )
        elif dest_graph_unresolved and (planned_lookup is None or not dest_planned_ok):
            log_info(
                "legacy_backup_migration_allocation_parent_unresolved",
                row_index=i,
                parent_excerpt=planned_parent_norm[:220],
            )
            counts["allocation_parent_unresolved_after_planned_lookup"] += 1

        dest_needs_parent = bool(planned_parent_norm)
        dest_ok = (not dest_needs_parent) or bool(str(r.get("DestinationParentItemId") or "").strip()) or bool(
            dest_planned_ok
        )
        unresolved = bool(src_unresolved or not dest_ok)

        if unresolved:
            counts["graph_unresolved_alloc"] += 1
            sta = str(r.get("Status") or "Pending")
            if "_LegacyMigrationUnresolved" not in sta:
                r["Status"] = f"{sta}_LegacyMigrationUnresolved"
            log_info(
                "legacy_backup_migration_graph_id_unresolved",
                row_index=i,
                kind="allocation",
            )

        row_notes.append(
            {
                "index": i,
                "kind": "allocation",
                "unresolved": unresolved,
                "planned_parent_kind": getattr(planned_lookup, "kind", "") if planned_lookup else "",
            }
        )
        out_alloc.append(r)

    # Foreign hub: first path segment must match anchor's top segment when anchor is set.
    # When the Graph library root is empty, legacy top segments are scaffold — do not enforce anchor match.
    if anchor and not empty_destination_graph:
        a0 = [x for x in anchor.split("\\") if x][0].lower() if anchor else ""
        for idx, chk in enumerate(out_alloc):
            stx = str(chk.get("Status") or "")
            if "_ForeignHubRejected" in stx:
                continue
            pth = str(chk.get("RequestedDestinationPath") or "")
            parts = [x for x in normalize_manifest_path(pth).split("\\") if x]
            if parts and a0 and parts[0].lower() != a0:
                counts["foreign_hub_rejected"] += 1
                counts["rows_rejected"] += 1
                st = str(chk.get("Status") or "Pending")
                if "_ForeignHubRejected" not in st:
                    chk["Status"] = f"{st}_ForeignHubRejected"
                log_info(
                    "legacy_backup_migration_foreign_hub_rejected",
                    row_index=idx,
                    kind="allocation",
                    top_segment=parts[0][:80],
                    anchor_segment=a0[:80],
                )
        for idx, chk in enumerate(out_prop):
            stx = str(chk.get("Status") or "")
            if "_ForeignHubRejected" in stx:
                continue
            pth = str(chk.get("DestinationPath") or "")
            parts = [x for x in normalize_manifest_path(pth).split("\\") if x]
            if parts and a0 and parts[0].lower() != a0:
                counts["foreign_hub_rejected"] += 1
                counts["rows_rejected"] += 1
                st = str(chk.get("Status") or "Proposed")
                if "_ForeignHubRejected" not in st:
                    chk["Status"] = f"{st}_ForeignHubRejected"
                log_info(
                    "legacy_backup_migration_foreign_hub_rejected",
                    row_index=idx,
                    kind="proposed",
                    top_segment=parts[0][:80],
                    anchor_segment=a0[:80],
                )

    ws_out = ws
    if isinstance(ws_out, dict):
        ws_out = dict(ws_out)
        pc = ws_out.get("planning_context")
        if isinstance(pc, dict):
            pc = dict(pc)
            ddest = pc.get("destination")
            if isinstance(ddest, dict):
                ddest = dict(ddest)
                ddest["library_id"] = str(mi.destination_drive_id)
                ddest["site_key"] = str(mi.destination_site_key)
                pc["destination"] = ddest
            ssrc = pc.get("source")
            if isinstance(ssrc, dict):
                ssrc = dict(ssrc)
                ssrc["library_id"] = str(mi.source_drive_id)
                ssrc["site_key"] = str(mi.source_site_key)
                pc["source"] = ssrc
            ws_out["planning_context"] = pc

    session["LastSavedUtc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Write outputs (non-destructive to source: we only write under stamp_dir)
    (stamp_dir / "Draft-SessionState.json").write_text(
        json.dumps(session, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (stamp_dir / "Draft-AllocationQueue.json").write_text(
        json.dumps(out_alloc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (stamp_dir / "Draft-ProposedFolders.json").write_text(
        json.dumps(out_prop, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if ws_out is not None:
        (stamp_dir / "WorkspaceSnapshot.json").write_text(
            json.dumps(ws_out, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    counts["skip_graph_resolution"] = bool(skip_graph_resolution)

    report = {
        "schema_version": 1,
        "migrated_utc": datetime.now(timezone.utc).isoformat(),
        "source_folder": str(source_folder),
        "output_folder": str(stamp_dir),
        "identity": {
            "source_drive_id": mi.source_drive_id,
            "destination_drive_id": mi.destination_drive_id,
            "source_site_key": mi.source_site_key,
            "destination_site_key": mi.destination_site_key,
        },
        "counts": counts,
        "legacy_shape_reasons": legacy_reasons,
        "graph_root_probe_kind": graph_root_probe_kind,
        "empty_destination_graph": empty_destination_graph,
        "conflicts": [c.to_report_dict() for c in conflicts],
        "row_notes": row_notes,
    }
    (stamp_dir / "LegacyMigrationReport.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_lines = [
        "# Legacy migration report",
        "",
        f"- Output: `{stamp_dir}`",
        f"- Allocations: {counts['allocations_input']} → migrated with {counts['graph_resolved_alloc']} graph-resolved rows",
        f"- Unresolved: {counts['graph_unresolved_alloc']}",
        f"- Conflicts: {len(conflicts)}",
        "",
    ]
    (stamp_dir / "LegacyMigrationReport.md").write_text("\n".join(md_lines), encoding="utf-8")

    log_info(
        "legacy_backup_migration_completed",
        output_folder=str(stamp_dir),
        rows_migrated=len(out_alloc) + len(out_prop),
        conflicts=len(conflicts),
    )
    return MigrationResult(ok=True, output_folder=stamp_dir, report=report)


def copy_original_bundle_preserving_readonly(source_folder: Path, output_parent: Path) -> Path:
    """Optional: copy raw backup alongside migrated output for diff/rollback (read-only semantics)."""
    snap = output_parent / f"LegacySourceCopy_{_now_stamp()}"
    shutil.copytree(source_folder, snap, dirs_exist_ok=False)
    return snap
