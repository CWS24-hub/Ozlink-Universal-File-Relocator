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

from .live_path_reanchor import (
    GRAPH_ROOT_EMPTY,
    GRAPH_ROOT_NON_EMPTY,
    GRAPH_ROOT_NO_LISTING,
    legacy_anchor_classification_from_reanchor,
    probe_live_destination_graph_root,
    reanchor_manifest_destination_path_against_live_skeleton,
    strip_internal_root_with_log_legacy_only,
)
from .shape import is_legacy_shaped_bundle
from .types import (
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
    }
    row_notes: list[dict[str, Any]] = []

    can_skeleton = bool(
        live_top_names
        and graph is not None
        and str(mi.destination_drive_id or "").strip()
        and not empty_destination_graph
    )

    out_alloc: list[dict[str, Any]] = []
    for i, row in enumerate(allocations):
        r = dict(row)
        dest_path = str(r.get("RequestedDestinationPath") or "").strip()

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
            elif ra.kind == "ambiguous":
                counts["path_reanchor_ambiguous"] += 1
                reanchor_unresolved = True
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorAmbiguous")
            elif ra.kind == "failed":
                counts["path_reanchor_failed"] += 1
                reanchor_unresolved = True
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyReanchorFailed")
            elif ra.kind == "foreign_root_blocked":
                counts["foreign_root_blocked"] += 1
                reanchor_unresolved = True
                r["Status"] = _apply_status_suffix(str(r.get("Status") or ""), "LegacyForeignRootBlocked")
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
                row_notes.append(
                    {
                        "index": i,
                        "kind": "allocation",
                        "unresolved": True,
                        "reanchor": ra.kind,
                    }
                )
                counts["graph_unresolved_alloc"] += 1
                out_alloc.append(r)
                continue
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

        if not str(r.get("DestinationDriveId") or "").strip():
            r["DestinationDriveId"] = str(mi.destination_drive_id)
            log_info(
                "legacy_backup_migration_row_stamped",
                row_index=i,
                kind="allocation",
                field="DestinationDriveId",
            )

        unresolved = False
        if not skip_graph_resolution and graph is not None and str(mi.destination_drive_id):
            rel_full = _rel_graph_path(str(r.get("RequestedDestinationPath") or ""), anchor)
            parent_rel = _parent_rel(str(r.get("RequestedDestinationPath") or ""))
            parent_drive = _rel_graph_path(parent_rel, anchor) if parent_rel else ""

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
                        counts["graph_resolved_alloc"] += 1
                        log_info(
                            "legacy_backup_migration_graph_id_resolved",
                            row_index=i,
                            kind="allocation_destination_parent",
                            field="DestinationParentItemId",
                        )
                    else:
                        unresolved = True
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
                log_info("legacy_backup_migration_failed", error=str(exc)[:200], phase="allocation_graph_row")
        elif not skip_graph_resolution and graph is None:
            unresolved = bool(
                not str(r.get("DestinationParentItemId") or "").strip()
                or not str(r.get("SourceItemId") or "").strip()
            )

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

        row_notes.append({"index": i, "kind": "allocation", "unresolved": unresolved})
        out_alloc.append(r)

    out_prop: list[dict[str, Any]] = []
    for j, row in enumerate(proposed):
        r = dict(row)
        if not str(r.get("StableKey") or "").strip():
            r["StableKey"] = uuid.uuid4().hex
            log_info("legacy_backup_migration_row_stamped", row_index=j, kind="proposed", field="StableKey")

        dp = str(r.get("DestinationPath") or "").strip()
        pp = str(r.get("ParentPath") or "").strip()
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

        unresolved = False
        rel_parent = _rel_graph_path(str(r.get("ParentPath") or ""), anchor) if str(r.get("ParentPath") or "").strip() else ""
        if (
            not skip_graph_resolution
            and graph is not None
            and rel_parent
            and getattr(graph, "get_drive_item_by_path", None)
        ):
            try:
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
                fold_rel = _rel_graph_path(str(r.get("DestinationPath") or ""), anchor)
                if fold_rel and live_duplicate_check:
                    ex = graph.get_drive_item_by_path(str(mi.destination_drive_id), fold_rel)
                    if ex:
                        conflicts.append(
                            MigrationConflictRecord(
                                kind="live_duplicate_proposed_folder",
                                path=fold_rel,
                                detail="live_item_exists_at_proposed_destination_path",
                            )
                        )
                        counts["live_duplicate_detected"] += 1
                        log_info(
                            "legacy_backup_migration_live_duplicate_detected",
                            row_index=j,
                            path_excerpt=fold_rel[:160],
                            kind="proposed",
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
        "conflicts": [{"kind": c.kind, "path": c.path, "detail": c.detail} for c in conflicts],
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
