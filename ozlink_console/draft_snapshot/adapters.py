"""Normalize legacy bundle, REQ JSON, and canonical snapshot JSON into one model."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ozlink_console.draft_snapshot.contracts import (
    SNAPSHOT_SCHEMA_V1,
    SNAPSHOT_ENGINE_VERSION_V1,
    AssignmentMode,
    CanonicalMappingItem,
    CanonicalSubmittedSnapshot,
    DestinationContextSnapshot,
    ItemType,
    ProposedFolderMappingItem,
    SiteLibraryContextSnapshot,
    SourceContextSnapshot,
    TenantIdentitySnapshot,
    new_snapshot_id,
    parse_canonical_submitted_snapshot_dict,
    utc_now_iso,
)
from ozlink_console.draft_snapshot.errors import SnapshotValidationError
from ozlink_console.draft_snapshot.path_normalization import to_library_relative_path
from ozlink_console.models import AllocationRow, ProposedFolder


def _normalize_imported_memory_path(value: Any) -> str:
    """Aligned with ``MemoryManager._normalize_imported_memory_path`` for bundle parity."""
    text = str(value or "").strip().replace("/", "\\")
    if not text:
        return ""
    while "\\\\" in text:
        text = text.replace("\\\\", "\\")
    lowered = text.lower()
    if lowered.startswith("documents\\root\\"):
        return text[len("Documents\\") :]
    if lowered == "documents\\root":
        return "Root"
    return text


def _normalize_imported_allocations_payload(payload: Any) -> Any:
    if not isinstance(payload, list):
        return payload
    out: list[Any] = []
    for row in payload:
        if isinstance(row, dict):
            nr = dict(row)
            nr["RequestedDestinationPath"] = _normalize_imported_memory_path(nr.get("RequestedDestinationPath", ""))
            out.append(nr)
        else:
            out.append(row)
    return out


def _normalize_imported_proposed_payload(payload: Any) -> Any:
    if not isinstance(payload, list):
        return payload
    out: list[Any] = []
    for row in payload:
        if isinstance(row, dict):
            nr = dict(row)
            nr["ParentPath"] = _normalize_imported_memory_path(nr.get("ParentPath", ""))
            nr["DestinationPath"] = _normalize_imported_memory_path(nr.get("DestinationPath", ""))
            out.append(nr)
        else:
            out.append(row)
    return out


def _normalize_imported_session_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    np = dict(payload)
    np["DestinationSelectedPath"] = _normalize_imported_memory_path(np.get("DestinationSelectedPath", ""))
    expanded = np.get("DestinationExpandedPaths", [])
    if isinstance(expanded, list):
        np["DestinationExpandedPaths"] = [_normalize_imported_memory_path(p) for p in expanded]
    return np


def _nullable(s: str) -> str | None:
    t = str(s or "").strip()
    return t if t else None


def _parse_item_type(source_type: str) -> ItemType:
    st = str(source_type or "").lower()
    return "folder" if "folder" in st else "file"


def _parse_assignment_mode_from_allocation(row: AllocationRow) -> AssignmentMode:
    """Derive explicit modes; recursive folder ops stay distinct from shallow copy/move."""
    m = str(row.AllocationMethod or "").lower()
    is_folder = _parse_item_type(row.SourceType) == "folder"
    recursive = "recursive" in m
    is_copy = "copy" in m
    is_move = "move" in m
    if is_folder and recursive:
        if is_copy:
            return "copy_recursive"
        if is_move:
            return "move_recursive"
        return "unknown"
    if is_folder:
        if is_copy:
            return "copy"
        if is_move:
            return "move"
        return "unknown"
    if is_copy:
        return "copy"
    if is_move:
        return "move"
    return "unknown"


def _site_library_from_context_dict(ctx: dict[str, Any]) -> SiteLibraryContextSnapshot:
    return SiteLibraryContextSnapshot(
        site_id=str(ctx.get("SiteId") or ctx.get("site_id") or ctx.get("SiteKey") or ctx.get("site_key") or ""),
        site_name=str(ctx.get("SiteName") or ctx.get("site_name") or ctx.get("SelectedSite") or ""),
        site_web_url=str(ctx.get("SiteWebUrl") or ctx.get("site_web_url") or ctx.get("WebUrl") or ""),
        library_drive_id=str(
            ctx.get("DriveId") or ctx.get("drive_id") or ctx.get("LibraryDriveId") or ctx.get("library_drive_id") or ""
        ),
        library_name=str(ctx.get("LibraryName") or ctx.get("library_name") or ctx.get("SelectedLibrary") or ""),
    )


def _allocation_row_to_mapping(row: AllocationRow, index: int) -> CanonicalMappingItem:
    mid = str(row.RequestId or "").strip() or f"alloc-{index}"
    return CanonicalMappingItem(
        mapping_id=mid,
        item_type=_parse_item_type(row.SourceType),
        source_path=str(row.SourcePath or ""),
        source_name=str(row.SourceItemName or ""),
        source_graph_item_id=_nullable(row.SourceItemId),
        source_graph_drive_id=_nullable(row.SourceDriveId),
        destination_path=str(row.RequestedDestinationPath or ""),
        destination_name=str(row.SourceItemName or ""),
        destination_graph_item_id=None,
        destination_parent_graph_item_id=_nullable(row.DestinationParentItemId),
        destination_graph_drive_id=_nullable(row.DestinationDriveId),
        assignment_mode=_parse_assignment_mode_from_allocation(row),
        depth=0,
        source_node_uid="",
        destination_node_uid="",
        legacy_request_id=str(row.RequestId or ""),
        raw={},
    )


def apply_library_relative_paths(snap: CanonicalSubmittedSnapshot) -> None:
    """Normalize mapping and proposed paths to be relative to source/destination library roots."""
    src_lib = str(snap.source.site_library.library_name or "").strip()
    dst_lib = str(snap.destination.site_library.library_name or "").strip()
    for m in snap.mapping_items:
        m.source_path = to_library_relative_path(m.source_path, src_lib)
        m.destination_path = to_library_relative_path(m.destination_path, dst_lib)
    for p in snap.proposed_folder_items:
        p.destination_path = to_library_relative_path(p.destination_path, dst_lib)
        p.parent_path = to_library_relative_path(p.parent_path, dst_lib)


def _proposed_to_item(pf: ProposedFolder, index: int) -> ProposedFolderMappingItem:
    pid = str(pf.DestinationId or "").strip() or f"proposed-{index}"
    d = pf.to_dict()
    return ProposedFolderMappingItem(
        proposed_id=pid,
        folder_name=str(pf.FolderName or ""),
        destination_path=str(pf.DestinationPath or ""),
        parent_path=str(pf.ParentPath or ""),
        destination_drive_id=_nullable(pf.DestinationDriveId),
        destination_parent_item_id=_nullable(pf.DestinationParentItemId),
        depth=0,
        raw={k: v for k, v in d.items() if k not in ("FolderName", "DestinationPath", "ParentPath", "DestinationDriveId", "DestinationParentItemId", "DestinationId")},
    )


def from_canonical_json_dict(raw: dict[str, Any]) -> CanonicalSubmittedSnapshot:
    """Parse strict canonical snapshot JSON object."""
    return parse_canonical_submitted_snapshot_dict(raw)


def from_canonical_json_bytes(data: bytes) -> CanonicalSubmittedSnapshot:
    try:
        obj = json.loads(data.decode("utf-8"))
    except UnicodeDecodeError as e:
        raise SnapshotValidationError("canonical JSON must be UTF-8") from e
    except json.JSONDecodeError as e:
        raise SnapshotValidationError("canonical JSON is not valid") from e
    if not isinstance(obj, dict):
        raise SnapshotValidationError("canonical JSON root must be an object")
    return parse_canonical_submitted_snapshot_dict(obj)


def from_req_payload_dict(raw: dict[str, Any], *, adapter_label: str = "req_json") -> CanonicalSubmittedSnapshot:
    """Normalize ``submit_request_package`` / REQ-*.json shape."""
    if not isinstance(raw, dict):
        raise SnapshotValidationError("REQ payload root must be a JSON object")
    tenant_raw = raw.get("Tenant") if isinstance(raw.get("Tenant"), dict) else {}
    src_ctx = raw.get("SourceContext") if isinstance(raw.get("SourceContext"), dict) else {}
    dst_ctx = raw.get("DestinationContext") if isinstance(raw.get("DestinationContext"), dict) else {}
    moves = raw.get("PlannedMoves")
    if moves is None:
        moves = raw.get("planned_moves")
    if not isinstance(moves, list):
        raise SnapshotValidationError("PlannedMoves must be a list")
    proposed_raw = raw.get("ProposedFolders")
    if proposed_raw is None:
        proposed_raw = []
    if not isinstance(proposed_raw, list):
        raise SnapshotValidationError("ProposedFolders must be a list or omitted")

    submitted = raw.get("SubmittedBy") if isinstance(raw.get("SubmittedBy"), dict) else {}
    submitted_by = str(submitted.get("DisplayName") or submitted.get("display_name") or "")

    mapping_items: list[CanonicalMappingItem] = []
    for i, row in enumerate(moves):
        if not isinstance(row, dict):
            raise SnapshotValidationError(f"PlannedMoves[{i}] must be an object")
        mapping_items.append(_allocation_row_to_mapping(AllocationRow.from_dict(row), i))

    proposed_items: list[ProposedFolderMappingItem] = []
    for i, row in enumerate(proposed_raw):
        if not isinstance(row, dict):
            raise SnapshotValidationError(f"ProposedFolders[{i}] must be an object")
        proposed_items.append(_proposed_to_item(ProposedFolder.from_dict(row), i))

    # Draft / snapshot content version vs application build: REQ ``Version`` is the former only.
    draft_version = str(
        raw.get("DraftVersion")
        or raw.get("draft_version")
        or raw.get("SnapshotContentVersion")
        or raw.get("snapshot_content_version")
        or raw.get("Version")
        or ""
    )
    app_version = str(
        raw.get("AppVersion")
        or raw.get("app_version")
        or raw.get("ConsoleVersion")
        or raw.get("console_version")
        or ""
    )
    snap = CanonicalSubmittedSnapshot(
        snapshot_id=new_snapshot_id(),
        draft_id=str(raw.get("DraftId") or raw.get("draft_id") or ""),
        draft_version=draft_version,
        submitted_at_utc=str(raw.get("CreatedOn") or raw.get("LastUpdatedOn") or utc_now_iso()),
        submitted_by=submitted_by,
        app_version=app_version,
        tenant=TenantIdentitySnapshot(
            tenant_id=str(tenant_raw.get("TenantId") or tenant_raw.get("tenant_id") or ""),
            tenant_domain=str(tenant_raw.get("TenantDomain") or tenant_raw.get("tenant_domain") or "").lower(),
            tenant_label=str(tenant_raw.get("TenantLabel") or tenant_raw.get("tenant_label") or ""),
            client_key=str(tenant_raw.get("ClientKey") or tenant_raw.get("client_key") or ""),
        ),
        source=SourceContextSnapshot(
            platform=str(src_ctx.get("platform", "sharepoint") or "sharepoint"),
            site_library=_site_library_from_context_dict(src_ctx),
            raw={k: v for k, v in src_ctx.items()},
        ),
        destination=DestinationContextSnapshot(
            platform=str(dst_ctx.get("platform", "sharepoint") or "sharepoint"),
            site_library=_site_library_from_context_dict(dst_ctx),
            raw={k: v for k, v in dst_ctx.items()},
        ),
        mapping_items=mapping_items,
        proposed_folder_items=proposed_items,
        execution_options={
            "req_request_id": str(raw.get("RequestId") or ""),
            "req_status": str(raw.get("Status") or ""),
            "needs_review": raw.get("NeedsReview") if isinstance(raw.get("NeedsReview"), list) else [],
        },
        snapshot_hash="",
        snapshot_schema=SNAPSHOT_SCHEMA_V1,
        engine_version=SNAPSHOT_ENGINE_VERSION_V1,
        adapter_source=adapter_label,
    )
    apply_library_relative_paths(snap)
    snap.snapshot_hash = snap.compute_snapshot_hash()
    return snap


def from_bundle_folder(source_folder: Path, *, adapter_label: str = "legacy_bundle") -> CanonicalSubmittedSnapshot:
    """
    Read the same required files as ``MemoryManager.import_bundle`` and build a canonical snapshot
    without writing live memory paths.
    """
    source_folder = Path(source_folder)
    required = ["Draft-SessionState.json", "Draft-AllocationQueue.json", "Draft-ProposedFolders.json"]
    for name in required:
        if not (source_folder / name).exists():
            raise FileNotFoundError(f"Import bundle missing required file: {name}")

    session_obj = json.loads((source_folder / "Draft-SessionState.json").read_text(encoding="utf-8"))
    allocations_obj = json.loads((source_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    proposed_obj = json.loads((source_folder / "Draft-ProposedFolders.json").read_text(encoding="utf-8"))

    session_obj = _normalize_imported_session_payload(session_obj)
    allocations_obj = _normalize_imported_allocations_payload(allocations_obj)
    proposed_obj = _normalize_imported_proposed_payload(proposed_obj)

    if not isinstance(session_obj, dict):
        raise SnapshotValidationError("Draft-SessionState.json must be a JSON object")
    if not isinstance(allocations_obj, list):
        raise SnapshotValidationError("Draft-AllocationQueue.json must be a JSON array")
    if not isinstance(proposed_obj, list):
        raise SnapshotValidationError("Draft-ProposedFolders.json must be a JSON array")

    export_meta: dict[str, Any] = {}
    meta_path = source_folder / "ExportMetadata.json"
    if meta_path.exists():
        try:
            loaded = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                export_meta = loaded
        except Exception:
            export_meta = {}

    mapping_items: list[CanonicalMappingItem] = []
    for i, row in enumerate(allocations_obj):
        if not isinstance(row, dict):
            raise SnapshotValidationError(f"allocations[{i}] must be an object")
        mapping_items.append(_allocation_row_to_mapping(AllocationRow.from_dict(row), i))

    proposed_items: list[ProposedFolderMappingItem] = []
    for i, row in enumerate(proposed_obj):
        if not isinstance(row, dict):
            raise SnapshotValidationError(f"proposed[{i}] must be an object")
        proposed_items.append(_proposed_to_item(ProposedFolder.from_dict(row), i))

    src_snap = session_obj.get("SourceTreeSnapshot", [])
    dst_snap = session_obj.get("DestinationTreeSnapshot", [])
    src_count = len(src_snap) if isinstance(src_snap, list) else 0
    dst_count = len(dst_snap) if isinstance(dst_snap, list) else 0

    snap = CanonicalSubmittedSnapshot(
        snapshot_id=new_snapshot_id(),
        draft_id=str(session_obj.get("DraftId", "") or ""),
        draft_version="",
        submitted_at_utc=str(export_meta.get("ExportedUtc") or utc_now_iso()),
        submitted_by=str(export_meta.get("MachineName") or "legacy_bundle"),
        app_version="legacy_bundle",
        tenant=TenantIdentitySnapshot(),
        source=SourceContextSnapshot(
            platform="sharepoint",
            site_library=SiteLibraryContextSnapshot(
                site_name=str(session_obj.get("SelectedSourceSite", "") or ""),
                library_name=str(session_obj.get("SelectedSourceLibrary", "") or ""),
                site_id=str(session_obj.get("SelectedSourceSiteKey", "") or ""),
            ),
            raw={"session_keys": ["SelectedSourceSite", "SelectedSourceSiteKey", "SelectedSourceLibrary"]},
        ),
        destination=DestinationContextSnapshot(
            platform="sharepoint",
            site_library=SiteLibraryContextSnapshot(
                site_name=str(session_obj.get("SelectedDestinationSite", "") or ""),
                library_name=str(session_obj.get("SelectedDestinationLibrary", "") or ""),
                site_id=str(session_obj.get("SelectedDestinationSiteKey", "") or ""),
            ),
            raw={"session_keys": ["SelectedDestinationSite", "SelectedDestinationSiteKey", "SelectedDestinationLibrary"]},
        ),
        mapping_items=mapping_items,
        proposed_folder_items=proposed_items,
        execution_options={
            "legacy_bundle": True,
            "export_metadata": export_meta,
            "source_selected_path": str(session_obj.get("SourceSelectedPath", "") or ""),
            "destination_selected_path": str(session_obj.get("DestinationSelectedPath", "") or ""),
            "source_tree_snapshot_count": src_count,
            "destination_tree_snapshot_count": dst_count,
        },
        snapshot_hash="",
        snapshot_schema=SNAPSHOT_SCHEMA_V1,
        engine_version=SNAPSHOT_ENGINE_VERSION_V1,
        adapter_source=adapter_label,
    )
    apply_library_relative_paths(snap)
    snap.snapshot_hash = snap.compute_snapshot_hash()
    return snap
