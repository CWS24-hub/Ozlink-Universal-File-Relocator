"""Heuristic detection of legacy-shaped memory bundles (incomplete modern identity + Graph ids)."""

from __future__ import annotations

from typing import Any


def is_legacy_shaped_bundle(
    session: dict[str, Any],
    allocations: list[Any],
    proposed: list[Any],
) -> tuple[bool, list[str]]:
    """Return (is_legacy, human-readable reasons). Conservative: prefer migration when uncertain."""

    reasons: list[str] = []

    has_planning = bool(
        any(str(r.get("RequestedDestinationPath") or "").strip() for r in allocations if isinstance(r, dict))
        or any(
            str(r.get("DestinationPath") or "").strip() or str(r.get("ParentPath") or "").strip()
            for r in proposed
            if isinstance(r, dict)
        )
    )
    if not has_planning:
        return False, []

    dest_lib = str(session.get("SelectedDestinationLibraryId") or "").strip()
    dst_env_site = str(session.get("DestinationTreeSnapshotIdentitySiteId") or "").strip()
    dst_env_drive = str(session.get("DestinationTreeSnapshotIdentityDriveId") or "").strip()

    if not dest_lib:
        reasons.append("missing_SelectedDestinationLibraryId")

    missing_row_ids = 0
    rows_with_path = 0
    for row in allocations or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("RequestedDestinationPath") or "").strip():
            rows_with_path += 1
        dd = str(row.get("DestinationDriveId") or "").strip()
        dp = str(row.get("DestinationParentItemId") or "").strip()
        if str(row.get("RequestedDestinationPath") or "").strip() and (not dd or not dp):
            missing_row_ids += 1

    if rows_with_path and missing_row_ids * 2 >= rows_with_path:
        reasons.append("allocation_rows_missing_destination_graph_ids")

    prop_missing = 0
    prop_n = 0
    for row in proposed or []:
        if not isinstance(row, dict):
            continue
        prop_n += 1
        if str(row.get("ParentPath") or "").strip():
            dd = str(row.get("DestinationDriveId") or "").strip()
            dp = str(row.get("DestinationParentItemId") or "").strip()
            if not dd or not dp:
                prop_missing += 1
    if prop_n and prop_missing * 2 >= prop_n:
        reasons.append("proposed_rows_missing_destination_graph_ids")

    if not dst_env_drive or not dst_env_site:
        reasons.append("incomplete_DestinationTreeSnapshotIdentity")

    is_legacy = bool(reasons)
    return is_legacy, reasons
