"""Needs Review row builder for unresolved destination anchor identity (session-level, no Graph mutations)."""

from __future__ import annotations

from typing import Any

from ozlink_console.models import SessionState

REVIEW_TYPE_DESTINATION_ANCHOR_MISSING = "destination_anchor_missing"


def _id_suffix(s: str, n: int = 12) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    return t[-n:] if len(t) > n else t


def review_signature_for_destination_anchor_row(
    *,
    anchor_path: str,
    item_id: str,
    drive_id: str,
    planned_moves_count: int,
    proposed_folders_count: int,
) -> str:
    """Stable string for log de-duplication when the review row content meaningfully changes."""
    return "|".join(
        [
            anchor_path.casefold(),
            item_id.casefold(),
            drive_id.casefold(),
            str(int(planned_moves_count)),
            str(int(proposed_folders_count)),
        ]
    )


def build_destination_anchor_missing_needs_review_row(
    session: SessionState,
    *,
    planned_moves_count: int,
    proposed_folders_count: int,
) -> dict[str, Any] | None:
    """
    When ``DestinationAnchorLiveUnresolved`` is true, surface a session-level Needs Review row.

    Callers must not set this flag when a rename was resolved while retaining the same Graph item id
    (``classify_anchor_tree_observation`` RENAMED / OK); only unresolved missing anchors use this path.
    """
    if not bool(getattr(session, "DestinationAnchorLiveUnresolved", False)):
        return None
    anchor_path = str(getattr(session, "DestinationAnchorDisplayPath", "") or "").strip()
    item_id = str(getattr(session, "DestinationAnchorItemId", "") or "").strip()
    drive_id = str(getattr(session, "DestinationAnchorDriveId", "") or "").strip()
    item_suffix = _id_suffix(item_id)
    drive_suffix = _id_suffix(drive_id)
    pm = int(planned_moves_count)
    pf = int(proposed_folders_count)
    affected = pm + pf
    sig = review_signature_for_destination_anchor_row(
        anchor_path=anchor_path,
        item_id=item_id,
        drive_id=drive_id,
        planned_moves_count=pm,
        proposed_folders_count=pf,
    )
    parts: list[str] = []
    if item_suffix:
        parts.append(f"Anchor id …{item_suffix}")
    if drive_suffix:
        parts.append(f"Drive …{drive_suffix}")
    if pm or pf:
        parts.append(f"{pm} planned move(s), {pf} proposed folder(s)")
    action = " · ".join(parts) if parts else "Review destination anchor and affected planning"
    return {
        "item_name": "Destination anchor folder needs review",
        "source_path": anchor_path or "(unknown anchor path)",
        "reason": (
            "The folder used as the live destination anchor may have been moved, renamed, or deleted in SharePoint. "
            "Review affected planning before execution."
        ),
        "action": action,
        "review_type": REVIEW_TYPE_DESTINATION_ANCHOR_MISSING,
        "anchor_item_id_suffix": item_suffix,
        "destination_drive_id_suffix": drive_suffix,
        "affected_planning_total": affected,
        "_review_sig": sig,
    }
