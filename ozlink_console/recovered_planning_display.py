"""Display-only labels for legacy-migrated planning rows (raw Status unchanged on disk)."""

from __future__ import annotations

from typing import Any, Mapping

from ozlink_console.legacy_backup_migration.types import (
    ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY,
    ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT,
)


def _row_get(row: Any, key: str, default: str = "") -> str:
    if row is None:
        return default
    if isinstance(row, Mapping):
        v = row.get(key, default)
        return str(v if v is not None else default)
    v = getattr(row, key, default)
    return str(v if v is not None else default)


def _row_truthy(row: Any, key: str) -> bool:
    v = _row_get(row, key, "")
    if isinstance(row, Mapping) and key in row:
        return bool(row[key])
    return v.lower() in ("1", "true", "yes") or (v == "True")


def _alnum_lower(s: str) -> str:
    return "".join(c.lower() for c in s if c.isalnum())


def _looks_real_graph_drive_id(s: str) -> bool:
    t = (s or "").strip()
    return bool(t.startswith("b!") and len(t) > 20)


def _destination_graph_identity_valid(row: Any) -> bool:
    drive = _row_get(row, "DestinationDriveId")
    dest = row.get("destination") if isinstance(row, Mapping) else None
    if isinstance(dest, Mapping):
        drive = drive or str(dest.get("drive_id") or "")
    return _looks_real_graph_drive_id(drive)


def _destination_parent_item_id_nonempty(row: Any) -> bool:
    pid = _row_get(row, "DestinationParentItemId")
    if pid.strip():
        return True
    dest = row.get("destination") if isinstance(row, Mapping) else None
    if isinstance(dest, Mapping):
        return bool(str(dest.get("id") or "").strip())
    return False


def _row_has_legacy_metadata(row: Any) -> bool:
    if _row_get(row, "LegacyMigrationDestinationParentResolution", "").strip():
        return True
    if _row_get(row, "LegacyMigrationAnchorClassification", "").strip():
        return True
    if _row_get(row, "LegacyMigrationPlannedParentMatchKind", "").strip():
        return True
    if _row_truthy(row, "LegacyMigrationPlannedParentResolved"):
        return True
    if _row_truthy(row, "LegacyMigrationPlannedScaffoldOnly"):
        return True
    if _row_truthy(row, "LegacyMigrationRootNotLiveConfirmed"):
        return True
    if _row_truthy(row, "LegacyMigrationUnresolvedGraphAnchor"):
        return True
    return False


def _looks_like_legacy_migrated_status(raw: str) -> bool:
    rl = raw.lower()
    return "legacy" in rl or "migrated" in rl


def recovered_planning_display_label(row: Any) -> str | None:
    """
    Friendly label for migrated / recovered rows; returns None to keep the caller's default caption.

    Caller should still persist ``Status`` and migration metadata fields verbatim in JSON.
    """
    raw = _row_get(row, "Status", "")
    if not raw.strip():
        return None
    if not (_looks_like_legacy_migrated_status(raw) or _row_has_legacy_metadata(row)):
        return None
    compact = _alnum_lower(raw)
    res = _row_get(row, "LegacyMigrationDestinationParentResolution", "").strip().lower()
    mk = _row_get(row, "LegacyMigrationPlannedParentMatchKind", "").strip().lower()
    cls = _row_get(row, "LegacyMigrationAnchorClassification", "").strip()
    ppr = _row_truthy(row, "LegacyMigrationPlannedParentResolved")
    scaffold_only = _row_truthy(row, "LegacyMigrationPlannedScaffoldOnly")

    # 1 — Graph parent resolved
    if res == "graph":
        return "Recovered — Graph parent resolved"
    if _destination_graph_identity_valid(row) and _destination_parent_item_id_nonempty(row) and res != "planned_parent":
        return "Recovered — Graph parent resolved"

    # 2 — Partial / missing descendant (before generic planned-parent resolved)
    if mk in ("missing_descendant", "partial"):
        return "Recovered — partial planned parent match"
    if cls == ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT or "planned_parent_missing_descendant" in cls.lower():
        return "Recovered — partial planned parent match"
    if "plannedparentpartial" in compact:
        return "Recovered — partial planned parent match"

    # 3 — Planned scaffold
    if cls == ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY or scaffold_only:
        return "Recovered — planned scaffold"

    # 4 — Planned parent resolved
    if res == "planned_parent" or ppr or "plannedparentresolved" in compact:
        return "Recovered — planned parent resolved"

    # 5 — Unresolved re-anchor (needs review)
    if "legacyreanchorfailed" in compact:
        if res not in ("graph", "planned_parent") and not ppr:
            return "Recovered — needs review"

    return None


def recovered_planning_status_tooltip(*, raw_status: str, display_label: str | None) -> str | None:
    """Optional tooltip: show stored status when the visible label is substituted."""
    raw = (raw_status or "").strip()
    if not raw:
        return None
    if display_label and display_label != raw:
        return f"Stored status: {raw}"
    return None
