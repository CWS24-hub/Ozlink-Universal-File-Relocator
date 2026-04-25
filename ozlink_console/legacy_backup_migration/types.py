from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class MigrationIdentityPreflight:
    """Confirmed source/destination identity before mutating planning rows (caller / wizard)."""

    source_site_key: str = ""
    source_site_id: str = ""
    source_drive_id: str = ""
    destination_site_key: str = ""
    destination_site_id: str = ""
    destination_drive_id: str = ""
    # Selected library display names (legacy app sometimes prefixed paths with these; stripped first).
    source_library_display_name: str = ""
    destination_library_display_name: str = ""
    # Visible library hub segment for path checks (mutable display label of the anchor folder — not durable identity).
    visible_destination_anchor: str = ""
    destination_anchor_item_id: str = ""
    destination_anchor_drive_id: str = ""
    destination_anchor_display_path: str = ""
    destination_anchor_path_verified_at_utc: str = ""
    destination_anchor_path_only_binding: bool = False


@dataclass
class MigrationConflictRecord:
    kind: str
    path: str
    detail: str = ""
    proposed_row_index: int | None = None
    proposed_stable_key: str = ""
    proposed_folder_name: str = ""
    proposed_parent_path: str = ""
    proposed_full_path: str = ""
    checked_graph_path: str = ""
    live_item_id: str = ""
    live_item_name: str = ""
    live_item_type: str = ""
    live_item_web_url: str = ""
    destination_drive_id: str = ""

    def to_report_dict(self) -> dict[str, Any]:
        """Serialize for LegacyMigrationReport.json (allocation conflicts stay minimal)."""
        out: dict[str, Any] = {"kind": self.kind, "path": self.path, "detail": self.detail}
        if self.kind != "live_duplicate_proposed_folder":
            return out
        out.update(
            {
                "proposed_row_index": self.proposed_row_index,
                "proposed_stable_key": self.proposed_stable_key,
                "proposed_folder_name": self.proposed_folder_name,
                "proposed_parent_path": self.proposed_parent_path,
                "proposed_full_path": self.proposed_full_path,
                "checked_graph_path": self.checked_graph_path or self.path,
                "live_item_id": self.live_item_id,
                "live_item_name": self.live_item_name,
                "live_item_type": self.live_item_type,
                "live_item_web_url": self.live_item_web_url,
                "destination_drive_id": self.destination_drive_id,
            }
        )
        return out


@dataclass
class MigrationResult:
    ok: bool
    output_folder: Path | None = None
    needs_identity_confirmation: bool = False
    error_message: str = ""
    report: dict[str, Any] = field(default_factory=dict)


# Legacy migration: first-segment / anchor classification (serialized on rows as LegacyMigrationAnchorClassification).
ANCHOR_CLASS_LIVE_MATCHED = "live_anchor_matched"
ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH = "reanchored_to_live_graph"
ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY = "planned_scaffold_in_empty_library"
ANCHOR_CLASS_UNRESOLVED_AMBIGUOUS_ANCHOR = "unresolved_ambiguous_anchor"
ANCHOR_CLASS_FOREIGN_ROOT_BLOCKED = "foreign_root_blocked"
ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_EXACT = "planned_parent_resolved_exact"
ANCHOR_CLASS_PLANNED_PARENT_RESOLVED_ANCESTOR = "planned_parent_resolved_ancestor"
ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT = "planned_parent_missing_descendant"


def migration_identity_complete(mi: MigrationIdentityPreflight | None) -> bool:
    if mi is None:
        return False
    return bool(
        str(mi.source_drive_id or "").strip()
        and str(mi.destination_drive_id or "").strip()
        and str(mi.destination_site_key or "").strip()
        and str(mi.source_site_key or "").strip()
    )
