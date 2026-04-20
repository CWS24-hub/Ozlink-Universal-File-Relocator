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
    # Visible library hub segment for path checks, e.g. Root3 (optional).
    visible_destination_anchor: str = ""


@dataclass
class MigrationConflictRecord:
    kind: str
    path: str
    detail: str = ""


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


def migration_identity_complete(mi: MigrationIdentityPreflight | None) -> bool:
    if mi is None:
        return False
    return bool(
        str(mi.source_drive_id or "").strip()
        and str(mi.destination_drive_id or "").strip()
        and str(mi.destination_site_key or "").strip()
        and str(mi.source_site_key or "").strip()
    )
