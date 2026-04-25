"""Map LegacyMigrationReport.json conflicts to Needs Review style rows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ozlink_console.logger import log_info


def load_migration_conflicts_for_review(folder: Path) -> list[dict[str, Any]]:
    """Read LegacyMigrationReport.json conflicts and return workflow review-shaped dicts."""
    folder = Path(folder)
    p = folder / "LegacyMigrationReport.json"
    if not p.is_file():
        return []
    try:
        rep = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    conflicts = rep.get("conflicts")
    if not isinstance(conflicts, list):
        return []
    out: list[dict[str, Any]] = []
    for c in conflicts:
        if not isinstance(c, dict):
            continue
        kind = str(c.get("kind") or "").strip()
        if kind == "live_duplicate_proposed_folder":
            log_info("migrated_live_duplicate_proposed_folder_loaded", path_excerpt=str(c.get("checked_graph_path") or c.get("path") or "")[:160])
        proposed_path = str(
            c.get("proposed_full_path")
            or c.get("path")
            or c.get("checked_graph_path")
            or ""
        )
        live_path = str(c.get("checked_graph_path") or c.get("path") or proposed_path)
        row = {
            "item_name": str(c.get("proposed_folder_name") or c.get("live_item_name") or "Migration conflict"),
            "source_path": proposed_path or "(migration)",
            "reason": f"Migration conflict: {kind} — live Graph item may already exist at the planned path. Live wins structurally; resolve proposed/planned rows.",
            "action": str(c.get("live_item_web_url") or live_path or "")[:500],
            "review_type": "migration_live_duplicate_proposed_folder",
            "migration_conflict": True,
            "migration_kind": kind,
            "migration_proposed_stable_key": str(c.get("proposed_stable_key") or ""),
            "migration_proposed_row_index": c.get("proposed_row_index"),
            "migration_live_item_id": str(c.get("live_item_id") or ""),
            "migration_live_item_web_url": str(c.get("live_item_web_url") or ""),
            "migration_checked_graph_path": str(c.get("checked_graph_path") or c.get("path") or ""),
        }
        out.append(row)
        log_info(
            "migrated_conflict_review_item_created",
            review_type=row["review_type"],
            kind=kind,
        )
    if out:
        log_info("migrated_conflicts_loaded", count=len(out), folder=str(folder))
    return out
