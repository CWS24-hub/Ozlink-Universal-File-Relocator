"""Runtime live Graph vs memory planning conflicts (delta branch scope; no silent merge)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

REVIEW_TYPE_LIVE_MEMORY_DUPLICATE = "live_memory_duplicate"


@dataclass
class LiveMemoryConflictRecord:
    kind: str  # live_memory_duplicate
    subtype: str
    planned_or_proposed_path: str
    live_graph_path: str
    live_item_id: str
    live_item_name: str
    live_item_type: str
    web_url: str = ""
    correlation: str = ""
    affected_branch: str = ""
    suggested_actions: str = "Review: live Graph row wins structurally; choose an action."

    def to_workflow_row(self) -> dict[str, Any]:
        return {
            "item_name": f"Live vs memory ({self.subtype})",
            "source_path": self.planned_or_proposed_path or self.live_graph_path,
            "reason": (
                f"Microsoft 365 already has an item at this path while planning lists a pending "
                f"{self.subtype.replace('live_duplicate_', '').replace('_', ' ')}. "
                f"{self.suggested_actions}"
            ),
            "action": self.live_graph_path,
            "review_type": REVIEW_TYPE_LIVE_MEMORY_DUPLICATE,
            "live_memory_subtype": self.subtype,
            "live_graph_path": self.live_graph_path,
            "live_item_id": self.live_item_id,
            "live_item_name": self.live_item_name,
            "live_item_type": self.live_item_type,
            "live_web_url": self.web_url,
            "correlation": self.correlation,
            "affected_branch": self.affected_branch,
            "resolution_stub_only": self.subtype != "live_duplicate_proposed_folder",
        }


def normalize_path_key(path: str) -> str:
    return str(path or "").replace("/", "\\").strip().casefold()


def detect_conflicts_for_live_paths(
    *,
    live_path_by_key: dict[str, dict[str, Any]],
    proposed_destination_paths: list[str],
    planned_destination_paths: list[str],
    allocation_targets: list[str],
) -> list[LiveMemoryConflictRecord]:
    """Pure comparison: normalized destination paths vs live paths (same drive scope assumed)."""
    out: list[LiveMemoryConflictRecord] = []
    if not live_path_by_key:
        return out

    def _hit(memory_path: str, live_meta: dict[str, Any], subtype: str) -> None:
        out.append(
            LiveMemoryConflictRecord(
                kind=REVIEW_TYPE_LIVE_MEMORY_DUPLICATE,
                subtype=subtype,
                planned_or_proposed_path=memory_path,
                live_graph_path=str(live_meta.get("path") or ""),
                live_item_id=str(live_meta.get("id") or ""),
                live_item_name=str(live_meta.get("name") or ""),
                live_item_type=str(live_meta.get("type") or "folder"),
                web_url=str(live_meta.get("webUrl") or live_meta.get("web_url") or ""),
                correlation=str(live_meta.get("correlation") or ""),
                affected_branch=str(live_meta.get("branch") or ""),
            )
        )

    prop_keys = {normalize_path_key(p): p for p in proposed_destination_paths if str(p).strip()}
    plan_keys = {normalize_path_key(p): p for p in planned_destination_paths if str(p).strip()}
    alloc_keys = {normalize_path_key(p): p for p in allocation_targets if str(p).strip()}

    for lk, meta in live_path_by_key.items():
        if not lk:
            continue
        is_file = str(meta.get("type") or "").lower() == "file"
        if lk in prop_keys:
            if is_file:
                _hit(prop_keys[lk], meta, "live_type_mismatch_folder_vs_file")
            else:
                _hit(prop_keys[lk], meta, "live_duplicate_proposed_folder")
            continue
        if lk in plan_keys:
            if is_file:
                _hit(plan_keys[lk], meta, "live_duplicate_planned_file")
            else:
                _hit(plan_keys[lk], meta, "live_duplicate_planned_folder")
            continue
        if lk in alloc_keys:
            _hit(alloc_keys[lk], meta, "live_duplicate_allocated_target")

    return out


def merge_unique(rows: list[LiveMemoryConflictRecord], existing: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge new conflict rows with existing workflow shapes, de-duplicating on path + subtype."""
    seen = {
        (str(r.get("review_type")), str(r.get("source_path")), str(r.get("live_memory_subtype")))
        for r in existing
        if isinstance(r, dict)
    }
    out = list(existing)
    for rec in rows:
        d = rec.to_workflow_row()
        key = (d.get("review_type"), d.get("source_path"), d.get("live_memory_subtype"))
        if key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out
