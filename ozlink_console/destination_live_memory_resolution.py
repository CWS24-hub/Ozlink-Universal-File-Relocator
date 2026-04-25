"""Planning-only resolution helpers for live Graph vs memory conflicts (no SharePoint mutations)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable

from ozlink_console.models import ProposedFolder

# Persisted on ProposedFolder.LiveMemoryDuplicateResolution when user accepts the live folder.
RESOLVED_BY_ACCEPT_EXISTING_LIVE_FOLDER = "accept_existing_live_folder"


def live_memory_resolution_utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def proposed_folder_accepts_existing_live_folder(pf: ProposedFolder) -> bool:
    return str(getattr(pf, "LiveMemoryDuplicateResolution", "") or "").strip() == RESOLVED_BY_ACCEPT_EXISTING_LIVE_FOLDER


def apply_accept_existing_live_folder_to_proposed_folder(
    pf: ProposedFolder,
    *,
    live_item_id: str,
    live_item_path: str,
    resolved_at_utc: str | None = None,
) -> ProposedFolder:
    ts = resolved_at_utc if resolved_at_utc is not None else live_memory_resolution_utc_now_iso()
    return replace(
        pf,
        LiveMemoryDuplicateResolution=RESOLVED_BY_ACCEPT_EXISTING_LIVE_FOLDER,
        LiveMemoryDuplicateLiveItemId=str(live_item_id or ""),
        LiveMemoryDuplicateLiveItemPath=str(live_item_path or ""),
        LiveMemoryDuplicateResolvedAtUtc=ts,
        Status="AcceptedLive",
    )


def filter_runtime_live_memory_rows(
    rows: list[dict[str, Any]],
    *,
    normalized_source_path: str,
    live_memory_subtype: str,
    normalize_path: Callable[[str], str],
) -> tuple[list[dict[str, Any]], int]:
    """Return rows without the matching conflict row; second value is how many were removed."""
    want_sp = normalize_path(str(normalized_source_path or ""))
    want_st = str(live_memory_subtype or "").strip()
    out: list[dict[str, Any]] = []
    removed = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        if str(r.get("live_memory_subtype") or "").strip() != want_st:
            out.append(r)
            continue
        if normalize_path(str(r.get("source_path") or "")) != want_sp:
            out.append(r)
            continue
        removed += 1
    return out, removed
