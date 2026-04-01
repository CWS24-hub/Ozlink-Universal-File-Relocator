"""
Phase 3: fingerprints and cache keys for destination future-model overlay builds.

Used to skip full graph rebuilds when planning + visible real tree state are unchanged,
or to extend an existing overlay when only new planned moves were appended.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_move_signature(move: dict[str, Any] | None) -> tuple:
    if not isinstance(move, dict):
        return ("", "", "", "", "", False, "")
    src = move.get("source") if isinstance(move.get("source"), dict) else {}
    return (
        str(move.get("request_id", "") or ""),
        str(move.get("source_path", "") or ""),
        str(move.get("destination_path", "") or ""),
        str(move.get("target_name", "") or ""),
        str(move.get("destination_name", "") or ""),
        bool(src.get("is_folder", False)),
        str(move.get("allocation_method", "") or ""),
    )


def move_list_signatures(moves: list | None) -> list[tuple]:
    return [stable_move_signature(m) for m in (moves or [])]


def aggregate_moves_signature(moves: list | None) -> str:
    sigs = move_list_signatures(moves)
    raw = json.dumps(sigs, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def proposed_folders_signature(proposed_folders: list | None) -> str:
    parts: list[tuple[str, str, str, str]] = []
    for pf in proposed_folders or []:
        parts.append(
            (
                str(getattr(pf, "DestinationPath", "") or ""),
                str(getattr(pf, "FolderName", "") or ""),
                str(getattr(pf, "ParentPath", "") or ""),
                str(getattr(pf, "Status", "") or ""),
            )
        )
    raw = json.dumps(parts, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def real_snapshot_signature(
    snapshot: list | None,
    *,
    drive_id: str,
    full_tree_entry_count: int,
) -> str:
    """Fingerprint visible + merged real rows (semantic_path set)."""
    paths = sorted(
        (str(e.get("semantic_path") or "") for e in (snapshot or [])),
        key=lambda s: s.lower(),
    )
    raw = json.dumps(
        {"drive_id": str(drive_id or ""), "full_tree_n": int(full_tree_entry_count), "paths": paths},
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def full_overlay_fingerprint(
    *,
    moves_sig: str,
    proposed_sig: str,
    snapshot_sig: str,
    skip_allocation_descendants: bool,
) -> str:
    raw = f"{moves_sig}|{proposed_sig}|{snapshot_sig}|{int(bool(skip_allocation_descendants))}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
