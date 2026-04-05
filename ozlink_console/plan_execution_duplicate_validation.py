"""Execution-safety duplicate detection for planned moves and manifest transfer steps.

Does not modify data — reports collisions only for blocking before export/run.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

MoveDict = dict[str, Any]
StepDict = dict[str, Any]


def norm_execution_key(path: str) -> str:
    return str(path or "").strip().replace("/", "\\").rstrip("\\").lower()


def transfer_step_file_destination_key(step: StepDict) -> str | None:
    """Stable file target key for a manifest transfer step (matches file copy semantics)."""
    if bool(step.get("is_source_folder", False)):
        return None
    dp = str(step.get("destination_path", "") or "").strip()
    leaf = str(step.get("destination_name", "") or step.get("source_name", "") or "").strip()
    if not dp or not leaf:
        return None
    dp_clean = dp.replace("/", "\\").rstrip("\\")
    parts = [p for p in dp_clean.split("\\") if p]
    if parts and parts[-1].lower() == leaf.lower():
        return dp_clean
    return f"{dp_clean}\\{leaf}"


def duplicate_collision_report_from_transfer_steps(steps: list[StepDict]) -> tuple[list[str], list[str]]:
    """
    Return (duplicate_source_displays, duplicate_destination_file_displays) using first-seen casing.
    Only considers steps with operation copy (default).
    """
    src_norm_to_display: dict[str, str] = {}
    src_norm_hits: dict[str, int] = {}
    dst_norm_to_display: dict[str, str] = {}
    dst_norm_hits: dict[str, int] = {}

    for st in steps or []:
        if not isinstance(st, dict):
            continue
        if str(st.get("operation", "copy") or "").lower() != "copy":
            continue
        sp = str(st.get("source_path", "") or "").strip()
        if sp:
            nk = norm_execution_key(sp)
            if nk:
                src_norm_to_display.setdefault(nk, sp)
                src_norm_hits[nk] = src_norm_hits.get(nk, 0) + 1
        dk = transfer_step_file_destination_key(st)
        if dk:
            nk = norm_execution_key(dk)
            if nk:
                dst_norm_to_display.setdefault(nk, dk)
                dst_norm_hits[nk] = dst_norm_hits.get(nk, 0) + 1

    dup_src = sorted(
        [src_norm_to_display[k] for k, n in src_norm_hits.items() if n > 1],
        key=lambda s: s.lower(),
    )
    dup_dst = sorted(
        [dst_norm_to_display[k] for k, n in dst_norm_hits.items() if n > 1],
        key=lambda s: s.lower(),
    )
    return dup_src, dup_dst


def duplicate_collision_report_from_moves(
    moves: list[MoveDict],
    *,
    canonical_source: Callable[[MoveDict], str],
    destination_file_key: Callable[[MoveDict], str | None],
) -> tuple[list[str], list[str]]:
    """Same as transfer-step report using MainWindow canonicalization callbacks."""
    src_norm_to_display: dict[str, str] = {}
    src_norm_hits: dict[str, int] = {}
    dst_norm_to_display: dict[str, str] = {}
    dst_norm_hits: dict[str, int] = {}

    for m in moves or []:
        if not isinstance(m, dict):
            continue
        cs = canonical_source(m)
        if cs:
            nk = norm_execution_key(cs)
            if nk:
                src_norm_to_display.setdefault(nk, str(cs).strip())
                src_norm_hits[nk] = src_norm_hits.get(nk, 0) + 1
        dk = destination_file_key(m)
        if dk:
            nk = norm_execution_key(dk)
            if nk:
                dst_norm_to_display.setdefault(nk, str(dk).strip())
                dst_norm_hits[nk] = dst_norm_hits.get(nk, 0) + 1

    dup_src = sorted(
        [src_norm_to_display[k] for k, n in src_norm_hits.items() if n > 1],
        key=lambda s: s.lower(),
    )
    dup_dst = sorted(
        [dst_norm_to_display[k] for k, n in dst_norm_hits.items() if n > 1],
        key=lambda s: s.lower(),
    )
    return dup_src, dup_dst


def grouped_duplicate_destination_moves(
    moves: list[MoveDict],
    *,
    destination_file_key: Callable[[MoveDict], str | None],
) -> dict[str, list[dict[str, Any]]]:
    """
    Group *file* planned moves by normalized destination file key (same semantics as
    duplicate_collision_report_from_moves / transfer steps). Folder moves are skipped.
    Returns only groups with 2+ members; keys are norm_execution_key strings.
    """
    buckets: dict[str, list[dict[str, Any]]] = {}
    norm_to_display: dict[str, str] = {}

    for i, m in enumerate(moves or []):
        if not isinstance(m, dict):
            continue
        dk = destination_file_key(m)
        if not dk:
            continue
        nk = norm_execution_key(dk)
        if nk not in norm_to_display:
            norm_to_display[nk] = str(dk).strip()
        display_dest = norm_to_display[nk]
        src = str(m.get("source_path", "") or "").strip()
        buckets.setdefault(nk, []).append(
            {"index": i, "source": src, "destination": display_dest}
        )

    return {k: v for k, v in buckets.items() if len(v) >= 2}


def format_execution_duplicate_message(dup_sources: list[str], dup_destinations: list[str]) -> str:
    if not dup_sources and not dup_destinations:
        return ""
    blocks: list[str] = []
    if dup_sources:
        lines = "\n".join(f"  • {p}" for p in dup_sources)
        blocks.append("Duplicate source assignment detected.\nThe same source path is assigned more than once:\n" + lines)
    if dup_destinations:
        lines = "\n".join(f"  • {p}" for p in dup_destinations)
        blocks.append(
            "Duplicate destination file target detected.\n"
            "Multiple file moves resolve to the same destination path:\n" + lines
        )
    return "\n\n".join(blocks)
