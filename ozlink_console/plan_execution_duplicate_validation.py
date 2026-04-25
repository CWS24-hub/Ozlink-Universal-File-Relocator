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


def duplicate_transfer_step_groups_by_destination(
    steps: list[StepDict],
) -> list[tuple[str, str, list[StepDict]]]:
    """
    File copy steps that share the same normalized destination file key.

    Returns tuples ``(norm_dest_key, display_destination, colliding_steps)``, sorted by display
    string (case-insensitive). Mirrors :func:`duplicate_collision_report_from_transfer_steps`
    eligibility (``operation`` copy, non-folder file destination key only).
    """
    norm_to_display: dict[str, str] = {}
    buckets: dict[str, list[StepDict]] = {}
    for st in steps or []:
        if not isinstance(st, dict):
            continue
        if str(st.get("operation", "copy") or "").lower() != "copy":
            continue
        dk = transfer_step_file_destination_key(st)
        if not dk:
            continue
        nk = norm_execution_key(dk)
        norm_to_display.setdefault(nk, str(dk).strip())
        buckets.setdefault(nk, []).append(st)
    out = [
        (nk, norm_to_display[nk], members)
        for nk, members in buckets.items()
        if len(members) > 1
    ]
    out.sort(key=lambda t: t[1].lower())
    return out


def duplicate_transfer_step_groups_by_source(
    steps: list[StepDict],
) -> list[tuple[str, str, list[StepDict]]]:
    """
    File copy steps that share the same normalized source path (duplicate source assignment).

    Returns ``(norm_source_key, display_source, colliding_steps)``, sorted by display source.
    """
    norm_to_display: dict[str, str] = {}
    buckets: dict[str, list[StepDict]] = {}
    for st in steps or []:
        if not isinstance(st, dict):
            continue
        if str(st.get("operation", "copy") or "").lower() != "copy":
            continue
        sp = str(st.get("source_path", "") or "").strip()
        if not sp:
            continue
        nk = norm_execution_key(sp)
        norm_to_display.setdefault(nk, sp)
        buckets.setdefault(nk, []).append(st)
    out = [
        (nk, norm_to_display[nk], members)
        for nk, members in buckets.items()
        if len(members) > 1
    ]
    out.sort(key=lambda t: t[1].lower())
    return out


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


def leaf_base_ext_for_execution_inspector_suffix(leaf: str) -> tuple[str, str]:
    """Split a destination file leaf into stem and extension (last dot), matching duplicate-review semantics."""
    leaf = str(leaf or "").strip()
    if not leaf:
        return "", ""
    dot = leaf.rfind(".")
    if dot > 0:
        return leaf[:dot], leaf[dot:]
    return leaf, ""


def build_execution_inspector_dest_suffix_preview_rows_for_moves(
    moves: list[dict[str, Any]],
    pr_sorted: list[int],
    *,
    destination_file_key: Callable[[dict[str, Any]], str | None],
    move_target_name: Callable[[dict[str, Any]], str],
    leaf_base_ext: Callable[[str], tuple[str, str]] | None = None,
) -> list[dict[str, Any]] | None:
    """Planning-only suggested names for duplicate *destination file* groups: stem.ext, stem (2).ext, …

    Used by the execution duplicate inspector; does not mutate ``moves``. Uniqueness is enforced with
    ``norm_execution_key(destination_file_key(trial_move))`` against other planned rows and within the group.
    """
    splitter = leaf_base_ext or leaf_base_ext_for_execution_inspector_suffix
    ntot = len(moves)
    idxs = sorted({int(i) for i in pr_sorted if int(i) >= 0})
    if len(idxs) < 2:
        return None
    if any(i >= ntot for i in idxs):
        return None
    group_set = set(idxs)
    used_nk: set[str] = set()
    for j, m in enumerate(moves):
        if j in group_set:
            continue
        dk = destination_file_key(m)
        if dk:
            used_nk.add(norm_execution_key(dk))

    stem, ext = splitter(move_target_name(moves[idxs[0]]))
    if not stem and not ext:
        stem = "file"
    out: list[dict[str, Any]] = []
    for rank, idx in enumerate(idxs):
        move = moves[idx]
        if bool((move.get("source") or {}).get("is_folder")):
            return None

        def candidates():
            if rank == 0:
                yield f"{stem}{ext}"
                n = 2
                while True:
                    yield f"{stem} ({n}){ext}"
                    n += 1
            else:
                n = rank + 1
                while True:
                    yield f"{stem} ({n}){ext}"
                    n += 1

        gen = candidates()
        leaf: str | None = None
        for _ in range(500):
            c = next(gen)
            trial = dict(move)
            trial["target_name"] = c
            dk = destination_file_key(trial)
            if not dk:
                continue
            nk = norm_execution_key(dk)
            if nk in used_nk:
                continue
            used_nk.add(nk)
            leaf = c
            break
        if not leaf:
            return None
        out.append(
            {
                "index": idx,
                "source_path": str(move.get("source_path", "") or "").strip(),
                "current_leaf": str(move_target_name(move) or "").strip(),
                "suggested_leaf": leaf,
            }
        )
    return out


def build_execution_inspector_chained_dest_suffix_preview_rows(
    moves: list[dict[str, Any]],
    group_pr_lists: list[list[int]],
    *,
    destination_file_key: Callable[[dict[str, Any]], str | None],
    move_target_name: Callable[[dict[str, Any]], str],
    leaf_base_ext: Callable[[str], tuple[str, str]] | None = None,
) -> list[dict[str, Any]] | None:
    """Suffix suggestions for multiple destination-duplicate groups, chaining occupancy via a scratch move list."""
    moves_live = list(moves)
    working: list[dict[str, Any]] = [dict(m) if isinstance(m, dict) else {} for m in moves_live]
    out: list[dict[str, Any]] = []
    for pr_list in sorted(group_pr_lists, key=lambda pl: min(pl) if pl else 0):
        pr_sorted = sorted({int(x) for x in pr_list if int(x) >= 0})
        if len(pr_sorted) < 2:
            continue
        sub = build_execution_inspector_dest_suffix_preview_rows_for_moves(
            working,
            pr_sorted,
            destination_file_key=destination_file_key,
            move_target_name=move_target_name,
            leaf_base_ext=leaf_base_ext,
        )
        if sub is None:
            return None
        for row in sub:
            ix = int(row["index"])
            if 0 <= ix < len(working):
                working[ix]["target_name"] = str(row.get("suggested_leaf", "") or "").strip()
        out.extend(sub)
    return out if out else None
