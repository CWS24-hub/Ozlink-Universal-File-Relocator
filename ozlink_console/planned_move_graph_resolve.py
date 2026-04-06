"""Resolve Microsoft Graph drive/item ids for planned moves from path strings (legacy drafts without ids)."""

from __future__ import annotations

import re
from typing import Any, Callable, MutableSet, Optional

from ozlink_console.graph import GraphClient
from ozlink_console.logger import log_info, log_trace, log_warn


def graph_dest_parent_negative_cache_key(
    dest_drive_id: str,
    parent_rel: str,
    dest_library_name: str,
    dest_site_name: str,
) -> str:
    """Session key for a failed Graph lookup of a destination parent folder (drive + library context)."""
    pr = (parent_rel or "").strip()
    marker = pr if pr else ":root:"
    return (
        f"{str(dest_drive_id or '').strip()}\x00{str(dest_library_name or '').strip()}\x00"
        f"{str(dest_site_name or '').strip()}\x00{marker}"
    )


def is_internal_proposed_destination_item_id(value: str) -> bool:
    """
    True for UI-only destination folder ids (not Microsoft Graph driveItem ids).

    Planned moves and tree rows use ``PROP-*`` / ``INLINE-PROP-*`` placeholders; treating them as
    real Graph ids skips path-based parent resolution and can send invalid parents to copy/mkdir.
    """
    s = str(value or "").strip()
    if not s:
        return False
    u = s.upper()
    return u.startswith("PROP-") or u.startswith("INLINE-PROP-")


def _path_segments(path: str) -> list[str]:
    return [p for p in str(path or "").replace("\\", "/").strip("/").split("/") if p]


def _strip_leading_site_library_parts(parts: list[str], site_l: str, lib_l: str) -> list[str]:
    """Remove repeated site/library prefix segments (import glitches, duplicate labels)."""
    out = list(parts)
    while out:
        head = out[0].strip().lower()
        if site_l and head == site_l:
            out = out[1:]
            continue
        if lib_l and head == lib_l:
            out = out[1:]
            continue
        break
    return out


def allocation_path_to_drive_relative(
    path: str,
    *,
    library_name: str = "",
    site_name: str = "",
) -> str:
    """
    Convert stored allocation / tree paths to a path relative to the document library root
    for ``GET /drives/{id}/root:/relative`` (Graph).

    Handles:
    - ``LibraryName\\FTBMRoot\\...`` (leading library segment)
    - ``Site / Library / FTBMRoot/...`` display paths
    - Already-relative ``Root\\HR\\...`` or ``FTBMRoot\\...``
    - Repeated site/library prefixes from bad imports
    """
    text = str(path or "").strip()
    if not text:
        return ""

    site_l = (site_name or "").strip().lower()
    lib_l = (library_name or "").strip().lower()

    if " / " in text:
        parts = [p.strip() for p in text.split(" / ") if p.strip()]
        parts = _strip_leading_site_library_parts(parts, site_l, lib_l)
        return "/".join(parts).replace("\\", "/").strip("/")

    segs = _path_segments(text)
    segs = [s.strip() for s in segs if s.strip()]
    segs = _strip_leading_site_library_parts(segs, site_l, lib_l)
    return "/".join(segs).strip("/")


def _parent_and_leaf(relative_path: str) -> tuple[str, str]:
    """Parent folder path (relative to drive root) and single-segment leaf name."""
    rel = str(relative_path or "").replace("\\", "/").strip("/")
    if not rel:
        return "", ""
    parts = [p for p in rel.split("/") if p]
    if len(parts) == 1:
        return "", parts[0]
    return "/".join(parts[:-1]), parts[-1]


def drive_relative_path_candidates(
    path: str,
    *,
    library_name: str = "",
    site_name: str = "",
    max_candidates: int = 16,
) -> list[str]:
    """
    Build ordered unique relative paths to try with ``GET .../root:/path`` when imports use odd shapes.

    First candidate is always ``allocation_path_to_drive_relative``; additional variants cover
    slash-only splits, embedded ``library/site`` in the middle, and shallow chomps of unknown prefixes.
    """
    candidates: list[str] = []
    seen: set[str] = set()
    lib_l = (library_name or "").strip().lower()
    site_l = (site_name or "").strip().lower()

    def add(rel: str) -> None:
        rel = str(rel or "").replace("\\", "/").strip("/")
        if not rel or rel in seen:
            return
        seen.add(rel)
        candidates.append(rel)

    text = str(path or "").strip()
    add(allocation_path_to_drive_relative(text, library_name=library_name, site_name=site_name))

    if not text:
        return candidates[:max_candidates]

    flex = [p.strip() for p in re.split(r"[/\\]+", text) if p.strip()]
    flex = _strip_leading_site_library_parts(flex, site_l, lib_l)
    if flex:
        add("/".join(flex))

    if " / " in text:
        parts = [p.strip() for p in text.split(" / ") if p.strip()]
        for i, p in enumerate(parts):
            if lib_l and p.strip().lower() == lib_l and i + 1 < len(parts):
                add("/".join(parts[i + 1 :]).replace("\\", "/").strip("/"))
        if len(parts) >= 2 and lib_l and parts[0].lower() == lib_l:
            add("/".join(parts[1:]).replace("\\", "/").strip("/"))

    segs = _path_segments(text)
    segs = [s.strip() for s in segs if s.strip()]
    # Avoid dropping arbitrary leading segments: in real drafts, those segments can include
    # client-specific folders, and dropping them can resolve the *wrong* destination parent id.
    # Only allow shallow chomp when dropped prefix segments are clearly "document-root-ish".
    rootish = {"root", "ftbmroot"}
    for drop in range(1, min(4, len(segs))):
        dropped = [s.strip().lower() for s in segs[:drop]]
        if dropped and all(d in rootish for d in dropped):
            chunk = _strip_leading_site_library_parts(segs[drop:], site_l, lib_l)
            if chunk:
                add("/".join(chunk))

    return candidates[:max_candidates]


def resolve_item_by_path_candidates(
    get_item_by_path: Callable[[str, str], Optional[dict[str, Any]]],
    drive_id: str,
    candidates: list[str],
    *,
    phase: str,
    log_context: dict[str, Any],
) -> tuple[Optional[dict[str, Any]], int]:
    """
    Try each candidate path until Graph returns an item. Returns (item_or_none, index_of_success).

    Logs a single warning with full context if every candidate fails; logs info when a non-primary
    candidate succeeds. Per-candidate 404s go to trace when ``OZLINK_FULL_TRACE`` is enabled.
    """
    drive_id = str(drive_id or "").strip()
    if not drive_id:
        log_warn(
            "graph_resolve_path_candidates_aborted",
            phase=phase,
            reason="missing_drive_id",
            **log_context,
        )
        return None, -1

    last_rel = ""
    for idx, rel in enumerate(candidates):
        if not rel:
            continue
        last_rel = rel
        try:
            item = get_item_by_path(drive_id, rel)
        except Exception as exc:
            log_warn(
                "graph_resolve_path_candidate_exception",
                phase=phase,
                candidate_index=idx,
                rel_attempt=rel[:240],
                error=str(exc)[:500],
                **log_context,
            )
            continue
        if item and item.get("id"):
            if idx > 0:
                log_info(
                    "graph_resolve_path_fallback_success",
                    phase=phase,
                    candidate_index=idx,
                    rel_used=rel[:240],
                    **log_context,
                )
            return item, idx
        log_trace(
            "graph_resolve",
            "path_candidate_miss",
            phase=phase,
            candidate_index=idx,
            rel_attempt=rel[:240],
            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            **{k: v for k, v in log_context.items() if k in ("move_index", "request_id", "proposed_index", "folder_name")},
        )

    log_warn(
        "graph_resolve_all_path_candidates_failed",
        phase=phase,
        reason="no_graph_match_for_any_candidate",
        candidates_tried=[c[:180] for c in candidates if c][:20],
        candidate_count=len([c for c in candidates if c]),
        last_rel_attempt=last_rel[:240],
        drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
        **log_context,
    )
    return None, -1


def refresh_planned_move_source_from_graph(
    move: dict[str, Any],
    *,
    get_raw_item: Callable[[str, str], Optional[dict[str, Any]]],
    source_drive_id: str,
    source_library_name: str,
    source_site_name: str = "",
    log_context: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Refresh ``source_path``, ``source_name``, and nested ``source`` from live Graph metadata using the
    stored drive + item id.

    SharePoint renames/moves within the same library update paths while the item id stays stable, so
    this keeps allocations aligned without redoing drag-and-drop.
    """
    src = move.setdefault("source", {})
    drive = str(src.get("drive_id") or move.get("source_drive_id") or source_drive_id or "").strip()
    iid = str(src.get("id") or move.get("source_id") or "").strip()
    lc = dict(log_context or {})
    if not drive or not iid:
        return False

    raw = get_raw_item(drive, iid)
    if not raw:
        log_warn(
            "graph_refresh_source_item_not_found",
            reason="get_item_returned_none_or_404",
            drive_id_suffix=drive[-16:] if len(drive) > 16 else drive,
            item_id_suffix=iid[-16:] if len(iid) > 16 else iid,
            source_path_excerpt=str(move.get("source_path") or "")[:200],
            **lc,
        )
        return False

    item_path = GraphClient.build_item_path(raw)
    display_path = GraphClient.build_display_path(
        str(source_site_name or "").strip(),
        str(source_library_name or "").strip(),
        item_path,
    )
    name = str(raw.get("name", "") or "").strip() or str(src.get("name", "") or "").strip() or "Unnamed Item"
    is_folder = "folder" in raw
    pr = raw.get("parentReference") or {}
    resolved_drive = str(pr.get("driveId") or drive).strip() or drive

    def _norm_item_path(p: str) -> str:
        s = str(p or "").replace("\\", "/").strip()
        if not s:
            return ""
        if not s.startswith("/"):
            s = "/" + s
        return s.rstrip("/") or "/"

    old_name = str(move.get("source_name") or src.get("name") or "")
    old_item_path = _norm_item_path(str(src.get("item_path") or ""))
    if not old_item_path or old_item_path == "/":
        old_item_path = _norm_item_path(
            allocation_path_to_drive_relative(
                str(move.get("source_path") or src.get("display_path") or ""),
                library_name=source_library_name,
                site_name=source_site_name,
            ).replace("\\", "/")
        )
        if old_item_path and not old_item_path.startswith("/"):
            old_item_path = "/" + old_item_path
        old_item_path = _norm_item_path(old_item_path)

    new_item_path = _norm_item_path(item_path)

    if (
        old_item_path == new_item_path
        and old_name == name
        and bool(src.get("is_folder")) == is_folder
        and str(src.get("drive_id") or "") == resolved_drive
    ):
        return False

    move["source_name"] = name
    move["source_path"] = display_path
    move["source_id"] = iid
    move["source_drive_id"] = resolved_drive

    src["id"] = iid
    src["drive_id"] = resolved_drive
    src["name"] = name
    src["item_path"] = item_path
    src["display_path"] = display_path
    src["is_folder"] = is_folder
    wu = raw.get("webUrl")
    if wu:
        src["web_url"] = wu

    log_info(
        "graph_refresh_source_applied",
        source_name_excerpt=name[:120],
        new_display_path_excerpt=display_path[:220],
        item_path_excerpt=str(item_path)[:200],
        **lc,
    )
    return True


def enrich_single_planned_move(
    move: dict[str, Any],
    *,
    get_item_by_path: Callable[[str, str], Optional[dict[str, Any]]],
    get_root_item: Callable[[str], Optional[dict[str, Any]]],
    source_drive_id: str,
    source_library_name: str,
    dest_drive_id: str,
    dest_library_name: str,
    source_site_name: str = "",
    dest_site_name: str = "",
    move_index: int | None = None,
    request_id: str = "",
    dest_parent_negative_cache: Optional[MutableSet[str]] = None,
) -> bool:
    """
    Fill ``source`` / ``destination`` nested dicts with Graph ids when missing.

    For SharePoint execution, ``destination_item_id`` is the **parent folder** id where the item
    will be copied; the leaf segment of the destination path is the child name.

    Returns True if at least one id was set.
    """
    changed = False
    src = move.setdefault("source", {})
    dst = move.setdefault("destination", {})

    s_drive = str(source_drive_id or "").strip()
    d_drive = str(dest_drive_id or "").strip()
    base_log: dict[str, Any] = {
        "move_index": move_index if move_index is not None else -1,
        "request_id": str(request_id or "")[:80],
        "source_name_excerpt": str(move.get("source_name") or "")[:120],
    }

    if not s_drive or not d_drive:
        log_warn(
            "graph_resolve_move_skipped",
            reason="missing_source_or_dest_drive_id",
            has_source_drive=bool(s_drive),
            has_dest_drive=bool(d_drive),
            **base_log,
        )
        return False

    src_path = str(move.get("source_path") or src.get("display_path") or src.get("item_path") or "")
    dst_path = str(move.get("destination_path") or dst.get("display_path") or dst.get("item_path") or "")

    raw_dest_id = str(dst.get("id") or move.get("destination_id") or "").strip()
    if raw_dest_id and is_internal_proposed_destination_item_id(raw_dest_id):
        dst.pop("id", None)
        move.pop("destination_id", None)

    need_source = not str(src.get("id") or move.get("source_id") or "").strip()
    need_dest = not str(dst.get("id") or move.get("destination_id") or "").strip()

    if need_source:
        if not src_path.strip():
            log_warn(
                "graph_resolve_source_skip",
                reason="empty_source_path",
                hint="cannot_resolve_source_item_id_without_path",
                **base_log,
            )
        else:
            candidates = drive_relative_path_candidates(
                src_path,
                library_name=source_library_name,
                site_name=source_site_name,
            )
            if not candidates:
                log_warn(
                    "graph_resolve_source_skip",
                    reason="no_path_candidates_after_normalization",
                    raw_source_path_excerpt=src_path[:240],
                    source_library=source_library_name[:80],
                    source_site=source_site_name[:80],
                    **base_log,
                )
            else:
                item, used_idx = resolve_item_by_path_candidates(
                    get_item_by_path,
                    s_drive,
                    candidates,
                    phase="planned_move_source",
                    log_context={
                        **base_log,
                        "raw_source_path_excerpt": src_path[:240],
                        "source_library": source_library_name[:80],
                        "source_site": source_site_name[:80],
                    },
                )
                if item and item.get("id"):
                    iid = str(item.get("id", "")).strip()
                    src["id"] = iid
                    src["drive_id"] = s_drive
                    move["source_id"] = iid
                    changed = True

    if need_dest:
        if not dst_path.strip():
            log_warn(
                "graph_resolve_destination_skip",
                reason="empty_destination_path",
                hint="parent_folder_must_exist_in_destination_library_for_graph_copy",
                destination_name_excerpt=str(move.get("destination_name") or "")[:120],
                **base_log,
            )
        else:
            candidates = drive_relative_path_candidates(
                dst_path,
                library_name=dest_library_name,
                site_name=dest_site_name,
            )
            if not candidates:
                log_warn(
                    "graph_resolve_destination_skip",
                    reason="no_path_candidates_after_normalization",
                    raw_destination_path_excerpt=dst_path[:240],
                    dest_library=dest_library_name[:80],
                    dest_site=dest_site_name[:80],
                    **base_log,
                )
            else:
                dest_resolved = False
                tried: list[str] = []
                cache_skipped = 0
                nc = dest_parent_negative_cache
                for idx, cand in enumerate(candidates):
                    parent_rel, leaf = _parent_and_leaf(cand)
                    label = parent_rel if parent_rel else "(library_root)"
                    tried.append(f"[{idx}] parent={label[:100]} leaf={leaf[:80] if leaf else ''}")
                    cache_key = graph_dest_parent_negative_cache_key(
                        d_drive, parent_rel or "", dest_library_name, dest_site_name
                    )
                    if nc is not None and cache_key in nc:
                        cache_skipped += 1
                        log_trace(
                            "graph_resolve",
                            "dest_parent_negative_cache_hit",
                            candidate_index=idx,
                            cache_key_excerpt=cache_key[:200],
                            **base_log,
                        )
                        continue
                    parent_item: Optional[dict[str, Any]] = None
                    if parent_rel:
                        try:
                            parent_item = get_item_by_path(d_drive, parent_rel)
                        except Exception as exc:
                            log_warn(
                                "graph_resolve_dest_parent_exception",
                                candidate_index=idx,
                                parent_rel_excerpt=parent_rel[:200],
                                error=str(exc)[:500],
                                **base_log,
                            )
                            if nc is not None:
                                nc.add(cache_key)
                            continue
                    else:
                        try:
                            parent_item = get_root_item(d_drive)
                        except Exception as exc:
                            log_warn(
                                "graph_resolve_dest_root_exception",
                                error=str(exc)[:500],
                                **base_log,
                            )
                            if nc is not None:
                                nc.add(cache_key)
                            break

                    if parent_item and parent_item.get("id"):
                        pid = str(parent_item.get("id", "")).strip()
                        dst["id"] = pid
                        dst["drive_id"] = d_drive
                        move["destination_id"] = pid
                        if leaf:
                            move["destination_name"] = leaf
                            dst["name"] = leaf
                        changed = True
                        dest_resolved = True
                        if idx > 0:
                            log_info(
                                "graph_resolve_path_fallback_success",
                                phase="planned_move_destination_parent",
                                candidate_index=idx,
                                rel_used=cand[:240],
                                parent_rel_excerpt=parent_rel[:200] if parent_rel else "",
                                leaf_name_excerpt=leaf[:120] if leaf else "",
                                dest_library=dest_library_name[:80],
                                dest_site=dest_site_name[:80],
                                **base_log,
                            )
                        break
                    log_trace(
                        "graph_resolve",
                        "dest_parent_candidate_miss",
                        candidate_index=idx,
                        parent_rel_excerpt=parent_rel[:200] if parent_rel else "(root)",
                        leaf_excerpt=leaf[:120] if leaf else "",
                        **base_log,
                    )
                    if nc is not None:
                        nc.add(cache_key)

                if not dest_resolved:
                    if cache_skipped == len(candidates) and candidates:
                        log_trace(
                            "graph_resolve",
                            "graph_resolve_dest_parent_all_candidates_failed_suppressed_negative_cache",
                            reason="all_parent_candidates_known_missing_this_session",
                            raw_destination_path_excerpt=dst_path[:240],
                            dest_library=dest_library_name[:80],
                            dest_site=dest_site_name[:80],
                            candidate_count=len(candidates),
                            **base_log,
                        )
                    else:
                        log_warn(
                            "graph_resolve_dest_parent_all_candidates_failed",
                            reason="parent_folder_not_found_in_destination_library",
                            hint="create_parent_folders_in_sharepoint_or_fix_destination_path",
                            raw_destination_path_excerpt=dst_path[:240],
                            dest_library=dest_library_name[:80],
                            dest_site=dest_site_name[:80],
                            attempts_summary=tried[:24],
                            candidate_count=len(candidates),
                            **base_log,
                        )

    return changed


def enrich_proposed_folder_record(
    pf: Any,
    *,
    get_item_by_path: Callable[[str, str], Optional[dict[str, Any]]],
    dest_drive_id: str,
    dest_library_name: str,
    dest_site_name: str = "",
    proposed_index: int | None = None,
) -> bool:
    """Set DestinationDriveId / DestinationParentItemId when missing (for Graph mkdir)."""
    d_drive = str(dest_drive_id or "").strip()
    plog: dict[str, Any] = {
        "proposed_index": proposed_index if proposed_index is not None else -1,
        "folder_name": str(getattr(pf, "FolderName", "") or "")[:120],
        "destination_id": str(getattr(pf, "DestinationId", "") or "")[:80],
    }
    if not d_drive:
        log_warn("graph_resolve_proposed_skip", reason="missing_dest_drive_id", **plog)
        return False
    dest_parent_cur = str(getattr(pf, "DestinationParentItemId", "") or "").strip()
    if (
        str(getattr(pf, "DestinationDriveId", "") or "").strip()
        and dest_parent_cur
        and not is_internal_proposed_destination_item_id(dest_parent_cur)
    ):
        return False

    parent_path = str(getattr(pf, "ParentPath", "") or "").strip()
    if not parent_path:
        log_warn(
            "graph_resolve_proposed_skip",
            reason="empty_parent_path",
            hint="proposed_folder_needs_parent_path_to_resolve_graph_parent_folder",
            **plog,
        )
        return False

    candidates = drive_relative_path_candidates(
        parent_path,
        library_name=dest_library_name,
        site_name=dest_site_name,
    )
    if not candidates:
        log_warn(
            "graph_resolve_proposed_skip",
            reason="no_path_candidates_after_normalization",
            raw_parent_path_excerpt=parent_path[:240],
            dest_library=dest_library_name[:80],
            dest_site=dest_site_name[:80],
            **plog,
        )
        return False

    item, used_idx = resolve_item_by_path_candidates(
        get_item_by_path,
        d_drive,
        candidates,
        phase="proposed_folder_parent",
        log_context={
            **plog,
            "raw_parent_path_excerpt": parent_path[:240],
            "dest_library": dest_library_name[:80],
            "dest_site": dest_site_name[:80],
        },
    )
    if not item or not item.get("id"):
        return False

    pf.DestinationDriveId = d_drive
    pf.DestinationParentItemId = str(item.get("id", "")).strip()
    log_info(
        "graph_resolve_proposed_ok",
        graph_parent_resolved_with_candidate_index=used_idx,
        destination_path_excerpt=str(getattr(pf, "DestinationPath", "") or "")[:200],
        **plog,
    )
    return True
