"""Resolve Microsoft Graph drive/item ids for planned moves from path strings (legacy drafts without ids)."""

from __future__ import annotations

import json
import re
from typing import Any, Callable, MutableSet, Optional, Sequence, Union

from ozlink_console.destination_path_bridge import remap_under_visible_library_anchor, strip_legacy_internal_root_prefix
from ozlink_console.graph import GraphClient
from ozlink_console.logger import log_info, log_trace, log_warn
from ozlink_console.paths import normalize_manifest_path


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


def _norm_path_casefold_backslash(path: str) -> str:
    s = normalize_manifest_path(str(path or "").replace("/", "\\"))
    return s.strip().casefold()


def collect_proposed_folder_destination_paths_casefold(
    proposed_folders: Optional[Sequence[Union[dict[str, Any], Any]]],
) -> set[str]:
    """
    Normalized (casefold) full destination paths for proposed folder rows.
    Used to detect planned moves whose destination lives under a folder that is only
    proposed (not created in SharePoint until execution).
    """
    out: set[str] = set()
    for pf in proposed_folders or []:
        dp = ""
        pp = ""
        fn = ""
        if isinstance(pf, dict):
            dp = str(pf.get("DestinationPath") or "").strip()
            pp = str(pf.get("ParentPath") or "").strip()
            fn = str(pf.get("FolderName") or "").strip()
        else:
            dp = str(getattr(pf, "DestinationPath", "") or "").strip()
            pp = str(getattr(pf, "ParentPath", "") or "").strip()
            fn = str(getattr(pf, "FolderName", "") or "").strip()
        if dp:
            out.add(_norm_path_casefold_backslash(dp))
        if pp and fn:
            out.add(_norm_path_casefold_backslash(f"{pp}\\{fn}"))
    return out


def is_destination_parent_planned_or_proposed_only(
    drive_relative_primary: str,
    proposed_paths_casefold: set[str],
) -> bool:
    """
    True when the first drive-relative path's parent (or the path itself for a single-segment
    place-holder) is covered by a *proposed folder* path — the folder does not have to exist in
    SharePoint until execution, so live Graph parent resolution is not a startup failure.
    """
    if not drive_relative_primary or not proposed_paths_casefold:
        return False
    rel = str(drive_relative_primary or "").replace("\\", "/").strip("/")
    if not rel:
        return False
    d_cf = _norm_path_casefold_backslash(rel)
    if d_cf in proposed_paths_casefold:
        return True
    parent_slash, _leaf = _parent_and_leaf(rel)
    p_cf = _norm_path_casefold_backslash(parent_slash.replace("/", "\\") if parent_slash else "")
    for marker in (d_cf, p_cf):
        if not marker:
            continue
        for prop in proposed_paths_casefold:
            if not prop:
                continue
            if marker == prop or marker.startswith(prop + "\\") or d_cf.startswith(prop + "\\"):
                return True
    return False


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


def _apply_visible_library_anchor_to_slash_relative(rel_slash: str, anchor_backslash: str) -> str:
    """If ``anchor_backslash`` is the live library hub (e.g. ``Root3``), prepend when path omits it."""
    rel_slash = str(rel_slash or "").replace("\\", "/").strip("/")
    anch = str(anchor_backslash or "").strip()
    if not anch or not rel_slash:
        return rel_slash
    rb = normalize_manifest_path(rel_slash.replace("/", "\\"))
    ab = normalize_manifest_path(anch.replace("/", "\\"))
    rb = strip_legacy_internal_root_prefix(rb)
    ab = strip_legacy_internal_root_prefix(ab)
    if not ab:
        return rel_slash
    out = remap_under_visible_library_anchor(rb, ab)
    return out.replace("\\", "/").strip("/")


def _join_segments_strip_legacy_internal_root(segments: list[str]) -> list[str]:
    """Join path segments and remove only the legacy internal ``Root\\`` prefix (not real ``Root3``, etc.)."""
    parts = [s.strip() for s in segments if str(s or "").strip()]
    if not parts:
        return []
    joined = normalize_manifest_path("\\".join(parts))
    stripped = strip_legacy_internal_root_prefix(joined)
    if not stripped:
        return []
    return [p for p in stripped.split("\\") if p]


def allocation_path_to_drive_relative(
    path: str,
    *,
    library_name: str = "",
    site_name: str = "",
    visible_library_anchor: str = "",
) -> str:
    """
    Convert stored allocation / tree paths to a path relative to the document library root
    for ``GET /drives/{id}/root:/relative`` (Graph).

    Handles:
    - ``LibraryName\\FTBMRoot\\...`` (leading library segment)
    - ``Site / Library / FTBMRoot/...`` display paths
    - Legacy internal ``Root\\`` prefix only (stripped via :func:`strip_legacy_internal_root_prefix`)
    - Repeated site/library prefixes from bad imports

    When ``visible_library_anchor`` is set (live Graph skeleton hub, e.g. ``Root3``), paths that omit
    that hub are remapped under it so Graph lookups and mkdir chains match the visible tree.
    """
    text = str(path or "").strip()
    if not text:
        return ""

    site_l = (site_name or "").strip().lower()
    lib_l = (library_name or "").strip().lower()

    if " / " in text:
        parts = [p.strip() for p in text.split(" / ") if p.strip()]
        parts = _strip_leading_site_library_parts(parts, site_l, lib_l)
        segs = _join_segments_strip_legacy_internal_root(parts)
        rel = "/".join(segs).strip("/")
        return _apply_visible_library_anchor_to_slash_relative(rel, visible_library_anchor)

    segs = _path_segments(text)
    segs = [s.strip() for s in segs if s.strip()]
    segs = _strip_leading_site_library_parts(segs, site_l, lib_l)
    segs = _join_segments_strip_legacy_internal_root(segs)
    rel = "/".join(segs).strip("/")
    return _apply_visible_library_anchor_to_slash_relative(rel, visible_library_anchor)


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
    visible_library_anchor: str = "",
    restrict_to_live_graph_skeleton: bool = False,
) -> list[str]:
    """
    Build ordered unique relative paths to try with ``GET .../root:/path`` when imports use odd shapes.

    First candidate is always ``allocation_path_to_drive_relative`` (with optional anchor).

    When ``restrict_to_live_graph_skeleton`` is True (SharePoint destination UI + known visible hub),
    only that primary anchored candidate is returned so Graph never sees de-anchored alternates
    (avoids mkdir / resolve at library root for paths that belong under the live hub).
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
    add(
        allocation_path_to_drive_relative(
            text,
            library_name=library_name,
            site_name=site_name,
            visible_library_anchor=visible_library_anchor,
        )
    )

    if not text:
        return candidates[:max_candidates]

    if restrict_to_live_graph_skeleton:
        return candidates[:max_candidates]

    flex = [p.strip() for p in re.split(r"[/\\]+", text) if p.strip()]
    flex = _strip_leading_site_library_parts(flex, site_l, lib_l)
    flex = _join_segments_strip_legacy_internal_root(flex)
    if flex:
        add(_apply_visible_library_anchor_to_slash_relative("/".join(flex), visible_library_anchor))

    if " / " in text:
        parts = [p.strip() for p in text.split(" / ") if p.strip()]
        for i, p in enumerate(parts):
            if lib_l and p.strip().lower() == lib_l and i + 1 < len(parts):
                add(
                    _apply_visible_library_anchor_to_slash_relative(
                        "/".join(parts[i + 1 :]).replace("\\", "/").strip("/"),
                        visible_library_anchor,
                    )
                )
        if len(parts) >= 2 and lib_l and parts[0].lower() == lib_l:
            add(
                _apply_visible_library_anchor_to_slash_relative(
                    "/".join(parts[1:]).replace("\\", "/").strip("/"),
                    visible_library_anchor,
                )
            )

    segs = _path_segments(text)
    segs = [s.strip() for s in segs if s.strip()]
    # Legacy-only: drop leading internal ``Root\`` segments via strip, never arbitrary client folders.
    joined = normalize_manifest_path("\\".join(segs))
    legacy_stripped = strip_legacy_internal_root_prefix(joined)
    tail_segs = [s for s in legacy_stripped.split("\\") if s] if legacy_stripped else []
    if tail_segs and tail_segs != segs:
        chunk = _strip_leading_site_library_parts(tail_segs, site_l, lib_l)
        if chunk:
            add(_apply_visible_library_anchor_to_slash_relative("/".join(chunk), visible_library_anchor))

    return candidates[:max_candidates]


def classify_planned_move_destination_parent_for_linkage(
    move: dict[str, Any],
    *,
    dest_library_name: str,
    dest_site_name: str,
    visible_library_anchor_destination: str = "",
    sharepoint_graph_authority_destination: bool = False,
    proposed_folder_paths_casefold: Optional[set[str]] = None,
) -> str:
    """
    Classify how a planned move's *destination* parent should be treated in Graph linkage:

    - ``planned_or_proposed_parent`` — the destination (or a prefix) is one of the proposed folder
      paths, so the parent is not required to exist in live SharePoint until execution.
    - ``live_existing_parent`` — not covered by a proposed path; a normal live Graph parent lookup
      is appropriate when IDs are missing.
    - ``no_destination_path`` — there is no usable destination path to classify.
    """
    dst = move.get("destination") if isinstance(move.get("destination"), dict) else {}
    dst_path = str(
        move.get("destination_path") or dst.get("display_path") or dst.get("item_path") or ""
    ).strip()
    if not dst_path:
        return "no_destination_path"
    prop = proposed_folder_paths_casefold or set()
    if not prop:
        return "live_existing_parent"
    sp_auth = bool(sharepoint_graph_authority_destination)
    anchor_d = str(visible_library_anchor_destination or "").strip()
    if sp_auth and not anchor_d:
        return "live_existing_parent"
    candidates = drive_relative_path_candidates(
        dst_path,
        library_name=dest_library_name,
        site_name=dest_site_name,
        visible_library_anchor=anchor_d if sp_auth else str(visible_library_anchor_destination or "").strip(),
        restrict_to_live_graph_skeleton=sp_auth,
    )
    if not candidates:
        return "live_existing_parent"
    if is_destination_parent_planned_or_proposed_only(candidates[0], prop):
        return "planned_or_proposed_parent"
    return "live_existing_parent"


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
    destination_parent_resolve_diag_sink: Optional[Callable[[dict[str, Any]], None]] = None,
    skip_dest_parent_negative_cache_read: bool = False,
    visible_library_anchor_destination: str = "",
    sharepoint_graph_authority_destination: bool = False,
    enrichment_mode: str = "full",
    proposed_folder_paths_casefold: Optional[set[str]] = None,
) -> bool:
    """
    Fill ``source`` / ``destination`` nested dicts with Graph ids when missing (read-only GETs).

    For SharePoint execution, ``destination_item_id`` is the **parent folder** id where the item
    will be copied; the leaf segment of the destination path is the child name.

    When ``sharepoint_graph_authority_destination`` is True, destination drive-relative candidates
    are restricted to the live skeleton coordinate system: a visible library anchor path is
    required, and de-anchored alternates are not emitted.

    When ``enrichment_mode == "skip"`` and the destination is under a *proposed folder* path
    (``proposed_folder_paths_casefold`` from the current draft), do **not** run live destination
    parent resolution — the folder is expected to be created on execution, not a startup Graph
    failure.

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
            sp_auth = bool(sharepoint_graph_authority_destination)
            anchor_d = str(visible_library_anchor_destination or "").strip()
            if sp_auth and not anchor_d:
                log_warn(
                    "graph_resolve_destination_skip",
                    reason="missing_visible_library_skeleton_anchor_under_graph_authority",
                    raw_destination_path_excerpt=dst_path[:240],
                    dest_library=dest_library_name[:80],
                    dest_site=dest_site_name[:80],
                    **base_log,
                )
                candidates = []
            else:
                candidates = drive_relative_path_candidates(
                    dst_path,
                    library_name=dest_library_name,
                    site_name=dest_site_name,
                    visible_library_anchor=anchor_d
                    if sp_auth
                    else str(visible_library_anchor_destination or "").strip(),
                    restrict_to_live_graph_skeleton=sp_auth,
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
                skip_mode = (enrichment_mode or "full").lower() == "skip"
                prop_cf = proposed_folder_paths_casefold or set()
                defer_dest = bool(
                    skip_mode
                    and prop_cf
                    and is_destination_parent_planned_or_proposed_only(
                        str(candidates[0] or "").replace("\\", "/").strip("/"), prop_cf
                    )
                )
                if defer_dest:
                    log_trace(
                        "graph_resolve",
                        "dest_parent_resolution_deferred",
                        reason="pending_proposed_parent",
                        first_candidate_excerpt=(candidates[0] or "")[:300],
                        **base_log,
                    )
                if not defer_dest:
                    dest_resolved = False
                    tried: list[str] = []
                    candidate_attempts: list[dict[str, Any]] = []
                    cache_skipped = 0
                    graph_miss = 0
                    exc_count = 0
                    aborted_root = False
                    nc = dest_parent_negative_cache
                    for idx, cand in enumerate(candidates):
                        parent_rel, leaf = _parent_and_leaf(cand)
                        label = parent_rel if parent_rel else "(library_root)"
                        tried.append(f"[{idx}] parent={label[:100]} leaf={leaf[:80] if leaf else ''}")
                        cache_key = graph_dest_parent_negative_cache_key(
                            d_drive, parent_rel or "", dest_library_name, dest_site_name
                        )
                        attempt: dict[str, Any] = {
                            "candidate_index": idx,
                            "full_drive_relative_candidate": cand[:500],
                            "parent_path_for_graph_api": parent_rel[:500] if parent_rel else "",
                            "leaf_segment": leaf[:200] if leaf else "",
                        }
                        if (
                            not skip_dest_parent_negative_cache_read
                            and nc is not None
                            and cache_key in nc
                        ):
                            cache_skipped += 1
                            attempt["outcome"] = "skipped_negative_cache"
                            candidate_attempts.append(attempt)
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
                                exc_count += 1
                                attempt["outcome"] = "parent_lookup_exception"
                                attempt["error_excerpt"] = str(exc)[:400]
                                candidate_attempts.append(attempt)
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
                                exc_count += 1
                                aborted_root = True
                                attempt["outcome"] = "root_lookup_exception"
                                attempt["error_excerpt"] = str(exc)[:400]
                                candidate_attempts.append(attempt)
                                log_warn(
                                    "graph_resolve_dest_root_exception",
                                    error=str(exc)[:500],
                                    **base_log,
                                )
                                if nc is not None:
                                    nc.add(cache_key)
                                break

                        if parent_item and parent_item.get("id"):
                            attempt["outcome"] = "parent_resolved_ok"
                            candidate_attempts.append(attempt)
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
                        graph_miss += 1
                        attempt["outcome"] = "parent_graph_response_missing_or_no_id"
                        candidate_attempts.append(attempt)
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
                        if aborted_root:
                            final_reason = "root_lookup_exception"
                        elif cache_skipped == len(candidates) and candidates:
                            final_reason = "negative_cache_all_candidates_skipped"
                        else:
                            final_reason = "parent_folder_not_found_in_destination_library"
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
                        forensic: dict[str, Any] = {
                            **base_log,
                            "phase": "planned_move_destination_parent",
                            "resolver_uses_real_sharepoint_parent_only": True,
                            "raw_source_path_excerpt": src_path[:500],
                            "raw_destination_path_excerpt": dst_path[:500],
                            "dest_site_excerpt": dest_site_name[:80],
                            "dest_library_excerpt": dest_library_name[:80],
                            "drive_relative_path_candidates": [c[:400] for c in candidates],
                            "candidate_parent_variants_attempted": tried[:40],
                            "candidate_attempts": candidate_attempts,
                            "final_failure_reason": final_reason,
                            "cache_skip_count": cache_skipped,
                            "graph_parent_miss_count": graph_miss,
                            "parent_lookup_exception_count": exc_count,
                        }
                        if destination_parent_resolve_diag_sink is not None:
                            try:
                                destination_parent_resolve_diag_sink(forensic)
                            except Exception:
                                pass
                        log_info(
                            "graph_resolve_dest_row_forensic",
                            candidate_attempt_detail_json=json.dumps(
                                forensic.get("candidate_attempts") or [], ensure_ascii=False
                            )[:12000],
                            projection_parent_hints_json=json.dumps(
                                forensic.get("projection_parent_hints") or [], ensure_ascii=False
                            )[:8000],
                            **{k: v for k, v in forensic.items() if k not in ("candidate_attempts", "projection_parent_hints")},
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
    proposed_parent_resolve_diag_sink: Optional[Callable[[dict[str, Any]], None]] = None,
    visible_library_anchor_destination: str = "",
    sharepoint_graph_authority_destination: bool = False,
) -> bool:
    """Set DestinationDriveId / DestinationParentItemId when missing (read-only GETs)."""
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

    sp_auth = bool(sharepoint_graph_authority_destination)
    anchor_d = str(visible_library_anchor_destination or "").strip()
    if sp_auth and not anchor_d:
        log_warn(
            "graph_resolve_proposed_skip",
            reason="missing_visible_library_skeleton_anchor_under_graph_authority",
            raw_parent_path_excerpt=parent_path[:240],
            dest_library=dest_library_name[:80],
            dest_site=dest_site_name[:80],
            **plog,
        )
        return False
    candidates = drive_relative_path_candidates(
        parent_path,
        library_name=dest_library_name,
        site_name=dest_site_name,
        visible_library_anchor=anchor_d,
        restrict_to_live_graph_skeleton=sp_auth,
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
        forensic_pf: dict[str, Any] = {
            **plog,
            "phase": "proposed_folder_parent",
            "resolver_uses_real_sharepoint_parent_only": True,
            "raw_parent_path_excerpt": parent_path[:500],
            "destination_path_excerpt": str(getattr(pf, "DestinationPath", "") or "")[:400],
            "dest_site_excerpt": dest_site_name[:80],
            "dest_library_excerpt": dest_library_name[:80],
            "drive_relative_path_candidates": [c[:400] for c in candidates],
            "final_failure_reason": "proposed_parent_folder_not_found_in_destination_library",
            "cache_skip_count": 0,
            "graph_parent_miss_count": 1,
            "parent_lookup_exception_count": 0,
        }
        if proposed_parent_resolve_diag_sink is not None:
            try:
                proposed_parent_resolve_diag_sink(forensic_pf)
            except Exception:
                pass
        log_info(
            "graph_resolve_proposed_row_forensic",
            projection_parent_hints_json=json.dumps(
                forensic_pf.get("projection_parent_hints") or [], ensure_ascii=False
            )[:8000],
            **{k: v for k, v in forensic_pf.items() if k != "projection_parent_hints"},
        )
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
