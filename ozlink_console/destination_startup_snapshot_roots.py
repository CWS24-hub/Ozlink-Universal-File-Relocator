"""
Destination startup snapshot — top-level root validation for SharePoint Graph authority.

Rejects persisted/sidecar top-level roots that cannot belong to the current destination
library (e.g. rows whose drive identity matches the source library or a foreign drive).

This module is intentionally generic (no name-based bans on specific folders).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ozlink_console.destination_legacy_snapshot_identity import (
    DestinationLibraryCandidate,
    InferenceConfidence,
    infer_destination_snapshot_identity_from_legacy_snapshot,
)
from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
    destination_payload_is_planned_workspace_row,
    destination_payload_workspace_row_state,
)


@dataclass
class DestinationStartupSnapshotRootContext:
    """Identity context available during session/workspace restore (may be partial)."""

    browse_mode: str  # "sharepoint" | "local" | ""
    destination_drive_id: str = ""
    source_drive_id: str = ""
    # Optional: casefolded Graph shallow root **names** when authority is already known.
    graph_shallow_root_names_cf: frozenset[str] | None = None
    # When destination drive is known and row is not planned, require row drive_id (SharePoint).
    strict_missing_row_drive: bool = True


@dataclass
class DestinationStartupSnapshotSanitizeStats:
    kept_top_level: int = 0
    pruned_top_level: int = 0
    uncertain_top_level: int = 0
    prune_reasons: list[str] = field(default_factory=list)


def _root_display_name(snap: dict[str, Any]) -> str:
    d = snap.get("data") if isinstance(snap.get("data"), dict) else {}
    return str(snap.get("text") or d.get("name") or d.get("base_display_label") or "").strip()


def _root_path_excerpt(snap: dict[str, Any]) -> str:
    d = snap.get("data") if isinstance(snap.get("data"), dict) else {}
    for k in ("item_path", "destination_path", "semantic_path", "display_path"):
        v = str(d.get(k) or "").strip()
        if v:
            return v[:200]
    return _root_display_name(snap)[:200]


def _drive_id_from_payload(pl: dict[str, Any]) -> str:
    if not isinstance(pl, dict):
        return ""
    return str(pl.get("drive_id") or pl.get("library_id") or pl.get("parent_drive_id") or "").strip()


def _should_keep_planned_top_level(pl: dict[str, Any]) -> bool:
    if destination_payload_is_planned_workspace_row(pl):
        return True
    st = destination_payload_workspace_row_state(pl)
    if st == WORKSPACE_ROW_STATE_PLANNED_ONLY:
        return True
    return False


def classify_destination_top_level_snapshot_root(
    snap: dict[str, Any],
    ctx: DestinationStartupSnapshotRootContext,
) -> tuple[str, str]:
    """Return (decision, reason) where decision is keep|prune|uncertain."""

    if not isinstance(snap, dict):
        return "prune", "not_dict"
    pl = snap.get("data") if isinstance(snap.get("data"), dict) else {}
    if pl.get("placeholder"):
        return "keep", "placeholder_scaffolding"

    mode = str(ctx.browse_mode or "").strip().lower()
    if mode == "local":
        return "keep", "local_browse_skip_sharepoint_root_checks"

    if mode and mode != "sharepoint":
        return "uncertain", f"unknown_browse_mode_{mode}"

    if _should_keep_planned_top_level(pl):
        return "keep", "planned_workspace_row"

    tr = str(pl.get("tree_role") or "").strip().lower()
    if tr == "source":
        return "prune", "tree_role_source"

    rid = _drive_id_from_payload(pl)
    dest = str(ctx.destination_drive_id or "").strip()
    src = str(ctx.source_drive_id or "").strip()

    if dest and rid and rid.casefold() != dest.casefold():
        if src and rid.casefold() == src.casefold():
            return "prune", "root_drive_matches_source_not_destination"
        return "prune", "root_drive_id_not_destination_library"

    if not dest and src and rid and rid.casefold() == src.casefold():
        return "prune", "root_drive_matches_source_destination_not_yet_bound"

    gset = ctx.graph_shallow_root_names_cf
    if gset and len(gset) > 0 and not _should_keep_planned_top_level(pl):
        nm = str(pl.get("name") or "").strip() or _root_display_name(snap)
        ncf = nm.casefold()
        if ncf and ncf not in gset:
            return "prune", "root_name_not_in_graph_shallow_authority"

    if not rid and not dest:
        return "uncertain", "no_drive_fingerprint_yet"

    if (
        dest
        and not rid
        and not _should_keep_planned_top_level(pl)
        and bool(getattr(ctx, "strict_missing_row_drive", True))
    ):
        return "prune", "strict_row_drive_missing_when_intended_known"

    return "keep", "passed_sharepoint_root_checks"


def sanitize_destination_startup_snapshot_top_level(
    snapshots: list | None,
    ctx: DestinationStartupSnapshotRootContext,
    *,
    selection_tag: str = "",
    log_validation_summary: bool = True,
) -> tuple[list, DestinationStartupSnapshotSanitizeStats]:
    """Drop invalid **top-level** roots only; nested rows under a kept root are left unchanged."""

    stats = DestinationStartupSnapshotSanitizeStats()
    out: list = []
    for snap in list(snapshots or []):
        if not isinstance(snap, dict):
            continue
        decision, reason = classify_destination_top_level_snapshot_root(snap, ctx)
        name_excerpt = _root_path_excerpt(snap)
        if decision == "prune":
            stats.pruned_top_level += 1
            stats.prune_reasons.append(reason)
            log_info(
                "destination_startup_snapshot_root_pruned_invalid",
                selection_tag=str(selection_tag or "")[:120],
                reason=str(reason)[:120],
                root_path_excerpt=name_excerpt[:200],
            )
            continue
        if decision == "uncertain":
            stats.uncertain_top_level += 1
            log_info(
                "destination_startup_snapshot_root_validation_uncertain",
                selection_tag=str(selection_tag or "")[:120],
                reason=str(reason)[:120],
                root_path_excerpt=name_excerpt[:200],
            )
        stats.kept_top_level += 1
        out.append(snap)

    if log_validation_summary:
        log_info(
            "destination_startup_snapshot_root_validation_summary",
            selection_tag=str(selection_tag or "")[:120],
            browse_mode=str(ctx.browse_mode or "")[:40],
            dest_drive_suffix=(str(ctx.destination_drive_id or "")[-16:] if ctx.destination_drive_id else ""),
            source_drive_suffix=(str(ctx.source_drive_id or "")[-16:] if ctx.source_drive_id else ""),
            kept_top_level=int(stats.kept_top_level),
            pruned_top_level=int(stats.pruned_top_level),
            uncertain_top_level=int(stats.uncertain_top_level),
        )
    return out, stats


def first_path_segment_cf(path: str) -> str:
    s = normalize_manifest_path(str(path or "").strip())
    if not s:
        return ""
    seg = s.replace("/", "\\").split("\\", 1)[0].strip()
    return seg.casefold() if seg else ""


def allowed_semantic_root_segments_cf_from_snapshot(snapshots: list | None) -> set[str]:
    """Build a casefolded set of first path segments from sanitized top-level roots."""

    out: set[str] = set()
    for snap in list(snapshots or []):
        if not isinstance(snap, dict):
            continue
        d = snap.get("data") if isinstance(snap.get("data"), dict) else {}
        path = ""
        for k in ("item_path", "destination_path", "semantic_path", "display_path"):
            v = str(d.get(k) or "").strip()
            if v:
                path = v
                break
        seg = first_path_segment_cf(path)
        if not seg:
            nm = str(d.get("name") or snap.get("text") or "").strip()
            seg = nm.casefold() if nm else ""
        if seg:
            out.add(seg)
    return out


def filter_promoted_semantic_paths_for_destination_roots(
    paths: list[str] | set[str],
    allowed_root_segments_cf: set[str] | None,
) -> list[str]:
    """Drop promoted paths whose first segment is not under an allowed destination root name."""

    if not allowed_root_segments_cf:
        return [str(p or "").strip() for p in paths or [] if str(p or "").strip()]
    filtered: list[str] = []
    dropped = 0
    for raw in paths or []:
        p = str(raw or "").strip()
        if not p:
            continue
        seg = first_path_segment_cf(p)
        if seg and seg not in allowed_root_segments_cf:
            dropped += 1
            continue
        filtered.append(p)
    if dropped:
        log_info(
            "destination_startup_promoted_paths_filtered_foreign_root_segment",
            dropped=int(dropped),
            allowed_root_segments=len(allowed_root_segments_cf),
        )
    return filtered


def apply_destination_snapshot_identity_gate_with_legacy(
    snapshots: list,
    *,
    intended_drive_id: str,
    snapshot_stored_drive_id: str,
    snapshot_stored_library_id: str,
    source: str,
    legacy_library_candidates: list[DestinationLibraryCandidate] | None,
    intended_site_id: str = "",
    snapshot_stored_site_id: str = "",
) -> tuple[list, str, dict[str, Any] | None]:
    """Like :func:`apply_destination_snapshot_identity_gate` but infers missing envelope when safe.

    Explicit envelope identity always uses the strict gate (unchanged). When the envelope is empty,
    candidates must be supplied for inference; otherwise the snapshot stays unresolved.
    """

    intended = str(intended_drive_id or "").strip()
    if not intended:
        log_info(
            "destination_startup_snapshot_blocked_unresolved_identity",
            source=str(source)[:40],
            reason="intended_destination_drive_unknown",
        )
        return [], "blocked_intended_unknown", None

    stored_d = str(snapshot_stored_drive_id or "").strip()
    stored_lib = str(snapshot_stored_library_id or "").strip()
    eff = stored_d or stored_lib
    if eff:
        gated, tag = apply_destination_snapshot_identity_gate(
            snapshots,
            intended_drive_id=intended_drive_id,
            snapshot_stored_drive_id=stored_d,
            snapshot_stored_library_id=stored_lib,
            source=source,
            intended_site_id=intended_site_id,
            snapshot_stored_site_id=str(snapshot_stored_site_id or "").strip(),
        )
        return gated, tag, None

    if not list(snapshots or []):
        log_info(
            "destination_startup_snapshot_blocked_unresolved_identity",
            source=str(source)[:40],
            reason="snapshot_envelope_missing_drive",
        )
        return [], "blocked_no_snapshot_envelope", None

    cands = list(legacy_library_candidates or [])
    if not cands:
        log_info(
            "destination_snapshot_legacy_identity_unresolved",
            source=str(source)[:40],
            reason="candidates_not_ready",
            detail="destination_site_libraries_unavailable",
        )
        return [], "blocked_legacy_no_candidates", None

    log_info(
        "destination_snapshot_legacy_identity_inference_begin",
        source=str(source)[:40],
        candidate_library_count=len(cands),
        snapshot_top_level=len(list(snapshots or [])),
    )
    inf = infer_destination_snapshot_identity_from_legacy_snapshot(snapshots, cands)
    meta = {"inference": inf.to_meta_dict()}

    if inf.confidence == InferenceConfidence.LOW or (
        inf.confidence == InferenceConfidence.MEDIUM
    ):
        log_info(
            "destination_snapshot_legacy_identity_unresolved",
            source=str(source)[:40],
            confidence=str(inf.confidence.value),
            score=float(inf.score),
            auto_apply_safe=False,
            reason="confidence_below_high",
        )
        tag = (
            "legacy_identity_medium_unresolved"
            if inf.confidence == InferenceConfidence.MEDIUM
            else "legacy_identity_low_unresolved"
        )
        return [], tag, meta

    # HIGH path — still require margin / auto_apply policy
    if not inf.auto_apply_safe or not inf.matched_drive_id:
        log_info(
            "destination_snapshot_legacy_identity_unresolved",
            source=str(source)[:40],
            confidence=str(inf.confidence.value),
            score=float(inf.score),
            auto_apply_safe=bool(inf.auto_apply_safe),
            reason="high_confidence_but_ambiguous_margin",
        )
        return [], "legacy_identity_high_ambiguous", meta

    if inf.matched_drive_id.casefold() != intended.casefold():
        log_info(
            "destination_snapshot_legacy_identity_unresolved",
            source=str(source)[:40],
            reason="inferred_library_not_intended_combo",
            inferred_suffix=str(inf.matched_drive_id)[-16:]
            if len(str(inf.matched_drive_id)) > 16
            else str(inf.matched_drive_id),
            intended_suffix=str(intended)[-16:] if len(intended) > 16 else intended,
        )
        return [], "legacy_identity_high_intended_mismatch", meta

    ins = str(intended_site_id or "").strip()
    inf_site = str(inf.matched_site_id or "").strip()
    if ins and inf_site and ins.casefold() != inf_site.casefold():
        log_info(
            "destination_snapshot_rejected_selected_site_mismatch",
            source=str(source)[:40],
            intended_site_suffix=ins[-16:] if len(ins) > 16 else ins,
            snapshot_site_suffix=inf_site[-16:] if len(inf_site) > 16 else inf_site,
        )
        return [], "legacy_identity_high_site_mismatch", meta

    gated, strict_tag = apply_destination_snapshot_identity_gate(
        snapshots,
        intended_drive_id=intended,
        snapshot_stored_drive_id=inf.matched_drive_id,
        snapshot_stored_library_id=inf.matched_library_id or inf.matched_drive_id,
        source=source,
        intended_site_id=intended_site_id,
        snapshot_stored_site_id=inf_site or str(snapshot_stored_site_id or "").strip(),
    )
    stamp = {
        "destination_snapshot_identity_inferred_from_legacy": True,
        "matched_drive_id": inf.matched_drive_id,
        "matched_library_id": inf.matched_library_id or inf.matched_drive_id,
        "matched_library_name": inf.matched_library_name,
        "matched_site_id": inf.matched_site_id,
        "inference": inf.to_meta_dict(),
    }
    log_info(
        "destination_snapshot_identity_inferred_from_legacy",
        source=str(source)[:40],
        drive_id_suffix=str(inf.matched_drive_id)[-16:]
        if len(str(inf.matched_drive_id)) > 16
        else str(inf.matched_drive_id),
        confidence=str(inf.confidence.value),
        score=float(inf.score),
    )
    return gated, strict_tag, stamp


def canonical_legacy_inference_block_reason(gate_tag: str) -> str:
    """Map identity gate tags to canonical ``meta['reason']`` values used for deferred retry."""

    t = str(gate_tag or "")
    if t == "blocked_legacy_no_candidates":
        return "blocked_legacy_no_candidates"
    if t in ("legacy_identity_low_unresolved", "legacy_identity_medium_unresolved"):
        return "low_confidence"
    if t in ("legacy_identity_high_ambiguous", "legacy_identity_high_intended_mismatch", "legacy_identity_high_site_mismatch"):
        return "insufficient_signal"
    return ""


def pick_legacy_inference_retry_meta(
    sess_gate_tag: str,
    side_gate_tag: str,
    n_sess_raw: int,
    n_side_raw: int,
    intended_drive_id: str,
) -> tuple[str | None, bool]:
    """Whether startup legacy inference is blocked in a way that may clear after libraries load."""

    if not str(intended_drive_id or "").strip():
        return None, False
    reasons: list[str] = []
    if int(n_sess_raw) > 0:
        r = canonical_legacy_inference_block_reason(str(sess_gate_tag))
        if r:
            reasons.append(r)
    if int(n_side_raw) > 0:
        r = canonical_legacy_inference_block_reason(str(side_gate_tag))
        if r:
            reasons.append(r)
    if not reasons:
        return None, False
    priority = ("blocked_legacy_no_candidates", "low_confidence", "insufficient_signal")
    for p in priority:
        if p in reasons:
            return p, True
    return reasons[0], True


def apply_destination_snapshot_identity_gate(
    snapshots: list,
    *,
    intended_drive_id: str,
    snapshot_stored_drive_id: str,
    snapshot_stored_library_id: str,
    source: str,
    intended_site_id: str = "",
    snapshot_stored_site_id: str = "",
) -> tuple[list, str]:
    """Enforce snapshot-level destination drive identity before any visible restore.

    Returns ``(snapshots_or_empty, outcome_tag)``. Empty list means do not render this source.
    """

    intended = str(intended_drive_id or "").strip()
    if not intended:
        log_info(
            "destination_startup_snapshot_blocked_unresolved_identity",
            source=str(source)[:40],
            reason="intended_destination_drive_unknown",
        )
        return [], "blocked_intended_unknown"

    stored_d = str(snapshot_stored_drive_id or "").strip()
    stored_lib = str(snapshot_stored_library_id or "").strip()
    eff = stored_d or stored_lib
    if not eff:
        log_info(
            "destination_startup_snapshot_blocked_unresolved_identity",
            source=str(source)[:40],
            reason="snapshot_envelope_missing_drive",
        )
        return [], "blocked_no_snapshot_envelope"

    if eff.casefold() != intended.casefold():
        log_info(
            "destination_snapshot_rejected_selected_library_mismatch",
            source=str(source)[:40],
            intended_drive_suffix=intended[-16:] if len(intended) > 16 else intended,
            stored_drive_suffix=eff[-16:] if len(eff) > 16 else eff,
        )
        log_info(
            "destination_startup_snapshot_rejected_identity_mismatch",
            source=str(source)[:40],
            intended_drive_suffix=intended[-16:] if len(intended) > 16 else intended,
            stored_drive_suffix=eff[-16:] if len(eff) > 16 else eff,
        )
        return [], "rejected_envelope_mismatch"

    ins = str(intended_site_id or "").strip()
    sts = str(snapshot_stored_site_id or "").strip()
    if ins and sts:
        if ins.casefold() != sts.casefold():
            log_info(
                "destination_snapshot_rejected_selected_site_mismatch",
                source=str(source)[:40],
                intended_site_suffix=ins[-16:] if len(ins) > 16 else ins,
                snapshot_site_suffix=sts[-16:] if len(sts) > 16 else sts,
            )
            return [], "rejected_site_mismatch"
    else:
        log_info(
            "destination_snapshot_site_identity_check_skipped",
            source=str(source)[:40],
            reason="one_or_both_site_ids_missing",
            has_intended_site=bool(ins),
            has_snapshot_site=bool(sts),
        )

    log_info(
        "destination_snapshot_identity_loaded",
        source=str(source)[:40],
        drive_id_suffix=eff[-16:] if len(eff) > 16 else eff,
        library_id_suffix=eff[-16:] if len(eff) > 16 else eff,
        library_name_excerpt="",
    )
    return list(snapshots or []), "identity_ok"


def prune_nested_snapshot_nodes_for_wrong_drive(
    snapshots: list | None,
    intended_drive_id: str,
    *,
    selection_tag: str = "",
    max_nodes: int = 100_000,
) -> tuple[list, int]:
    """Remove subtrees whose payload drive_id contradicts the intended destination drive (bounded)."""

    intended = str(intended_drive_id or "").strip()
    if not intended or not snapshots:
        return list(snapshots or []), 0

    pruned = 0
    visited = 0

    def walk(node: Any) -> dict | None:
        nonlocal pruned, visited
        if visited >= max_nodes:
            return None
        if not isinstance(node, dict):
            return None
        visited += 1
        pl = node.get("data") if isinstance(node.get("data"), dict) else {}
        rid = _drive_id_from_payload(pl)
        if rid and rid.casefold() != intended.casefold():
            pruned += 1
            log_info(
                "destination_startup_snapshot_row_pruned_identity_mismatch",
                selection_tag=str(selection_tag or "")[:120],
                reason="nested_drive_mismatch",
                drive_suffix=rid[-16:] if len(rid) > 16 else rid,
            )
            return None
        ch_in = list(node.get("children") or [])
        ch_out: list = []
        for ch in ch_in:
            kept = walk(ch)
            if kept is not None:
                ch_out.append(kept)
        out = dict(node)
        out["children"] = ch_out
        return out

    out_roots: list = []
    for snap in list(snapshots or []):
        if isinstance(snap, dict):
            kept = walk(snap)
            if kept is not None:
                out_roots.append(kept)
    return out_roots, pruned


def select_validated_destination_startup_snapshot(
    session_destination_snaps: list,
    workspace_sidecar_destination_snaps: list | None,
    ctx: DestinationStartupSnapshotRootContext,
    *,
    session_envelope_drive_id: str = "",
    session_envelope_library_id: str = "",
    session_envelope_site_id: str = "",
    sidecar_envelope_drive_id: str = "",
    sidecar_envelope_library_id: str = "",
    sidecar_envelope_site_id: str = "",
    intended_drive_id: str = "",
    intended_site_id: str = "",
    legacy_library_candidates: list[DestinationLibraryCandidate] | None = None,
) -> tuple[list, str, int, int, dict[str, Any]]:
    """Choose session vs sidecar after identity gate + sanitization; validity outranks raw node count.

    Returns (selected_list, label, n_sess_nodes_after, n_side_nodes_after, meta).
    """

    session_list = list(session_destination_snaps or [])
    side_list = list(workspace_sidecar_destination_snaps or [])

    sess_gated, sess_gate_tag, sess_legacy_stamp = apply_destination_snapshot_identity_gate_with_legacy(
        session_list,
        intended_drive_id=intended_drive_id,
        snapshot_stored_drive_id=session_envelope_drive_id,
        snapshot_stored_library_id=session_envelope_library_id or session_envelope_drive_id,
        source="session",
        legacy_library_candidates=legacy_library_candidates,
        intended_site_id=intended_site_id,
        snapshot_stored_site_id=session_envelope_site_id,
    )
    side_gated, side_gate_tag, side_legacy_stamp = apply_destination_snapshot_identity_gate_with_legacy(
        side_list,
        intended_drive_id=intended_drive_id,
        snapshot_stored_drive_id=sidecar_envelope_drive_id,
        snapshot_stored_library_id=sidecar_envelope_library_id or sidecar_envelope_drive_id,
        source="sidecar",
        legacy_library_candidates=legacy_library_candidates,
        intended_site_id=intended_site_id,
        snapshot_stored_site_id=sidecar_envelope_site_id,
    )

    sess_gated, _npr_s = prune_nested_snapshot_nodes_for_wrong_drive(
        sess_gated, intended_drive_id, selection_tag="session_nested_prune"
    )
    side_gated, _npr_t = prune_nested_snapshot_nodes_for_wrong_drive(
        side_gated, intended_drive_id, selection_tag="sidecar_nested_prune"
    )

    sess_san, st_sess = sanitize_destination_startup_snapshot_top_level(
        sess_gated, ctx, selection_tag="session_candidate", log_validation_summary=False
    )
    side_san, st_side = sanitize_destination_startup_snapshot_top_level(
        side_gated, ctx, selection_tag="sidecar_candidate", log_validation_summary=False
    )

    log_info(
        "destination_startup_snapshot_validation_summary_strict",
        session_identity_gate=str(sess_gate_tag)[:80],
        sidecar_identity_gate=str(side_gate_tag)[:80],
        intended_drive_suffix=str(intended_drive_id or "")[-16:],
        session_kept_top=int(st_sess.kept_top_level),
        session_pruned_top=int(st_sess.pruned_top_level),
        sidecar_kept_top=int(st_side.kept_top_level),
        sidecar_pruned_top=int(st_side.pruned_top_level),
    )

    n_sess = snapshot_node_count_recursive(sess_san)
    n_side = snapshot_node_count_recursive(side_san)
    n_sess_raw = snapshot_node_count_recursive(session_list)
    n_side_raw = snapshot_node_count_recursive(side_list)

    log_info(
        "destination_startup_snapshot_candidate_sanitized",
        session_nodes_after=int(n_sess),
        sidecar_nodes_after=int(n_side),
        session_nodes_before=int(n_sess_raw),
        sidecar_nodes_before=int(n_side_raw),
        session_pruned_top_level=int(st_sess.pruned_top_level),
        sidecar_pruned_top_level=int(st_side.pruned_top_level),
    )

    usable_sess = n_sess > 0
    usable_side = n_side > 0

    label = "SessionState.DestinationTreeSnapshot"
    chosen: list = sess_san
    if usable_sess and not usable_side:
        label = "SessionState.DestinationTreeSnapshot"
        chosen = sess_san
    elif usable_side and not usable_sess:
        label = "WorkspaceSnapshot.destination_tree_snapshot"
        chosen = side_san
    elif usable_sess and usable_side:
        if n_side > n_sess:
            label = "WorkspaceSnapshot.destination_tree_snapshot"
            chosen = side_san
        else:
            label = "SessionState.DestinationTreeSnapshot"
            chosen = sess_san
    else:
        # Both empty after sanitization — prefer session (usually fewer stale sidecars).
        chosen = sess_san
        label = "SessionState.DestinationTreeSnapshot_fallback_empty"

    reason, legacy_inference_retry_recommended = pick_legacy_inference_retry_meta(
        str(sess_gate_tag),
        str(side_gate_tag),
        int(n_sess_raw),
        int(n_side_raw),
        str(intended_drive_id),
    )
    meta = {
        "session_sanitized_nodes": n_sess,
        "sidecar_sanitized_nodes": n_side,
        "session_raw_nodes": int(n_sess_raw),
        "sidecar_raw_nodes": int(n_side_raw),
        "chosen_label": label,
        "session_identity_gate": str(sess_gate_tag),
        "sidecar_identity_gate": str(side_gate_tag),
        "session_legacy_identity_stamp": sess_legacy_stamp,
        "sidecar_legacy_identity_stamp": side_legacy_stamp,
        "reason": reason,
        "legacy_inference_retry_recommended": bool(legacy_inference_retry_recommended),
    }
    log_info(
        "destination_startup_snapshot_selection_after_validation",
        selected_source=str(label)[:120],
        chosen_nodes=int(snapshot_node_count_recursive(chosen)),
        usable_session=bool(usable_sess),
        usable_sidecar=bool(usable_side),
        legacy_inference_retry_recommended=bool(legacy_inference_retry_recommended),
        legacy_inference_block_reason=str(reason or "")[:80],
    )
    return chosen, label, n_sess, n_side, meta


def snapshot_node_count_recursive(snapshots: list | None) -> int:
    n = 0

    def walk(node: Any) -> None:
        nonlocal n
        if isinstance(node, dict):
            n += 1
            for ch in list(node.get("children") or []):
                walk(ch)

    for r in list(snapshots or []):
        walk(r)
    return n
