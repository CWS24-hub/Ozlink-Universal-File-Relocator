"""
Legacy destination tree snapshot identity inference (startup restore migration).

Older session/workspace files may omit snapshot-level destination library identity while still
containing structurally valid trees. This module scores candidate document libraries from the
current destination site context and optionally infers a safe drive/library binding.

Strict explicit identity (when present) is handled elsewhere — never bypassed here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ozlink_console.logger import log_info


class InferenceConfidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# Score thresholds (documented; tune with telemetry if needed).
# HIGH: strong row-drive consensus and/or distinctive shallow-root fingerprint + margin vs runner-up.
SCORE_HIGH_MIN = 64.0
SCORE_MEDIUM_MIN = 42.0
WIN_MARGIN_FOR_AUTO = 12.0  # top1 - top2 must exceed this for auto_apply_safe when scores are close


@dataclass
class DestinationLibraryCandidate:
    """One destination document library available in the current site/selector context."""

    drive_id: str
    library_id: str = ""
    display_name: str = ""
    site_id: str = ""
    # Optional: casefolded shallow Graph root **names** for this library when cached (Tier 2).
    graph_shallow_root_names_cf: frozenset[str] | None = None


@dataclass
class LegacyDestinationSnapshotIdentityResult:
    matched_drive_id: str = ""
    matched_library_id: str = ""
    matched_library_name: str = ""
    matched_site_id: str = ""
    confidence: InferenceConfidence = InferenceConfidence.LOW
    score: float = 0.0
    second_best_score: float = 0.0
    auto_apply_safe: bool = False
    reasons: list[str] = field(default_factory=list)
    signals: dict[str, Any] = field(default_factory=dict)
    inferred_from_legacy: bool = True

    def to_meta_dict(self) -> dict[str, Any]:
        return {
            "matched_drive_id": self.matched_drive_id,
            "matched_library_id": self.matched_library_id or self.matched_drive_id,
            "matched_library_name": self.matched_library_name,
            "matched_site_id": self.matched_site_id,
            "confidence": self.confidence.value,
            "score": self.score,
            "second_best_score": self.second_best_score,
            "auto_apply_safe": self.auto_apply_safe,
            "reasons": list(self.reasons)[:24],
            "signals": dict(self.signals),
            "inferred_from_legacy": self.inferred_from_legacy,
        }


def _suffix_excerpt(s: str, n: int = 16) -> str:
    s = str(s or "").strip()
    return s[-n:] if len(s) > n else s


def _walk_snapshot_payloads(snapshots: list | None, max_nodes: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    visited = 0

    def walk(node: Any) -> None:
        nonlocal visited
        if visited >= max_nodes:
            return
        if not isinstance(node, dict):
            return
        visited += 1
        pl = node.get("data") if isinstance(node.get("data"), dict) else {}
        if isinstance(pl, dict):
            out.append(pl)
        for ch in list(node.get("children") or []):
            walk(ch)

    for r in list(snapshots or []):
        walk(r)
    return out


def _drive_vote_tier_score(
    payloads: list[dict[str, Any]], candidates: list[DestinationLibraryCandidate]
) -> tuple[float, str, dict[str, int], str]:
    """Tier 1: row-level drive_id / library_id consistency. Returns (score, winner_cf, votes, tag)."""
    cand_ids = {str(c.drive_id or "").strip().casefold() for c in candidates if str(c.drive_id or "").strip()}
    votes: dict[str, int] = {}
    for pl in payloads:
        did = str(pl.get("drive_id") or pl.get("library_id") or pl.get("parent_drive_id") or "").strip()
        if not did:
            continue
        k = did.casefold()
        if cand_ids and k not in cand_ids:
            continue
        votes[k] = votes.get(k, 0) + 1
    if not votes:
        return 0.0, "", {}, "no_row_drive_votes"
    sorted_v = sorted(votes.items(), key=lambda kv: kv[1], reverse=True)
    top_k, top_n = sorted_v[0]
    total = sum(votes.values())
    ratio = float(top_n) / float(total) if total else 0.0
    score = 0.0
    if top_n >= 8:
        score = 76.0
    elif top_n >= 5:
        score = 70.0
    elif top_n >= 3:
        score = 58.0
    elif top_n >= 1:
        score = 44.0 * ratio
    if ratio >= 0.85 and top_n >= 2:
        score += 8.0
    return min(80.0, score), top_k, votes, "row_drive_majority"


def _root_fingerprint_tier(
    snapshots: list | None,
    candidate: DestinationLibraryCandidate,
    *,
    all_candidates: list[DestinationLibraryCandidate],
) -> float:
    """Tier 2: top-level root segments vs candidate shallow Graph roots (uniqueness bonus)."""
    from ozlink_console.destination_startup_snapshot_roots import allowed_semantic_root_segments_cf_from_snapshot

    g = candidate.graph_shallow_root_names_cf
    if not g or len(g) == 0:
        return 0.0
    segs = allowed_semantic_root_segments_cf_from_snapshot(snapshots)
    if not segs:
        return 0.0
    inter = {s for s in segs if s in g}
    if not inter:
        return 0.0
    # Uniqueness: only this candidate's g intersects segs
    other_union: set[str] = set()
    for c in all_candidates:
        if c is candidate:
            continue
        og = c.graph_shallow_root_names_cf
        if og:
            other_union |= set(og)
    distinctive = inter - other_union
    if distinctive:
        return 38.0
    if inter:
        return 22.0
    return 0.0


def _path_consistency_tier(payloads: list[dict[str, Any]], candidate: DestinationLibraryCandidate) -> float:
    """Tier 3: descendant path first-segment alignment with candidate display name (weak)."""
    from ozlink_console.destination_startup_snapshot_roots import first_path_segment_cf

    dn = str(candidate.display_name or "").strip().casefold()
    if not dn:
        return 0.0
    hits = 0
    total = 0
    for pl in payloads:
        for k in ("item_path", "destination_path", "semantic_path", "display_path"):
            p = str(pl.get(k) or "").strip()
            if not p:
                continue
            total += 1
            if first_path_segment_cf(p) == dn:
                hits += 1
    if total == 0:
        return 0.0
    r = hits / total
    if r >= 0.55:
        return 18.0 * r
    return 0.0


def infer_destination_snapshot_identity_from_legacy_snapshot(
    snapshots: list | None,
    candidates: list[DestinationLibraryCandidate],
    *,
    max_payload_nodes: int = 8000,
) -> LegacyDestinationSnapshotIdentityResult:
    """Score each candidate; pick best. Does not compare to intended drive — caller enforces that."""
    out = LegacyDestinationSnapshotIdentityResult()
    cands = [c for c in (candidates or []) if str(c.drive_id or "").strip()]
    if not cands:
        out.reasons.append("no_destination_library_candidates")
        log_info(
            "destination_snapshot_legacy_identity_inference_result",
            outcome="no_candidates",
            confidence=out.confidence.value,
            score=0.0,
            auto_apply_safe=False,
        )
        return out

    log_info(
        "destination_snapshot_legacy_identity_inference_begin",
        candidate_count=len(cands),
        snapshot_top_level=len(list(snapshots or [])),
    )

    payloads = _walk_snapshot_payloads(snapshots, max_payload_nodes)
    tier1_score, winner_cf, vote_tally, tier1_tag = _drive_vote_tier_score(payloads, cands)

    scores: list[tuple[float, DestinationLibraryCandidate, list[str]]] = []
    for c in cands:
        reasons: list[str] = []
        s = 0.0
        cid = str(c.drive_id or "").strip().casefold()
        if tier1_score > 0 and winner_cf and cid == winner_cf:
            s += tier1_score
            reasons.append(f"tier1_row_drive_votes={vote_tally.get(winner_cf, 0)}")
        t2 = _root_fingerprint_tier(snapshots, c, all_candidates=cands)
        s += t2
        if t2 > 0:
            reasons.append("tier2_shallow_root_overlap")
        t3 = _path_consistency_tier(payloads, c)
        s += t3
        if t3 > 0:
            reasons.append("tier3_path_name_anchor")
        scores.append((s, c, reasons))
        log_info(
            "destination_snapshot_legacy_identity_candidate_scored",
            drive_id_suffix=_suffix_excerpt(c.drive_id),
            display_name_excerpt=str(c.display_name or "")[:80],
            score=round(s, 2),
            reasons=";".join(reasons)[:220],
        )

    scores.sort(key=lambda x: x[0], reverse=True)
    top_s, top_c, top_reasons = scores[0]
    second_s = scores[1][0] if len(scores) > 1 else 0.0

    out.matched_drive_id = str(top_c.drive_id or "").strip()
    out.matched_library_id = str(top_c.library_id or top_c.drive_id or "").strip()
    out.matched_library_name = str(top_c.display_name or "").strip()
    out.matched_site_id = str(top_c.site_id or "").strip()
    out.score = float(top_s)
    out.second_best_score = float(second_s)
    out.signals = {"tier1_tag": tier1_tag, "vote_tally_keys": len(vote_tally), "payload_rows": len(payloads)}
    out.reasons = top_reasons

    if top_s >= SCORE_HIGH_MIN:
        out.confidence = InferenceConfidence.HIGH
    elif top_s >= SCORE_MEDIUM_MIN:
        out.confidence = InferenceConfidence.MEDIUM
    else:
        out.confidence = InferenceConfidence.LOW

    margin = top_s - second_s
    out.auto_apply_safe = bool(
        out.confidence == InferenceConfidence.HIGH and margin >= WIN_MARGIN_FOR_AUTO and out.matched_drive_id
    )

    log_info(
        "destination_snapshot_legacy_identity_inference_result",
        best_drive_suffix=_suffix_excerpt(out.matched_drive_id),
        confidence=out.confidence.value,
        score=round(out.score, 2),
        second_best=round(second_s, 2),
        margin=round(margin, 2),
        auto_apply_safe=out.auto_apply_safe,
        reasons=";".join(out.reasons)[:260],
    )
    return out
