"""
Hybrid destination preview (Browse/Plan/Execute — destination slice).

Authoritative spec: ``docs/architecture/browse_plan_execute_contract.md`` (hybrid + badge sections).

* Saved plan / session snapshot is shown first; live Microsoft Graph is merged on expand/refresh.
* This module is intentionally small: env gates + display badges derived from existing payload fields.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_planned_workspace_row


def destination_memory_rehydrate_is_repair_truth_audit(audit_ctx: str) -> bool:
    """
    True when a memory branch rehydrate is tagged as repair/truth work (Execute/Validate lane).

    Brows/startup / display_preview hydration must not use these tags — they may trigger
    invariant/overlay repair paths. See :meth:`MainWindow._destination_repair_truth_hydration_scope`.
    """
    a = str(audit_ctx or "").strip().casefold()
    if not a:
        return False
    if a == "overlay_projection_invariant_repair":
        return True
    if "overlay_projection_invariant_repair" in a:
        return True
    if "projection_repair_after" in a and "descendant" in a:
        return True
    return False


def destination_preview_rehydrate_audit_is_idempotent_merge_log(audit_ctx: str) -> bool:
    """True for browse/preview rehydrate audits that should log idempotent merge stats."""
    a = str(audit_ctx or "").strip().casefold()
    if not a:
        return False
    if "post_shell" in a:
        return True
    if "rich_branch_scan" in a or "post_shell_rich" in a:
        return True
    return False


def hybrid_destination_preview_browse_first_enabled() -> bool:
    """
    When True, provisional startup skips phase-2 expand/hydrate/branch-refresh (broad rehydrate)
    until the user needs it; preview remains browse-first. Opt out with
    OZLINK_HYBRID_DESTINATION_PREVIEW=0|false|no|off.
    """
    raw = str(os.environ.get("OZLINK_HYBRID_DESTINATION_PREVIEW", "") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _ws(pl: dict) -> str:
    return str(pl.get("workspace_row_state") or "").strip().lower()


def destination_hybrid_preview_badge_text(payload: Any) -> str:
    """
    Return a short bracketed badge for the name column, or "" when none.

    Contract mapping (first slice, derived from existing model fields only):
    * [Live + Planned] — graph_vs_planned == live_planned (set when expand merges live Graph into a planned row)
    * [Not refreshed] — cached_provisional
    * [Planned] / [Proposed] — strict planned workspace rows
    * [Live] — live Graph row without planning overlay
    """
    if not isinstance(payload, dict) or payload.get("placeholder"):
        return ""
    gvp = str(payload.get("graph_vs_planned") or "").strip().lower()
    if gvp == "live_planned":
        return "[Live + Planned]"
    ws = _ws(payload)
    if ws == "cached_provisional":
        return "[Not refreshed]"
    if destination_payload_is_planned_workspace_row(payload):
        if bool(payload.get("proposed")) or str(payload.get("node_origin") or "").strip().lower() == "proposed":
            return "[Proposed]"
        if bool(payload.get("is_folder", True)) and not str(payload.get("id") or "").strip():
            return "[Pending creation]"
        return "[Planned]"
    if ws == "live_confirmed":
        return "[Live]"
    if gvp in ("live_graph", "live"):
        return "[Live]"
    rk = str(payload.get("row_kind") or "").lower()
    if str(payload.get("id") or "").strip() and rk.startswith("live_"):
        return "[Live]"
    return ""


REPO_CONTRACT_REL = Path("docs") / "architecture" / "browse_plan_execute_contract.md"


def contract_doc_file_present(repo_root: Path | None = None) -> bool:
    """True when the browse/plan/execute contract markdown exists (repo layout)."""
    here = Path(__file__).resolve().parent
    for base in (repo_root,) if repo_root is not None else (here.parent.parent, here.parent):
        p = (base / REPO_CONTRACT_REL).resolve()
        if p.is_file():
            return True
    return False


def hybrid_destination_preview_state_for_log() -> dict[str, Any]:
    """
    One-shot fields for :log:`destination_hybrid_preview_mode_state` (import-safe; no MainWindow dep).
    """
    raw = str(os.environ.get("OZLINK_HYBRID_DESTINATION_PREVIEW", "") or "")
    return {
        "enabled": bool(hybrid_destination_preview_browse_first_enabled()),
        "env_value": raw,
        "contract_doc_present": bool(contract_doc_file_present()),
    }


def _snapshot_data_raw_path(d: dict) -> str:
    return str(
        d.get("item_path")
        or d.get("destination_path")
        or d.get("display_path")
        or ""
    ).strip()


def preview_row_strength(d: dict) -> int:
    """
    Keep strongest row when deduping sibling snapshot nodes.
    Order: Live + Planned > Live > Proposed > Planned (memory) > Not refreshed > other.
    """
    if not isinstance(d, dict) or d.get("placeholder"):
        return 0
    gvp = str(d.get("graph_vs_planned") or "").strip().lower()
    ws = str(d.get("workspace_row_state") or "").strip().lower()
    is_prop = bool(d.get("proposed")) or str(d.get("node_origin") or "").strip().lower() == "proposed"
    planned = destination_payload_is_planned_workspace_row(d)
    live = gvp in ("live_graph", "live", "live_planned") or str(d.get("row_kind") or "").lower().startswith(
        "live_"
    )
    live_planned = gvp == "live_planned"
    if live_planned:
        return 50
    if live and planned:
        return 45
    if live and str(d.get("id") or "").strip():
        return 40
    if is_prop:
        return 35
    if planned:
        return 30
    if ws == "cached_provisional":
        return 10
    if planned or ws:
        return 5
    return 1


def _dedupe_one_level_children(children: list) -> tuple[list, dict[str, int]]:
    """
    Deduplicate immediate children with the same casefolded path key; recurse into each kept child.
    Non-data / placeholder children are kept (deduped only by recursion).
    """
    st = {
        "before_count": 0,
        "after_count": 0,
        "removed_count": 0,
        "duplicate_path_count": 0,
    }
    if not children:
        return [], st
    passthrough: list[dict] = []
    keyed: dict[str, dict] = {}
    key_order: list[str] = []
    for ch in list(children):
        if not isinstance(ch, dict):
            continue
        d0 = ch.get("data") if isinstance(ch.get("data"), dict) else None
        if not isinstance(d0, dict) or d0.get("placeholder"):
            nrec, s2 = sanitize_destination_tree_snapshot_subtree(ch)
            passthrough.append(nrec)
            for k, v in s2.items():
                st[k] = st.get(k, 0) + int(v or 0)
            continue
        k = str(_snapshot_data_raw_path(d0)).casefold()
        st["before_count"] += 1
        nrec, s2 = sanitize_destination_tree_snapshot_subtree(ch)
        for kk, v in s2.items():
            st[kk] = st.get(kk, 0) + int(v or 0)
        d1 = nrec.get("data") if isinstance(nrec.get("data"), dict) else None
        if not k or not isinstance(d1, dict) or d1.get("placeholder"):
            passthrough.append(nrec)
            st["after_count"] += 1
            continue
        if k not in keyed:
            keyed[k] = nrec
            key_order.append(k)
            st["after_count"] += 1
            continue
        st["duplicate_path_count"] += 1
        prev = keyed[k]
        pd = prev.get("data") if isinstance(prev.get("data"), dict) else {}
        if preview_row_strength(d1) > preview_row_strength(pd if isinstance(pd, dict) else {}):
            keyed[k] = nrec
        st["removed_count"] += 1
    out = passthrough + [keyed[k] for k in key_order if k in keyed]
    return out, st


def sanitize_destination_tree_snapshot_subtree(node: dict) -> tuple[dict, dict[str, int]]:
    """Deduplicate duplicate canonical child paths; recurse. Returns (node, per-subtree stats)."""
    empty = {
        "before_count": 0,
        "after_count": 0,
        "removed_count": 0,
        "duplicate_path_count": 0,
    }
    if not isinstance(node, dict):
        return node, empty
    ch0 = list(node.get("children") or [])
    if not ch0:
        return node, empty
    ch1, st = _dedupe_one_level_children(ch0)
    n2 = dict(node)
    n2["children"] = ch1
    return n2, st


def sanitize_destination_tree_snapshot_roots(roots: list) -> tuple[list, dict[str, int]]:
    """
    Startup / persist: remove duplicate destination preview rows (same canonical path under the same parent).
    Logs are emitted by the caller to avoid import cycles.
    """
    if not isinstance(roots, list) or not roots:
        return (roots if isinstance(roots, list) else []), {
            "before_count": 0,
            "after_count": 0,
            "removed_count": 0,
            "duplicate_path_count": 0,
        }
    out2: list = []
    tot = {
        "before_count": 0,
        "after_count": 0,
        "removed_count": 0,
        "duplicate_path_count": 0,
    }
    for r in roots:
        if not isinstance(r, dict):
            continue
        r2, st = sanitize_destination_tree_snapshot_subtree(r)
        out2.append(r2)
        for k, v in st.items():
            tot[k] = int(tot.get(k, 0)) + int(v or 0)
    return out2, tot


def destination_hybrid_name_column_text(base_label: str, payload: Any) -> str:
    """Append hybrid badge to the explorer name when browse-first is enabled and a badge applies."""
    if not hybrid_destination_preview_browse_first_enabled():
        return str(base_label or "")
    b = str(base_label or "")
    tag = destination_hybrid_preview_badge_text(payload)
    if not tag:
        return b
    return f"{b}  {tag}" if b else tag
