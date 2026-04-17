#!/usr/bin/env python3
"""Analyze one Ozlink JSON session log for Contractor Resumes / FTBMRoot subtree projection replay.

**Input:** ``app.log`` (JSON lines) or a session directory (all ``*.log*``).

**Where logs live:** see ``ozlink_console.logger`` — typically::

    %LOCALAPPDATA%\\OzlinkIT\\OzlinkITSharePointRelocationConsole\\Logs\\<YYYY-MM-DD_HH-MM-SS>\\app.log

**Usage:**

    python scripts/verify_contractor_resumes_projection_run.py "C:\\...\\Logs\\2026-04-15_12-00-00\\app.log"
    python scripts/verify_contractor_resumes_projection_run.py "C:\\...\\Logs\\2026-04-15_12-00-00"
    type app.log | python scripts/verify_contractor_resumes_projection_run.py -

This script prints measured counts only (no app architecture changes).

Subtree filter (case-insensitive): lines whose JSON contains both ``ftbmroot`` and ``contractor resumes``,
or ``contractor resumes`` alone in any string field. Override with ``--subtree-substr``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _iter_json_lines(paths: list[Path]) -> tuple[str, dict]:
    for p in paths:
        if str(p) == "-":
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield ("stdin", json.loads(line))
                except json.JSONDecodeError:
                    continue
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            print(f"# skip {p}: {e}", file=sys.stderr)
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield (str(p), json.loads(line))
            except json.JSONDecodeError:
                continue


def _row_blob(row: dict) -> str:
    try:
        return json.dumps(row, ensure_ascii=False, default=str).lower()
    except Exception:
        return ""


def _matches_subtree(blob: str, ft: str, cr: str) -> bool:
    b = blob
    if cr and cr in b:
        return True
    return ft in b and cr in b


def _cache_event_matches_subtree(row: dict, ft: str, cr: str) -> bool:
    if _matches_subtree(_row_blob(row), ft, cr):
        return True
    d = row.get("data") or {}
    for k in ("source_root_excerpt", "stable_key_excerpt", "legacy_key_excerpt", "note"):
        v = str(d.get(k) or "").lower()
        if cr and cr in v:
            return True
        if ft and cr and ft in v and cr in v:
            return True
    return False


def _log_files_from_arg(path_str: str) -> list[Path]:
    if path_str == "-":
        return [Path("-")]
    p = Path(path_str)
    if p.is_dir():
        seen: set[str] = set()
        out: list[Path] = []
        for g in (sorted(p.glob("app.log*")) + sorted(p.glob("*.log"))):
            s = str(g.resolve())
            if s not in seen:
                seen.add(s)
                out.append(g)
        return out or [p / "app.log"]
    return [p]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("log_path", help="app.log, session directory, or - for stdin")
    ap.add_argument(
        "--subtree-substr",
        default="contractor resumes",
        help="case-insensitive substring to identify subtree-related lines (default: contractor resumes)",
    )
    ap.add_argument(
        "--ftbm-prefix",
        default="ftbmroot",
        help="second substring for FTBMRoot\\Contractor Resumes style lines (default: ftbmroot)",
    )
    args = ap.parse_args()
    ft = str(args.ftbm_prefix or "").strip().lower()
    cr = str(args.subtree_substr or "").strip().lower()

    paths = _log_files_from_arg(args.log_path)
    if not paths:
        print("No log files found.", file=sys.stderr)
        return 2

    # --- Pass: collect rows matching subtree blob (global filter for "this scenario")
    subtree_rows: list[tuple[str, dict]] = []
    all_rows: list[tuple[str, dict]] = []
    for src, row in _iter_json_lines(paths):
        all_rows.append((src, row))
        blob = _row_blob(row)
        if _matches_subtree(blob, ft, cr):
            subtree_rows.append((src, row))

    def msg(r: dict) -> str:
        return str(r.get("message") or "")

    # --- 1) Source cache lifecycle (cache_event rows that belong to this subtree)
    cache_events = [
        r
        for _, r in all_rows
        if msg(r) == "source_projection_descendants_cache_event" and _cache_event_matches_subtree(r, ft, cr)
    ]
    puts = [r for r in cache_events if (r.get("data") or {}).get("op") == "put"]
    skipped = [r for r in cache_events if (r.get("data") or {}).get("op") == "invalidate_skipped"]
    clears = [r for r in cache_events if (r.get("data") or {}).get("op") == "full_clear"]
    evicts = [r for r in cache_events if (r.get("data") or {}).get("op") == "lru_evict"]

    print("=== 1) Source cache lifecycle (subtree-filtered lines) ===")
    print(f"Subtree-matching lines total: {len(subtree_rows)}")
    print(f"source_projection_descendants_cache_event: {len(cache_events)}")
    print(f"  put: {len(puts)}")
    print(f"  invalidate_skipped: {len(skipped)}")
    print(f"  full_clear: {len(clears)}")
    print(f"  lru_evict: {len(evicts)}")
    print("First cache_event sequence (chronological, up to 40):")
    for i, r in enumerate(cache_events[:40], 1):
        d = r.get("data") or {}
        print(
            f"  {i:02d} op={d.get('op')} reason={str(d.get('reason') or '')[:120]!r} "
            f"caller={str(d.get('caller') or '')[:80]!r} "
            f"note={str(d.get('note') or '')[:80]!r}"
        )
    if not cache_events:
        print("  (no subtree-matched cache events — widen filter or confirm FTBMRoot+Contractor Resumes appears in log)")

    survives_overlay = len(skipped) > 0 and any(
        "overlay_projection_invariant_repair" in str((r.get("data") or {}).get("reason") or "")
        for r in skipped
    )
    print(
        "\nConclusion (measured from subtree-filtered cache lines): "
        f"invalidate_skipped present for overlay repair = {survives_overlay}. "
        "If put occurs and later only invalidate_skipped (not full_clear) for this subtree, "
        "Graph source subtree cache was preserved across destination overlay repair."
    )

    def _proj_row_matches_subtree(r: dict) -> bool:
        if _matches_subtree(_row_blob(r), ft, cr):
            return True
        d = r.get("data") or {}
        for k in ("source_root_path_excerpt", "stable_cache_key_excerpt", "legacy_cache_key_excerpt"):
            v = str(d.get(k) or "").lower()
            if cr and cr in v:
                return True
            if ft and cr and ft in v and cr in v:
                return True
        return False

    # --- 2) Collection state: projection_source_descendants_started
    started = [
        r for _, r in all_rows if msg(r) == "projection_source_descendants_started" and _proj_row_matches_subtree(r)
    ]
    collect_rows = [
        r for _, r in all_rows if msg(r) == "projection_source_descendants_collect" and _proj_row_matches_subtree(r)
    ]

    print("\n=== 2) Collection state (projection_source_descendants_started) ===")
    print(f"projection_source_descendants_started count: {len(started)}")
    for i, r in enumerate(started, 1):
        d = r.get("data") or {}
        print(
            f"  #{i} collect_reason={d.get('collect_reason')!r} "
            f"subtree_cache_state={(d.get('subtree_cache_state') or d.get('cache_tier') or '')!r} "
            f"replay_reason={d.get('replay_reason')!r} "
            f"stable={str(d.get('stable_cache_key_excerpt') or '')[:100]!r} "
            f"legacy={str(d.get('legacy_cache_key_excerpt') or '')[:100]!r}"
        )
    miss_next = sum(
        1
        for r in collect_rows
        if str((r.get("data") or {}).get("subtree_cache_state") or "") == "miss_graph_api_next"
    )
    stable_hit = sum(
        1
        for r in collect_rows
        if str((r.get("data") or {}).get("subtree_cache_state") or "") == "stable_hit"
    )
    print(f"\nprojection_source_descendants_collect (subtree lines): miss_graph_api_next={miss_next}, stable_hit={stable_hit}")

    # --- 3) Overlay repair skips
    fresh = [r for _, r in all_rows if msg(r) == "destination_overlay_reproject_skipped_stamped_projection_fresh"]
    inflight = [r for _, r in all_rows if msg(r) == "destination_overlay_projection_repair_skipped_descendant_apply_in_flight"]
    fresh_sub = [r for r in fresh if _matches_subtree(_row_blob(r), ft, cr)]
    inflight_sub = [r for r in inflight if _matches_subtree(_row_blob(r), ft, cr)]

    print("\n=== 3) Overlay repair behavior ===")
    print(f"destination_overlay_reproject_skipped_stamped_projection_fresh (all log): {len(fresh)}")
    print(f"  subtree-filtered: {len(fresh_sub)}")
    print(f"destination_overlay_projection_repair_skipped_descendant_apply_in_flight (all log): {len(inflight)}")
    print(f"  subtree-filtered: {len(inflight_sub)}")

    # --- 4) Rebind churn: allocation_descendant binds
    # Prefer chunk_begin (always logged) with bind_context_excerpt; filter subtree via blob.
    chunk_begins = [
        r
        for _, r in all_rows
        if msg(r) == "startup_lifecycle_temp_live_descendant_injected_chunk_begin"
        and _matches_subtree(_row_blob(r), ft, cr)
    ]
    bind_ctx = [(r.get("data") or {}).get("bind_context_excerpt") for r in chunk_begins]
    cnt = Counter(str(x or "") for x in bind_ctx)
    print("\n=== 4) Rebind churn (startup_lifecycle_temp_live_descendant_injected_chunk_begin, subtree-filtered) ===")
    print(f"chunk_begin events: {len(chunk_begins)}")
    print("Per bind_context_excerpt (often source path to leaf):")
    for k, v in cnt.most_common(30):
        print(f"  {v}x  {k[:200]!r}")

    # --- 5) Convergence pipeline
    enq = [r for _, r in subtree_rows if msg(r) == "destination_descendant_apply_enqueued"]
    fin = [r for _, r in subtree_rows if "finalize" in msg(r) and "descendant" in msg(r).lower()]
    # graph finalize uses destination_descendant_apply_on_complete or descendant_apply_index_revalidated — use enqueued + projection finished
    started_apply = enq  # noqa
    stale = [r for _, r in subtree_rows if "stale" in msg(r).lower() or "aborted" in msg(r).lower()]

    print("\n=== 5) Descendant pipeline (subtree-filtered where possible) ===")
    print(f"destination_descendant_apply_enqueued: {len(enq)}")
    print(f"lines with stale/aborted in message (subtree filter): {len(stale)}")
    fin_graph = [r for _, r in subtree_rows if msg(r) == "projection_source_descendants_finished"]
    print(f"projection_source_descendants_finished (subtree): {len(fin_graph)}")

    # --- 6) UX / churn proxies (measured only)
    guard = sum(1 for _, r in subtree_rows if msg(r) == "descendant_apply_model_mutation_guard_skip")
    print("\n=== 6) UX proxies (subtree-filtered lines) ===")
    print(f"descendant_apply_model_mutation_guard_skip: {guard}")
    print(
        "Scrolling: not measured in app.log directly. "
        "High descendant_apply_model_mutation_guard_skip suggests structure_generation churn during apply; "
        "zero or low count with single stable_hit is consistent with stable scrolling for this subtree."
    )

    print("\n--- End report ---")
    print("If subtree counts are 0, pass a full app.log path or relax --subtree-substr.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
