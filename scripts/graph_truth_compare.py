#!/usr/bin/env python3
"""Offline comparison of graph_truth_app_audit.jsonl vs graph_truth_live_graph.jsonl.

Does not call Microsoft Graph; use after exporting from the app (Developer → Graph truth forensic).

Example:
  python scripts/graph_truth_compare.py ^
    --app graph_truth_app_audit.jsonl ^
    --live graph_truth_live_graph.jsonl ^
    --out graph_truth_comparison.jsonl ^
    --summary graph_truth_summary.txt ^
    --violations graph_truth_violations.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _violations_to_jsonl(app_rows: list[dict], live_map: dict) -> list[dict]:
    out: list[dict] = []
    for row in app_rows:
        if str(row.get("verification_state") or "").strip() != "planned_only":
            continue
        if not row.get("visible_in_tree", True):
            continue
        path = str(row.get("canonical_path") or "").strip()
        key = path.casefold()
        if not key or key not in live_map:
            continue
        live = live_map[key]
        out.append(
            {
                "event": "destination_graph_truth_violation",
                "canonical_path": path,
                "app_verification_state": row.get("verification_state"),
                "app_row_kind": row.get("row_kind"),
                "app_graph_item_id": row.get("graph_item_id"),
                "live_graph_item_id": live.get("graph_item_id"),
                "live_is_folder": live.get("is_folder"),
                "app_is_folder": row.get("is_folder"),
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare app vs live Graph truth JSONL exports.")
    ap.add_argument("--app", type=Path, required=True, help="graph_truth_app_audit.jsonl")
    ap.add_argument("--live", type=Path, required=True, help="graph_truth_live_graph.jsonl")
    ap.add_argument("--out", type=Path, required=True, help="Output comparison JSONL")
    ap.add_argument("--summary", type=Path, help="Optional human-readable summary .txt")
    ap.add_argument("--violations", type=Path, help="Optional JSONL of planned-vs-live path violations")
    args = ap.parse_args()

    try:
        from ozlink_console.destination_graph_truth_export import (
            build_graph_truth_comparison_records,
            comparison_summary_text,
            index_live_rows_by_canonical_path,
            write_jsonl,
        )
    except ImportError:
        print("Run from repo root with PYTHONPATH set.", file=sys.stderr)
        raise

    app_rows = _read_jsonl(args.app)
    live_rows = _read_jsonl(args.live)
    comp = build_graph_truth_comparison_records(app_rows, live_rows)
    write_jsonl(str(args.out), comp)

    live_map = index_live_rows_by_canonical_path(live_rows)
    viol = _violations_to_jsonl(app_rows, live_map)
    if args.violations:
        write_jsonl(str(args.violations), viol)

    summary = comparison_summary_text(comp)
    summary += f"\n\ndestination_graph_truth_violation count (offline): {len(viol)}\n"
    if args.summary:
        args.summary.write_text(summary, encoding="utf-8")
    else:
        print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
