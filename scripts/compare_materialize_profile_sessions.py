#!/usr/bin/env python3
"""Compare two session folders: materialize profile phases + reconcile phase totals.

Use the **same workload** twice (checkout old build vs new build, or copy logs from two runs).

Scans all ``*.log*`` under each directory for JSON lines with
``message == "destination_materialize_profile"``.

Example::

    python scripts/compare_materialize_profile_sessions.py \\
        "C:\\...\\Logs\\2026-04-10_10-00-00" \\
        "C:\\...\\Logs\\2026-04-13_15-00-00"

Also prints summed ``total_ms`` for phase ``destination_reconcile_all_planned_parents_after_graph_update``
(call_count summed from ``top_steps`` entries).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

RECON_PHASE = "destination_reconcile_all_planned_parents_after_graph_update"


def _accumulate_session(session_dir: Path) -> tuple[dict[str, dict], int, float]:
    """Returns (phase_stats, n_profile_events, reconcile_total_ms).

    phase_stats[ph] = {"total_ms", "call_count", "worst_call_ms"}
    """
    phase_total: dict[str, float] = defaultdict(float)
    phase_cc: dict[str, int] = defaultdict(int)
    phase_worst: dict[str, float] = defaultdict(float)
    n_events = 0
    reconcile_ms = 0.0

    files = sorted(session_dir.glob("*.log*")) if session_dir.is_dir() else [session_dir]
    for fp in files:
        if not fp.is_file():
            continue
        try:
            with fp.open(encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line.startswith("{"):
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if row.get("message") != "destination_materialize_profile":
                        continue
                    n_events += 1
                    steps = (row.get("data") or {}).get("top_steps") or []
                    if not isinstance(steps, list):
                        continue
                    for step in steps:
                        if not isinstance(step, dict):
                            continue
                        ph = str(step.get("phase") or "")
                        if not ph:
                            continue
                        try:
                            ms = float(step.get("total_ms") or 0.0)
                        except (TypeError, ValueError):
                            ms = 0.0
                        try:
                            cc = int(step.get("call_count") or 0)
                        except (TypeError, ValueError):
                            cc = 0
                        try:
                            w = float(step.get("worst_call_ms") or 0.0)
                        except (TypeError, ValueError):
                            w = 0.0
                        phase_total[ph] += ms
                        if cc > 0:
                            phase_cc[ph] += cc
                        phase_worst[ph] = max(phase_worst[ph], w)
                        if ph == RECON_PHASE:
                            reconcile_ms += ms
        except OSError:
            continue

    stats: dict[str, dict] = {}
    for ph, tot in phase_total.items():
        stats[ph] = {
            "total_ms": tot,
            "call_count": phase_cc.get(ph, 0),
            "worst_call_ms": phase_worst[ph],
        }
    return stats, n_events, reconcile_ms


def _top3(stats: dict[str, dict]) -> list[tuple[str, dict]]:
    ranked = sorted(stats.items(), key=lambda x: -x[1]["total_ms"])
    return ranked[:3]


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: compare_materialize_profile_sessions.py <before_session_dir> <after_session_dir>",
            file=sys.stderr,
        )
        sys.exit(2)
    before = Path(sys.argv[1])
    after = Path(sys.argv[2])
    sb, nb, rb = _accumulate_session(before)
    sa, na, ra = _accumulate_session(after)

    print("=== BEFORE ===", before.resolve(), f"profile_events={nb}", sep="\n")
    for i, (ph, d) in enumerate(_top3(sb), 1):
        print(
            f"  {i}) phase={ph!r} call_count={d['call_count']} "
            f"total_ms={d['total_ms']:.2f} worst_call_ms={d['worst_call_ms']:.2f}"
        )
    print(f"  {RECON_PHASE} summed total_ms across events: {rb:.2f}")

    print("\n=== AFTER ===", after.resolve(), f"profile_events={na}", sep="\n")
    for i, (ph, d) in enumerate(_top3(sa), 1):
        print(
            f"  {i}) phase={ph!r} call_count={d['call_count']} "
            f"total_ms={d['total_ms']:.2f} worst_call_ms={d['worst_call_ms']:.2f}"
        )
    print(f"  {RECON_PHASE} summed total_ms across events: {ra:.2f}")

    if rb > 0:
        pct = (ra - rb) / rb * 100.0
        print(f"\nReconcile phase total_ms change: {pct:.1f}% (negative = reduction)")
    if nb and na:
        # crude overlay-pass proxy: sum all phase totals per session
        tb = sum(x["total_ms"] for x in sb.values())
        ta = sum(x["total_ms"] for x in sa.values())
        if tb > 0:
            print(f"Sum of all profile phase total_ms (session): before={tb:.2f} after={ta:.2f} ({(ta - tb) / tb * 100:.1f}%)")


if __name__ == "__main__":
    main()
