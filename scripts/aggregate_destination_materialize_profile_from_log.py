#!/usr/bin/env python3
"""Aggregate ``destination_materialize_profile`` JSON lines from a real Ozlink session.

**Where logs live (from code):**

* ``logs_root()`` = ``%LOCALAPPDATA%\\OzlinkIT\\OzlinkITSharePointRelocationConsole\\Logs``
* Each run: ``<logs_root>/<YYYY-MM-DD_HH-MM-SS>/``
* **Profile lines** use message ``destination_materialize_profile`` → routed to
  **destination_preview.log** (substring ``destination_materialize`` in
  ``ozlink_console.logger.resolve_log_stream``), not ``app.log``.
* Pointer: ``<logs_root>/CURRENT_SESSION.txt`` → ``session_log_dir=...``

**Required env when capturing:** ``OZLINK_DEST_MATERIALIZE_PROFILE=1``

Usage::

    python scripts/aggregate_destination_materialize_profile_from_log.py "C:\\...\\Logs\\2026-04-13_14-30-00"
    python scripts/aggregate_destination_materialize_profile_from_log.py "C:\\...\\destination_preview.log"
    type destination_preview.log | python scripts/aggregate_destination_materialize_profile_from_log.py -
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _target_paths(argv_path: str | None) -> tuple[list[Path], str]:
    """Return log files to scan and a label for errors."""
    if argv_path is None or argv_path == "-":
        return [], "stdin"
    p = Path(argv_path)
    if p.is_dir():
        files = sorted(p.glob("*.log*"))
        return files, str(p.resolve())
    return [p], str(p.resolve())


def _process_row(
    row: dict,
    phase_total: dict[str, float],
    phase_worst_step_ms: dict[str, float],
    phase_worst_call_ms: dict[str, float],
    phase_invocation_count: dict[str, int],
    profile_events: list[tuple[float, str, list]],
) -> None:
    if row.get("message") != "destination_materialize_profile":
        return
    data = row.get("data") or {}
    reason = str(data.get("materialize_reason") or "")
    steps = data.get("top_steps") or []
    if not isinstance(steps, list):
        return
    line_sum = 0.0
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
            wcm = float(step.get("worst_call_ms") or 0.0)
        except (TypeError, ValueError):
            wcm = 0.0
        try:
            cc = int(step.get("call_count") or 0)
        except (TypeError, ValueError):
            cc = 0
        phase_total[ph] += ms
        phase_worst_step_ms[ph] = max(phase_worst_step_ms[ph], ms)
        phase_worst_call_ms[ph] = max(phase_worst_call_ms[ph], wcm)
        if cc > 0:
            phase_invocation_count[ph] += cc
        line_sum += ms
    if steps:
        profile_events.append((line_sum, reason, steps))


def _scan_file(path: Path, **kwargs) -> None:
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            _process_row(row, **kwargs)


def main() -> None:
    argv = [a for a in sys.argv[1:] if a]
    hint = ""
    try:
        from ozlink_console.paths import logs_root

        hint = f"\nResolved logs_root() from code: {logs_root()}\n"
    except Exception:
        logs_root = None  # type: ignore[assignment]

    path_arg = argv[0] if argv else None
    phase_total: dict[str, float] = defaultdict(float)
    phase_worst_step_ms: dict[str, float] = defaultdict(float)
    phase_worst_call_ms: dict[str, float] = defaultdict(float)
    phase_invocation_count: dict[str, int] = defaultdict(int)
    profile_events: list[tuple[float, str, list]] = []

    if path_arg is None or path_arg == "-":
        for line in sys.stdin.read().splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            _process_row(
                row,
                phase_total=phase_total,
                phase_worst_step_ms=phase_worst_step_ms,
                phase_worst_call_ms=phase_worst_call_ms,
                phase_invocation_count=phase_invocation_count,
                profile_events=profile_events,
            )
        src = "stdin"
    else:
        files, src = _target_paths(path_arg)
        if not files:
            print(f"No *.log* files under directory: {src}", file=sys.stderr)
            sys.exit(2)
        for fp in files:
            _scan_file(
                fp,
                phase_total=phase_total,
                phase_worst_step_ms=phase_worst_step_ms,
                phase_worst_call_ms=phase_worst_call_ms,
                phase_invocation_count=phase_invocation_count,
                profile_events=profile_events,
            )

    if not phase_total:
        print("No destination_materialize_profile entries found.", file=sys.stderr)
        print(f"Source: {src}", file=sys.stderr)
        if hint:
            print(hint.strip(), file=sys.stderr)
        print(
            "Tip: profile lines are in destination_preview.log inside the session folder "
            "(not app.log). Open CURRENT_SESSION.txt under logs_root to find the folder.",
            file=sys.stderr,
        )
        sys.exit(2)

    ranked = sorted(phase_total.items(), key=lambda x: -x[1])
    print("=== Session totals (sum of total_ms per phase across all profile events) ===")
    for ph, tot in ranked[:30]:
        cc = phase_invocation_count.get(ph, 0)
        print(
            f"  {ph}: total_ms={tot:.2f}  "
            f"call_count(sum over events)={cc}  "
            f"max_step_total_ms_on_one_line={phase_worst_step_ms[ph]:.2f}  "
            f"max_worst_call_ms_seen={phase_worst_call_ms[ph]:.2f}"
        )

    print("\n=== Top 3 phases by summed total_ms (required metrics) ===")
    for i, (ph, tot) in enumerate(ranked[:3], start=1):
        cc = phase_invocation_count.get(ph, 0)
        print(
            f"  {i}) phase={ph!r}  call_count={cc}  total_ms={tot:.2f}  "
            f"worst_call_ms(max over events)={phase_worst_call_ms[ph]:.2f}"
        )

    if profile_events:
        profile_events.sort(key=lambda x: -x[0])
        worst_sum, worst_reason, worst_steps = profile_events[0]
        print("\n=== Single worst materialize event (sum of top_steps total_ms on that line) ===")
        print(f"  materialize_reason={worst_reason[:220]!r}")
        print(f"  line_top_steps_sum_ms={worst_sum:.2f}")
        print("  top_steps on that line:")
        for step in worst_steps[:18]:
            if isinstance(step, dict):
                print(
                    f"    {step.get('phase')}: total_ms={step.get('total_ms')} "
                    f"call_count={step.get('call_count')} worst_call_ms={step.get('worst_call_ms')}"
                )


if __name__ == "__main__":
    main()
