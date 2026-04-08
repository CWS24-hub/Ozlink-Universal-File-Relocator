"""
Destination tree scroll-window profiler (optional diagnostic).

Controlled at runtime via MainWindow._dest_scroll_profile_enabled (Developer menu
checkbox; persisted in QSettings under ``debug/dest_scroll_profile``).

Optional environment override: ``OZLINK_DEST_SCROLL_PROFILE=0`` (or ``false`` /
``off``) forces profiling off; ``1`` / ``true`` forces it on. When unset, only
the UI flag (and QSettings) apply.

Emits structured log lines via ozlink_console.logger.log_info when each scroll
window closes (~150 ms idle after last scroll-related event), and appends the
same payload as one JSON line per event to ``dest_scroll_profile.log`` in the
session log directory (singleton rotating handler).

Removable: delete this module and strip _dest_scroll_profiler hooks from main_window.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from typing import Any, Callable, Optional

from PySide6.QtCore import QTimer

from ozlink_console.logger import log_dest_scroll_profile_json, log_info

_ENV_ON = frozenset({"1", "true", "yes"})
_ENV_OFF = frozenset({"0", "false", "no", "off"})


def create_dest_scroll_profiler(main_window, *, enabled_fn: Optional[Callable[[], bool]] = None):
    """Always returns a profiler instance; gating is via ``enabled_fn`` (and env override)."""
    return DestScrollProfiler(main_window, enabled_fn=enabled_fn)


class DestScrollProfiler:
    """One active scroll window: first scroll pulse until IDLE_MS without another pulse."""

    IDLE_MS = 150
    MAX_TIMELINE = 500

    def __init__(self, main_window, *, enabled_fn: Optional[Callable[[], bool]] = None) -> None:
        self._mw = main_window
        self._enabled_fn = enabled_fn
        self._idle = QTimer(main_window)
        self._idle.setSingleShot(True)
        self._idle.timeout.connect(self._flush_window)
        self._capturing = False
        self._window_t0_perf: float | None = None
        self._window_t0_wall: float = 0.0
        # (category, name) -> count, total_ms, max_ms
        self._by_key: dict[tuple[str, str], dict[str, float | int]] = defaultdict(
            lambda: {"count": 0, "total_ms": 0.0, "max_ms": 0.0}
        )
        self._cat_totals: dict[str, dict[str, float | int]] = defaultdict(
            lambda: {"count": 0, "total_ms": 0.0, "max_ms": 0.0}
        )
        self._timeline: list[dict[str, Any]] = []
        self._scroll_pulse_labels: list[str] = []

    def _active(self) -> bool:
        v = os.environ.get("OZLINK_DEST_SCROLL_PROFILE", "").strip().lower()
        if v in _ENV_OFF:
            return False
        if v in _ENV_ON:
            return True
        if self._enabled_fn is None:
            return False
        try:
            return bool(self._enabled_fn())
        except Exception:
            return False

    def install_on_destination_tree(self, tree, model) -> None:
        """Connect scrollbar + model signals (after setModel)."""
        try:
            vsb = tree.verticalScrollBar()
            vsb.valueChanged.connect(lambda *_: self.note_scroll("scrollbar_valueChanged"))
        except Exception:
            pass
        try:
            hsb = tree.horizontalScrollBar()
            hsb.valueChanged.connect(lambda *_: self.note_scroll("h_scrollbar_valueChanged"))
        except Exception:
            pass
        if model is None:
            return
        try:

            def _rows_in(name: str, parent, first: int, last: int) -> None:
                n = max(0, int(last) - int(first) + 1)
                self.note_model_signal(name, row_span=n)

            model.rowsInserted.connect(lambda p, a, b, n="rowsInserted": _rows_in(n, p, a, b))
            model.rowsRemoved.connect(lambda p, a, b, n="rowsRemoved": _rows_in(n, p, a, b))
        except Exception:
            pass
        try:
            model.layoutChanged.connect(lambda: self.note_model_signal("layoutChanged"))
        except Exception:
            pass
        try:
            model.modelReset.connect(lambda: self.note_model_signal("modelReset"))
        except Exception:
            pass
        # Intentionally omit QAbstractItemModel.dataChanged: can fire per-cell and overwhelm the timeline.
        try:
            sig = getattr(model, "destination_structure_changed", None)
            if sig is not None:
                sig.connect(lambda: self.note_model_signal("destination_structure_changed"))
        except Exception:
            pass

    def note_scroll(self, kind: str) -> None:
        if not self._active():
            return
        now = time.perf_counter()
        if not self._capturing:
            self._capturing = True
            self._window_t0_perf = now
            self._window_t0_wall = time.time()
            self._scroll_pulse_labels = [kind]
            self._timeline.append({"t_ms": 0.0, "kind": "window_open", "detail": kind})
        else:
            self._scroll_pulse_labels.append(kind)
            base = self._window_t0_perf or now
            self._timeline.append({"t_ms": (now - base) * 1000.0, "kind": "scroll_pulse", "detail": kind})
        self._idle.start(self.IDLE_MS)

    def note_model_signal(self, name: str, *, row_span: int = 1) -> None:
        if not self._active():
            return
        if not self._capturing:
            return
        key = ("model_signals", name)
        b = self._by_key[key]
        b["count"] = int(b["count"]) + 1
        ct = self._cat_totals["model_signals"]
        ct["count"] = int(ct["count"]) + 1
        base = self._window_t0_perf or time.perf_counter()
        self._timeline.append(
            {
                "t_ms": (time.perf_counter() - base) * 1000.0,
                "kind": "model_signals",
                "detail": name,
                "row_span": row_span,
            }
        )

    def record(self, category: str, name: str, elapsed_sec: float) -> None:
        if not self._active():
            return
        if not self._capturing:
            return
        ms = float(elapsed_sec) * 1000.0
        key = (category, name)
        b = self._by_key[key]
        b["count"] = int(b["count"]) + 1
        b["total_ms"] = float(b["total_ms"]) + ms
        b["max_ms"] = max(float(b["max_ms"]), ms)
        ct = self._cat_totals[category]
        ct["count"] = int(ct["count"]) + 1
        ct["total_ms"] = float(ct["total_ms"]) + ms
        ct["max_ms"] = max(float(ct["max_ms"]), ms)
        base = self._window_t0_perf or time.perf_counter()
        if len(self._timeline) < self.MAX_TIMELINE:
            self._timeline.append(
                {
                    "t_ms": (time.perf_counter() - base) * 1000.0,
                    "kind": category,
                    "detail": name,
                    "ms": round(ms, 3),
                }
            )

    def _flush_window(self) -> None:
        if not self._capturing or self._window_t0_perf is None:
            return
        if not self._active():
            self._idle.stop()
            self._capturing = False
            self._window_t0_perf = None
            self._by_key.clear()
            self._cat_totals.clear()
            self._timeline.clear()
            self._scroll_pulse_labels.clear()
            return
        t1 = time.perf_counter()
        wall_ms = (t1 - self._window_t0_perf) * 1000.0
        # Top keys by total_ms
        ranked = sorted(
            self._by_key.items(),
            key=lambda kv: float(kv[1]["total_ms"]),
            reverse=True,
        )[:12]
        top5 = [
            {"category": k[0], "name": k[1], **{a: round(float(v), 3) if a != "count" else int(v) for a, v in d.items()}}
            for k, d in ranked[:5]
        ]
        cat_summary = {
            c: {
                "count": int(v["count"]),
                "total_ms": round(float(v["total_ms"]), 3),
                "max_ms": round(float(v["max_ms"]), 3),
            }
            for c, v in self._cat_totals.items()
        }
        sum_tracked = sum(float(v["total_ms"]) for v in self._cat_totals.values())
        pct_note = {}
        if wall_ms > 0:
            for c, v in self._cat_totals.items():
                pct_note[c] = round(100.0 * float(v["total_ms"]) / wall_ms, 2)
        timeline_trim = self._timeline[:80]
        payload = {
            "event": "dest_scroll_profile_window",
            "wall_ms": round(wall_ms, 3),
            "window_wall_start_unix": self._window_t0_wall,
            "scroll_pulses": len(self._scroll_pulse_labels),
            "scroll_pulse_kinds_sample": self._scroll_pulse_labels[:24],
            "category_totals": cat_summary,
            "pct_of_wall_ms_overlap_note": pct_note,
            "sum_tracked_ms": round(sum_tracked, 3),
            "overlap_warning": "Percents sum nested/overlapping work; can exceed 100% of wall_ms.",
            "top_hotspots": top5[:5],
            "timeline_sample_json": json.dumps(timeline_trim, separators=(",", ":")),
        }
        log_info("dest_scroll_profile_window", **payload)
        log_dest_scroll_profile_json(payload)

        self._capturing = False
        self._window_t0_perf = None
        self._by_key.clear()
        self._cat_totals.clear()
        self._timeline.clear()
        self._scroll_pulse_labels.clear()
