"""Non-UI helpers to summarize migration reports for import dialogs."""

from __future__ import annotations

from typing import Any


def build_migration_report_summary_lines(report: dict[str, Any] | None) -> list[str]:
    """Human-readable lines for a LegacyMigrationReport.json dict (or engine report)."""
    if not report or not isinstance(report, dict):
        return ["(No migration report data.)"]
    counts = report.get("counts")
    if not isinstance(counts, dict):
        counts = {}
    ai = counts.get("allocations_input", "—")
    pi = counts.get("proposed_input", "—")
    lines = [
        f"Allocations (input): {ai}",
        f"Allocations — source Graph resolved / unresolved: {counts.get('graph_resolved_alloc', '—')} / {counts.get('graph_unresolved_alloc', '—')}",
        f"Proposed folders (input): {pi}",
        f"Proposed — Graph resolved / unresolved: {counts.get('graph_resolved_prop', '—')} / {counts.get('graph_unresolved_prop', '—')}",
        f"Destination parent Graph resolved (alloc): {counts.get('allocation_parent_graph_resolved', '—')}",
        f"Destination planned-parent resolved (alloc): {counts.get('allocation_planned_parent_resolved', '—')}",
        f"Unresolved after planned lookup: {counts.get('allocation_parent_unresolved_after_planned_lookup', '—')}",
        f"Live duplicate / conflict detections: {counts.get('live_duplicate_detected', '—')}",
        f"Rows rejected: {counts.get('rows_rejected', '—')}",
    ]
    if counts.get("skip_graph_resolution") is True:
        lines.append("Graph resolution was OFF for this migration (offline/skip mode).")
    out_dir = report.get("output_folder") or report.get("migrated_utc")
    if out_dir:
        lines.append(f"Output folder: {out_dir}")
    return lines


def build_migration_report_summary_text(report: dict[str, Any] | None) -> str:
    return "\n".join(build_migration_report_summary_lines(report))
