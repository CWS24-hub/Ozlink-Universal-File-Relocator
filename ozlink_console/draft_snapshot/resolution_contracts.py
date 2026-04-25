"""Resolved snapshot contracts for ID resolution readiness (no execution wiring)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.contracts import CanonicalSubmittedSnapshot, ItemType

ResolutionState = Literal["resolved", "unresolved", "ambiguous", "skipped"]
ResolutionItemKind = Literal["mapping_item", "proposed_folder"]


@dataclass
class ItemResolutionResult:
    item_kind: ResolutionItemKind
    mapping_id: str
    item_type: ItemType | Literal["folder"]
    status: ResolutionState
    message: str = ""
    source_drive_id: str = ""
    source_item_id: str = ""
    destination_drive_id: str = ""
    destination_parent_item_id: str = ""
    destination_name: str = ""
    unresolved_reasons: list[str] = field(default_factory=list)
    ambiguous_candidates: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolutionSummary:
    total_items: int = 0
    resolved_count: int = 0
    unresolved_count: int = 0
    ambiguous_count: int = 0
    skipped_count: int = 0
    unresolved_mapping_ids: list[str] = field(default_factory=list)
    ambiguous_mapping_ids: list[str] = field(default_factory=list)


@dataclass
class ResolvedSnapshot:
    """Canonical submitted snapshot + structured resolution outcomes."""

    snapshot: CanonicalSubmittedSnapshot
    run_id: str
    resolver_name: str
    summary: ResolutionSummary
    mapping_results: list[ItemResolutionResult] = field(default_factory=list)
    proposed_folder_results: list[ItemResolutionResult] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def snapshot_id(self) -> str:
        return self.snapshot.snapshot_id

    def unresolved(self) -> list[ItemResolutionResult]:
        return [x for x in (*self.mapping_results, *self.proposed_folder_results) if x.status == "unresolved"]

    def ambiguous(self) -> list[ItemResolutionResult]:
        return [x for x in (*self.mapping_results, *self.proposed_folder_results) if x.status == "ambiguous"]


def summarize_resolution(
    mapping_results: list[ItemResolutionResult],
    proposed_results: list[ItemResolutionResult],
) -> ResolutionSummary:
    all_results = [*mapping_results, *proposed_results]
    summary = ResolutionSummary(total_items=len(all_results))
    for r in all_results:
        if r.status == "resolved":
            summary.resolved_count += 1
        elif r.status == "unresolved":
            summary.unresolved_count += 1
            summary.unresolved_mapping_ids.append(r.mapping_id)
        elif r.status == "ambiguous":
            summary.ambiguous_count += 1
            summary.ambiguous_mapping_ids.append(r.mapping_id)
        elif r.status == "skipped":
            summary.skipped_count += 1
    return summary


def resolution_result_as_dict(result: ItemResolutionResult) -> dict[str, Any]:
    return asdict(result)

