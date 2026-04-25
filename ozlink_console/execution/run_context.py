"""Run context created at execution start and passed through orchestration layers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

ExecutionKind = Literal["manifest", "snapshot_pipeline"]
SourceOfTruth = Literal["manifest", "execution_plan"]


@dataclass
class RunContext:
    run_id: str
    execution_kind: ExecutionKind
    source_of_truth: SourceOfTruth
    snapshot_id: str | None
    plan_id: str | None
    environment_context: dict[str, Any]
    dry_run: bool
    correlation: dict[str, Any]
    current_phase: str
    started_at: datetime
