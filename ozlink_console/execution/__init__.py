"""Execution orchestration package (Phase 1 shell)."""

from __future__ import annotations

from ozlink_console.execution.orchestrator import ExecutionOrchestrator
from ozlink_console.execution.run_context import RunContext
from ozlink_console.execution.snapshot_summary import (
    SnapshotRunResultSummary,
    build_snapshot_run_result_summary,
    snapshot_internal_comparison_record,
)
from ozlink_console.execution.stop_reason import ExecutionStopReason
from ozlink_console.execution.validation_capture import (
    INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION,
    InternalValidationRunRecord,
    append_internal_validation_jsonl,
    build_internal_validation_run_record_from_snapshot_summary,
    maybe_append_snapshot_validation_capture,
)

__all__ = [
    "INTERNAL_VALIDATION_RECORD_SCHEMA_VERSION",
    "ExecutionOrchestrator",
    "ExecutionStopReason",
    "InternalValidationRunRecord",
    "RunContext",
    "SnapshotRunResultSummary",
    "append_internal_validation_jsonl",
    "build_internal_validation_run_record_from_snapshot_summary",
    "build_snapshot_run_result_summary",
    "maybe_append_snapshot_validation_capture",
    "snapshot_internal_comparison_record",
]
