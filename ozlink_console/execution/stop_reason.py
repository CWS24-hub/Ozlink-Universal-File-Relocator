"""Controlled stop reasons for execution orchestration (no free-text stop reasons)."""

from __future__ import annotations

from enum import Enum


class ExecutionStopReason(str, Enum):
    """Finite set of orchestrator-level stop reasons."""

    SNAPSHOT_PIPELINE_NOT_IMPLEMENTED = "snapshot_pipeline_not_implemented"
    SNAPSHOT_PIPELINE_EXCEPTION = "snapshot_pipeline_exception"
    ENVIRONMENT_VALIDATION_FAILED = "environment_validation_failed"
    GRAPH_CLIENT_REQUIRED = "graph_client_required"
    RESOLUTION_BLOCKERS = "resolution_blockers"
    PLAN_BUILD_FAILED = "plan_build_failed"
    PLAN_OVERRIDE_MISMATCH = "plan_override_mismatch"
    BRIDGE_EXECUTION_FAILED = "bridge_execution_failed"
    BRIDGE_STEP_FAILED = "bridge_step_failed"
    BRIDGE_COMPATIBILITY_BLOCKED = "bridge_compatibility_blocked"
