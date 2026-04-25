"""Canonical tokens for snapshot failure surfaces (internal validation / logs only)."""

from __future__ import annotations

# --- boundary_detail: single source of truth (harness + orchestrator must use these) ---

ENVIRONMENT_VALIDATION = "environment_validation"
GRAPH_CLIENT_REQUIRED = "graph_client_required"
PLAN_OVERRIDE_MISMATCH = "plan_override_mismatch"
RESOLUTION_UNRESOLVED_OR_AMBIGUOUS = "unresolved_or_ambiguous"
PLAN_BUILD_EXCEPTION = "plan_build_exception"
BRIDGE_STEP_FAILED = "bridge_step_failed"
BRIDGE_COMPATIBILITY_BLOCKED = "bridge_compatibility_blocked"
RUNNER_UNCAUGHT_EXCEPTION = "runner_uncaught_exception"
# Fallback when bridge failed but detail was not set (older harness / callers)
BRIDGE_OUTCOME_UNSPECIFIED = "bridge_outcome_unspecified"

CANONICAL_BOUNDARY_DETAILS: frozenset[str] = frozenset(
    {
        ENVIRONMENT_VALIDATION,
        GRAPH_CLIENT_REQUIRED,
        PLAN_OVERRIDE_MISMATCH,
        RESOLUTION_UNRESOLVED_OR_AMBIGUOUS,
        PLAN_BUILD_EXCEPTION,
        BRIDGE_STEP_FAILED,
        BRIDGE_COMPATIBILITY_BLOCKED,
        RUNNER_UNCAUGHT_EXCEPTION,
        BRIDGE_OUTCOME_UNSPECIFIED,
    }
)

# Inbound aliases from older logs or fallbacks → canonical token
_BOUNDARY_DETAIL_ALIASES: dict[str, str] = {
    "bridge_execution": BRIDGE_OUTCOME_UNSPECIFIED,
}

# When stopped_at is set but boundary_detail is missing (legacy results)
_STOPPED_AT_DEFAULT_DETAIL: dict[str, str] = {
    "environment_validation": ENVIRONMENT_VALIDATION,
    "graph_client_required": GRAPH_CLIENT_REQUIRED,
    "resolution": RESOLUTION_UNRESOLVED_OR_AMBIGUOUS,
    "plan_build": PLAN_BUILD_EXCEPTION,
    "plan_override_mismatch": PLAN_OVERRIDE_MISMATCH,
}


def normalize_boundary_detail(
    raw: str,
    *,
    stopped_at: str = "",
    failure_boundary: str = "",
) -> str:
    """Coalesce aliases, empty fallbacks, and legacy ``stopped_at`` echoes into canonical tokens."""
    s = (raw or "").strip()
    if s in CANONICAL_BOUNDARY_DETAILS:
        return s
    if s in _BOUNDARY_DETAIL_ALIASES:
        return _BOUNDARY_DETAIL_ALIASES[s]
    st = (stopped_at or "").strip()
    if not s and st:
        return _STOPPED_AT_DEFAULT_DETAIL.get(st, st)
    if not s and failure_boundary == "bridge":
        return BRIDGE_OUTCOME_UNSPECIFIED
    return s


def internal_outcome_phrase(
    *,
    final_status: str,
    failure_boundary: str,
    boundary_detail: str,
    stopped_at: str,
) -> str:
    """Short human-readable clause for status labels (internal operators)."""
    if final_status == "completed":
        return "completed OK"
    if final_status == "runner_failed":
        return "uncaught exception in pipeline (see exception_type in logs)"
    if failure_boundary == "environment":
        if boundary_detail == GRAPH_CLIENT_REQUIRED:
            return "blocked: not signed in or no Graph client (environment)"
        return "blocked: environment / tenant validation"
    if failure_boundary == "resolution":
        return "blocked: unresolved or ambiguous resolution"
    if failure_boundary == "plan_build":
        if boundary_detail == PLAN_OVERRIDE_MISMATCH:
            return "blocked: plan override mismatch"
        return "blocked: plan build / materialization"
    if failure_boundary == "bridge":
        if boundary_detail == BRIDGE_COMPATIBILITY_BLOCKED:
            return "failed: bridge compatibility guard"
        if boundary_detail == BRIDGE_STEP_FAILED:
            return "failed: bridge step error"
        return "failed: bridge execution"
    if stopped_at:
        return f"stopped at {stopped_at}"
    return f"status={final_status}"
