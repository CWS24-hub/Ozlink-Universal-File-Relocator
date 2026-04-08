from __future__ import annotations

from ozlink_console.execution.boundary_vocabulary import (
    BRIDGE_OUTCOME_UNSPECIFIED,
    ENVIRONMENT_VALIDATION,
    normalize_boundary_detail,
)


def test_normalize_alias_bridge_execution():
    assert normalize_boundary_detail("bridge_execution") == BRIDGE_OUTCOME_UNSPECIFIED


def test_normalize_empty_bridge_uses_unspecified():
    assert normalize_boundary_detail("", failure_boundary="bridge") == BRIDGE_OUTCOME_UNSPECIFIED


def test_normalize_stopped_at_fallback():
    assert normalize_boundary_detail("", stopped_at="environment_validation") == ENVIRONMENT_VALIDATION
