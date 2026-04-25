"""Execution plan builder interface contract (not wired into execution)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ozlink_console.draft_snapshot.execution_plan_contracts import ExecutionPlan, ExecutionPolicy, default_materialization_policy
from ozlink_console.draft_snapshot.resolution_contracts import ResolvedSnapshot
from ozlink_console.draft_snapshot.resolver_service import GraphResolutionClient


@dataclass
class BuildExecutionPlanRequest:
    resolved_snapshot: ResolvedSnapshot
    run_id: str
    policy: ExecutionPolicy = field(default_factory=default_materialization_policy)
    graph_client: GraphResolutionClient | None = None


class ExecutionPlanBuilder(Protocol):
    def build_plan(self, request: BuildExecutionPlanRequest) -> ExecutionPlan: ...


class ContractOnlyExecutionPlanBuilder:
    """
    Contract/stub implementation used in this phase.
    Real planning logic is intentionally deferred.
    """

    def build_plan(self, request: BuildExecutionPlanRequest) -> ExecutionPlan:
        raise NotImplementedError("Execution plan building is not wired in this phase.")

