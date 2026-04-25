"""Run-scoped structured logging for import/normalization/validation/resolution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.environment import EnvironmentValidationReport
from ozlink_console.logger import log_info, log_warn

PipelinePhase = Literal[
    "snapshot_import",
    "normalization",
    "environment_validation",
    "resolution",
    "plan_build",
    "execution_bridge",
    "harness",
]


@dataclass
class SnapshotPipelineEvent:
    """Correlation-ready payload for JSON line logs (``log_info`` data=)."""

    phase: PipelinePhase
    snapshot_id: str
    run_id: str
    adapter_source: str = ""
    import_kind: str = ""
    mapping_id: str = ""
    message: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_log_data(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "pipeline": "draft_snapshot",
            "phase": self.phase,
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
        }
        if self.adapter_source:
            d["adapter_source"] = self.adapter_source
        if self.import_kind:
            d["import_kind"] = self.import_kind
        if self.mapping_id:
            d["mapping_id"] = self.mapping_id
        if self.extra:
            d.update(_stringify_keys(self.extra))
        return d


def _stringify_keys(d: dict[str, Any]) -> dict[str, Any]:
    return {str(k): v for k, v in d.items()}


def log_pipeline_info(ev: SnapshotPipelineEvent) -> None:
    msg = ev.message or f"draft_snapshot.{ev.phase}"
    log_info(msg, **ev.to_log_data())


def log_pipeline_warn(ev: SnapshotPipelineEvent) -> None:
    msg = ev.message or f"draft_snapshot.{ev.phase}"
    log_warn(msg, **ev.to_log_data())


def event_from_detached_import(
    phase: PipelinePhase,
    *,
    snapshot_id: str,
    run_id: str,
    adapter_source: str,
    import_kind: str,
    mapping_id: str = "",
    message: str = "",
    **extra: Any,
) -> SnapshotPipelineEvent:
    return SnapshotPipelineEvent(
        phase=phase,
        snapshot_id=snapshot_id,
        run_id=run_id,
        adapter_source=adapter_source,
        import_kind=import_kind,
        mapping_id=mapping_id,
        message=message,
        extra=dict(extra),
    )


def log_environment_validation_summary(
    *,
    snapshot_id: str,
    run_id: str,
    passed: bool,
    error_count: int,
    warning_count: int,
    adapter_source: str = "",
) -> None:
    ev = SnapshotPipelineEvent(
        phase="environment_validation",
        snapshot_id=snapshot_id,
        run_id=run_id,
        adapter_source=adapter_source,
        message="draft_snapshot.environment_validation.summary",
        extra={
            "passed": passed,
            "error_count": error_count,
            "warning_count": warning_count,
        },
    )
    log_pipeline_info(ev)


def validation_report_as_dict(report: EnvironmentValidationReport) -> dict[str, Any]:
    """Serialize ``EnvironmentValidationReport`` for structured logs."""
    return {
        "passed": report.passed,
        "snapshot_id": report.snapshot_id,
        "run_id": report.run_id,
        "checks": [asdict(c) for c in report.checks],
    }


def log_plan_build_phase(
    *,
    phase: Literal["start", "end"],
    snapshot_id: str,
    run_id: str,
    plan_id: str,
    adapter_source: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Structured plan materialization boundaries (correlation: snapshot_id, run_id, plan_id)."""
    ev = SnapshotPipelineEvent(
        phase="plan_build",
        snapshot_id=snapshot_id,
        run_id=run_id,
        adapter_source=adapter_source,
        mapping_id="",
        message=f"draft_snapshot.plan_build.{phase}",
        extra={
            "plan_build_phase": phase,
            "plan_id": plan_id,
            **(extra or {}),
        },
    )
    log_pipeline_info(ev)


def log_resolution_item_state(*, snapshot_id: str, run_id: str, result: Any, adapter_source: str = "") -> None:
    status = str(getattr(result, "status", "unknown"))
    ev = SnapshotPipelineEvent(
        phase="resolution",
        snapshot_id=snapshot_id,
        run_id=run_id,
        adapter_source=adapter_source,
        mapping_id=str(getattr(result, "mapping_id", "") or ""),
        message="draft_snapshot.resolution.item",
        extra={
            "item_kind": str(getattr(result, "item_kind", "")),
            "item_type": str(getattr(result, "item_type", "")),
            "status": status,
            "item_message": str(getattr(result, "message", "")),
            "unresolved_reasons": list(getattr(result, "unresolved_reasons", []) or []),
            "ambiguous_candidates": list(getattr(result, "ambiguous_candidates", []) or []),
        },
    )
    if status in ("unresolved", "ambiguous"):
        log_pipeline_warn(ev)
    else:
        log_pipeline_info(ev)


def log_execution_bridge_step(
    *,
    snapshot_id: str,
    run_id: str,
    plan_id: str,
    step_id: str,
    mapping_id: str,
    event: str,
    status: str,
    extra: dict[str, Any] | None = None,
    warn: bool = False,
) -> None:
    ev = SnapshotPipelineEvent(
        phase="execution_bridge",
        snapshot_id=snapshot_id,
        run_id=run_id,
        mapping_id=mapping_id,
        message=f"draft_snapshot.execution_bridge.{event}",
        extra={
            "plan_id": plan_id,
            "step_id": step_id,
            "status": status,
            **(extra or {}),
        },
    )
    if warn:
        log_pipeline_warn(ev)
    else:
        log_pipeline_info(ev)


def log_harness_phase(
    *,
    subphase: str,
    snapshot_id: str,
    run_id: str,
    plan_id: str = "",
    adapter_source: str = "",
    import_kind: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Correlated harness orchestration markers (import → … → summary)."""
    ev = SnapshotPipelineEvent(
        phase="harness",
        snapshot_id=snapshot_id,
        run_id=run_id,
        adapter_source=adapter_source,
        import_kind=import_kind,
        mapping_id="",
        message=f"draft_snapshot.harness.{subphase}",
        extra={
            "harness_subphase": subphase,
            "plan_id": plan_id,
            **(extra or {}),
        },
    )
    log_pipeline_info(ev)
