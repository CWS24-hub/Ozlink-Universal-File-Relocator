"""Run-scoped structured logging for snapshot import / normalization / environment validation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.environment import EnvironmentValidationReport
from ozlink_console.logger import log_info, log_warn

PipelinePhase = Literal["snapshot_import", "normalization", "environment_validation"]


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
