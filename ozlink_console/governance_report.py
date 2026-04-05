"""Structured job report for manifests (integrity summary; suitable for future SaaS upload)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ozlink_console.transfer_job_runner import StepRunRecord


@dataclass
class JobReport:
    schema: str
    job_id: str
    completed_at_utc: str
    dry_run: bool
    manifest_version: Any
    summary: dict[str, int]
    steps: list[dict[str, Any]] = field(default_factory=list)

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def _record_to_step_dict(rec: StepRunRecord) -> dict[str, Any]:
    d: dict[str, Any] = {
        "phase": rec.phase,
        "step_index": rec.step_index,
        "status": rec.status,
        "detail": rec.detail,
        "attempts": rec.attempts,
    }
    if rec.source_sha256:
        d["source_sha256"] = rec.source_sha256
    if rec.dest_sha256:
        d["dest_sha256"] = rec.dest_sha256
    if rec.integrity_verified is not None:
        d["integrity_verified"] = rec.integrity_verified
    return d


def build_job_report(
    *,
    job_id: str,
    manifest: dict[str, Any],
    records: list[StepRunRecord],
    dry_run: bool,
) -> dict[str, Any]:
    ok = sum(1 for r in records if r.status == "ok")
    skipped = sum(1 for r in records if r.status == "skipped")
    failed = sum(1 for r in records if r.status == "failed")
    dry = sum(1 for r in records if r.status == "dry_run")
    verified = sum(
        1 for r in records if r.integrity_verified is True
    )
    failed_integrity = sum(
        1 for r in records if r.integrity_verified is False
    )
    report = JobReport(
        schema="ozlink.job_report/v1",
        job_id=job_id,
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
        dry_run=dry_run,
        manifest_version=manifest.get("manifest_version"),
        summary={
            "ok": ok,
            "skipped": skipped,
            "failed": failed,
            "dry_run": dry,
            "integrity_verified": verified,
            "integrity_failed": failed_integrity,
        },
        steps=[_record_to_step_dict(r) for r in records],
    )
    return report.to_json_dict()


def write_job_report_json(path: str | Path, report: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
