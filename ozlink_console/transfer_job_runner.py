"""Run transfer manifests on the local filesystem (Windows paths). Graph execution is not implemented."""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ozlink_console.logger import log_error, log_info

SUPPORTED_MANIFEST_VERSIONS = frozenset({1})


def is_absolute_local_path(path: str) -> bool:
    """True for `C:\\...`, UNC `\\\\server\\share\\...`, or POSIX `/...` (for local runner tests and macOS/Linux)."""
    p = (path or "").strip()
    if len(p) >= 3 and p[1] == ":" and p[2] in "\\/":
        return True
    if p.startswith("\\\\"):
        return True
    if p.startswith("/"):
        return True
    return False


def _norm_path(p: str) -> Path:
    return Path(p.strip().replace("/", "\\"))


def _path_depth_for_sort(path: str) -> int:
    parts = [x for x in _norm_path(path).parts if x]
    return len(parts)


def load_manifest_json(path: str | Path) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    ver = manifest.get("manifest_version")
    if ver not in SUPPORTED_MANIFEST_VERSIONS:
        errors.append(f"Unsupported manifest_version {ver!r}; supported: {sorted(SUPPORTED_MANIFEST_VERSIONS)}")
    if not isinstance(manifest.get("transfer_steps"), list):
        errors.append("transfer_steps must be a list")
    if not isinstance(manifest.get("proposed_folder_steps", []), list):
        errors.append("proposed_folder_steps must be a list")
    return errors


def _step_graph_ready(step: dict[str, Any]) -> bool:
    return bool(
        str(step.get("source_drive_id", "") or "").strip()
        and str(step.get("source_item_id", "") or "").strip()
        and str(step.get("destination_drive_id", "") or "").strip()
    )


def manifest_execution_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    """Classify steps for UI copy (no I/O)."""
    transfer = list(manifest.get("transfer_steps") or [])
    proposed = list(manifest.get("proposed_folder_steps") or [])
    local_transfer = 0
    graph_transfer = 0
    skipped_transfer = 0
    local_mkdir = 0
    skipped_mkdir = 0
    for step in transfer:
        op = str(step.get("operation", "copy")).lower()
        if op != "copy":
            skipped_transfer += 1
            continue
        src = str(step.get("source_path", "") or "")
        dst = str(step.get("destination_path", "") or "")
        if is_absolute_local_path(src) and is_absolute_local_path(dst):
            local_transfer += 1
        elif _step_graph_ready(step):
            graph_transfer += 1
        else:
            skipped_transfer += 1
    for step in proposed:
        if str(step.get("operation", "")).lower() != "ensure_folder":
            skipped_mkdir += 1
            continue
        dst = str(step.get("destination_path", "") or "")
        if is_absolute_local_path(dst):
            local_mkdir += 1
        else:
            skipped_mkdir += 1
    return {
        "transfer_steps_total": len(transfer),
        "proposed_folder_steps_total": len(proposed),
        "local_filesystem_transfer": local_transfer,
        "graph_transfer_pending": graph_transfer,
        "transfer_skipped_non_local": skipped_transfer,
        "local_mkdir": local_mkdir,
        "proposed_skipped_non_local": skipped_mkdir,
    }


@dataclass
class StepRunRecord:
    phase: str
    step_index: int
    status: str
    detail: str = ""
    attempts: int = 0


@dataclass
class RunManifestResult:
    dry_run: bool
    records: list[StepRunRecord] = field(default_factory=list)
    log_path: str | None = None

    def summary_line(self) -> str:
        ok = sum(1 for r in self.records if r.status == "ok")
        skipped = sum(1 for r in self.records if r.status == "skipped")
        failed = sum(1 for r in self.records if r.status == "failed")
        dry = sum(1 for r in self.records if r.status == "dry_run")
        return f"ok={ok} skipped={skipped} failed={failed} dry_run={dry}"


def _retry_call(
    fn: Callable[[], None],
    *,
    max_retries: int,
    base_delay_sec: float,
) -> tuple[int, Exception | None]:
    last_exc: Exception | None = None
    for attempt in range(max(1, int(max_retries))):
        try:
            fn()
            return attempt + 1, None
        except OSError as exc:
            last_exc = exc
            if attempt + 1 >= max_retries:
                break
            time.sleep(base_delay_sec * (2**attempt))
    return max_retries, last_exc


def run_manifest_local_filesystem(
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
    max_retries: int = 3,
    base_delay_sec: float = 0.35,
    on_step: Callable[[StepRunRecord], None] | None = None,
    log_file: str | Path | None = None,
) -> RunManifestResult:
    """
    Execute proposed_folder_steps (mkdir) and transfer_steps (copy file or tree) when paths are
    absolute local paths. Skips Graph-only steps. Folders use shutil.copytree(..., dirs_exist_ok=True).
    """
    errors = validate_manifest(manifest)
    if errors:
        raise ValueError("; ".join(errors))

    records: list[StepRunRecord] = []
    log_lines: list[str] = []

    def emit(rec: StepRunRecord) -> None:
        records.append(rec)
        line = f"{rec.phase}\tidx={rec.step_index}\t{rec.status}\t{rec.detail}"
        log_lines.append(line)
        log_info(
            "transfer_job_step",
            phase=rec.phase,
            step_index=rec.step_index,
            status=rec.status,
            detail=rec.detail[:500],
            attempts=rec.attempts,
        )
        if on_step:
            on_step(rec)

    proposed = sorted(
        list(manifest.get("proposed_folder_steps") or []),
        key=lambda s: _path_depth_for_sort(str(s.get("destination_path", "") or "")),
    )
    for step in proposed:
        idx = int(step.get("index", -1))
        op = str(step.get("operation", "")).lower()
        if op != "ensure_folder":
            emit(StepRunRecord("proposed_folder", idx, "skipped", f"unsupported operation {op!r}"))
            continue
        dst = str(step.get("destination_path", "") or "")
        if not is_absolute_local_path(dst):
            emit(StepRunRecord("proposed_folder", idx, "skipped", "destination is not a local absolute path"))
            continue
        if dry_run:
            emit(StepRunRecord("proposed_folder", idx, "dry_run", f"mkdir {dst}"))
            continue

        def _mkdir() -> None:
            _norm_path(dst).mkdir(parents=True, exist_ok=True)

        attempts, exc = _retry_call(_mkdir, max_retries=max_retries, base_delay_sec=base_delay_sec)
        if exc is not None:
            emit(
                StepRunRecord(
                    "proposed_folder",
                    idx,
                    "failed",
                    f"mkdir {dst}: {exc}",
                    attempts=attempts,
                )
            )
            log_error("transfer_job_mkdir_failed", destination=dst, error=str(exc))
        else:
            emit(StepRunRecord("proposed_folder", idx, "ok", f"mkdir {dst}", attempts=attempts))

    for step in manifest.get("transfer_steps") or []:
        idx = int(step.get("index", -1))
        op = str(step.get("operation", "copy")).lower()
        if op != "copy":
            emit(StepRunRecord("transfer", idx, "skipped", f"unsupported operation {op!r}"))
            continue

        src = str(step.get("source_path", "") or "")
        dst = str(step.get("destination_path", "") or "")
        is_folder = bool(step.get("is_source_folder", False))

        if _step_graph_ready(step) and not (is_absolute_local_path(src) and is_absolute_local_path(dst)):
            emit(
                StepRunRecord(
                    "transfer",
                    idx,
                    "skipped",
                    "Microsoft Graph copy is not implemented in this build (drive/item ids present)",
                )
            )
            continue

        if not is_absolute_local_path(src) or not is_absolute_local_path(dst):
            emit(
                StepRunRecord(
                    "transfer",
                    idx,
                    "skipped",
                    "source or destination is not a local absolute path (SharePoint library paths need Graph executor)",
                )
            )
            continue

        src_p = _norm_path(src)
        dst_p = _norm_path(dst)

        if not src_p.exists():
            emit(StepRunRecord("transfer", idx, "failed", f"source missing: {src_p}"))
            continue

        if dry_run:
            kind = "copytree" if is_folder and src_p.is_dir() else "copy2"
            emit(StepRunRecord("transfer", idx, "dry_run", f"{kind} {src_p} -> {dst_p}"))
            continue

        if is_folder and src_p.is_dir():

            def _tree() -> None:
                dst_p.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(src_p, dst_p, dirs_exist_ok=True)

            attempts, exc = _retry_call(_tree, max_retries=max_retries, base_delay_sec=base_delay_sec)
            if exc is not None:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "failed",
                        f"copytree {src_p} -> {dst_p}: {exc}",
                        attempts=attempts,
                    )
                )
                log_error("transfer_job_copytree_failed", source=str(src_p), dest=str(dst_p), error=str(exc))
            else:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "ok",
                        f"copytree {src_p} -> {dst_p}",
                        attempts=attempts,
                    )
                )
        elif src_p.is_file():

            def _file() -> None:
                dst_p.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_p, dst_p)

            attempts, exc = _retry_call(_file, max_retries=max_retries, base_delay_sec=base_delay_sec)
            if exc is not None:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "failed",
                        f"copy2 {src_p} -> {dst_p}: {exc}",
                        attempts=attempts,
                    )
                )
                log_error("transfer_job_copy2_failed", source=str(src_p), dest=str(dst_p), error=str(exc))
            else:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "ok",
                        f"copy2 {src_p} -> {dst_p}",
                        attempts=attempts,
                    )
                )
        else:
            emit(StepRunRecord("transfer", idx, "skipped", f"source is not a file or directory: {src_p}"))

    log_path: str | None = None
    if log_file:
        lp = Path(log_file)
        lp.parent.mkdir(parents=True, exist_ok=True)
        lp.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log_path = str(lp)

    return RunManifestResult(dry_run=dry_run, records=records, log_path=log_path)
