"""Run transfer manifests: local filesystem paths and optional Microsoft Graph (SharePoint) copy/create."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import requests

from ozlink_console.audit_log import append_audit_event
from ozlink_console.integrity import verify_copied_file, verify_copied_tree
from ozlink_console.logger import flush_logger, log_error, log_info

SUPPORTED_MANIFEST_VERSIONS = frozenset({1, 2})


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


def _graph_async_monitor_auth_poll_failure(exc: BaseException) -> bool:
    """True when the async copy monitor poll failed with HTTP 401 (copy may still have completed)."""
    return isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code == 401


def _verify_graph_copy_destination_item_present(
    graph_client: Any,
    step: dict[str, Any],
    dest_name: str,
) -> tuple[bool, str]:
    """
    Best-effort check that the destination driveItem exists at the planned path after a monitor error.
    Used only to avoid false failures when Graph accepted the copy (202) but monitor GET returned 401.
    """
    get_by_path = getattr(graph_client, "get_drive_item_by_path", None)
    if not callable(get_by_path):
        return False, "no_get_drive_item_by_path"
    drive_id = str(step.get("destination_drive_id", "") or "").strip()
    dest_path = str(step.get("destination_path", "") or "").strip().replace("\\", "/")
    name = str(dest_name or "").strip()
    if not drive_id or not dest_path:
        return False, "missing_destination_drive_or_path"

    hit = get_by_path(drive_id, dest_path)
    if isinstance(hit, dict) and str(hit.get("id") or "").strip():
        resolved_name = str(hit.get("name") or "").strip()
        if name and resolved_name and resolved_name.lower() != name.lower():
            return False, f"name_mismatch_resolved={resolved_name!r}_expected={name!r}"
        return True, "destination_path"

    if name:
        parent = dest_path.rstrip("/")
        if parent:
            combined = f"{parent}/{name}".replace("//", "/")
            hit2 = get_by_path(drive_id, combined)
            if isinstance(hit2, dict) and str(hit2.get("id") or "").strip():
                resolved_name = str(hit2.get("name") or "").strip()
                if name and resolved_name and resolved_name.lower() != name.lower():
                    return False, f"name_mismatch_resolved={resolved_name!r}_expected={name!r}"
                return True, "parent_plus_destination_name"

    return False, "destination_item_not_found"


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
    """True when manifest has Graph ids for a driveItem copy (destination_item_id = parent folder)."""
    return bool(
        str(step.get("source_drive_id", "") or "").strip()
        and str(step.get("source_item_id", "") or "").strip()
        and str(step.get("destination_drive_id", "") or "").strip()
        and str(step.get("destination_item_id", "") or "").strip()
    )


def _proposed_graph_ready(step: dict[str, Any]) -> bool:
    return bool(
        str(step.get("folder_name", "") or "").strip()
        and str(step.get("destination_drive_id", "") or "").strip()
        and str(step.get("destination_parent_item_id", "") or "").strip()
    )


def manifest_execution_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    """Classify steps for UI copy (no I/O)."""
    transfer = list(manifest.get("transfer_steps") or [])
    proposed = list(manifest.get("proposed_folder_steps") or [])
    local_transfer = 0
    graph_transfer = 0
    skipped_transfer = 0
    local_mkdir = 0
    graph_mkdir = 0
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
        if _proposed_graph_ready(step):
            graph_mkdir += 1
        elif is_absolute_local_path(dst):
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
        "graph_folder_create": graph_mkdir,
        "proposed_skipped_non_local": skipped_mkdir,
    }


@dataclass
class StepRunRecord:
    phase: str
    step_index: int
    status: str
    detail: str = ""
    attempts: int = 0
    source_sha256: str = ""
    dest_sha256: str = ""
    integrity_verified: bool | None = None


@dataclass
class RunManifestResult:
    dry_run: bool
    records: list[StepRunRecord] = field(default_factory=list)
    log_path: str | None = None
    job_id: str = ""
    job_report_path: str | None = None

    def summary_line(self) -> str:
        ok = sum(1 for r in self.records if r.status == "ok")
        skipped = sum(1 for r in self.records if r.status == "skipped")
        failed = sum(1 for r in self.records if r.status == "failed")
        dry = sum(1 for r in self.records if r.status == "dry_run")
        v = sum(1 for r in self.records if r.integrity_verified is True)
        vi = sum(1 for r in self.records if r.integrity_verified is False)
        base = f"ok={ok} skipped={skipped} failed={failed} dry_run={dry}"
        if v or vi:
            base += f" integrity_ok={v} integrity_failed={vi}"
        return base


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
    audit_file: str | Path | None = None,
    job_report_file: str | Path | None = None,
    job_id: str | None = None,
    verify_integrity: bool | None = None,
    graph_client: Any | None = None,
) -> RunManifestResult:
    """
    Execute proposed_folder_steps and transfer_steps.

    * **Local paths** (``C:\\``, UNC, POSIX ``/``): mkdir and shutil copies; optional integrity checks.
    * **Graph steps** (when ``graph_client`` is set): copy driveItems between libraries/sites using
      manifest drive/item ids; create proposed folders via Graph when the manifest includes
      ``destination_drive_id`` and ``destination_parent_item_id``.

    Without ``graph_client``, Graph-eligible steps are skipped with a clear log line.

    **Pilot / partial live test** (``execution_options``): set ``pilot_max_graph_operations`` to a
    positive integer **only for non-dry-run** jobs to cap how many Graph **mutations** run (each
    proposed-folder create and each Graph copy counts as one). Remaining Graph steps are recorded as
    ``skipped`` with a clear reason. Dry runs ignore this limit so previews stay complete.

    Governance: optional ``audit_file`` JSONL and ``job_report_file`` JSON (``ozlink.job_report/v1``).
    """
    from ozlink_console.governance_report import build_job_report, write_job_report_json

    errors = validate_manifest(manifest)
    if errors:
        raise ValueError("; ".join(errors))

    opts: dict[str, Any] = dict(manifest.get("execution_options") or {})
    do_verify = bool(opts.get("verify_integrity", True))
    if verify_integrity is not None:
        do_verify = bool(verify_integrity)
    file_graph_timeout = float(opts.get("graph_copy_file_timeout_sec", 600))
    folder_graph_timeout = float(opts.get("graph_copy_folder_timeout_sec", 7200))
    graph_conflict = str(opts.get("graph_copy_conflict_behavior", "rename") or "rename")
    pilot_max_graph_ops = int(opts.get("pilot_max_graph_operations") or 0)
    pilot_proposed_folder_name = str(opts.get("pilot_proposed_folder_name") or "").strip()
    pilot_proposed_folder_destination_paths = set(
        str(x).strip()
        for x in (opts.get("pilot_proposed_folder_destination_paths") or [])
        if str(x).strip()
    )
    pilot_transfer_destination_keys = set(
        str(x).strip()
        for x in (opts.get("pilot_transfer_destination_keys") or [])
        if str(x).strip()
    )
    pilot_transfer_step_uids = set(
        str(x).strip()
        for x in (opts.get("pilot_transfer_step_uids") or [])
        if str(x).strip()
    )
    pilot_transfer_step_indices = set(
        int(x)
        for x in (opts.get("pilot_transfer_step_indices") or [])
        if str(x).strip()
    )
    pilot_transfer_destination_paths = set(
        str(x).strip()
        for x in (opts.get("pilot_transfer_destination_paths") or [])
        if str(x).strip()
    )
    if dry_run:
        pilot_max_graph_ops = 0
    graph_ops_used = 0

    def _pilot_allows_graph_mutation() -> bool:
        if pilot_max_graph_ops <= 0:
            return True
        return graph_ops_used < pilot_max_graph_ops

    if audit_file is None and opts.get("audit_jsonl_path"):
        audit_file = opts["audit_jsonl_path"]
    if job_report_file is None and opts.get("job_report_path"):
        job_report_file = opts["job_report_path"]
    lp_log = Path(log_file) if log_file else None
    if lp_log is not None:
        if audit_file is None:
            audit_file = str(lp_log.with_suffix(".audit.jsonl"))
        if job_report_file is None:
            job_report_file = str(lp_log.parent / f"{lp_log.stem}_report.json")
    jid = str(job_id or opts.get("job_id") or "").strip() or uuid.uuid4().hex

    records: list[StepRunRecord] = []
    log_lines: list[str] = []

    append_audit_event(
        audit_file,
        job_id=jid,
        event_type="job_started",
        payload={
            "dry_run": dry_run,
            "verify_integrity": do_verify,
            "pilot_max_graph_operations": pilot_max_graph_ops or None,
        },
    )

    def emit(rec: StepRunRecord) -> None:
        records.append(rec)
        extra = ""
        if rec.source_sha256:
            extra += f"\tsha_src={rec.source_sha256[:16]}…"
        if rec.dest_sha256:
            extra += f"\tsha_dst={rec.dest_sha256[:16]}…"
        if rec.integrity_verified is not None:
            extra += f"\tintegrity={'ok' if rec.integrity_verified else 'fail'}"
        line = f"{rec.phase}\tidx={rec.step_index}\t{rec.status}\t{rec.detail}{extra}"
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
        append_audit_event(
            audit_file,
            job_id=jid,
            event_type="step",
            payload={
                "phase": rec.phase,
                "step_index": rec.step_index,
                "status": rec.status,
                "integrity_verified": rec.integrity_verified,
            },
        )

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
        fname = str(step.get("folder_name", "") or "").strip()
        d_drive = str(step.get("destination_drive_id", "") or "").strip()
        d_parent = str(step.get("destination_parent_item_id", "") or "").strip()

        if pilot_proposed_folder_destination_paths and dst.strip() not in pilot_proposed_folder_destination_paths:
            emit(
                StepRunRecord(
                    "proposed_folder",
                    idx,
                    "skipped",
                    f"pilot_proposed_folder_destination_paths filter: destination_path {dst!r} not selected",
                )
            )
            continue

        if pilot_proposed_folder_name and fname != pilot_proposed_folder_name:
            emit(
                StepRunRecord(
                    "proposed_folder",
                    idx,
                    "skipped",
                    f"pilot_proposed_folder_name filter: {fname!r} != {pilot_proposed_folder_name!r}",
                )
            )
            continue

        if _proposed_graph_ready(step):
            if graph_client is None:
                emit(
                    StepRunRecord(
                        "proposed_folder",
                        idx,
                        "skipped",
                        "Graph folder create requires Microsoft 365 sign-in (graph client not provided)",
                    )
                )
                continue
            if dry_run:
                emit(
                    StepRunRecord(
                        "proposed_folder",
                        idx,
                        "dry_run",
                        f"graph create folder {fname!r} under parent in drive {d_drive[-12:] if len(d_drive) > 12 else d_drive}",
                    )
                )
                continue

            if not _pilot_allows_graph_mutation():
                emit(
                    StepRunRecord(
                        "proposed_folder",
                        idx,
                        "skipped",
                        f"pilot_max_graph_operations reached ({pilot_max_graph_ops}); remaining Graph steps not run",
                    )
                )
                continue

            try:
                # Count the mutation attempt even if it fails (409 conflicts etc.).
                graph_ops_used += 1
                graph_client.create_child_folder(
                    d_drive,
                    d_parent,
                    fname,
                    conflict_behavior=str(opts.get("graph_mkdir_conflict_behavior", "fail") or "fail"),
                )
                emit(StepRunRecord("proposed_folder", idx, "ok", f"graph mkdir {fname!r}"))
            except Exception as exc:
                emit(
                    StepRunRecord(
                        "proposed_folder",
                        idx,
                        "failed",
                        f"graph mkdir {fname!r}: {exc}",
                    )
                )
                log_error("transfer_job_graph_mkdir_failed", folder=fname, error=str(exc))
            continue

        if not is_absolute_local_path(dst):
            emit(
                StepRunRecord(
                    "proposed_folder",
                    idx,
                    "skipped",
                    "not a local folder path and step lacks destination_drive_id + destination_parent_item_id "
                    "(re-save manifest after adding proposed folders under SharePoint in planning).",
                )
            )
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
        dest_name = str(step.get("destination_name", "") or step.get("source_name", "") or "").strip()
        step_uid = str(step.get("step_uid", "") or "").strip()

        if pilot_transfer_step_indices and idx not in pilot_transfer_step_indices:
            emit(
                StepRunRecord(
                    "transfer",
                    idx,
                    "skipped",
                    f"pilot_transfer_step_indices filter: idx {idx} not selected",
                )
            )
            continue

        if pilot_transfer_step_uids and step_uid not in pilot_transfer_step_uids:
            emit(
                StepRunRecord(
                    "transfer",
                    idx,
                    "skipped",
                    f"pilot_transfer_step_uids filter: step_uid {step_uid!r} not selected",
                )
            )
            continue

        if pilot_transfer_destination_keys:
            key = f"{dst.strip()}|||{dest_name}"
            if key not in pilot_transfer_destination_keys:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "skipped",
                        f"pilot_transfer_destination_keys filter: key {key!r} not selected",
                    )
                )
                continue

        if pilot_transfer_destination_paths:
            # destination_path uniquely identifies the parent + leaf in our manifests.
            if dst.strip() not in pilot_transfer_destination_paths:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "skipped",
                        f"pilot_transfer_destination_paths filter: destination_path {dst!r} not selected",
                    )
                )
                continue

        if _step_graph_ready(step) and not (is_absolute_local_path(src) and is_absolute_local_path(dst)):
            if graph_client is None:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "skipped",
                        "Microsoft Graph copy requires Microsoft 365 sign-in (graph client not provided)",
                    )
                )
                continue

            if dry_run:
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "dry_run",
                        f"graph copy driveItem -> parent in dest drive (name={dest_name!r}, folder={is_folder})",
                    )
                )
                continue

            if not _pilot_allows_graph_mutation():
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "skipped",
                        f"pilot_max_graph_operations reached ({pilot_max_graph_ops}); remaining Graph steps not run",
                    )
                )
                continue

            try:
                # Count the mutation attempt even if it fails (409 conflicts etc.).
                graph_ops_used += 1
                src_drive_id = str(step.get("source_drive_id", "") or "").strip()
                src_item_id = str(step.get("source_item_id", "") or "").strip()
                dst_drive_id = str(step.get("destination_drive_id", "") or "").strip()
                dst_parent_item_id = str(step.get("destination_item_id", "") or "").strip()
                monitor = graph_client.start_drive_item_copy(
                    source_drive_id=src_drive_id,
                    source_item_id=src_item_id,
                    dest_drive_id=dst_drive_id,
                    dest_parent_item_id=dst_parent_item_id,
                    name=dest_name or None,
                    conflict_behavior=graph_conflict,
                )
                timeout_sec = folder_graph_timeout if is_folder else file_graph_timeout
                log_info(
                    "transfer_job_graph_copy_async_handoff",
                    step_index=idx,
                    is_folder=bool(is_folder),
                    timeout_sec=float(timeout_sec),
                    monitor_url_excerpt=str(monitor or "")[:220],
                    destination_name_excerpt=str(dest_name or "")[:200] or None,
                )
                flush_logger()
                graph_client.wait_graph_async_operation(monitor, timeout_sec=timeout_sec)
                emit(
                    StepRunRecord(
                        "transfer",
                        idx,
                        "ok",
                        f"graph copy completed (name={dest_name!r})",
                    )
                )
            except Exception as exc:
                recovered = False
                recovery_how = ""
                if _graph_async_monitor_auth_poll_failure(exc):
                    ok_probe, recovery_how = _verify_graph_copy_destination_item_present(
                        graph_client, step, dest_name
                    )
                    if ok_probe:
                        recovered = True
                        log_info(
                            "transfer_job_graph_copy_recovered_after_monitor_401",
                            step_index=idx,
                            destination_name_excerpt=str(dest_name or "")[:200] or None,
                            recovery_probe=recovery_how,
                            destination_path_excerpt=str(dst or "")[:220] or None,
                        )
                        flush_logger()
                if recovered:
                    emit(
                        StepRunRecord(
                            "transfer",
                            idx,
                            "ok",
                            f"graph copy completed (name={dest_name!r}); "
                            f"async monitor polling returned 401 but destination item was verified via path "
                            f"({recovery_how})",
                        )
                    )
                    continue
                payload_hint = (
                    f"graph copy: {exc} "
                    f"(source_drive_id={src_drive_id!r}, source_item_id={src_item_id!r}, "
                    f"destination_drive_id={dst_drive_id!r}, destination_parent_item_id={dst_parent_item_id!r}, "
                    f"name={dest_name!r}, conflict_behavior={graph_conflict!r}, "
                    f"destination_path={dst!r}, is_folder={is_folder})"
                )
                emit(StepRunRecord("transfer", idx, "failed", payload_hint))
                log_error(
                    "transfer_job_graph_copy_failed",
                    step_index=idx,
                    error=str(exc),
                    source_drive_id=src_drive_id,
                    source_item_id=src_item_id,
                    destination_drive_id=dst_drive_id,
                    destination_parent_item_id=dst_parent_item_id,
                    destination_name=dest_name,
                    destination_path=dst,
                    conflict_behavior=graph_conflict,
                    is_folder=bool(is_folder),
                    recovered_after_monitor_401=False,
                )
            continue

        if not is_absolute_local_path(src) or not is_absolute_local_path(dst):
            emit(
                StepRunRecord(
                    "transfer",
                    idx,
                    "skipped",
                    "SharePoint-style paths but step lacks full Graph ids "
                    "(need source_drive_id, source_item_id, destination_drive_id, destination_item_id=parent folder); "
                    "or use local C:\\/UNC paths. Re-save manifest from planning with live trees.",
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
                rec = StepRunRecord(
                    "transfer",
                    idx,
                    "ok",
                    f"copytree {src_p} -> {dst_p}",
                    attempts=attempts,
                )
                if do_verify:
                    ok_v, msg = verify_copied_tree(src_p, dst_p)
                    rec.integrity_verified = ok_v
                    if not ok_v:
                        rec.status = "failed"
                        rec.detail = f"{rec.detail}; integrity_failed {msg}"
                emit(rec)
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
                rec = StepRunRecord(
                    "transfer",
                    idx,
                    "ok",
                    f"copy2 {src_p} -> {dst_p}",
                    attempts=attempts,
                )
                if do_verify:
                    ok_v, msg, hs, hd = verify_copied_file(src_p, dst_p)
                    rec.source_sha256 = hs
                    rec.dest_sha256 = hd
                    rec.integrity_verified = ok_v
                    if not ok_v:
                        rec.status = "failed"
                        rec.detail = f"{rec.detail}; integrity_failed {msg}"
                emit(rec)
        else:
            emit(StepRunRecord("transfer", idx, "skipped", f"source is not a file or directory: {src_p}"))

    log_path: str | None = None
    if log_file:
        lp = Path(log_file)
        lp.parent.mkdir(parents=True, exist_ok=True)
        lp.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log_path = str(lp)

    report = build_job_report(job_id=jid, manifest=manifest, records=records, dry_run=dry_run)
    job_report_path: str | None = None
    if job_report_file:
        rp = Path(job_report_file)
        write_job_report_json(rp, report)
        job_report_path = str(rp)

    append_audit_event(
        audit_file,
        job_id=jid,
        event_type="job_finished",
        payload={"summary": report.get("summary", {})},
    )

    return RunManifestResult(
        dry_run=dry_run,
        records=records,
        log_path=log_path,
        job_id=jid,
        job_report_path=job_report_path,
    )
