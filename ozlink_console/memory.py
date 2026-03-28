from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .logger import log_info, log_trace, log_warn
from .models import AllocationRow, ProposedFolder, SessionState, MemoryManifest
from .paths import (
    memory_root,
    legacy_memory_root,
    backups_root,
    quarantine_root,
    python_primary_storage_root,
    legacy_compatibility_root,
    user_scoped_storage_root,
)

class MemoryManager:
    def __init__(self, *, tenant_domain: str = "", operator_upn: str = "") -> None:
        self.tenant_domain = str(tenant_domain or "").strip().lower()
        self.operator_upn = str(operator_upn or "").strip().lower()
        self.expected_fingerprint = f"{self.operator_upn}|{self.tenant_domain}".strip("|")
        self.storage_scope_root = (
            user_scoped_storage_root(self.tenant_domain, self.operator_upn)
            if self.expected_fingerprint
            else python_primary_storage_root()
        )
        self.root = self.storage_scope_root / "Memory"
        self.legacy_root = legacy_memory_root()
        self.backups = self.root / "Backups"
        self.quarantine = self.root / "Quarantine"
        self.exports = self.storage_scope_root / "Exports"
        self.python_primary_storage_root = python_primary_storage_root()
        self.legacy_compatibility_root = legacy_compatibility_root()
        self.current_restore_source = "python"
        self.current_write_root = self.root

        self.paths = {
            "allocations": self.root / "Draft-AllocationQueue.json",
            "allocations_recovery": self.root / "Draft-AllocationQueue.recovery.json",
            "proposed": self.root / "Draft-ProposedFolders.json",
            "proposed_recovery": self.root / "Draft-ProposedFolders.recovery.json",
            "session": self.root / "Draft-SessionState.json",
            "session_recovery": self.root / "Draft-SessionState.recovery.json",
            "manifest": self.root / "MemoryManifest.json",
        }
        self.legacy_paths = {
            "allocations": self.legacy_root / "Draft-AllocationQueue.json",
            "allocations_recovery": self.legacy_root / "Draft-AllocationQueue.recovery.json",
            "proposed": self.legacy_root / "Draft-ProposedFolders.json",
            "proposed_recovery": self.legacy_root / "Draft-ProposedFolders.recovery.json",
            "session": self.legacy_root / "Draft-SessionState.json",
            "session_recovery": self.legacy_root / "Draft-SessionState.recovery.json",
            "manifest": self.legacy_root / "MemoryManifest.json",
        }
        self.backups.mkdir(parents=True, exist_ok=True)
        self.quarantine.mkdir(parents=True, exist_ok=True)
        self.exports.mkdir(parents=True, exist_ok=True)
        self.initialize_store()
        log_info(
            "Memory roots configured.",
            python_primary_storage_root=str(self.python_primary_storage_root),
            legacy_compatibility_root=str(self.legacy_compatibility_root),
            memory_write_root=str(self.root),
            memory_scope_root=str(self.storage_scope_root),
            expected_fingerprint=self.expected_fingerprint,
        )

    def _read_json_path(self, path: Path, fallback: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return fallback

    def _normalize_imported_memory_path(self, value: Any) -> str:
        text = str(value or "").strip().replace("/", "\\")
        if not text:
            return ""
        while "\\\\" in text:
            text = text.replace("\\\\", "\\")
        lowered = text.lower()
        if lowered.startswith("documents\\root\\"):
            return text[len("Documents\\") :]
        if lowered == "documents\\root":
            return "Root"
        return text

    def _normalize_imported_allocations_payload(self, payload: Any) -> Any:
        if not isinstance(payload, list):
            return payload
        normalized_payload = []
        for row in payload:
            if isinstance(row, dict):
                normalized_row = dict(row)
                normalized_row["RequestedDestinationPath"] = self._normalize_imported_memory_path(
                    normalized_row.get("RequestedDestinationPath", "")
                )
                normalized_payload.append(normalized_row)
            else:
                normalized_payload.append(row)
        return normalized_payload

    def _normalize_imported_proposed_payload(self, payload: Any) -> Any:
        if not isinstance(payload, list):
            return payload
        normalized_payload = []
        for row in payload:
            if isinstance(row, dict):
                normalized_row = dict(row)
                normalized_row["ParentPath"] = self._normalize_imported_memory_path(normalized_row.get("ParentPath", ""))
                normalized_row["DestinationPath"] = self._normalize_imported_memory_path(
                    normalized_row.get("DestinationPath", "")
                )
                normalized_payload.append(normalized_row)
            else:
                normalized_payload.append(row)
        return normalized_payload

    def _normalize_imported_session_payload(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        normalized_payload = dict(payload)
        normalized_payload["DestinationSelectedPath"] = self._normalize_imported_memory_path(
            normalized_payload.get("DestinationSelectedPath", "")
        )
        expanded_paths = normalized_payload.get("DestinationExpandedPaths", [])
        if isinstance(expanded_paths, list):
            normalized_payload["DestinationExpandedPaths"] = [
                self._normalize_imported_memory_path(path) for path in expanded_paths
            ]
        return normalized_payload

    def _list_count(self, payload: Any) -> int:
        return len(payload) if isinstance(payload, list) else 0

    def _normalize_candidate_timestamp(self, timestamp: datetime | None) -> tuple[datetime, str]:
        if timestamp is None:
            return datetime.min.replace(tzinfo=timezone.utc), "missing"
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            normalized = timestamp.replace(tzinfo=timezone.utc)
            return normalized, "naive_assumed_utc"
        return timestamp.astimezone(timezone.utc), "aware_normalized_utc"

    def _parse_candidate_timestamp(self, value: str) -> tuple[datetime | None, str]:
        text = str(value or "").strip()
        if not text:
            return None, "missing"

        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            timestamp, kind = self._normalize_candidate_timestamp(parsed)
            return timestamp, f"iso_{kind}"
        except Exception:
            pass

        for fmt in ("%m/%d/%Y %I:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                timestamp, kind = self._normalize_candidate_timestamp(parsed)
                return timestamp, f"strptime_{fmt}_{kind}"
            except Exception:
                continue

        return None, "invalid"

    def _latest_backup_file(self, backup_root: Path, prefixes: tuple[str, ...], *, require_populated: bool = False) -> Path | None:
        candidates: list[Path] = []
        for prefix in prefixes:
            candidates.extend(sorted(backup_root.glob(f"{prefix}_*.json"), key=lambda item: item.stat().st_mtime, reverse=True))

        for path in candidates:
            if not require_populated:
                return path
            payload = self._read_json_path(path, [])
            if self._list_count(payload) > 0:
                return path

        return None

    def _inspect_candidate(self, name: str, session_path: Path, allocations_path: Path, proposed_path: Path) -> dict[str, Any]:
        session_raw = self._read_json_path(session_path, {})
        allocations_raw = self._read_json_path(allocations_path, [])
        proposed_raw = self._read_json_path(proposed_path, [])
        session_state = SessionState.from_dict(session_raw if isinstance(session_raw, dict) else {})
        allocation_count = self._list_count(allocations_raw)
        proposed_count = self._list_count(proposed_raw)
        timestamp = None
        timestamp_kind = "missing"
        if isinstance(session_raw, dict):
            timestamp, timestamp_kind = self._parse_candidate_timestamp(session_raw.get("LastSavedUtc", ""))
            if timestamp is None:
                timestamp, timestamp_kind = self._parse_candidate_timestamp(session_raw.get("CreatedUtc", ""))
        if timestamp is None:
            try:
                raw_timestamp = datetime.fromtimestamp(max(
                    session_path.stat().st_mtime if session_path.exists() else 0,
                    allocations_path.stat().st_mtime if allocations_path.exists() else 0,
                    proposed_path.stat().st_mtime if proposed_path.exists() else 0,
                ))
                timestamp, normalized_kind = self._normalize_candidate_timestamp(raw_timestamp)
                timestamp_kind = f"filesystem_fallback_{normalized_kind}"
            except Exception:
                timestamp, normalized_kind = self._normalize_candidate_timestamp(None)
                timestamp_kind = f"datetime_min_fallback_{normalized_kind}"

        populated = allocation_count > 0 or proposed_count > 0
        valid = isinstance(session_raw, dict) and isinstance(allocations_raw, list) and isinstance(proposed_raw, list)
        timestamp_sort_value = timestamp.timestamp() if timestamp != datetime.min.replace(tzinfo=timezone.utc) else float("-inf")
        log_info(
            "Memory candidate timestamp normalized.",
            candidate=name,
            parsed_timestamp_kind=timestamp_kind,
            normalized_comparison_value=timestamp_sort_value,
            timestamp_iso=timestamp.isoformat(),
        )
        return {
            "name": name,
            "storage_root": str(session_path.parent.parent) if session_path.parent.name.lower() == "memory" else str(session_path.parent),
            "session_path": session_path,
            "allocations_path": allocations_path,
            "proposed_path": proposed_path,
            "session_state": session_state,
            "session_raw": session_raw if isinstance(session_raw, dict) else {},
            "allocations_raw": allocations_raw if isinstance(allocations_raw, list) else [],
            "proposed_raw": proposed_raw if isinstance(proposed_raw, list) else [],
            "draft_id": session_state.DraftId or str((session_raw or {}).get("ActiveDraftId", "") or ""),
            "fingerprint": session_state.SessionFingerprint,
            "allocation_count": allocation_count,
            "proposed_count": proposed_count,
            "populated": populated,
            "valid": valid,
            "timestamp": timestamp,
            "timestamp_kind": timestamp_kind,
            "timestamp_sort_value": timestamp_sort_value,
        }

    def discover_restore_candidates(self) -> list[dict[str, Any]]:
        candidates = [
            self._inspect_candidate(
                "python_live_primary",
                self.paths["session"],
                self.paths["allocations"],
                self.paths["proposed"],
            ),
            self._inspect_candidate(
                "python_live_recovery",
                self.paths["session_recovery"],
                self.paths["allocations_recovery"],
                self.paths["proposed_recovery"],
            ),
        ]

        backup_session = self._latest_backup_file(self.backups, ("Draft-SessionState",), require_populated=False)
        backup_allocations = self._latest_backup_file(self.backups, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True)
        backup_proposed = self._latest_backup_file(self.backups, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False)
        if backup_session and backup_allocations and backup_proposed:
            candidates.append(self._inspect_candidate(
                "python_backup_latest",
                backup_session,
                backup_allocations,
                backup_proposed,
            ))

        global_root = memory_root()
        if self.root != global_root:
            global_paths = {
                "session": global_root / "Draft-SessionState.json",
                "allocations": global_root / "Draft-AllocationQueue.json",
                "proposed": global_root / "Draft-ProposedFolders.json",
                "session_recovery": global_root / "Draft-SessionState.recovery.json",
                "allocations_recovery": global_root / "Draft-AllocationQueue.recovery.json",
                "proposed_recovery": global_root / "Draft-ProposedFolders.recovery.json",
            }
            candidates.extend([
                self._inspect_candidate(
                    "python_global_primary",
                    global_paths["session"],
                    global_paths["allocations"],
                    global_paths["proposed"],
                ),
                self._inspect_candidate(
                    "python_global_recovery",
                    global_paths["session_recovery"],
                    global_paths["allocations_recovery"],
                    global_paths["proposed_recovery"],
                ),
            ])
            global_backups = global_root / "Backups"
            backup_session = self._latest_backup_file(global_backups, ("Draft-SessionState",), require_populated=False) if global_backups.exists() else None
            backup_allocations = self._latest_backup_file(global_backups, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True) if global_backups.exists() else None
            backup_proposed = self._latest_backup_file(global_backups, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False) if global_backups.exists() else None
            if backup_session and backup_allocations and backup_proposed:
                candidates.append(self._inspect_candidate(
                    "python_global_backup_latest",
                    backup_session,
                    backup_allocations,
                    backup_proposed,
                ))

        candidates.extend([
            self._inspect_candidate(
                "legacy_live_primary",
                self.legacy_paths["session"],
                self.legacy_paths["allocations"],
                self.legacy_paths["proposed"],
            ),
            self._inspect_candidate(
                "legacy_live_recovery",
                self.legacy_paths["session_recovery"],
                self.legacy_paths["allocations_recovery"],
                self.legacy_paths["proposed_recovery"],
            ),
        ])

        legacy_backup_root = self.legacy_root / "Backups"
        backup_session = self._latest_backup_file(legacy_backup_root, ("Draft-SessionState",), require_populated=False) if legacy_backup_root.exists() else None
        backup_allocations = self._latest_backup_file(legacy_backup_root, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True) if legacy_backup_root.exists() else None
        backup_proposed = self._latest_backup_file(legacy_backup_root, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False) if legacy_backup_root.exists() else None
        if backup_session and backup_allocations and backup_proposed:
            candidates.append(self._inspect_candidate(
                "legacy_backup_latest",
                backup_session,
                backup_allocations,
                backup_proposed,
            ))

        log_trace(
            "memory",
            "discover_restore_candidates",
            candidate_count=len(candidates),
            candidate_names=[str(c.get("name", "")) for c in candidates],
        )
        return candidates

    def select_restore_candidate(self, candidates: list[dict[str, Any]] | None = None) -> tuple[dict[str, Any] | None, str]:
        inspected = candidates or self.discover_restore_candidates()
        if not inspected:
            log_trace("memory", "select_restore_candidate", selected_name=None, reason="no candidates")
            return None, "no candidates"

        if self.expected_fingerprint:
            matching = [
                candidate for candidate in inspected
                if str(candidate.get("fingerprint", "")).strip().lower() == self.expected_fingerprint
            ]
            if matching:
                inspected = matching
            else:
                current_scope_root = str(self.root).lower()
                inspected = [
                    candidate for candidate in inspected
                    if str(candidate.get("storage_root", "")).lower().startswith(current_scope_root)
                ]
                if not inspected:
                    log_trace(
                        "memory",
                        "select_restore_candidate",
                        selected_name=None,
                        reason="no_user_scoped_candidates",
                        expected_fingerprint_excerpt=str(self.expected_fingerprint)[:80],
                    )
                    return None, f"no user-scoped memory candidates for {self.expected_fingerprint}"

        ranked = sorted(
            inspected,
            key=lambda candidate: (
                1 if candidate.get("valid") else 0,
                1 if candidate.get("populated") else 0,
                candidate.get("allocation_count", 0) + candidate.get("proposed_count", 0),
                1 if candidate.get("draft_id") else 0,
                candidate.get("timestamp_sort_value", float("-inf")),
            ),
            reverse=True,
        )
        selected = ranked[0]
        reason = (
            f"selected={selected.get('name')} valid={selected.get('valid')} "
            f"populated={selected.get('populated')} "
            f"allocations={selected.get('allocation_count', 0)} "
            f"proposed={selected.get('proposed_count', 0)} "
            f"draft_id={selected.get('draft_id', '') or 'none'} "
            f"timestamp={selected.get('timestamp')} "
            f"timestamp_kind={selected.get('timestamp_kind', 'unknown')} "
            f"timestamp_sort_value={selected.get('timestamp_sort_value', float('-inf'))}"
        )
        log_trace(
            "memory",
            "select_restore_candidate",
            selected_name=str(selected.get("name", "")),
            reason_excerpt=reason[:400],
        )
        return selected, reason

    def _python_store_has_populated_draft(self, candidates: list[dict[str, Any]]) -> bool:
        return any(
            candidate.get("name", "").startswith("python_") and candidate.get("populated")
            for candidate in candidates
        )

    def _copy_candidate_into_current_root(self, candidate: dict[str, Any]) -> bool:
        try:
            shutil.copy2(candidate["session_path"], self.paths["session"])
            shutil.copy2(candidate["allocations_path"], self.paths["allocations"])
            shutil.copy2(candidate["proposed_path"], self.paths["proposed"])
            shutil.copy2(self.paths["session"], self.paths["session_recovery"])
            shutil.copy2(self.paths["allocations"], self.paths["allocations_recovery"])
            shutil.copy2(self.paths["proposed"], self.paths["proposed_recovery"])
            self.refresh_manifest(
                draft_id=str(candidate.get("draft_id", "") or ""),
                fingerprint=str(candidate.get("fingerprint", "") or ""),
                status="Healthy",
            )
            log_info(
                "Draft copied into active memory root.",
                source_candidate=candidate.get("name"),
                source_root=candidate.get("storage_root"),
                write_root=str(self.root),
            )
            return True
        except Exception as exc:
            log_warn(
                "Draft copy into active memory root failed.",
                error=str(exc),
                source_candidate=candidate.get("name"),
                source_root=candidate.get("storage_root"),
                write_root=str(self.root),
            )
            return False

    def prepare_selected_candidate_for_runtime(self, candidate: dict[str, Any] | None, candidates: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        if not candidate:
            return candidate

        selected_name = str(candidate.get("name", ""))
        self.current_restore_source = "legacy" if selected_name.startswith("legacy_") else "python"
        log_info(
            "Memory restore source selected.",
            restore_source=self.current_restore_source,
            selected_candidate=selected_name,
            selected_storage_root=candidate.get("storage_root", ""),
            write_root=str(self.root),
        )
        log_trace(
            "memory",
            "prepare_selected_candidate_for_runtime",
            selected_name=selected_name,
            restore_source=self.current_restore_source,
        )

        inspected = candidates or []
        selected_storage_root = str(candidate.get("storage_root", ""))
        current_storage_root = str(self.root)
        should_copy_to_current_root = (
            selected_storage_root
            and Path(selected_storage_root) != self.root
            and (
                self.current_restore_source == "legacy"
                or not self._python_store_has_populated_draft(inspected)
                or str(candidate.get("name", "")).startswith("python_global_")
            )
        )
        if should_copy_to_current_root:
            copied = self._copy_candidate_into_current_root(candidate)
            if copied:
                python_candidate = self._inspect_candidate(
                    "python_live_primary",
                    self.paths["session"],
                    self.paths["allocations"],
                    self.paths["proposed"],
                )
                self.current_restore_source = "python"
                log_info(
                    "Legacy draft copied and switched to Python root.",
                    selected_candidate=python_candidate.get("name"),
                    selected_storage_root=python_candidate.get("storage_root", ""),
                    write_root=str(self.root),
                )
                return python_candidate

        return candidate

    def load_candidate_payload(
        self,
        candidate: dict[str, Any],
    ) -> tuple[SessionState, list[AllocationRow], list[ProposedFolder], dict[str, Any]]:
        session_state = candidate.get("session_state") if isinstance(candidate.get("session_state"), SessionState) else SessionState()
        allocations = [AllocationRow.from_dict(item) for item in candidate.get("allocations_raw", [])]
        proposed = [ProposedFolder.from_dict(item) for item in candidate.get("proposed_raw", [])]
        return session_state, allocations, proposed, candidate.get("session_raw", {})

    def initialize_store(self) -> None:
        defaults = {
            "allocations": [],
            "allocations_recovery": [],
            "proposed": [],
            "proposed_recovery": [],
        }
        for key, default_value in defaults.items():
            self._ensure_json_file(self.paths[key], default_value)
        for key in ("session", "session_recovery"):
            self._ensure_json_file(self.paths[key], SessionState().to_dict())
        if not self.paths["manifest"].exists():
            self.save_manifest(MemoryManifest(
                AllocationQueuePath=str(self.paths["allocations"]),
                ProposedFoldersPath=str(self.paths["proposed"]),
                SessionStatePath=str(self.paths["session"]),
            ))

    def _ensure_json_file(self, path: Path, default_obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            self._atomic_write_text(path, json.dumps(default_obj, indent=2))
            return
        try:
            raw = path.read_text(encoding="utf-8").strip()
            if not raw:
                raise ValueError("empty")
            json.loads(raw)
        except Exception:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            quarantine_path = self.quarantine / f"{path.stem}.corrupt-{stamp}{path.suffix}"
            try:
                shutil.move(str(path), str(quarantine_path))
            except Exception:
                pass
            self._atomic_write_text(path, json.dumps(default_obj, indent=2))
            log_warn("Memory file re-initialized after corruption detection.", file=str(path), quarantine=str(quarantine_path))

    def _read_json(self, path: Path, fallback: Any) -> Any:
        return self._read_json_path(path, fallback)

    def _json_count(self, path: Path) -> int:
        data = self._read_json(path, [])
        return len(data) if isinstance(data, list) else 0

    def _backup_file(self, path: Path, prefix: str) -> None:
        if not path.exists():
            return
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
        backup_path = self.backups / f"{prefix}_{stamp}.json"
        shutil.copy2(path, backup_path)

    def _atomic_write_text(self, target: Path, text: str) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=f"{target.stem}.",
            suffix=f"{target.suffix}.tmp",
            dir=str(target.parent),
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            temp_path.replace(target)
            try:
                dir_fd = os.open(str(target.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass

    def _write_json_safely(
        self,
        target: Path,
        recovery: Path | None,
        payload: Any,
        new_count: int,
        allow_dangerous_empty_overwrite: bool = False,
        label: str = "Memory",
    ) -> None:
        serialized = json.dumps(payload, indent=2, ensure_ascii=False)
        json.loads(serialized)

        existing_count = self._json_count(target)
        if not allow_dangerous_empty_overwrite and existing_count > 0 and new_count == 0:
            raise ValueError(f"Memory protection blocked empty overwrite for {label}. ExistingCount={existing_count} NewCount={new_count}")

        self._backup_file(target, target.stem)
        self._atomic_write_text(target, serialized)

        if recovery is not None:
            self._atomic_write_text(recovery, serialized)

    def load_allocations(self) -> list[AllocationRow]:
        data = self._read_json(self.paths["allocations"], [])
        rows = [AllocationRow.from_dict(x) for x in data]
        log_trace("memory", "load_allocations", row_count=len(rows), path_excerpt=str(self.paths["allocations"])[-80:])
        return rows

    def save_allocations(self, rows: list[AllocationRow], *, allow_empty: bool = False) -> None:
        payload = [r.to_dict() for r in rows]
        self._write_json_safely(
            self.paths["allocations"],
            self.paths["allocations_recovery"],
            payload,
            new_count=len(payload),
            allow_dangerous_empty_overwrite=allow_empty,
            label="AllocationQueue",
        )
        log_trace("memory", "save_allocations", row_count=len(payload), allow_empty=allow_empty)

    def load_proposed(self) -> list[ProposedFolder]:
        data = self._read_json(self.paths["proposed"], [])
        rows = [ProposedFolder.from_dict(x) for x in data]
        log_trace("memory", "load_proposed", row_count=len(rows))
        return rows

    def save_proposed(self, rows: list[ProposedFolder], *, allow_empty: bool = False) -> None:
        payload = [r.to_dict() for r in rows]
        self._write_json_safely(
            self.paths["proposed"],
            self.paths["proposed_recovery"],
            payload,
            new_count=len(payload),
            allow_dangerous_empty_overwrite=allow_empty,
            label="ProposedFolders",
        )
        log_trace("memory", "save_proposed", row_count=len(payload), allow_empty=allow_empty)

    def load_session(self) -> SessionState:
        state = SessionState.from_dict(self._read_json(self.paths["session"], {}))
        log_trace(
            "memory",
            "load_session",
            draft_id_excerpt=str(getattr(state, "DraftId", "") or "")[:40],
        )
        return state

    def save_session(self, state: SessionState) -> None:
        existing_raw = self._read_json_path(self.paths["session"], {})
        payload = state.to_dict()
        if isinstance(existing_raw, dict):
            existing_raw.update(payload)
            payload = existing_raw

        self._write_json_safely(
            self.paths["session"],
            self.paths["session_recovery"],
            payload,
            new_count=1,
            allow_dangerous_empty_overwrite=True,
            label="SessionState",
        )
        log_trace("memory", "save_session", draft_id_excerpt=str(getattr(state, "DraftId", "") or "")[:40])

    def save_manifest(self, manifest: MemoryManifest) -> None:
        self._backup_file(self.paths["manifest"], self.paths["manifest"].stem)
        self._atomic_write_text(self.paths["manifest"], json.dumps(manifest.to_dict(), indent=2))
        log_trace(
            "memory",
            "save_manifest",
            draft_id_excerpt=str(getattr(manifest, "DraftId", "") or "")[:40],
            save_status=str(getattr(manifest, "SaveStatus", "") or ""),
        )

    def refresh_manifest(self, draft_id: str = "", fingerprint: str = "", status: str = "Healthy") -> None:
        manifest = MemoryManifest(
            DraftId=draft_id,
            SessionFingerprint=fingerprint,
            AllocationQueueCount=self._json_count(self.paths["allocations"]),
            ProposedFolderCount=self._json_count(self.paths["proposed"]),
            LastGoodSaveUtc=datetime.utcnow().isoformat(),
            SaveStatus=status,
            AllocationQueuePath=str(self.paths["allocations"]),
            ProposedFoldersPath=str(self.paths["proposed"]),
            SessionStatePath=str(self.paths["session"]),
        )
        self.save_manifest(manifest)

    def export_bundle(self, reason: str = "Manual", destination: Path | None = None) -> Path:
        if destination is None:
            destination = self.exports / ("Export_" + datetime.now().strftime("%Y%m%d-%H%M%S"))
        destination.mkdir(parents=True, exist_ok=True)
        for path in self.paths.values():
            if path.exists():
                shutil.copy2(path, destination / path.name)
        meta = {
            "ExportedUtc": datetime.utcnow().isoformat(),
            "Reason": reason,
            "MachineName": __import__("platform").node(),
        }
        (destination / "ExportMetadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        log_info("Memory bundle exported.", destination=str(destination), reason=reason)
        log_trace("memory", "export_bundle", reason=reason, destination_excerpt=str(destination)[-100:])
        return destination

    def export_bundle_zip(self, bundle_folder: Path, destination_zip: Path | None = None) -> Path:
        bundle_folder = Path(bundle_folder)
        if destination_zip is None:
            destination_zip = bundle_folder.with_suffix(".zip")
        destination_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(destination_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(bundle_folder.iterdir()):
                if path.is_file():
                    archive.write(path, arcname=path.name)
        log_info("Memory bundle zip exported.", bundle=str(bundle_folder), destination=str(destination_zip))
        return destination_zip

    def import_bundle(self, source_folder: Path) -> None:
        required = ["Draft-SessionState.json", "Draft-AllocationQueue.json", "Draft-ProposedFolders.json"]
        for name in required:
            if not (source_folder / name).exists():
                raise FileNotFoundError(f"Import bundle missing required file: {name}")
            json.loads((source_folder / name).read_text(encoding="utf-8"))

        stamp = "ImportBefore_" + datetime.now().strftime("%Y%m%d-%H%M%S")
        qdir = self.quarantine / stamp
        qdir.mkdir(parents=True, exist_ok=True)
        for path in self.paths.values():
            if path.exists():
                shutil.copy2(path, qdir / path.name)

        mapping = {
            "Draft-SessionState.json": self.paths["session"],
            "Draft-AllocationQueue.json": self.paths["allocations"],
            "Draft-ProposedFolders.json": self.paths["proposed"],
            "MemoryManifest.json": self.paths["manifest"],
        }
        for name, target in mapping.items():
            src = source_folder / name
            if src.exists():
                payload = json.loads(src.read_text(encoding="utf-8"))
                if name == "Draft-SessionState.json":
                    payload = self._normalize_imported_session_payload(payload)
                elif name == "Draft-AllocationQueue.json":
                    payload = self._normalize_imported_allocations_payload(payload)
                elif name == "Draft-ProposedFolders.json":
                    payload = self._normalize_imported_proposed_payload(payload)
                self._atomic_write_text(target, json.dumps(payload, indent=2))

        session_payload = self._read_json_path(self.paths["session"], {})
        if isinstance(session_payload, dict):
            if self.expected_fingerprint:
                session_payload["SessionFingerprint"] = self.expected_fingerprint
            if not session_payload.get("LastSavedUtc"):
                session_payload["LastSavedUtc"] = datetime.utcnow().isoformat()
            self.paths["session"].write_text(json.dumps(session_payload, indent=2), encoding="utf-8")

        shutil.copy2(self.paths["session"], self.paths["session_recovery"])
        shutil.copy2(self.paths["allocations"], self.paths["allocations_recovery"])
        shutil.copy2(self.paths["proposed"], self.paths["proposed_recovery"])
        self.refresh_manifest(
            draft_id=str(session_payload.get("DraftId", "")) if isinstance(session_payload, dict) else "",
            fingerprint=self.expected_fingerprint,
            status="Healthy",
        )
        log_info("Memory bundle imported.", source=str(source_folder))
        log_trace("memory", "import_bundle", source_excerpt=str(source_folder)[-100:])

    def import_bundle_zip(self, source_zip: Path) -> None:
        source_zip = Path(source_zip)
        if not source_zip.exists():
            raise FileNotFoundError(f"Import bundle zip not found: {source_zip}")

        with tempfile.TemporaryDirectory(prefix="ozlink_draft_import_") as temp_dir:
            extract_root = Path(temp_dir) / "bundle"
            extract_root.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(source_zip, "r") as archive:
                archive.extractall(extract_root)
            self.import_bundle(extract_root)
        log_info("Memory bundle zip imported.", source=str(source_zip))
        log_trace("memory", "import_bundle_zip", source_excerpt=str(source_zip)[-100:])

    def submit_request_package(self, user_display: str, user_email: str, tenant_id: str, tenant_label: str,
                               source_context: dict[str, Any], destination_context: dict[str, Any]) -> Path:
        request_id = "REQ-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        payload = {
            "RequestId": request_id,
            "Status": "Submitted",
            "SubmittedBy": {
                "DisplayName": user_display,
                "WorkEmail": user_email,
                "UserPrincipalName": user_email,
            },
            "Tenant": {
                "TenantId": tenant_id,
                "TenantLabel": tenant_label,
            },
            "SourceContext": source_context,
            "DestinationContext": destination_context,
            "PlannedMoves": [x.to_dict() for x in self.load_allocations()],
            "ProposedFolders": [x.to_dict() for x in self.load_proposed()],
            "NeedsReview": [],
            "CreatedOn": datetime.now().isoformat(timespec="seconds"),
            "LastUpdatedOn": datetime.now().isoformat(timespec="seconds"),
            "Version": "Python-PySide6-v1",
        }
        from .paths import requests_root
        out = requests_root() / f"{request_id}.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log_info("Request package saved.", path=str(out), request_id=request_id)
        return out
