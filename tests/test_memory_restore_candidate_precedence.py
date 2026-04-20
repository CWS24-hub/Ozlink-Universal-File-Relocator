"""Restore candidate selection: live Memory files vs backups; sparse-primary promotion rules."""

from __future__ import annotations

from datetime import datetime, timezone

from ozlink_console.memory import MemoryManager
from ozlink_console.models import SessionState


def _base_candidate(
    name: str,
    *,
    valid: bool,
    populated: bool,
    allocation_count: int,
    proposed_count: int = 0,
    ts: float = 1.0,
    draft_id: str = "",
    session_state: SessionState | None = None,
) -> dict:
    ss = session_state if isinstance(session_state, SessionState) else SessionState()
    if draft_id:
        ss.DraftId = draft_id
    did = str(getattr(ss, "DraftId", "") or draft_id or "").strip()
    return {
        "name": name,
        "valid": valid,
        "populated": populated,
        "allocation_count": allocation_count,
        "proposed_count": proposed_count,
        "draft_id": did,
        "fingerprint": "user@t.com|t.com",
        "timestamp_sort_value": ts,
        "session_path": None,
        "allocations_path": None,
        "proposed_path": None,
        "session_state": ss,
        "session_raw": {},
        "allocations_raw": [],
        "proposed_raw": [],
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
        "timestamp_kind": "test",
    }


def test_select_restore_candidate_promotes_populated_backup_when_primary_sparse_same_draft():
    mm = MemoryManager()
    backup_ts = datetime(2025, 6, 1, tzinfo=timezone.utc).timestamp()
    live_ts = datetime(2025, 5, 1, tzinfo=timezone.utc).timestamp()
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=True,
            populated=False,
            allocation_count=0,
            draft_id="RECOVERED-X",
            ts=live_ts,
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=17,
            draft_id="RECOVERED-X",
            ts=backup_ts,
        ),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_backup_latest"
    assert "authoritative_python_backup_promoted" in reason


def test_select_restore_candidate_uses_live_primary_when_populated_same_as_backup():
    mm = MemoryManager()
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=True,
            populated=True,
            allocation_count=14,
            draft_id="DRAFT-A",
            ts=100.0,
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=76,
            draft_id="DRAFT-A",
            ts=200.0,
        ),
    ]
    selected, _reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_primary"
    assert selected["allocation_count"] == 14


def test_select_restore_candidate_does_not_promote_backup_different_draft():
    mm = MemoryManager()
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=True,
            populated=False,
            allocation_count=0,
            draft_id="DRAFT-PRIMARY",
            ts=300.0,
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=50,
            draft_id="DRAFT-BACKUP",
            ts=400.0,
        ),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_primary"
    assert "authoritative_python_live_primary" in reason


def test_select_restore_primary_wins_when_backup_sparse_but_primary_has_selectors():
    ss = SessionState()
    ss.DraftId = "DRAFT-Z"
    ss.SelectedDestinationLibraryId = "b!lib123"
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=True,
            populated=False,
            allocation_count=0,
            session_state=ss,
            ts=100.0,
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=3,
            draft_id="DRAFT-Z",
            ts=200.0,
        ),
    ]
    candidates[0]["draft_id"] = "DRAFT-Z"
    mm = MemoryManager()
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected["name"] == "python_live_primary"
    assert "authoritative_python_live_primary" in reason


def test_select_restore_candidate_uses_recovery_when_primary_invalid():
    mm = MemoryManager()
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=False,
            populated=True,
            allocation_count=99,
            ts=300.0,
            draft_id="",
        ),
        _base_candidate(
            "python_live_recovery",
            valid=True,
            populated=True,
            allocation_count=3,
            ts=100.0,
            draft_id="",
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=76,
            ts=400.0,
            draft_id="",
        ),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_recovery"
    assert "authoritative_python_live_recovery" in reason


def test_select_restore_candidate_fallback_to_backup_when_live_invalid():
    mm = MemoryManager()
    candidates = [
        _base_candidate("python_live_primary", valid=False, populated=False, allocation_count=0, ts=1.0),
        _base_candidate("python_live_recovery", valid=False, populated=False, allocation_count=0, ts=2.0),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=12,
            draft_id="X",
            ts=3.0,
        ),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_backup_latest"
    assert "fallback_selected" in reason
