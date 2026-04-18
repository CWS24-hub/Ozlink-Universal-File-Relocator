"""
SharePoint planning library combo — session restore matching (drive id first, name fallback for legacy).

Pure helpers keep MainWindow.restore tests small and deterministic.
"""

from __future__ import annotations

from typing import Any


def _norm_token(value: str) -> str:
    return str(value or "").strip().casefold()


def library_combo_index_for_session_restore(
    *,
    stored_drive_id: str,
    stored_display_name: str,
    item_rows: list[tuple[str, Any]],
) -> tuple[int, str]:
    """Return ``(index, outcome_tag)`` for restoring a library selection.

    Policy:
    - If ``stored_drive_id`` is non-empty: match **only** ``payload["id"]`` (Graph drive id).
      No name fallback (avoids binding the wrong library when ids diverge).
    - If ``stored_drive_id`` is empty (legacy sessions): match normalized display name /
      payload name / item text (legacy name-only restore).
    - If nothing to match: ``( -1, "no_session_hint" )``.
    """

    did = str(stored_drive_id or "").strip()
    name = str(stored_display_name or "").strip()

    if not did and not name:
        return -1, "no_session_hint"

    if did:
        dcf = did.casefold()
        for i, (_, payload) in enumerate(item_rows):
            if not isinstance(payload, dict):
                continue
            pid = str(payload.get("id") or "").strip()
            if pid and pid.casefold() == dcf:
                return i, "drive_id_match"
        return -1, "unmatched_drive_id"

    # Legacy: name-only sessions
    ncf = _norm_token(name)
    for i, (text, payload) in enumerate(item_rows):
        cands = {_norm_token(text)}
        if isinstance(payload, dict):
            cands.add(_norm_token(payload.get("name", "")))
        cands.discard("")
        if ncf in cands:
            return i, "legacy_name_only"

    return -1, "unmatched_name"
