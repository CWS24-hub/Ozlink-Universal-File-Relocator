"""
Product rule: **local-first planning edits**.

Direct user actions on the planning workspace (rename, drag move, cut/paste, assign, unassign,
retarget, remove, create proposed folder, and similar) must:

1. Update the in-memory / visible model **immediately** so the UI reflects the edit at once.
2. Allow the user to **continue working** without waiting on correctness passes.
3. **Not** synchronously run broad replay, full-tree work, global overlay rebuilds, or unscoped
   path refresh on the hot path of the gesture.
4. Defer heavy convergence (reconcile, graph validation, chunked projection completion) to
   **background / idle / chunked** paths, scoped to the **smallest affected subtree** when possible.
5. Allow later background work to **patch** the workspace; it must not block the initial gesture.

Execution model (target):

  user action → immediate local model + visible UI → persist planning change → mark dirty →
  narrow deferred follow-up

Call sites that schedule destination overlay / materialize passes should use reasons recognized by
:func:`is_local_first_deferred_materialize_reason` and prefer deferred, chunked overlay application
(``allow_defer=True``, ``prefer_chunked_projection=True``) with narrowed bind scope when available.
"""

from __future__ import annotations

_LOCAL_FIRST_EDIT_PREFIX = "local_first_edit_"

_LOCAL_FIRST_DEFERRED_MATERIALIZE_REASONS: frozenset[str] = frozenset(
    (
        "planned_item_moved",
        "planned_item_moved_manual_drag",
        "planned_item_moved_paste_quick",
    )
)

# Deferred planning refresh / idle overlay may prefix reasons with ``deferred_`` (e.g. combined refresh).
_NARROW_PLANNED_ITEM_MOVE_OVERLAY_REASONS: frozenset[str] = frozenset(
    (
        "planned_item_moved",
        "planned_item_moved_manual_drag",
        "planned_item_moved_paste_quick",
    )
)


def _strip_deferred_planning_reason_prefix(reason: str) -> str:
    r = str(reason or "").strip()
    if r.startswith("deferred_"):
        return r[len("deferred_") :]
    return r


def is_narrow_planned_item_move_overlay_reason(reason: str) -> bool:
    """True for planned-move follow-ups that must use narrow replay, not full destination materialize."""
    return _strip_deferred_planning_reason_prefix(reason) in _NARROW_PLANNED_ITEM_MOVE_OVERLAY_REASONS


def is_local_first_deferred_materialize_reason(reason: str) -> bool:
    """True when the idle/deferred materialize pass is a follow-up to a direct user planning edit.

    Recognizes explicit reasons (e.g. ``planned_item_moved``) and any ``local_first_edit_*`` idle pass,
    including: rename, assign, unassign, retarget, cut/paste, proposed folder create/remove — use
    ``local_first_edit_<action>`` when scheduling deferred overlay.
    """
    r = _strip_deferred_planning_reason_prefix(reason)
    if r in _LOCAL_FIRST_DEFERRED_MATERIALIZE_REASONS:
        return True
    if r.startswith(_LOCAL_FIRST_EDIT_PREFIX):
        return True
    return False
