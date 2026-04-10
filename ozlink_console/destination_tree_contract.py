"""
Destination tree contract — shared constants and small helpers.

Product rules live in docs/destination_tree_refresh_contract.md:
DESTINATION READ INVARIANT, VISIBLE STRUCTURE UNIQUENESS, RECONCILE CONSTRAINT,
and Read → Load → Repaint.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

# User-visible empty library (planning model placeholder row + status copy).
EMPTY_LIBRARY_MESSAGE = "This document library is empty. Propose a folder to start planning."

# Shown from the start of a new destination library root Graph read until the first skeleton commit.
DESTINATION_STRUCTURAL_LOADING_STATUS_MESSAGE = "Loading root content..."

# Logged phases for Read → Load → Repaint (see docs/destination_tree_refresh_contract.md).
DESTINATION_REFRESH_PHASE_READ = "read"
DESTINATION_REFRESH_PHASE_LOAD = "load"
DESTINATION_REFRESH_PHASE_REPAINT = "repaint"


def new_destination_refresh_correlation_id() -> str:
    """Correlation id for a single destination refresh cycle (support / log stitching)."""
    return str(uuid.uuid4())


def destination_overlay_only_visible_structure_contract(
    *, lazy_mode: bool, uses_planning_model_view: bool
) -> bool:
    """Lazy SharePoint + QTreeView planning model: Graph owns visible structure; future model is overlay only.

    Mirrors ``MainWindow._destination_overlay_only_visible_structure_contract`` for tests and tooling.
    """
    return bool(lazy_mode and uses_planning_model_view)


def destination_structure_child_ref_resolved(ref: Any, model: Optional[Any] = None) -> bool:
    """True if ``ref`` is a usable child ref after ``_find_destination_child_by_path`` (QModelIndex path).

    ``_find_destination_child_by_path`` may return ``None``; callers must not call ``.isValid()`` blindly.
    """
    try:
        from PySide6.QtCore import QModelIndex
    except Exception:
        return False
    if ref is None:
        return False
    if isinstance(ref, QModelIndex):
        if not ref.isValid():
            return False
        if model is not None and hasattr(model, "is_index_live"):
            try:
                return bool(model.is_index_live(ref))
            except Exception:
                return False
        return True
    return False
