"""
SharePoint destination tree — explicit structural authority contract.

When :func:`graph_owns_visible_real_destination_structure` is true (non-local destination browsing
with a planning model attached):

* **Visible real rows** (folders/files that represent live SharePoint content) must be inserted only
  by Graph root bind and per-folder load success handlers (e.g. ``replace_all_children`` on the
  destination planning model from Graph payloads).

* **Overlays** attach to existing Graph-backed indices via
  :meth:`~PySide6.QtCore.QAbstractItemModel.update_payload_for_index`, queue :class:`FolderLoadWorker`
  when a child path is missing, and (while expanding) show a loading row via ``replace_all_children``
  until Graph returns children.

* **Planned workspace rows** (``row_kind`` ``planned_*``, ``verification_state`` ``planned_only``) may be
  appended under live or planned parents so projections are visible before SharePoint contains those
  folders. They are **not** live Graph structure (no driveItem ``id``); reconciliation merges them with
  real children after Graph loads. Legacy local-disk browsing may still use ``append_child_payloads`` for
  other scaffolding.

* **Full-library snapshot** (``MainWindow._destination_full_tree_snapshot``) is **background /
  trust / digest / validation / reconcile assistance** only. It must not become the source of new
  visible real rows in this mode (structural rows come only from Graph loads; overlays attach via
  ``MainWindow._apply_destination_planning_overlays``).

* Canonical destination planning paths are **graph-relative** (e.g. ``RootTest2\\Finance``) with no
  leading internal ``Root\\`` segment; legacy persisted values may still carry that prefix and are
  stripped when normalized. Only live Graph rows appear in the pane. Overlay topology may use
  ``OVERLAY_LIB_ROOT_SEMANTIC`` in the in-memory future model (not rendered); it is not the string ``Root``.

Local disk destination browsing does not use this contract (future model may still carry real nodes
from filesystem snapshot semantics).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class _DestinationAuthorityHost(Protocol):
    def _planning_browse_mode(self, panel_key: str) -> str: ...


def sharepoint_planning_tree_active(host: _DestinationAuthorityHost) -> bool:
    """True when destination is not local *and* a destination planning model is present."""
    if host._planning_browse_mode("destination") == "local":
        return False
    return getattr(host, "destination_planning_model", None) is not None


def graph_owns_visible_real_destination_structure(host: _DestinationAuthorityHost) -> bool:
    """
    When True, visible real destination structure is owned by Graph loads, not future-model bind.

    This is the single predicate for “SharePoint live graph structural authority” mode.
    """
    return sharepoint_planning_tree_active(host)


def future_model_bind_may_insert_visible_real_rows(host: _DestinationAuthorityHost) -> bool:
    """
    Legacy helper: the old future-model sync bind path has been removed. When Graph owns structure,
    this is always False so any remaining call sites treat the tree as overlay-only.
    """
    return not graph_owns_visible_real_destination_structure(host)


def full_tree_snapshot_may_author_visible_real_rows(host: _DestinationAuthorityHost) -> bool:
    """When False, enumerate snapshot must not be imported into the future model as visible real bind input."""
    return not graph_owns_visible_real_destination_structure(host)
