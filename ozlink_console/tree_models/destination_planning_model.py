"""
QAbstractItemModel for destination planning tree (v2 / QTreeView path).
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QBrush, QColor

from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
    destination_payload_is_planned_workspace_row,
    destination_payload_workspace_row_state,
)
from ozlink_console.tree_models.explorer_columns import (
    EXPLORER_COLUMN_COUNT,
    EXPLORER_COLUMN_LABELS,
    explorer_date_label,
    explorer_icon_for_node,
    explorer_size_label,
    explorer_type_label,
)

NestedSpec = Tuple[Dict[str, Any], List["NestedSpec"]]


class _Node:
    __slots__ = ("parent", "row", "payload", "_children")

    def __init__(self, parent: Optional["_Node"], row: int, payload: Dict[str, Any], children: Optional[List["_Node"]]):
        self.parent = parent
        self.row = row
        self.payload = payload
        self._children = children

    def is_placeholder(self) -> bool:
        pl = getattr(self, "payload", None)
        if not isinstance(pl, dict):
            return True
        return bool(pl.get("placeholder"))

    def is_folder(self) -> bool:
        if self.is_placeholder():
            return False
        pl = getattr(self, "payload", None)
        if not isinstance(pl, dict):
            return False
        return bool(pl.get("is_folder"))


class DestinationPlanningTreeModel(QAbstractItemModel):
    """Emitted after row structure changes (insert/remove/replace/reset). Used to invalidate UI caches."""

    destination_structure_changed = Signal()

    def __init__(self, parent=None, column_labels=None, destination_index_key_fn: Optional[Callable[[Dict[str, Any]], str]] = None):
        super().__init__(parent)
        labels = list(column_labels) if column_labels else list(EXPLORER_COLUMN_LABELS)
        while len(labels) < EXPLORER_COLUMN_COUNT:
            labels.append(EXPLORER_COLUMN_LABELS[len(labels)])
        self._column_labels = labels[:EXPLORER_COLUMN_COUNT]
        self._invisible = _Node(None, -1, {}, [])
        self._invisible._children = []
        self._destination_index_key_fn = destination_index_key_fn
        self._path_to_nodes: Dict[str, List[_Node]] = {}
        # casefold(primary_key) -> exact dict key for O(1) lookup when canonical strings differ only by case.
        self._path_cf_to_key: Dict[str, str] = {}
        self._structure_generation: int = 0
        self._invalid_internal_pointer_logged_gen: int = -1
        self._coalesce_dest_structure_signal_depth: int = 0
        self._pending_dest_structure_signal: bool = False
        # Set by MainWindow to :class:`ozlink_console.dest_scroll_profiler.DestScrollProfiler` when enabled.
        self._dest_scroll_profiler_ref: Any = None

    def begin_coalesce_destination_structure_signal(self) -> None:
        """Batch multiple structural mutations; emit :attr:`destination_structure_changed` once on end."""
        self._coalesce_dest_structure_signal_depth = int(getattr(self, "_coalesce_dest_structure_signal_depth", 0) or 0) + 1

    def end_coalesce_destination_structure_signal(self) -> None:
        d = int(getattr(self, "_coalesce_dest_structure_signal_depth", 0) or 1) - 1
        self._coalesce_dest_structure_signal_depth = max(0, d)
        if d == 0 and getattr(self, "_pending_dest_structure_signal", False):
            self._pending_dest_structure_signal = False
            self.destination_structure_changed.emit()

    def _notify_structure_changed(self) -> None:
        self._structure_generation += 1
        if int(getattr(self, "_coalesce_dest_structure_signal_depth", 0) or 0) > 0:
            self._pending_dest_structure_signal = True
            return
        self.destination_structure_changed.emit()

    def _log_invalid_internal_pointer_once(self, *, context: str, ptr: Any) -> None:
        gen = int(self._structure_generation)
        if self._invalid_internal_pointer_logged_gen == gen:
            return
        self._invalid_internal_pointer_logged_gen = gen
        try:
            tname = type(ptr).__name__
        except Exception:
            tname = "unknown"
        log_info(
            "destination_invalid_internal_pointer_type",
            context=context,
            model_generation=gen,
            pointer_type=tname,
        )

    def structure_generation(self) -> int:
        return int(self._structure_generation)

    def is_index_live(self, index: QModelIndex) -> bool:
        """True if ``index`` still points at a row attached under its parent (safe for model access)."""
        try:
            if not index.isValid():
                return False
            node = self._node(index)
            if node is None:
                return False
            parent_ix = index.parent()
            parent_node = self._invisible if not parent_ix.isValid() else self._node(parent_ix)
            if parent_node is None:
                return False
            children = parent_node._children
            if not children:
                return False
            row = index.row()
            if row < 0 or row >= len(children):
                return False
            return children[row] is node
        except RuntimeError:
            return False

    def _node(self, index: QModelIndex) -> Optional[_Node]:
        try:
            if not index.isValid():
                return None
            p = index.internalPointer()
        except RuntimeError:
            return None
        if p is None:
            return None
        if not isinstance(p, _Node):
            self._log_invalid_internal_pointer_once(context="index_internal_pointer", ptr=p)
            return None
        return p

    def index(self, row: int, column: int, parent: QModelIndex) -> QModelIndex:
        if column < 0 or column >= EXPLORER_COLUMN_COUNT or row < 0:
            return QModelIndex()
        parent_node = self._invisible
        if parent.isValid():
            p = self._node(parent)
            if p is None or p._children is None:
                return QModelIndex()
            parent_node = p
        ch = parent_node._children
        if row >= len(ch):
            return QModelIndex()
        el = ch[row]
        if not isinstance(el, _Node):
            self._log_invalid_internal_pointer_once(context="child_slot_not_node", ptr=el)
            return QModelIndex()
        return self.createIndex(row, column, el)

    def parent(self, index: QModelIndex) -> QModelIndex:
        if not index.isValid():
            return QModelIndex()
        node = self._node(index)
        if node is None or node.parent is None:
            return QModelIndex()
        parent_node = node.parent
        if parent_node.parent is None:
            return QModelIndex()
        gp = parent_node.parent
        siblings = gp._children or []
        try:
            pr = siblings.index(parent_node)
        except ValueError:
            return QModelIndex()
        return self.createIndex(pr, 0, parent_node)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole and 0 <= section < len(self._column_labels):
            return self._column_labels[section]
        return super().headerData(section, orientation, role)

    def rowCount(self, parent: QModelIndex) -> int:
        if parent.isValid() and parent.column() > 0:
            return 0
        if not parent.isValid():
            return len(self._invisible._children)
        node = self._node(parent)
        if node is None or node._children is None:
            return 0
        return len(node._children)

    def columnCount(self, parent: QModelIndex) -> int:
        return EXPLORER_COLUMN_COUNT

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        prof = getattr(self, "_dest_scroll_profiler_ref", None)
        _t0 = None
        _prof_detail = "DestinationPlanningTreeModel.data"
        if prof is not None and getattr(prof, "should_record_fine_grained", lambda: False)():
            _t0 = time.perf_counter()
        try:
            if not index.isValid():
                _prof_detail = "DestinationPlanningTreeModel.data:invalid_index"
                return None
            node = self._node(index)
            if node is None or not isinstance(node, _Node):
                _prof_detail = "DestinationPlanningTreeModel.data:no_node"
                return None
            p = getattr(node, "payload", None)
            if not isinstance(p, dict):
                _prof_detail = "DestinationPlanningTreeModel.data:no_payload"
                return None
            col = index.column()
            _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}"
            if role == Qt.DisplayRole:
                if col == 0:
                    return p.get("base_display_label") or ""
                if col == 1:
                    return explorer_size_label(p)
                if col == 2:
                    return explorer_type_label(p)
                if col == 3:
                    return explorer_date_label(p)
                return None
            if role == Qt.UserRole:
                return p if col == 0 else None
            if role == Qt.ForegroundRole:
                c = p.get("_model_foreground")
                if c is not None:
                    return QBrush(c)
                if col != 0:
                    return None
                ws = destination_payload_workspace_row_state(p)
                if ws == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL:
                    return QBrush(QColor(145, 105, 45))
                if destination_payload_is_planned_workspace_row(p) or ws == WORKSPACE_ROW_STATE_PLANNED_ONLY:
                    return QBrush(QColor(65, 105, 175))
                if ws == WORKSPACE_ROW_STATE_LIVE_CONFIRMED:
                    return None
                return None
            if role == Qt.BackgroundRole:
                c = p.get("_model_background")
                return QBrush(c) if c is not None else None
            if role == Qt.ToolTipRole:
                tip = p.get("_model_tooltip")
                return tip if tip else None
            if role == Qt.DecorationRole and col == 0:
                return explorer_icon_for_node(p)
            return None
        finally:
            if _t0 is not None:
                prof.record(
                    "model",
                    _prof_detail,
                    time.perf_counter() - _t0,
                )

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags
        node = self._node(index)
        pl = getattr(node, "payload", None) if node is not None else None
        if (
            node
            and node.is_placeholder()
            and isinstance(pl, dict)
            and pl.get("placeholder_role") == "empty_library_message"
        ):
            return Qt.NoItemFlags
        if node and node.is_placeholder():
            return Qt.NoItemFlags
        if isinstance(pl, dict) and (
            pl.get("_inline_new_proposed") or pl.get("_inline_rename_proposed")
        ):
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def hasChildren(self, parent: QModelIndex) -> bool:
        if not parent.isValid():
            return len(self._invisible._children) > 0
        node = self._node(parent)
        if node is None or node.is_placeholder() or not node.is_folder():
            return False
        if node._children is None:
            return True
        if len(node._children) > 0:
            return True
        pl = getattr(node, "payload", None)
        return isinstance(pl, dict) and bool(pl.get("_destination_expand_affordance"))

    def _reindex(self, parent_node: _Node) -> None:
        ch = parent_node._children or []
        for i, c in enumerate(ch):
            c.row = i
            c.parent = parent_node

    def _path_key_for_payload(self, payload: Dict[str, Any]) -> str:
        fn = self._destination_index_key_fn
        if fn is None:
            return ""
        try:
            return str(fn(payload) or "").strip()
        except Exception:
            return ""

    def _iter_subtree_nodes(self, node: _Node):
        yield node
        ch = node._children
        if not ch:
            return
        for c in ch:
            yield from self._iter_subtree_nodes(c)

    def _bucket_remove_node(self, node: _Node, payload_snapshot: Optional[Dict[str, Any]] = None) -> None:
        pl = payload_snapshot if payload_snapshot is not None else node.payload
        k = self._path_key_for_payload(pl)
        if not k:
            return
        lst = self._path_to_nodes.get(k)
        if not lst:
            return
        try:
            lst.remove(node)
        except ValueError:
            pass
        if not lst:
            del self._path_to_nodes[k]
            self._path_cf_to_key.pop(k.casefold(), None)

    def _bucket_add_node(self, node: _Node) -> None:
        if self._destination_index_key_fn is None or node.is_placeholder():
            return
        k = self._path_key_for_payload(node.payload)
        if not k:
            return
        lst = self._path_to_nodes.setdefault(k, [])
        if node not in lst:
            lst.append(node)
        self._path_cf_to_key[k.casefold()] = k

    def _unregister_subtree_paths(self, node: _Node) -> None:
        if self._destination_index_key_fn is None:
            return
        for n in self._iter_subtree_nodes(node):
            self._bucket_remove_node(n)

    def _register_subtree_paths(self, node: _Node) -> None:
        if self._destination_index_key_fn is None:
            return
        for n in self._iter_subtree_nodes(node):
            self._bucket_add_node(n)

    def _register_subtree_paths_from_roots(self, roots: List[_Node]) -> None:
        """Register path index entries for many new subtree roots in one pass (e.g. batched append)."""
        if self._destination_index_key_fn is None or not roots:
            return
        for root in roots:
            for n in self._iter_subtree_nodes(root):
                self._bucket_add_node(n)

    def _rebuild_path_index(self) -> None:
        self._path_to_nodes.clear()
        self._path_cf_to_key.clear()
        if self._destination_index_key_fn is None:
            return
        for c in self._invisible._children or []:
            self._register_subtree_paths(c)

    def _index_for_node(self, node: _Node) -> QModelIndex:
        if node is None or node.parent is None:
            return QModelIndex()
        pnode = node.parent
        siblings = pnode._children or []
        try:
            row = siblings.index(node)
        except ValueError:
            return QModelIndex()
        parent_ix = QModelIndex() if pnode.parent is None else self._index_for_node(pnode)
        return self.index(row, 0, parent_ix)

    def find_indices_for_canonical_destination_path(self, canonical_key: str) -> List[QModelIndex]:
        if not canonical_key or self._destination_index_key_fn is None:
            return []
        nodes = self._path_to_nodes.get(canonical_key) or []
        if not nodes:
            pk = self._path_cf_to_key.get(canonical_key.casefold())
            if pk:
                nodes = self._path_to_nodes.get(pk) or []
        if not nodes:
            cf = canonical_key.casefold()
            n_buckets = len(self._path_to_nodes)
            if n_buckets and n_buckets <= 4096:
                for k, lst in self._path_to_nodes.items():
                    if k.casefold() == cf:
                        nodes = lst
                        self._path_cf_to_key[cf] = k
                        break
        out: List[QModelIndex] = []
        for n in nodes:
            ix = self._index_for_node(n)
            if ix.isValid():
                out.append(ix)
        return out

    def clear(self) -> None:
        self.beginResetModel()
        self._invisible._children = []
        self._path_to_nodes.clear()
        self._path_cf_to_key.clear()
        self.endResetModel()
        self._notify_structure_changed()

    def reset_root_payloads(self, payloads: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        children: List[_Node] = []
        for i, pl in enumerate(payloads):
            ch = None if pl.get("is_folder") else []
            children.append(_Node(self._invisible, i, pl, ch))
        self._invisible._children = children
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._notify_structure_changed()

    def _remove_root_row(self, row: int) -> None:
        parent_node = self._invisible
        ch = parent_node._children or []
        if row < 0 or row >= len(ch):
            return
        old = ch[row]
        self._unregister_subtree_paths(old)
        inv = QModelIndex()
        self.beginRemoveRows(inv, row, row)
        ch.pop(row)
        parent_node._children = ch
        self.endRemoveRows()
        self._reindex(parent_node)
        self._notify_structure_changed()

    def _insert_root_child_at(self, row: int, pl: Dict[str, Any]) -> None:
        parent_node = self._invisible
        ch = list(parent_node._children or [])
        row = max(0, min(int(row), len(ch)))
        child_list = None if pl.get("is_folder") else []
        inv = QModelIndex()
        self.beginInsertRows(inv, row, row)
        node = _Node(parent_node, row, dict(pl), child_list)
        ch.insert(row, node)
        parent_node._children = ch
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths_from_roots([node])
        self._notify_structure_changed()

    @staticmethod
    def _sort_key_graph_root(pl: Dict[str, Any]) -> Tuple[bool, str]:
        return (not bool(pl.get("is_folder")), str(pl.get("name") or "").lower())

    def _root_graph_insertion_row(self, incoming_pl: Dict[str, Any]) -> int:
        """Return insert row for a new structural root child, ordered with existing structural rows."""
        inv = QModelIndex()
        sk = self._sort_key_graph_root(incoming_pl)
        n = self.rowCount(inv)
        for r in range(n):
            pl = self.index(r, 0, inv).data(Qt.UserRole) or {}
            if not isinstance(pl, dict):
                continue
            if destination_payload_is_planned_workspace_row(pl):
                return r
            ws = destination_payload_workspace_row_state(pl)
            if ws == WORKSPACE_ROW_STATE_PLANNED_ONLY:
                return r
            if pl.get("placeholder"):
                continue
            if self._sort_key_graph_root(pl) > sk:
                return r
        return n

    def _merge_preserves_root_row_without_graph_id(self, pl: Dict[str, Any]) -> bool:
        """Keep snapshot / provisional scaffolding roots when Graph lists only drive-backed rows."""
        if not isinstance(pl, dict):
            return False
        if destination_payload_is_planned_workspace_row(pl):
            return True
        if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_PLANNED_ONLY:
            return True
        if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL:
            return True
        if str(pl.get("row_kind") or "").strip().lower() == "cached_provisional_shell":
            return True
        return False

    def _graph_root_enrich_only_prune_unmatched(
        self,
        pl: Dict[str, Any],
        incoming_by_id: Dict[str, Dict[str, Any]],
        incoming_by_path: Dict[str, Dict[str, Any]],
    ) -> tuple[bool, str]:
        """When Graph shallow roots are known, drop enrich-only retention for authority-invalid rows."""

        if not isinstance(pl, dict) or not incoming_by_id:
            return False, ""
        if destination_payload_is_planned_workspace_row(pl):
            return False, ""
        if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_PLANNED_ONLY:
            return False, ""
        ref = next(iter(incoming_by_id.values()), None)
        inc_drive = str((ref or {}).get("drive_id") or "").strip() if isinstance(ref, dict) else ""
        row_drive = str(pl.get("drive_id") or "").strip()
        if row_drive and inc_drive and row_drive.casefold() != inc_drive.casefold():
            return True, "root_drive_mismatch_vs_graph_shallow"
        gid = str(pl.get("id") or "").strip()
        if self._merge_preserves_root_row_without_graph_id(pl):
            return False, ""
        if gid and gid not in incoming_by_id:
            pk = self._merge_root_row_path_key(pl)
            if pk and pk in incoming_by_path:
                return False, ""
            return True, "structural_root_id_not_in_graph_shallow_listing"
        return False, ""

    @staticmethod
    def _merge_root_row_path_key(pl: Dict[str, Any]) -> str:
        """Canonical key for matching a snapshot root row to an incoming Graph root by path (ids may change)."""
        if not isinstance(pl, dict):
            return ""
        raw = str(pl.get("item_path") or pl.get("semantic_path") or pl.get("destination_path") or "").strip()
        if raw:
            return normalize_manifest_path(raw)
        nm = str(pl.get("name") or "").strip()
        return normalize_manifest_path(nm) if nm else ""

    def merge_sharepoint_library_root_graph_children(
        self,
        graph_payloads: List[Dict[str, Any]],
        *,
        enrich_only: bool = False,
    ) -> Dict[str, int]:
        """Merge live Graph root children into the existing tree without resetting the model.

        Preserves planned workspace rows, updates matching rows by drive item id, removes structural
        rows missing from the Graph listing, and inserts new Graph rows in sorted order.

        When ``enrich_only`` is True (quiet startup / snapshot already visible), matching rows are
        updated and new Graph rows are inserted, but existing structural root rows are not removed
        just because the shallow root listing omitted them.
        """
        inv = QModelIndex()
        stats = {"updated": 0, "inserted": 0, "removed": 0, "skipped_planned": 0, "enrich_only": int(bool(enrich_only))}

        incoming: List[Dict[str, Any]] = [
            dict(p) for p in (graph_payloads or []) if isinstance(p, dict) and str(p.get("id") or "").strip()
        ]
        incoming.sort(key=self._sort_key_graph_root)
        incoming_by_id = {str(p["id"]).strip(): p for p in incoming}
        incoming_by_path: Dict[str, Dict[str, Any]] = {}
        for p in incoming:
            pk = self._merge_root_row_path_key(p)
            if pk and pk not in incoming_by_path:
                incoming_by_path[pk] = p

        used: set[str] = set()
        for r in range(self.rowCount(inv)):
            ix = self.index(r, 0, inv)
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            if destination_payload_is_planned_workspace_row(pl):
                stats["skipped_planned"] += 1
                continue
            if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_PLANNED_ONLY:
                stats["skipped_planned"] += 1
                continue
            gid = str(pl.get("id") or "").strip()
            if not gid:
                continue
            inc = incoming_by_id.get(gid)
            if inc is None:
                continue
            used.add(gid)
            prev_children_loaded = bool(pl.get("children_loaded")) if pl.get("is_folder") else False
            inc_copy = dict(inc)

            def mutator(payload: Dict[str, Any], _prev=prev_children_loaded, _inc=inc_copy) -> None:
                payload.update(_inc)
                payload["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                if payload.get("is_folder") and _prev:
                    payload["children_loaded"] = True

            self.update_payload_for_index(ix, mutator)
            stats["updated"] += 1

        for r in range(self.rowCount(inv) - 1, -1, -1):
            pl = self.index(r, 0, inv).data(Qt.UserRole) or {}
            if not isinstance(pl, dict):
                continue
            if destination_payload_is_planned_workspace_row(pl):
                continue
            if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_PLANNED_ONLY:
                continue
            if pl.get("placeholder"):
                role = str(pl.get("placeholder_role") or "")
                if incoming_by_id and role in ("empty_library_message", "loading_in_progress", "terminal_empty"):
                    self._remove_root_row(r)
                    stats["removed"] += 1
                continue
            gid = str(pl.get("id") or "").strip()
            if not gid:
                if incoming_by_id and not self._merge_preserves_root_row_without_graph_id(pl) and not enrich_only:
                    self._remove_root_row(r)
                    stats["removed"] += 1
                continue
            if gid not in incoming_by_id:
                pk = self._merge_root_row_path_key(pl)
                inc_path = incoming_by_path.get(pk) if pk else None
                if inc_path is not None:
                    inc_gid = str(inc_path.get("id") or "").strip()
                    if inc_gid and inc_gid not in used:
                        ix = self.index(r, 0, inv)
                        prev_children_loaded = bool(pl.get("children_loaded")) if pl.get("is_folder") else False
                        inc_copy = dict(inc_path)

                        def mutator_path(
                            payload: Dict[str, Any], _prev=prev_children_loaded, _inc=inc_copy
                        ) -> None:
                            payload.update(_inc)
                            payload["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                            if payload.get("is_folder") and _prev:
                                payload["children_loaded"] = True

                        self.update_payload_for_index(ix, mutator_path)
                        used.add(inc_gid)
                        stats["updated"] += 1
                        continue
                do_remove = not enrich_only
                prune_reason = ""
                if enrich_only:
                    do_remove, prune_reason = self._graph_root_enrich_only_prune_unmatched(
                        pl, incoming_by_id, incoming_by_path
                    )
                if do_remove:
                    log_info(
                        "destination_root_authority_merge_pruned_unmatched_root",
                        reason=str(prune_reason or "non_enrich_removal")[:120],
                        enrich_only=bool(enrich_only),
                        had_graph_incoming=bool(incoming_by_id),
                    )
                    self._remove_root_row(r)
                    stats["removed"] += 1

        for inc in incoming:
            gid = str(inc.get("id") or "").strip()
            if not gid or gid in used:
                continue
            row_ins = self._root_graph_insertion_row(inc)
            self._insert_root_child_at(row_ins, inc)
            used.add(gid)
            stats["inserted"] += 1

        self._rebuild_path_index()
        log_info(
            "destination_root_authority_merge_summary",
            updated=int(stats.get("updated", 0) or 0),
            inserted=int(stats.get("inserted", 0) or 0),
            removed=int(stats.get("removed", 0) or 0),
            skipped_planned=int(stats.get("skipped_planned", 0) or 0),
            enrich_only=bool(enrich_only),
            incoming_graph_root_count=len(incoming_by_id),
        )
        return stats

    def merge_bootstrap_cached_provisional_folder_paths(
        self,
        folder_paths: List[str],
        *,
        drive_id: str = "",
    ) -> Dict[str, int]:
        """Insert missing ``cached_provisional`` folder rows along canonical paths (legacy import / no Graph id).

        Reuses existing rows when ``find_indices_for_canonical_destination_path`` hits. Intended to run
        after a Graph root bind (or on top of a stamped snapshot shell) so allocation/projection
        overlays can attach before drive-item ids exist.
        """
        stats = {"inserted": 0, "reused": 0, "skipped": 0}
        if self._destination_index_key_fn is None:
            return stats
        did = str(drive_id or "").strip()
        uniq: List[str] = []
        seen: set[str] = set()
        for p in folder_paths or []:
            n = normalize_manifest_path(str(p or "").strip())
            if not n or n in seen:
                continue
            seen.add(n)
            uniq.append(n)
        uniq.sort(key=lambda x: (len(x.split("\\")), x.lower()))
        for raw in uniq:
            parts = [x for x in raw.replace("/", "\\").split("\\") if x]
            if not parts:
                continue
            parent_ix = QModelIndex()
            for i, seg in enumerate(parts):
                full = normalize_manifest_path("\\".join(parts[: i + 1]))
                hits = self.find_indices_for_canonical_destination_path(full)
                if hits:
                    parent_ix = hits[0]
                    stats["reused"] += 1
                    continue
                pl: Dict[str, Any] = {
                    "name": seg,
                    "base_display_label": str(seg or ""),
                    "tree_label": "Folder",
                    "is_folder": True,
                    "semantic_path": full,
                    "item_path": full,
                    "destination_path": full,
                    "tree_role": "destination",
                    "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                    "children_loaded": False,
                    "load_failed": False,
                    "drive_id": did,
                    "id": "",
                    "row_kind": "cached_provisional_shell",
                    "verification_state": "cached_provisional_shell",
                }
                self.append_child_payloads(parent_ix, [pl])
                stats["inserted"] += 1
                nh = self.find_indices_for_canonical_destination_path(full)
                if not nh:
                    stats["skipped"] += 1
                    break
                parent_ix = nh[0]
        if stats["inserted"] or stats["reused"]:
            log_info(
                "destination_model_bootstrap_folder_paths_merged",
                inserted=int(stats["inserted"]),
                reused=int(stats["reused"]),
                skipped=int(stats["skipped"]),
                path_count=len(uniq),
            )
        return stats

    def set_empty_library_message(self, text: str) -> None:
        payload = {
            "placeholder": True,
            "placeholder_role": "empty_library_message",
            "base_display_label": text,
            "tree_role": "destination",
        }
        self.beginResetModel()
        self._invisible._children = [_Node(self._invisible, 0, payload, [])]
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._notify_structure_changed()

    def replace_all_children(
        self,
        parent: QModelIndex,
        child_payloads: List[Dict[str, Any]],
        *,
        graph_child_bind: bool = False,
    ) -> None:
        parent_node = self._node(parent)
        if parent_node is None:
            return
        old_count = self.rowCount(parent)
        prov_by_id: Dict[str, Dict[str, Any]] = {}
        if graph_child_bind and old_count:
            for old_child in list(parent_node._children or []):
                opl = getattr(old_child, "payload", None)
                if not isinstance(opl, dict) or opl.get("placeholder"):
                    continue
                if str(opl.get("workspace_row_state") or "").strip() != WORKSPACE_ROW_STATE_CACHED_PROVISIONAL:
                    continue
                gid0 = str(opl.get("id") or "").strip()
                if gid0:
                    prov_by_id[gid0] = dict(opl)
        if graph_child_bind and prov_by_id:
            merged: List[Dict[str, Any]] = []
            for inc in child_payloads:
                if not isinstance(inc, dict):
                    continue
                gid = str(inc.get("id") or "").strip()
                if gid and gid in prov_by_id:
                    m = dict(prov_by_id[gid])
                    m.update(dict(inc))
                    m["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                    merged.append(m)
                else:
                    merged.append(dict(inc))
            child_payloads = merged
        if old_count:
            for old_child in list(parent_node._children or []):
                self._unregister_subtree_paths(old_child)
            self.beginRemoveRows(parent, 0, old_count - 1)
            parent_node._children = []
            self.endRemoveRows()
        n = len(child_payloads)
        if not n:
            self.beginInsertRows(parent, 0, 0)
            empty_pl = {
                "placeholder": True,
                "placeholder_role": "terminal_empty",
                "base_display_label": "This folder is empty.",
                "tree_role": "destination",
            }
            parent_node._children = [_Node(parent_node, 0, empty_pl, [])]
            self._reindex(parent_node)
            self.endInsertRows()
            self._register_subtree_paths(parent_node._children[0])
            self._notify_structure_changed()
            return
        self.beginInsertRows(parent, 0, n - 1)
        new_children: List[_Node] = []
        for i, pl in enumerate(child_payloads):
            ch = None if pl.get("is_folder") else []
            new_children.append(_Node(parent_node, i, pl, ch))
        parent_node._children = new_children
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths_from_roots(new_children)
        self._notify_structure_changed()

    def set_loading_children(self, parent: QModelIndex) -> None:
        parent_node = self._node(parent)
        if parent_node is None:
            return
        old_count = self.rowCount(parent)
        if old_count == 1:
            only = parent_node._children[0] if parent_node._children else None
            opl = getattr(only, "payload", None) if only is not None else None
            if (
                isinstance(opl, dict)
                and opl.get("placeholder")
                and str(opl.get("placeholder_role") or "") == "loading_in_progress"
            ):
                return
        if old_count:
            for old_child in list(parent_node._children or []):
                self._unregister_subtree_paths(old_child)
            self.beginRemoveRows(parent, 0, old_count - 1)
            parent_node._children = []
            self.endRemoveRows()
        load_pl = {
            "placeholder": True,
            "placeholder_role": "loading_in_progress",
            "base_display_label": "Loading...",
            "tree_role": "destination",
        }
        self.beginInsertRows(parent, 0, 0)
        parent_node._children = [_Node(parent_node, 0, load_pl, [])]
        self._reindex(parent_node)
        self.endInsertRows()
        self._notify_structure_changed()
        log_info(
            "destination_loading_placeholder_model",
            action="set",
            parent_row=parent.row(),
        )

    def remove_placeholder_children(self, parent: QModelIndex) -> None:
        if parent.isValid() and parent.column() != 0:
            parent = parent.siblingAtColumn(0)
            if not parent.isValid():
                return
        parent_node = self._invisible if not parent.isValid() else self._node(parent)
        if parent_node is None or not parent_node._children:
            return
        removed_roles: List[str] = []
        for row in range(len(parent_node._children) - 1, -1, -1):
            ch_pl = getattr(parent_node._children[row], "payload", None)
            if isinstance(ch_pl, dict) and ch_pl.get("placeholder"):
                role = str(ch_pl.get("placeholder_role") or "")
                victim = parent_node._children[row]
                self._unregister_subtree_paths(victim)
                self.beginRemoveRows(parent, row, row)
                parent_node._children.pop(row)
                self.endRemoveRows()
                if role:
                    removed_roles.append(role)
        self._reindex(parent_node)
        self._notify_structure_changed()
        if removed_roles:
            log_info(
                "destination_loading_placeholder_model",
                action="remove_placeholder",
                parent_row=parent.row(),
                roles=removed_roles,
            )

    def append_child_payloads(self, parent: QModelIndex, payloads: List[Dict[str, Any]]) -> None:
        parent_node = self._invisible if not parent.isValid() else self._node(parent)
        if parent_node is None or not payloads:
            return
        if parent_node._children is None:
            parent_node._children = []
        start = len(parent_node._children)
        n = len(payloads)
        self.beginInsertRows(parent, start, start + n - 1)
        for i, pl in enumerate(payloads):
            ch = None if pl.get("is_folder") else []
            parent_node._children.append(_Node(parent_node, start + i, pl, ch))
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths_from_roots(parent_node._children[start : start + n])
        self._notify_structure_changed()

    def update_payload_for_index(self, index: QModelIndex, mutator) -> None:
        node = self._node(index)
        if node is None:
            return
        old_snapshot = dict(node.payload)
        self._bucket_remove_node(node, old_snapshot)
        old_children = node._children
        old_child_count = len(old_children) if old_children else 0
        mutator(node.payload)
        pl = node.payload
        stripped_children = False
        if isinstance(pl, dict) and not pl.get("is_folder", True):
            if old_child_count > 0:
                self.beginRemoveRows(index, 0, old_child_count - 1)
                for c in list(old_children or []):
                    self._unregister_subtree_paths(c)
                node._children = []
                self.endRemoveRows()
                stripped_children = True
            elif old_children is None:
                node._children = []
        self._bucket_add_node(node)
        parent = index.parent()
        row = index.row()
        top_left = self.index(row, 0, parent)
        bottom_right = self.index(row, EXPLORER_COLUMN_COUNT - 1, parent)
        self.dataChanged.emit(
            top_left,
            bottom_right,
            [Qt.DisplayRole, Qt.DecorationRole, Qt.UserRole, Qt.ForegroundRole, Qt.BackgroundRole, Qt.ToolTipRole],
        )
        if stripped_children:
            self._notify_structure_changed()

    def emit_payload_changed(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        parent = index.parent()
        row = index.row()
        top_left = self.index(row, 0, parent)
        bottom_right = self.index(row, EXPLORER_COLUMN_COUNT - 1, parent)
        self.dataChanged.emit(
            top_left,
            bottom_right,
            [
                Qt.DisplayRole,
                Qt.DecorationRole,
                Qt.UserRole,
                Qt.ForegroundRole,
                Qt.BackgroundRole,
                Qt.ToolTipRole,
            ],
        )

    def find_index_by_drive_item(self, drive_id: str, item_id: str) -> QModelIndex:
        d = (drive_id or "").strip()
        iid = (item_id or "").strip()
        if not iid:
            return QModelIndex()

        def walk(par: QModelIndex) -> QModelIndex:
            rows = self.rowCount(par)
            for r in range(rows):
                ix = self.index(r, 0, par)
                node = self._node(ix)
                if node is not None and not node.is_placeholder():
                    pl = node.payload
                    nid = pl.get("id")
                    node_drive = str(pl.get("drive_id") or pl.get("library_id") or "").strip()
                    if nid == iid and (not d or not node_drive or node_drive == d):
                        return ix
                sub = walk(ix)
                if sub.isValid():
                    return sub
            return QModelIndex()

        return walk(QModelIndex())

    def iter_depth_first(self) -> List[QModelIndex]:
        out: List[QModelIndex] = []

        def walk(par: QModelIndex) -> None:
            for r in range(self.rowCount(par)):
                ix = self.index(r, 0, par)
                out.append(ix)
                walk(ix)

        walk(QModelIndex())
        return out

    def reset_nested(self, roots: List[NestedSpec]) -> None:
        self.beginResetModel()
        children: List[_Node] = []
        for i, (pl, kids) in enumerate(roots):
            children.append(self._make_nested_node(self._invisible, i, pl, kids))
        self._invisible._children = children
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._notify_structure_changed()

    def replace_row_with_nested(self, index: QModelIndex, nested: NestedSpec) -> None:
        """Replace the row at ``index`` and its entire subtree with a fresh nested spec (no full model reset)."""
        if not index.isValid():
            return
        parent_ix = index.parent()
        row = index.row()
        parent_node = self._invisible if not parent_ix.isValid() else self._node(parent_ix)
        if parent_node is None or not parent_node._children or row < 0 or row >= len(parent_node._children):
            return
        old_node = parent_node._children[row]
        self._unregister_subtree_paths(old_node)
        self.beginRemoveRows(parent_ix, row, row)
        parent_node._children.pop(row)
        self.endRemoveRows()
        self.beginInsertRows(parent_ix, row, row)
        new_node = self._make_nested_node(parent_node, row, nested[0], nested[1])
        parent_node._children.insert(row, new_node)
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths(new_node)
        self._notify_structure_changed()

    def _make_nested_node(self, parent_node: _Node, row: int, pl: Dict[str, Any], kids: List[NestedSpec]) -> _Node:
        if kids:
            node = _Node(parent_node, row, pl, [])
            ch_nodes: List[_Node] = []
            for i, kn in enumerate(kids):
                ch_nodes.append(self._make_nested_node(node, i, kn[0], kn[1]))
            node._children = ch_nodes
            return node
        if pl.get("is_folder"):
            if pl.get("_destination_expand_affordance"):
                return _Node(parent_node, row, pl, [])
            # Snapshot / authoritative bind: empty subtree is real rowCount 0, not lazy-unloaded.
            if bool(pl.get("children_loaded")):
                return _Node(parent_node, row, pl, [])
            return _Node(parent_node, row, pl, None)
        return _Node(parent_node, row, pl, [])

    def _serialize_nested_node(self, node: _Node) -> NestedSpec:
        pl = dict(node.payload)
        if node._children is None:
            return (pl, [])
        return (pl, [self._serialize_nested_node(c) for c in node._children])

    def remove_node_at(self, index: QModelIndex) -> Optional[NestedSpec]:
        if not index.isValid():
            return None
        row = index.row()
        parent_ix = index.parent()
        parent_node = self._invisible if not parent_ix.isValid() else self._node(parent_ix)
        if parent_node is None or not parent_node._children or row < 0 or row >= len(parent_node._children):
            return None
        nested = self._serialize_nested_node(parent_node._children[row])
        removed = parent_node._children[row]
        self._unregister_subtree_paths(removed)
        self.beginRemoveRows(parent_ix, row, row)
        parent_node._children.pop(row)
        self._reindex(parent_node)
        self.endRemoveRows()
        self._notify_structure_changed()
        return nested

    def append_nested_child(self, parent_ix: QModelIndex, nested: NestedSpec) -> QModelIndex:
        parent_node = self._invisible if not parent_ix.isValid() else self._node(parent_ix)
        if parent_node is None:
            return QModelIndex()
        if parent_node._children is None:
            parent_node._children = []
        row = len(parent_node._children)
        self.beginInsertRows(parent_ix, row, row)
        new_node = self._make_nested_node(parent_node, row, nested[0], nested[1])
        parent_node._children.append(new_node)
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths(new_node)
        self._notify_structure_changed()
        return self.index(row, 0, parent_ix)
