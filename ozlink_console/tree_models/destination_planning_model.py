"""
QAbstractItemModel for destination planning tree (v2 / QTreeView path).
"""

from __future__ import annotations

import time
from typing import AbstractSet, Any, Callable, Dict, List, Optional, Tuple, Set

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QBrush, QColor

from ozlink_console.hybrid_destination_preview import destination_hybrid_name_column_text
from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
    destination_payload_is_live_graph_row,
    destination_payload_is_memory_overlay_row_for_reuse,
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

# Paint-time only: whether the destination name column should show plan-leaf exclusion strikethrough.
# Delegates should prefer this over Qt.UserRole when probing exclusion, so scroll does not pull full payloads.
DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE = Qt.UserRole + 48


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
        # (structure_generation, internalPointer _Node ref, payload dict) — hot-path for Qt.UserRole col 0 only.
        self._ur0_return_cache: Optional[Tuple[int, Any, Any]] = None
        # Set by MainWindow to :class:`ozlink_console.dest_scroll_profiler.DestScrollProfiler` when enabled.
        self._dest_scroll_profiler_ref: Any = None
        # Canonical source paths in PlanLeafExclusions + path normalizer; used only for DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE.
        self._plan_leaf_exclusion_canonical_paths: Optional[AbstractSet[str]] = None
        self._canonical_source_projection_path_fn: Optional[Callable[[str], str]] = None

    def beginResetModel(self) -> None:
        self._ur0_return_cache = None
        super().beginResetModel()

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
        self._ur0_return_cache = None
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

    def _row_slot_references_node(self, n: _Node, row: int) -> bool:
        pr = n.parent
        if pr is None:
            return False
        ch = pr._children
        if not ch or row < 0 or row >= len(ch):
            return False
        return ch[row] is n

    def _internal_node_if_mounted(self, index: QModelIndex) -> Optional[_Node]:
        """
        Read internal id only when it is a structurally valid :class:`_Node` (avoids treating garbage as a node).
        :meth:`parent` must use this — not :meth:`_node` — so ``QModelIndex.parent()`` can walk the tree
        without circular calls into :meth:`_node` (path fallback uses that walk).
        """
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
        if not self._row_slot_references_node(p, int(index.row())):
            return None
        return p

    def _row_path_from_index(self, index: QModelIndex) -> Optional[Tuple[int, ...]]:
        """Build (root-to-leaf) row path via :meth:`QModelIndex.parent` (valid after :meth:`parent` stays non-circular)."""
        try:
            if not index.isValid():
                return None
            m = index.model()
            if m is not None and m is not self:
                return None
            path: List[int] = []
            cur: QModelIndex = index
            depth = 0
            while cur.isValid():
                path.append(int(cur.row()))
                cur = cur.parent()
                depth += 1
                if depth > 1_000_000:
                    return None
            path.reverse()
            return tuple(path) if path else None
        except RuntimeError:
            return None

    def _node_at_path(self, path: Tuple[int, ...]) -> Optional[_Node]:
        n: _Node = self._invisible
        for r in path:
            ch = n._children
            if ch is None or r < 0 or r >= len(ch):
                return None
            nxt = ch[int(r)]
            if not isinstance(nxt, _Node):
                return None
            n = nxt
        return n

    def _node(self, index: QModelIndex) -> Optional[_Node]:
        n0 = self._internal_node_if_mounted(index)
        if n0 is not None:
            return n0
        path = self._row_path_from_index(index)
        if not path:
            return None
        return self._node_at_path(path)

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
        node = self._internal_node_if_mounted(index)
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
        _fine = bool(prof is not None and getattr(prof, "should_record_fine_grained", lambda: False)())
        if _fine:
            _t0 = time.perf_counter()
        _prof_detail = "DestinationPlanningTreeModel.data"
        try:
            if not index.isValid():
                _prof_detail = "DestinationPlanningTreeModel.data:invalid_index"
                return None
            col = index.column()
            _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}"
            if role == DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE:
                if col != 0:
                    return None
                excl = self._plan_leaf_exclusion_canonical_paths
                canon_fn = self._canonical_source_projection_path_fn
                if not excl or not canon_fn:
                    return False
                node = self._node(index)
                if node is None or not isinstance(node, _Node):
                    return False
                p = getattr(node, "payload", None)
                if not isinstance(p, dict):
                    return False
                if p.get("is_folder") is not False:
                    return False
                canon = canon_fn(str(p.get("source_path", "") or ""))
                return bool(canon and canon in excl)
            # Qt.UserRole (=256) col 0: row payload dict; delegate + views query often — cache last resolve.
            if role == Qt.UserRole:
                if col != 0:
                    return None
                gen = int(self._structure_generation)
                c = getattr(self, "_ur0_return_cache", None)
                try:
                    ptr = index.internalPointer()
                except RuntimeError:
                    ptr = None
                if (
                    c
                    and c[0] == gen
                    and ptr is not None
                    and ptr is c[1]
                    and isinstance(ptr, _Node)
                ):
                    pl_hit = getattr(ptr, "payload", None)
                    if isinstance(pl_hit, dict) and pl_hit is c[2]:
                        return pl_hit
                node = self._node(index)
                if node is None or not isinstance(node, _Node):
                    _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}:no_node"
                    return None
                p = getattr(node, "payload", None)
                if not isinstance(p, dict):
                    _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}:no_payload"
                    return None
                self._ur0_return_cache = (gen, ptr if isinstance(ptr, _Node) else node, p)
                return p
            node = self._node(index)
            if node is None or not isinstance(node, _Node):
                _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}:no_node"
                return None
            p = getattr(node, "payload", None)
            if not isinstance(p, dict):
                _prof_detail = f"DestinationPlanningTreeModel.data:r{int(role)}:c{col}:no_payload"
                return None
            if role == Qt.DisplayRole:
                if col == 0:
                    base0 = p.get("base_display_label") or ""
                    return destination_hybrid_name_column_text(str(base0 or ""), p)
                if col == 1:
                    return explorer_size_label(p)
                if col == 2:
                    return explorer_type_label(p)
                if col == 3:
                    return explorer_date_label(p)
                return None
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

    def set_plan_leaf_exclusion_paint_contract(
        self,
        paths: Optional[AbstractSet[str]],
        canon_fn: Optional[Callable[[str], str]],
    ) -> None:
        """Wire PlanLeafExclusions + canonical source projection for DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE."""
        self._plan_leaf_exclusion_canonical_paths = frozenset(paths) if paths else None
        self._canonical_source_projection_path_fn = canon_fn
        self._emit_plan_leaf_exclusion_paint_changed()

    def _emit_plan_leaf_exclusion_paint_changed(self) -> None:
        r = DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE

        def emit_for_parent(par: QModelIndex) -> None:
            n = self.rowCount(par)
            if n <= 0:
                return
            top = self.index(0, 0, par)
            bottom = self.index(n - 1, 0, par)
            self.dataChanged.emit(top, bottom, [r])
            for ridx in range(n):
                emit_for_parent(self.index(ridx, 0, par))

        emit_for_parent(QModelIndex())

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

    def _graph_overlay_stable_reorder_siblings_in_place(
        self,
        parent_ix: QModelIndex,
        *,
        emit_model_layout: bool = True,
        apply: bool = True,
    ) -> bool:
        """
        Reorder *existing* child nodes under ``parent_ix`` in-place: live graph rows first,
        then leaf ``name`` / ``base_display_label`` (casefold). Ties use original row order (stable).

        No rows are created or removed — only sibling order. Returns whether order *would* change
        (or did change, when ``apply`` is true).
        When ``apply`` is false, only a dry run is done (no mutation, no layout signals).
        When ``apply`` is true and ``emit_model_layout`` is true, emits a single round of layout
        signals for this parent only.
        """
        parent_node = self._invisible if not parent_ix.isValid() else self._node(parent_ix)
        if parent_node is None or not parent_node._children or len(parent_node._children) < 2:
            return False
        ch = list(parent_node._children)
        indexed = list(enumerate(ch))  # (row, _Node) — tie-breaker preserves current order

        def _skey(tup: Tuple[int, _Node]) -> Tuple[int, str, int]:
            _r, n = tup
            pl = n.payload if isinstance(n.payload, dict) else {}
            pr = 0 if destination_payload_is_live_graph_row(pl) else 1
            nm = str(pl.get("name") or pl.get("base_display_label") or "").casefold()
            return (pr, nm, _r)

        srt = sorted(indexed, key=_skey)
        ch_new = [t[1] for t in srt]
        if all(ch[i] is ch_new[i] for i in range(len(ch))):
            return False
        if not apply:
            return True
        if emit_model_layout:
            self.layoutAboutToBeChanged.emit()
        parent_node._children = ch_new
        self._reindex(parent_node)
        self._ur0_return_cache = None
        if emit_model_layout:
            self.layoutChanged.emit()
        return True

    def graph_overlay_apply_stable_sibling_order_all_parents(self) -> tuple[int, int, bool]:
        """
        Reorder all sibling groups with 2+ rows using :meth:`_graph_overlay_stable_reorder_siblings_in_place`.
        A single pair of ``layoutAboutToBeChanged`` / ``layoutChanged`` is emitted if any order changed
        (after a dry pass so no layout is emitted when nothing reorders).

        Returns ``(reordered_parent_count, unchanged_parent_count, any_sibling_reorder)``.
        """
        to_check: List[QModelIndex] = []
        inv = QModelIndex()
        if int(self.rowCount(inv)) > 1:
            to_check.append(inv)
        for p_ix in list(self.iter_depth_first()):
            try:
                if int(self.rowCount(p_ix)) > 1:
                    to_check.append(p_ix)
            except Exception:
                continue
        n_reo = 0
        n_unch = 0
        if not to_check:
            return 0, 0, False
        to_apply: List[QModelIndex] = []
        for p in to_check:
            if self._graph_overlay_stable_reorder_siblings_in_place(
                p, emit_model_layout=False, apply=False
            ):
                to_apply.append(p)
        n_unch = len(to_check) - len(to_apply)
        if not to_apply:
            return 0, n_unch, False
        self.layoutAboutToBeChanged.emit()
        for p in to_apply:
            self._graph_overlay_stable_reorder_siblings_in_place(
                p, emit_model_layout=False, apply=True
            )
            n_reo += 1
        self._ur0_return_cache = None
        self.layoutChanged.emit()
        return n_reo, n_unch, True

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

    def _prune_foreign_top_level_roots_for_intended_identity(
        self,
        incoming_by_id: Dict[str, Dict[str, Any]],
        *,
        intended_drive_id: str,
        intended_site_id: str,
        strict_planned_root_identity: bool,
    ) -> tuple[int, int]:
        """Remove top-level hubs that cannot belong to the selected destination before Graph merge."""
        inv = QModelIndex()
        intended_d = str(intended_drive_id or "").strip()
        intended_s = str(intended_site_id or "").strip()
        if not intended_d and not intended_s and not strict_planned_root_identity:
            return 0, 0
        pruned = 0
        pruned_missing = 0
        for r in range(self.rowCount(inv) - 1, -1, -1):
            pl = self.index(r, 0, inv).data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            gid = str(pl.get("id") or "").strip()
            if gid and gid in incoming_by_id:
                continue
            row_drive = str(pl.get("drive_id") or "").strip()
            row_site = str(pl.get("site_id") or "").strip()
            if intended_s and row_site and intended_s.casefold() != row_site.casefold():
                self._remove_root_row(r)
                pruned += 1
                log_info(
                    "destination_foreign_top_level_hub_pruned",
                    reason="site_mismatch",
                    row_site_suffix=row_site[-16:] if len(row_site) > 16 else row_site,
                    intended_site_suffix=intended_s[-16:] if len(intended_s) > 16 else intended_s,
                )
                continue
            if intended_d and row_drive and row_drive.casefold() != intended_d.casefold():
                self._remove_root_row(r)
                pruned += 1
                log_info(
                    "destination_foreign_top_level_hub_pruned",
                    reason="drive_mismatch",
                    row_drive_suffix=row_drive[-16:] if len(row_drive) > 16 else row_drive,
                    intended_drive_suffix=intended_d[-16:] if len(intended_d) > 16 else intended_d,
                )
                continue
            scaffold = (
                destination_payload_is_planned_workspace_row(pl)
                or destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_PLANNED_ONLY
                or destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
                or self._merge_preserves_root_row_without_graph_id(pl)
            )
            if scaffold and strict_planned_root_identity and intended_d:
                if not row_drive or row_drive.casefold() != intended_d.casefold():
                    self._remove_root_row(r)
                    pruned_missing += 1
                    log_info(
                        "destination_foreign_top_level_hub_pruned_missing_identity",
                        had_row_drive=bool(row_drive),
                        intended_drive_suffix=intended_d[-16:] if len(intended_d) > 16 else intended_d,
                    )
        return pruned, pruned_missing

    def substantive_destination_folder_child_row_count(self, index: QModelIndex) -> int:
        """Child rows excluding ``loading_in_progress`` placeholders (treated as empty for load eligibility)."""
        if not index.isValid():
            return 0
        try:
            rc = int(self.rowCount(index))
        except Exception:
            return 0
        n = 0
        for r in range(rc):
            try:
                ix = self.index(r, 0, index)
            except Exception:
                continue
            if not ix.isValid():
                continue
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict):
                n += 1
                continue
            if pl.get("placeholder") and str(pl.get("placeholder_role") or "") == "loading_in_progress":
                continue
            n += 1
        return n

    @staticmethod
    def _payload_is_planning_protected_descendant_row(pl: Dict[str, Any]) -> bool:
        if not isinstance(pl, dict) or pl.get("placeholder"):
            return False
        if destination_payload_is_planned_workspace_row(pl) or destination_payload_is_memory_overlay_row_for_reuse(pl):
            return True
        if bool(pl.get("proposed")):
            return True
        gvp = str(pl.get("graph_vs_planned") or "").strip().casefold()
        if gvp in ("live_planned", "planned", "proposed"):
            return True
        if str(pl.get("workspace_row_state") or "").strip().casefold() in ("planned_only", "cached_provisional", ""):
            if pl.get("planned_allocation") or pl.get("workspace_planned_row") or pl.get("planned_allocation_descendant"):
                return True
        return False

    def count_planning_protected_descendant_rows(
        self, parent_folder_ix: QModelIndex, *, max_nodes: int = 16_000
    ) -> int:
        """Recursive count of planned/proposed/overlay rows under a folder (descendants only)."""
        if not parent_folder_ix.isValid():
            return 0
        stack: list[QModelIndex] = []
        for r in range(self.rowCount(parent_folder_ix)):
            stack.append(self.index(r, 0, parent_folder_ix))
        n = 0
        seen = 0
        while stack and seen < max_nodes:
            ix = stack.pop()
            if not ix.isValid():
                continue
            seen += 1
            pl = ix.data(Qt.UserRole) or {}
            pld = pl if isinstance(pl, dict) else {}
            if self._payload_is_planning_protected_descendant_row(pld):
                n += 1
            for r2 in range(self.rowCount(ix)):
                stack.append(self.index(r2, 0, ix))
        return n

    def folder_subtree_has_planning_protected_descendant(
        self, parent_folder_ix: QModelIndex, *, max_nodes: int = 16_000
    ) -> bool:
        return int(self.count_planning_protected_descendant_rows(parent_folder_ix, max_nodes=max_nodes) or 0) > 0

    @staticmethod
    def _direct_child_row_is_planned_union_eligible(cix: QModelIndex) -> bool:
        """True when this *direct* child of a merge parent must be preserved in Graph ⊃ planned/proposed tree."""
        try:
            pl: Any = cix.data(Qt.UserRole) or {}
        except Exception:
            pl = {}
        if not isinstance(pl, dict) or pl.get("placeholder"):
            return False
        if DestinationPlanningTreeModel._payload_is_planning_protected_descendant_row(pl):
            return True
        try:
            model = cix.model()
        except Exception:
            model = None
        if (
            model is not None
            and hasattr(model, "folder_subtree_has_planning_protected_descendant")
            and pl.get("is_folder", True)
        ):
            try:
                if model.folder_subtree_has_planning_protected_descendant(cix):  # type: ignore[union-attr]
                    return True
            except Exception:
                pass
        return False

    @staticmethod
    def _overlay_payload_strength_for_dedup(pl: Dict[str, Any]) -> int:
        gvp = str(pl.get("graph_vs_planned") or "").strip().casefold()
        is_prop = bool(pl.get("proposed")) or str(pl.get("node_origin") or "").strip().casefold() == "proposed"
        planned = destination_payload_is_planned_workspace_row(pl)
        live = destination_payload_is_live_graph_row(pl)
        live_planned = gvp == "live_planned"
        if live_planned:
            return 50
        if live and planned:
            return 45
        if live and str(pl.get("id") or "").strip():
            return 40
        if is_prop:
            return 35
        if planned:
            return 30
        wss = str(pl.get("workspace_row_state") or "").strip().casefold()
        if wss == "cached_provisional":
            return 10
        return 1

    @staticmethod
    def _nested_spec_subtree_has_protected_work(spec: "NestedSpec") -> bool:
        pl, kids = spec[0], spec[1] if len(spec) > 1 else []
        if not isinstance(pl, dict):
            return any(
                DestinationPlanningTreeModel._nested_spec_subtree_has_protected_work(c)
                for c in (kids or [])
            )
        if DestinationPlanningTreeModel._payload_is_planning_protected_descendant_row(pl):
            return True
        for c in kids or []:
            if DestinationPlanningTreeModel._nested_spec_subtree_has_protected_work(c):
                return True
        return False

    @staticmethod
    def _count_protected_rows_in_nested_spec(nested: "NestedSpec") -> int:
        pl, kids = nested[0], nested[1] if len(nested) > 1 else []
        n0 = 1 if DestinationPlanningTreeModel._payload_is_planning_protected_descendant_row(pl) else 0
        t = n0
        for c in kids or []:
            t += DestinationPlanningTreeModel._count_protected_rows_in_nested_spec(c)
        return t

    def _find_first_direct_child_by_path_cf(self, parent_ix: QModelIndex, kcf: str) -> Optional[QModelIndex]:
        if not kcf:
            return None
        if not parent_ix.isValid():
            col0 = QModelIndex()
        else:
            col0 = parent_ix.siblingAtColumn(0) if parent_ix.column() != 0 else parent_ix
        n = int(self.rowCount(col0))
        for r in range(n):
            ix = self.index(r, 0, col0)
            if not ix.isValid():
                continue
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            pkk = (self._path_key_for_payload(pl) or "").casefold()
            if pkk and pkk == kcf:
                return ix
        return None

    def _union_merge_preserved_nests(
        self,
        parent_ix: QModelIndex,
        stashed: list[NestedSpec],
        *,
        only_planning: bool = True,
    ) -> tuple[int, int, int, int]:
        """
        Re-attach stashed :class:`NestedSpec` direct children so Graph rows ∪ planning rows holds.
        Returns (matched_merged, unmatched_reinserted, reinserted_protected_node_est, max_depth_merged)
        """
        m_m, m_u, pr_ins, d_max = 0, 0, 0, 0
        for spec in list(stashed or []):
            if not isinstance(spec, tuple) or len(spec) < 1:
                continue
            if only_planning and not self._nested_spec_subtree_has_protected_work(spec):
                continue
            pl0, kids0 = spec[0], spec[1] if len(spec) > 1 else []
            if not isinstance(pl0, dict) or pl0.get("placeholder"):
                continue
            kcf = (self._path_key_for_payload(pl0) or "").casefold()
            if not kcf:
                continue
            cix = self._find_first_direct_child_by_path_cf(parent_ix, kcf)
            if cix is None or not cix.isValid():
                self.append_nested_child(parent_ix, spec)
                m_u += 1
                pr_ins += int(self._count_protected_rows_in_nested_spec(spec))
                d_max = max(d_max, 1)
                continue
            m_m += 1
            if kids0:
                km, ku, pr, d2 = self._union_merge_preserved_nests(
                    cix, list(kids0), only_planning=only_planning
                )
                m_m += km
                m_u += ku
                pr_ins += pr
                d_max = max(d_max, 1 + d2)
        return m_m, m_u, int(pr_ins), int(d_max)

    def _coalesce_direct_children_duplicate_path_keys(
        self, parent_ix: QModelIndex, *, _log_tag: str = "graph_branch_union"
    ) -> int:
        """When snapshot bugs produced duplicate same-path direct siblings, keep highest-strength; merge subtrees, drop rest."""
        if not parent_ix.isValid():
            col0 = QModelIndex()
        else:
            col0 = parent_ix.siblingAtColumn(0) if parent_ix.column() != 0 else parent_ix
        n = int(self.rowCount(col0))
        if n < 2:
            return 0
        by_cf: dict[str, list[tuple[int, int, QModelIndex]]] = {}
        for r in range(n):
            ix = self.index(r, 0, col0)
            if not ix.isValid():
                continue
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            k = (self._path_key_for_payload(pl) or "").strip()
            if not k:
                continue
            st = int(self._overlay_payload_strength_for_dedup(pl))
            by_cf.setdefault(k.casefold(), []).append((r, st, ix))
        removed = 0
        for kcf, grp0 in by_cf.items():
            if len(grp0) < 2:
                continue
            grp0.sort(key=lambda e: (-e[1], e[0]))  # strength, then row for stability
            to_drop = sorted(grp0[1:], key=lambda e: e[0], reverse=True)  # drop high rows first
            for _r, _st, ix_drop in to_drop:
                nnode = self._node(ix_drop) if ix_drop is not None and ix_drop.isValid() else None
                if nnode is None:
                    continue
                spec_full: NestedSpec = self._serialize_nested_node(nnode)
                if self.remove_node_at(ix_drop) is None:
                    continue
                removed += 1
                ch = spec_full[1] if len(spec_full) > 1 else []
                if not ch:
                    continue
                k_dest = self._find_first_direct_child_by_path_cf(parent_ix, kcf)
                if k_dest is None or not k_dest.isValid():
                    continue
                for sub in ch:
                    self._union_merge_preserved_nests(k_dest, [sub], only_planning=True)
        return removed

    def reconcile_top_level_live_graph_children_loaded_when_subtree_empty(self, *, reason: str = "unspecified") -> int:
        """Clear ``children_loaded`` on live top-level folders when the model has zero child rows.

        Snapshot / provisional shells may carry ``children_loaded=True`` without visible subtree rows.
        Skeleton first-level child load skips those rows; resetting makes them eligible after shallow root bind.
        """
        inv = QModelIndex()
        reset = 0
        for r in range(self.rowCount(inv)):
            ix = self.index(r, 0, inv)
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            if not pl.get("is_folder"):
                continue
            if not destination_payload_is_live_graph_row(pl):
                continue
            if not pl.get("children_loaded"):
                continue
            n_sub = self.substantive_destination_folder_child_row_count(ix)
            if n_sub > 0:
                continue

            def _mut(p: Dict[str, Any]) -> None:
                p["children_loaded"] = False

            self.update_payload_for_index(ix, _mut)
            reset += 1
            _iid = str(pl.get("id", "") or "")
            _did = str(pl.get("drive_id", "") or "")
            log_info(
                "destination_graph_skeleton_children_loaded_reset_for_empty_row",
                reason=str(reason)[:160],
                eligibility_tag="children_loaded_but_empty_reset",
                row=int(r),
                name_excerpt=str(pl.get("name", "") or "")[:120],
                item_id_suffix=_iid[-16:] if len(_iid) > 16 else _iid,
                drive_id_suffix=_did[-16:] if len(_did) > 16 else _did,
            )
        return reset

    def reconcile_folder_children_loaded_if_empty_subtree(self, index: QModelIndex, *, reason: str = "unspecified") -> bool:
        """Clear ``children_loaded`` for a single live folder row when it has no substantive child rows."""
        if not index.isValid():
            return False
        pl = index.data(Qt.UserRole) or {}
        if not isinstance(pl, dict) or pl.get("placeholder"):
            return False
        if not pl.get("is_folder"):
            return False
        if not destination_payload_is_live_graph_row(pl):
            return False
        if not pl.get("children_loaded"):
            return False
        n_sub = self.substantive_destination_folder_child_row_count(index)
        if n_sub > 0:
            return False

        def _mut(p: Dict[str, Any]) -> None:
            p["children_loaded"] = False

        self.update_payload_for_index(index, _mut)
        _iid = str(pl.get("id", "") or "")
        _did = str(pl.get("drive_id", "") or "")
        log_info(
            "destination_graph_skeleton_children_loaded_reset_for_empty_row",
            reason=str(reason)[:160],
            eligibility_tag="children_loaded_but_empty_reset_nested",
            name_excerpt=str(pl.get("name", "") or "")[:120],
            item_id_suffix=_iid[-16:] if len(_iid) > 16 else _iid,
            drive_id_suffix=_did[-16:] if len(_did) > 16 else _did,
        )
        return True

    def merge_sharepoint_library_root_graph_children(
        self,
        graph_payloads: List[Dict[str, Any]],
        *,
        enrich_only: bool = False,
        intended_drive_id: str = "",
        intended_site_id: str = "",
        strict_planned_root_identity: bool = False,
    ) -> Dict[str, int]:
        """Merge live Graph root children into the existing tree without resetting the model.

        Preserves planned workspace rows, updates matching rows by drive item id, removes structural
        rows missing from the Graph listing, and inserts new Graph rows in sorted order.

        When ``enrich_only`` is True (quiet startup / snapshot already visible), matching rows are
        updated and new Graph rows are inserted, but existing structural root rows are not removed
        just because the shallow root listing omitted them.
        """
        inv = QModelIndex()
        stats = {
            "updated": 0,
            "inserted": 0,
            "removed": 0,
            "skipped_planned": 0,
            "enrich_only": int(bool(enrich_only)),
            "foreign_pruned": 0,
            "foreign_pruned_missing_identity": 0,
        }

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

        fp, fpm = self._prune_foreign_top_level_roots_for_intended_identity(
            incoming_by_id,
            intended_drive_id=intended_drive_id,
            intended_site_id=intended_site_id,
            strict_planned_root_identity=strict_planned_root_identity,
        )
        stats["foreign_pruned"] = int(fp)
        stats["foreign_pruned_missing_identity"] = int(fpm)

        _audit_leaf_names: Set[str] = {"it", "marketing", "root3", "sales", "finance", "hr", "management"}
        log_info(
            "destination_graph_live_row_received",
            event="root_merge_incoming",
            graph_root_incoming_count=len(incoming),
            root_names_excerpt=",".join(
                str(p.get("name") or "")[:64] for p in incoming[:48]
            )[:2000],
        )
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
            inc_copy = dict(inc)
            _ix_match = ix

            def mutator(payload: Dict[str, Any], _inc=inc_copy, _ix=_ix_match) -> None:
                payload.update(_inc)
                payload["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                if payload.get("is_folder"):
                    try:
                        n_sub = int(self.rowCount(_ix))
                    except Exception:
                        n_sub = 0
                    # Shallow Graph library-root listing does not load folder children; never carry
                    # snapshot ``children_loaded=True`` forward unless subtree rows already exist in-model.
                    payload["children_loaded"] = bool(n_sub > 0)
                    if enrich_only and n_sub > 0:
                        payload["graph_children_verified"] = False
                        payload["needs_live_child_refresh"] = True
                        payload["destination_snapshot_cached"] = True
                    elif n_sub == 0:
                        payload["graph_children_verified"] = True
                        payload["needs_live_child_refresh"] = False
                        payload["destination_snapshot_cached"] = False

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
                        inc_copy = dict(inc_path)
                        _ix_path = ix

                        def mutator_path(payload: Dict[str, Any], _inc=inc_copy, _ix=_ix_path) -> None:
                            payload.update(_inc)
                            payload["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                            if payload.get("is_folder"):
                                try:
                                    n_sub = int(self.rowCount(_ix))
                                except Exception:
                                    n_sub = 0
                                payload["children_loaded"] = bool(n_sub > 0)
                                if enrich_only and n_sub > 0:
                                    payload["graph_children_verified"] = False
                                    payload["needs_live_child_refresh"] = True
                                    payload["destination_snapshot_cached"] = True
                                elif n_sub == 0:
                                    payload["graph_children_verified"] = True
                                    payload["needs_live_child_refresh"] = False
                                    payload["destination_snapshot_cached"] = False

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
            _nmc = str(inc.get("name") or "").strip().casefold()
            if _nmc in _audit_leaf_names or len(incoming) <= 48:
                log_info(
                    "destination_graph_live_row_inserted",
                    level="root",
                    name_excerpt=str(inc.get("name") or "")[:120],
                    path_key_excerpt=str(self._merge_root_row_path_key(inc))[:400],
                    graph_item_id_suffix=str(inc.get("id") or "")[-16:],
                )

        stats["children_loaded_reset_empty_subtree"] = int(
            self.reconcile_top_level_live_graph_children_loaded_when_subtree_empty(
                reason="merge_sharepoint_library_root_graph_children"
            )
        )

        self._rebuild_path_index()
        try:
            self._graph_root_merge_last_at_monotonic = time.monotonic()
        except Exception:
            pass
        log_info(
            "destination_graph_live_row_skipped",
            context="root_merge",
            skipped_planned_root_rows=int(stats.get("skipped_planned", 0) or 0),
            reason="planned_workspace_or_planned_only_state",
        )
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

    def merge_graph_branch_union_at_parent(
        self,
        parent: QModelIndex,
        graph_child_payloads: List[Dict[str, Any]],
        *,
        parent_canonical_path: str = "",
    ) -> Dict[str, int]:
        """
        Union-merge live Graph :meth:`/children` payloads into an *existing* parent without removing
        model rows that Graph did not return (no replace-all, no :meth:`reset_nested`).

        * Matching rows (by drive item id or full canonical child path) are upgraded in place.
        * New Graph-only children are inserted.
        * Existing planned/overlay/cached children stay unless explicitly matched to a Graph row.
        * ``QModelIndex()`` (invalid) denotes the model root: children of the document library
          (siblings under the invisible root).
        """
        if not parent.isValid():
            parent_col0 = QModelIndex()
        else:
            parent_col0 = parent.siblingAtColumn(0) if parent.column() != 0 else parent
        if parent_col0.isValid() and parent_col0.column() != 0:
            parent_col0 = parent_col0.siblingAtColumn(0)
        parent_pl = self._node(parent_col0) if parent_col0.isValid() else None
        ppl: Dict[str, Any] = (
            parent_pl.payload if parent_pl is not None and isinstance(parent_pl.payload, dict) else {}
        ) or {}
        pcan = str(parent_canonical_path or "").strip()
        if not pcan:
            pcan = str(
                ppl.get("semantic_path") or ppl.get("item_path") or ppl.get("destination_path") or ""
            ).strip()
        pcan = normalize_manifest_path(pcan) if pcan else ""

        _PRES = (
            "planning_uuid",
            "allocation_id",
            "proposed_folder_stable_id",
            "proposed",
            "workspace_planned_row",
            "planned_allocation",
            "planned_allocation_descendant",
            "request_id",
            "StableKey",
            "stable_key",
        )

        def _child_cpath(n: str) -> str:
            nm = str(n or "").strip()
            if not nm:
                return ""
            if not pcan:
                return normalize_manifest_path(nm)
            return normalize_manifest_path(f"{pcan}\\{nm}")

        log_info(
            "destination_graph_branch_union_started",
            parent_path_excerpt=pcan[:500],
            graph_incoming_count=int(len(graph_child_payloads or [])),
        )

        self.remove_placeholder_children(parent_col0)
        n_before = int(self.rowCount(parent_col0))

        by_id: Dict[str, QModelIndex] = {}
        by_path: Dict[str, QModelIndex] = {}
        for r in range(n_before):
            cix = self.index(r, 0, parent_col0)
            pl0 = cix.data(Qt.UserRole) or {}
            if not isinstance(pl0, dict):
                continue
            if pl0.get("placeholder"):
                continue
            iid0 = str(pl0.get("id") or pl0.get("graph_item_id") or "").strip()
            if iid0 and iid0 not in by_id:
                by_id[iid0] = cix
            pk0 = str(self._path_key_for_payload(pl0) or "").strip()
            if not pk0:
                pk0 = str(_child_cpath(str(pl0.get("name") or "")) or "").strip()
            if pk0:
                kcf0 = pk0.casefold()
                if kcf0 and kcf0 not in by_path:
                    by_path[kcf0] = cix  # one row per casefolded key

        n_up = 0
        n_preserved = 0
        n_matched_subtree_protected = 0
        matched_rows: Set[int] = set()
        to_insert: List[Dict[str, Any]] = []
        pre_overlay_eligible = 0
        for r_pre in range(int(n_before)):
            c_pre = self.index(int(r_pre), 0, parent_col0)
            if not c_pre.isValid():
                continue
            if self._direct_child_row_is_planned_union_eligible(c_pre):
                pre_overlay_eligible += 1
        try:
            protected_desc_before = int(
                self.count_planning_protected_descendant_rows(parent_col0) if parent_col0.isValid() else 0
            )
        except Exception:
            protected_desc_before = 0
        union_matched, union_unmatched, union_reinsert = 0, 0, 0

        for gpi0 in list(graph_child_payloads or []):
            if not isinstance(gpi0, dict):
                continue
            gpi = dict(gpi0)
            gid = str(gpi.get("id") or "").strip()
            nm = str(gpi.get("name") or "").strip()
            ch_path = _child_cpath(nm)
            cix_hit: Optional[QModelIndex] = None
            if gid and gid in by_id:
                cix_hit = by_id[gid]
            if cix_hit is None and ch_path:
                cix_hit = by_path.get(str(ch_path).casefold())
            n_desc = 0
            subtree_planning = False
            plan_desc_count = 0
            row_state_before = ""
            if cix_hit is not None and cix_hit.isValid():
                opl0 = cix_hit.data(Qt.UserRole) or {}
                if (
                    isinstance(opl0, dict)
                    and cix_hit.isValid()
                    and opl0.get("is_folder", True)
                ):
                    try:
                        n_desc = int(self.substantive_destination_folder_child_row_count(cix_hit)) or 0
                    except Exception:
                        n_desc = 0
                    try:
                        subtree_planning = bool(
                            opl0.get("is_folder", True) and self.folder_subtree_has_planning_protected_descendant(cix_hit)
                        )
                        if bool(opl0.get("is_folder", True)):
                            plan_desc_count = int(
                                self.count_planning_protected_descendant_rows(cix_hit) or 0
                            )
                    except Exception:
                        subtree_planning = False
                        plan_desc_count = 0
                opl: Dict[str, Any] = dict(opl0) if isinstance(opl0, dict) else {}
                row_state_before = str((opl0 if isinstance(opl0, dict) else {}).get("workspace_row_state") or "")[:64]

                stashed_nests: list[NestedSpec] = []
                nnode0 = self._node(cix_hit) if cix_hit.isValid() else None
                if (
                    nnode0 is not None
                    and (
                        subtree_planning
                        or int(plan_desc_count) > 0
                        or destination_payload_is_planned_workspace_row(opl)
                        or destination_payload_is_memory_overlay_row_for_reuse(opl)
                    )
                ):
                    for chn0 in list(nnode0._children or []):
                        stashed_nests.append(self._serialize_nested_node(chn0))

                def _mut(
                    p: Dict[str, Any],
                    _inc: Dict[str, Any] = gpi,
                    _old: Dict[str, Any] = opl,
                    _nd: int = n_desc,
                    _st: bool = bool(subtree_planning),
                    _pn: int = int(plan_desc_count or 0),
                ) -> None:
                    p.update(_inc)
                    if (
                        _st
                        or int(_pn) > 0
                        or destination_payload_is_planned_workspace_row(_old)
                        or destination_payload_is_memory_overlay_row_for_reuse(_old)
                        or bool(_old.get("workspace_planned_row") or _old.get("planned_allocation"))
                    ) and (bool(_old.get("is_folder", True)) or int(_nd) > 0):
                        p["is_folder"] = True
                    elif int(_nd) > 0 and (not destination_payload_is_live_graph_row(_old) or _st or int(_pn) > 0):
                        p["is_folder"] = True
                    for _k in _PRES:
                        if _k in _old and _old.get(_k) not in (None, ""):
                            p[_k] = _old[_k]
                    ovl = str(_old.get("overlay_state") or "").strip()
                    if ovl:
                        p["overlay_state"] = ovl
                    dok = str(_old.get("destination_overlay_kind") or "").strip()
                    if dok:
                        p["destination_overlay_kind"] = dok
                    p["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
                    p["row_kind"] = "live_folder" if bool(p.get("is_folder", True)) else "live_file"
                    p["verification_state"] = "live_confirmed"
                    p.pop("non_graph_structural_authority", None)
                    if p.get("is_folder", True):
                        if _nd > 0:
                            p["children_loaded"] = True
                            p["graph_children_verified"] = False
                            p["needs_live_child_refresh"] = True
                            log_info(
                                "destination_graph_branch_union_child_marked_needs_refresh",
                                child_path_excerpt=str(ch_path)[:500],
                                graph_item_id_suffix=gid[-16:] if len(gid) > 16 else gid,
                            )
                        else:
                            p["children_loaded"] = False
                            p["graph_children_verified"] = True
                            p["needs_live_child_refresh"] = False
                    else:
                        p["graph_children_verified"] = True
                        p["needs_live_child_refresh"] = False
                    if destination_payload_is_planned_workspace_row(_old) or bool(
                        _old.get("workspace_planned_row")
                        or _old.get("planned_allocation")
                    ):
                        p["graph_vs_planned"] = "live_planned"
                    else:
                        p["graph_vs_planned"] = str(
                            p.get("graph_vs_planned") or _old.get("graph_vs_planned") or "live_graph"
                        ).strip() or "live_graph"

                self.update_payload_for_index(cix_hit, _mut)  # type: ignore[union-attr, unused-ignore]
                n_up += 1
                if cix_hit.isValid():
                    try:
                        matched_rows.add(int(cix_hit.row()))
                    except Exception:
                        pass
                plan_count_after = 0
                if stashed_nests and cix_hit.isValid():
                    um, uu, upr, _d = self._union_merge_preserved_nests(
                        cix_hit, stashed_nests, only_planning=True
                    )
                    union_matched += int(um)
                    union_unmatched += int(uu)
                    union_reinsert += int(upr)
                try:
                    plan_count_after = int(
                        self.count_planning_protected_descendant_rows(cix_hit) if cix_hit.isValid() else 0
                    )
                except Exception:
                    plan_count_after = 0
                opl_after = cix_hit.data(Qt.UserRole) or {}
                row_state_after = str(
                    (opl_after if isinstance(opl_after, dict) else {}).get("workspace_row_state") or ""
                )[:64]
                is_common = (
                    destination_payload_is_planned_workspace_row(opl)
                    or (not destination_payload_is_live_graph_row(opl) and n_desc > 0)
                    or bool(subtree_planning)
                )
                if (subtree_planning or int(plan_desc_count) > 0) and ch_path:
                    n_matched_subtree_protected += 1
                    log_info(
                        "destination_graph_branch_union_preserved_planned_descendants",
                        parent_path=str(pcan)[:500],
                        graph_child_path=str(ch_path)[:500],
                        preserved_descendant_count=int(plan_count_after),
                        n_planning_protected_subtree_ref=int(plan_desc_count),
                        row_state_before=str(row_state_before)[:64],
                        row_state_after=str(row_state_after)[:64],
                    )
                log_info(
                    "destination_graph_branch_union_existing_child_upgraded",
                    child_path_excerpt=str(ch_path)[:500],
                    graph_item_id_suffix=gid[-16:] if len(gid) > 16 else gid,
                    had_planned=bool(is_common),
                )
                if is_common and ch_path:
                    log_info(
                        "destination_graph_branch_union_common_path_merged",
                        child_path_excerpt=str(ch_path)[:500],
                    )
                if n_desc or subtree_planning or int(plan_desc_count) > 0:
                    log_info(
                        "destination_graph_branch_union_planned_descendants_preserved",
                        child_path_excerpt=str(ch_path)[:500],
                        n_descendant_rows=n_desc,
                        n_planning_protected_subtree_descendants=int(plan_desc_count),
                        subtree_planning_had_protected=bool(subtree_planning),
                    )
                continue

            to_insert.append(gpi)

        for r in range(self.rowCount(parent_col0)):
            cix2 = self.index(r, 0, parent_col0)
            p2 = cix2.data(Qt.UserRole) or {}
            if not isinstance(p2, dict) or p2.get("placeholder"):
                continue
            if cix2.row() in matched_rows:
                continue
            n_preserved += 1
            n_sub = 0
            if p2.get("is_folder", True) and cix2.isValid():
                try:
                    n_sub = int(self.substantive_destination_folder_child_row_count(cix2)) or 0
                except Exception:
                    n_sub = 0
            if n_preserved <= 32:
                log_info(
                    "destination_graph_branch_union_overlay_child_preserved",
                    name_excerpt=str(p2.get("name") or "")[:120],
                    was_planned=bool(destination_payload_is_planned_workspace_row(p2)),
                    substantive_subtree_descendants=int(n_sub),
                )

        to_insert.sort(
            key=lambda p: (not p.get("is_folder", False), str(p.get("name") or "").casefold())
        )
        n_ins = 0
        if to_insert:
            self.append_child_payloads(parent_col0, to_insert)
            n_ins = len(to_insert)
            for p_ins in to_insert:
                chp = _child_cpath(str(p_ins.get("name") or ""))
                log_info(
                    "destination_graph_branch_union_live_child_inserted",
                    child_path_excerpt=str(chp)[:500],
                    name_excerpt=str(p_ins.get("name") or "")[:120],
                    graph_item_id_suffix=str(p_ins.get("id") or "")[-16:],
                )

        try:
            dedupe_removed = int(self._coalesce_direct_children_duplicate_path_keys(parent_col0))
        except Exception:
            dedupe_removed = 0
        try:
            protected_desc_after = int(
                self.count_planning_protected_descendant_rows(parent_col0) if parent_col0.isValid() else 0
            )
        except Exception:
            protected_desc_after = 0
        final_n = int(self.rowCount(parent_col0)) if parent_col0.isValid() else 0
        log_info(
            "destination_graph_branch_union_enforced_planned_overlay_union",
            parent_path=str(pcan)[:500],
            graph_child_count=int(len(list(graph_child_payloads or []))),
            existing_child_count=int(n_before),
            preserved_overlay_child_count=int(pre_overlay_eligible),
            matched_overlay_child_count=int(union_matched),
            unmatched_overlay_child_count=int(union_unmatched),
            protected_descendant_count=int(protected_desc_after),
            protected_descendant_count_before_merge=int(protected_desc_before),
            reinserted_protected_nodes_est=int(union_reinsert),
            final_child_count=int(final_n),
            duplicate_path_rows_coalesced=int(dedupe_removed),
        )

        self._rebuild_path_index()
        _overlay_preserved = int(n_preserved) + int(n_matched_subtree_protected) + int(union_unmatched)
        log_info(
            "destination_graph_branch_union_completed",
            parent_path_excerpt=pcan[:500],
            inserted=int(n_ins),
            upgraded=int(n_up),
            overlay_preserved=int(_overlay_preserved),
            overlay_preserved_unmatched_graph_children=int(n_preserved),
            overlay_preserved_matched_protected_subtrees=int(n_matched_subtree_protected),
            union_merge_matched_child_paths=int(union_matched),
            union_merge_reinserted_full_rows=int(union_unmatched),
        )
        return {
            "inserted": n_ins,
            "upgraded": n_up,
            "preserved": n_preserved,
            "matched_subtree_protected": int(n_matched_subtree_protected),
            "union_matched": int(union_matched),
            "union_unmatched_reinserted": int(union_unmatched),
        }

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
        # Graph /children inserts new folder rows at any depth: ensure workspace state so expand/lazy-load
        # eligibility (reconcile merge-target predicates) is not lost on rows that did not merge into a
        # cached_provisional shell.
        if graph_child_bind:
            for pl in child_payloads:
                if not isinstance(pl, dict) or not pl.get("is_folder"):
                    continue
                if str(pl.get("id") or "").strip() and not str(pl.get("workspace_row_state") or "").strip():
                    pl["workspace_row_state"] = WORKSPACE_ROW_STATE_LIVE_CONFIRMED
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
