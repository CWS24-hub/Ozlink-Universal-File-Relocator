"""
QAbstractItemModel for the source SharePoint tree (v2 / QTreeView path).

Rows store the same payload dicts historically attached to QTreeWidgetItem UserRole.
Unexpanded folders use ``_children is None`` and report ``rowCount`` 0 so Qt draws an
expand affordance without materializing child rows.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt
from PySide6.QtGui import QBrush

from ozlink_console.logger import log_info
from ozlink_console.tree_models.explorer_columns import (
    EXPLORER_COLUMN_COUNT,
    EXPLORER_COLUMN_LABELS,
    explorer_date_label,
    explorer_icon_for_node,
    explorer_size_label,
    explorer_type_label,
)


class _Node:
    __slots__ = ("parent", "row", "payload", "_children")

    def __init__(
        self,
        parent: Optional["_Node"],
        row: int,
        payload: Dict[str, Any],
        children: Optional[List["_Node"]],
    ):
        self.parent = parent
        self.row = row
        self.payload = payload
        self._children = children  # None => folder not yet populated; list => loaded (maybe placeholders)

    def is_placeholder(self) -> bool:
        return bool(self.payload.get("placeholder"))

    def is_folder(self) -> bool:
        if self.is_placeholder():
            return False
        return bool(self.payload.get("is_folder"))


class SharePointSourceTreeModel(QAbstractItemModel):
    def __init__(self, parent=None, column_labels=None, source_index_key_fn: Optional[Callable[[Dict[str, Any]], str]] = None):
        super().__init__(parent)
        labels = list(column_labels) if column_labels else list(EXPLORER_COLUMN_LABELS)
        while len(labels) < EXPLORER_COLUMN_COUNT:
            labels.append(EXPLORER_COLUMN_LABELS[len(labels)])
        self._column_labels = labels[:EXPLORER_COLUMN_COUNT]
        self._invisible = _Node(None, -1, {}, [])
        self._invisible._children = []
        # Canonical path key -> node (O(1) lookup for find_visible_source_item_by_path when fn is set).
        self._source_index_key_fn = source_index_key_fn
        self._path_to_node: Dict[str, _Node] = {}
        self._structure_generation: int = 0

    def _bump_structure_generation(self) -> None:
        self._structure_generation += 1

    def structure_generation(self) -> int:
        return int(self._structure_generation)

    def is_index_live(self, index: QModelIndex) -> bool:
        """True if ``index`` still points at a row attached under its parent (safe for model mutations)."""
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

    def _node(self, index: QModelIndex) -> Optional[_Node]:
        if not index.isValid():
            return None
        return index.internalPointer()

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
        return self.createIndex(row, column, ch[row])

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
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self._column_labels):
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
        node = self._node(index)
        if node is None:
            return None
        p = node.payload
        col = index.column()
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
            if col != 0:
                return None
            return p
        if role == Qt.ForegroundRole:
            c = p.get("_model_foreground")
            return QBrush(c) if c is not None else None
        if role == Qt.BackgroundRole:
            c = p.get("_model_background")
            return QBrush(c) if c is not None else None
        if role == Qt.ToolTipRole:
            tip = p.get("_model_tooltip")
            return tip if tip else None
        if role == Qt.DecorationRole and col == 0:
            return explorer_icon_for_node(p)
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags
        node = self._node(index)
        if node and node.is_placeholder() and node.payload.get("placeholder_role") == "empty_library_message":
            return Qt.NoItemFlags
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def hasChildren(self, parent: QModelIndex) -> bool:
        if not parent.isValid():
            return len(self._invisible._children) > 0
        node = self._node(parent)
        if node is None or node.is_placeholder():
            return False
        if not node.is_folder():
            return False
        if node._children is None:
            return True
        return len(node._children) > 0

    def _reindex(self, parent_node: _Node) -> None:
        ch = parent_node._children or []
        for i, c in enumerate(ch):
            c.row = i
            c.parent = parent_node

    def _path_key_for_payload(self, payload: Dict[str, Any]) -> str:
        fn = self._source_index_key_fn
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

    def _unregister_subtree_paths(self, node: _Node) -> None:
        if self._source_index_key_fn is None:
            return
        for n in self._iter_subtree_nodes(node):
            if n.is_placeholder():
                continue
            k = self._path_key_for_payload(n.payload)
            if k and self._path_to_node.get(k) is n:
                del self._path_to_node[k]

    def _register_subtree_paths(self, node: _Node) -> None:
        if self._source_index_key_fn is None:
            return
        for n in self._iter_subtree_nodes(node):
            if n.is_placeholder():
                continue
            k = self._path_key_for_payload(n.payload)
            if k:
                self._path_to_node[k] = n

    def _rebuild_path_index(self) -> None:
        self._path_to_node.clear()
        if self._source_index_key_fn is None:
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

    def find_index_for_canonical_source_path(self, canonical_key: str) -> QModelIndex:
        if not canonical_key or self._source_index_key_fn is None:
            return QModelIndex()
        node = self._path_to_node.get(canonical_key)
        if node is None:
            return QModelIndex()
        return self._index_for_node(node)

    def clear(self) -> None:
        self.beginResetModel()
        self._invisible._children = []
        self._path_to_node.clear()
        self.endResetModel()
        self._bump_structure_generation()

    def reset_root_payloads(self, payloads: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        children: List[_Node] = []
        for i, pl in enumerate(payloads):
            if pl.get("is_folder"):
                ch: Optional[List[_Node]] = None
            else:
                ch = []
            children.append(_Node(self._invisible, i, pl, ch))
        self._invisible._children = children
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._bump_structure_generation()

    def set_empty_library_message(self, text: str) -> None:
        payload = {
            "placeholder": True,
            "placeholder_role": "empty_library_message",
            "base_display_label": text,
            "tree_role": "source",
        }
        self.beginResetModel()
        self._invisible._children = [_Node(self._invisible, 0, payload, [])]
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._bump_structure_generation()

    def replace_all_children(
        self,
        parent: QModelIndex,
        child_payloads: List[Dict[str, Any]],
        *,
        log_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Remove existing rows under ``parent`` and insert new child nodes from payloads."""
        ctx = dict(log_context) if log_context else {}
        gen = self.structure_generation()
        if not self.is_index_live(parent):
            log_info(
                "source_replace_children_invalid_index",
                model_generation=gen,
                child_count=len(child_payloads),
                **{
                    k: ctx[k]
                    for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                    if k in ctx
                },
            )
            return
        parent_node = self._node(parent)
        if parent_node is None:
            log_info(
                "source_replace_children_skip_stale_parent",
                reason="parent_node_none_after_liveness",
                model_generation=gen,
                child_count=len(child_payloads),
                **{
                    k: ctx[k]
                    for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                    if k in ctx
                },
            )
            return
        old_count = self.rowCount(parent)
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
                "tree_role": "source",
            }
            parent_node._children = [_Node(parent_node, 0, empty_pl, [])]
            self._reindex(parent_node)
            self.endInsertRows()
            self._register_subtree_paths(parent_node._children[0])
            self._bump_structure_generation()
            log_info(
                "source_replace_children_complete",
                model_generation=self.structure_generation(),
                child_count=1,
                mode="empty_placeholder",
                **{
                    k: ctx[k]
                    for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                    if k in ctx
                },
            )
            return
        self.beginInsertRows(parent, 0, n - 1)
        new_children: List[_Node] = []
        for i, pl in enumerate(child_payloads):
            if pl.get("is_folder"):
                ch = None
            else:
                ch = []
            new_children.append(_Node(parent_node, i, pl, ch))
        parent_node._children = new_children
        self._reindex(parent_node)
        self.endInsertRows()
        for c in new_children:
            self._register_subtree_paths(c)
        self._bump_structure_generation()
        log_info(
            "source_replace_children_complete",
            model_generation=self.structure_generation(),
            child_count=n,
            mode="payloads",
            **{
                k: ctx[k]
                for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                if k in ctx
            },
        )

    def set_loading_children(self, parent: QModelIndex) -> None:
        if not self.is_index_live(parent):
            return
        parent_node = self._node(parent)
        if parent_node is None:
            return
        old_count = self.rowCount(parent)
        if old_count:
            for old_child in list(parent_node._children or []):
                self._unregister_subtree_paths(old_child)
            self.beginRemoveRows(parent, 0, old_count - 1)
            parent_node._children = []
            self.endRemoveRows()
        load_pl = {
            "placeholder": True,
            "placeholder_role": "loading_in_progress",
            "base_display_label": "Loading folder contents...",
            "tree_role": "source",
        }
        self.beginInsertRows(parent, 0, 0)
        parent_node._children = [_Node(parent_node, 0, load_pl, [])]
        self._reindex(parent_node)
        self.endInsertRows()
        self._bump_structure_generation()

    def update_payload_for_index(self, index: QModelIndex, mutator) -> None:
        node = self._node(index)
        if node is None:
            return
        old_snapshot = dict(node.payload)
        old_key = self._path_key_for_payload(old_snapshot)
        mutator(node.payload)
        new_key = self._path_key_for_payload(node.payload)
        if self._source_index_key_fn is not None:
            if old_key and self._path_to_node.get(old_key) is node:
                del self._path_to_node[old_key]
            if new_key and not node.is_placeholder():
                self._path_to_node[new_key] = node
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

        def walk(parent: QModelIndex) -> QModelIndex:
            rows = self.rowCount(parent)
            for r in range(rows):
                ix = self.index(r, 0, parent)
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

        def walk(parent: QModelIndex) -> None:
            for r in range(self.rowCount(parent)):
                ix = self.index(r, 0, parent)
                out.append(ix)
                walk(ix)

        walk(QModelIndex())
        return out
