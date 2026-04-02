"""
QAbstractItemModel for destination planning tree (v2 / QTreeView path).
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QBrush

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
        return bool(self.payload.get("placeholder"))

    def is_folder(self) -> bool:
        return not self.is_placeholder() and bool(self.payload.get("is_folder"))


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
            return p if col == 0 else None
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
        if node and node.is_placeholder():
            return Qt.NoItemFlags
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
        return bool(node.payload.get("_destination_expand_affordance"))

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

    def _bucket_add_node(self, node: _Node) -> None:
        if self._destination_index_key_fn is None or node.is_placeholder():
            return
        k = self._path_key_for_payload(node.payload)
        if not k:
            return
        lst = self._path_to_nodes.setdefault(k, [])
        if node not in lst:
            lst.append(node)

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
        self.endResetModel()
        self.destination_structure_changed.emit()

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
        self.destination_structure_changed.emit()

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
        self.destination_structure_changed.emit()

    def replace_all_children(self, parent: QModelIndex, child_payloads: List[Dict[str, Any]]) -> None:
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
            self.destination_structure_changed.emit()
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
        self.destination_structure_changed.emit()

    def set_loading_children(self, parent: QModelIndex) -> None:
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
            "tree_role": "destination",
        }
        self.beginInsertRows(parent, 0, 0)
        parent_node._children = [_Node(parent_node, 0, load_pl, [])]
        self._reindex(parent_node)
        self.endInsertRows()
        self.destination_structure_changed.emit()

    def remove_placeholder_children(self, parent: QModelIndex) -> None:
        parent_node = self._node(parent)
        if parent_node is None or not parent_node._children:
            return
        for row in range(len(parent_node._children) - 1, -1, -1):
            if parent_node._children[row].payload.get("placeholder"):
                victim = parent_node._children[row]
                self._unregister_subtree_paths(victim)
                self.beginRemoveRows(parent, row, row)
                parent_node._children.pop(row)
                self.endRemoveRows()
        self._reindex(parent_node)
        self.destination_structure_changed.emit()

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
        self.destination_structure_changed.emit()

    def update_payload_for_index(self, index: QModelIndex, mutator) -> None:
        node = self._node(index)
        if node is None:
            return
        old_snapshot = dict(node.payload)
        self._bucket_remove_node(node, old_snapshot)
        mutator(node.payload)
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
        self.destination_structure_changed.emit()

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
        self.destination_structure_changed.emit()
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
        self.destination_structure_changed.emit()
        return self.index(row, 0, parent_ix)
