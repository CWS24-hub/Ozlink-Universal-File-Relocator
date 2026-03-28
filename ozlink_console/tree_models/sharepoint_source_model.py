"""
QAbstractItemModel for the source SharePoint tree (v2 / QTreeView path).

Rows store the same payload dicts historically attached to QTreeWidgetItem UserRole.
Unexpanded folders use ``_children is None`` and report ``rowCount`` 0 so Qt draws an
expand affordance without materializing child rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt
from PySide6.QtGui import QBrush


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
    def __init__(self, parent=None):
        super().__init__(parent)
        self._invisible = _Node(None, -1, {}, [])
        self._invisible._children = []

    def _node(self, index: QModelIndex) -> Optional[_Node]:
        if not index.isValid():
            return None
        return index.internalPointer()

    def index(self, row: int, column: int, parent: QModelIndex) -> QModelIndex:
        if column != 0 or row < 0:
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
        return self.createIndex(row, 0, ch[row])

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

    def rowCount(self, parent: QModelIndex) -> int:
        if parent.column() > 0:
            return 0
        if not parent.isValid():
            return len(self._invisible._children)
        node = self._node(parent)
        if node is None or node._children is None:
            return 0
        return len(node._children)

    def columnCount(self, parent: QModelIndex) -> int:
        return 1

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        node = self._node(index)
        if node is None:
            return None
        p = node.payload
        if role == Qt.DisplayRole:
            return p.get("base_display_label") or ""
        if role == Qt.UserRole:
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

    def clear(self) -> None:
        self.beginResetModel()
        self._invisible._children = []
        self.endResetModel()

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

    def replace_all_children(self, parent: QModelIndex, child_payloads: List[Dict[str, Any]]) -> None:
        """Remove existing rows under ``parent`` and insert new child nodes from payloads."""
        parent_node = self._node(parent)
        if parent_node is None:
            return
        old_count = self.rowCount(parent)
        if old_count:
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

    def set_loading_children(self, parent: QModelIndex) -> None:
        parent_node = self._node(parent)
        if parent_node is None:
            return
        old_count = self.rowCount(parent)
        if old_count:
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

    def update_payload_for_index(self, index: QModelIndex, mutator) -> None:
        node = self._node(index)
        if node is None:
            return
        mutator(node.payload)
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.UserRole, Qt.ForegroundRole, Qt.BackgroundRole, Qt.ToolTipRole])

    def emit_payload_changed(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.UserRole, Qt.ForegroundRole, Qt.BackgroundRole, Qt.ToolTipRole])

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
