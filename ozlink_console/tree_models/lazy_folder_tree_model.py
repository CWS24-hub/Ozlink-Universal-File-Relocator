"""
Lazily populated QAbstractItemModel for QTreeView.

v2 direction: replace QTreeWidgetItem forests with a view that only instantiates
visible rows. This model uses the standard Qt pattern rowCount=0 + canFetchMore +
fetchMore for folder nodes, so children are loaded when the user expands (or when
code calls fetchMore explicitly).

`load_children(path)` receives a tuple of display name segments (top-level names
only under the invisible root) and returns rows as (noun, name) e.g. ("Folder","X").
"""

from __future__ import annotations

from typing import Callable, List, Optional, Sequence, Tuple

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt

ChildRow = Tuple[str, str]  # noun ("Folder"|"File"), display name (no prefix in name)
LoadChildrenFn = Callable[[Tuple[str, ...]], Sequence[ChildRow]]


class _Node:
    __slots__ = ("parent", "row", "noun", "name", "_children")

    def __init__(
        self,
        parent: Optional["_Node"],
        row: int,
        noun: str,
        name: str,
    ):
        self.parent = parent
        self.row = row
        self.noun = noun
        self.name = name
        # None => not yet fetched (only meaningful for Folder); [] => fetched empty; non-empty => loaded
        self._children: Optional[List["_Node"]] = None

    def path(self) -> Tuple[str, ...]:
        if self.parent is None:
            return ()
        if self.parent.parent is None:
            # Parent is the invisible root; this is a top-level row.
            return (self.name,)
        return self.parent.path() + (self.name,)

    def display_text(self) -> str:
        return f"{self.noun}: {self.name}"

    def is_folder(self) -> bool:
        return self.noun == "Folder"


class LazyFolderTreeModel(QAbstractItemModel):
    """Single-column tree with lazy child loading via fetchMore."""

    def __init__(
        self,
        load_children: Optional[LoadChildrenFn] = None,
        parent=None,
    ):
        super().__init__(parent)
        self._load_children: LoadChildrenFn = load_children or (lambda _path: ())
        # Invisible root (not shown); its children are top-level rows.
        self._invisible = _Node(None, -1, "Root", "")
        self._invisible._children = []

    def set_load_children(self, fn: LoadChildrenFn) -> None:
        self._load_children = fn

    def set_top_level(self, rows: Sequence[ChildRow]) -> None:
        """Replace all content. Each row is (noun, name)."""
        self.beginResetModel()
        self._invisible._children = []
        for i, (noun, name) in enumerate(rows):
            node = _Node(self._invisible, i, noun, name)
            if node.is_folder():
                node._children = None
            else:
                node._children = []
            self._invisible._children.append(node)
        self._reindex_children(self._invisible)
        self.endResetModel()

    def clear(self) -> None:
        self.beginResetModel()
        self._invisible._children = []
        self.endResetModel()

    @staticmethod
    def _reindex_children(parent: _Node) -> None:
        children = parent._children or []
        for i, ch in enumerate(children):
            ch.row = i
            ch.parent = parent

    def _node_from_index(self, index: QModelIndex) -> Optional[_Node]:
        if not index.isValid():
            return None
        return index.internalPointer()

    def index(self, row: int, column: int, parent: QModelIndex) -> QModelIndex:
        if column != 0 or row < 0:
            return QModelIndex()
        parent_node = self._invisible
        if parent.isValid():
            p = self._node_from_index(parent)
            if p is None:
                return QModelIndex()
            if not p._children:
                return QModelIndex()
            parent_node = p
        children = parent_node._children or []
        if row >= len(children):
            return QModelIndex()
        return self.createIndex(row, 0, children[row])

    def parent(self, index: QModelIndex) -> QModelIndex:
        if not index.isValid():
            return QModelIndex()
        node = self._node_from_index(index)
        if node is None or node.parent is None:
            return QModelIndex()
        parent_node = node.parent
        if parent_node.parent is None:
            return QModelIndex()
        gp = parent_node.parent
        children = gp._children or []
        try:
            row = children.index(parent_node)
        except ValueError:
            return QModelIndex()
        return self.createIndex(row, 0, parent_node)

    def rowCount(self, parent: QModelIndex) -> int:
        if parent.column() > 0:
            return 0
        if not parent.isValid():
            return len(self._invisible._children or [])
        node = self._node_from_index(parent)
        if node is None or not node.is_folder():
            return 0
        ch = node._children
        if ch is None:
            return 0
        return len(ch)

    def columnCount(self, parent: QModelIndex) -> int:
        return 1

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        node = self._node_from_index(index)
        if node is None:
            return None
        if role == Qt.DisplayRole:
            return node.display_text()
        if role == Qt.FontRole:
            return None
        return None

    def hasChildren(self, parent: QModelIndex) -> bool:
        if not parent.isValid():
            return len(self._invisible._children or []) > 0
        node = self._node_from_index(parent)
        if node is None or not node.is_folder():
            return False
        if node._children is None:
            return True
        return len(node._children) > 0

    def canFetchMore(self, parent: QModelIndex) -> bool:
        if not parent.isValid():
            return False
        node = self._node_from_index(parent)
        if node is None or not node.is_folder():
            return False
        return node._children is None

    def fetchMore(self, parent: QModelIndex) -> None:
        if not parent.isValid():
            return
        node = self._node_from_index(parent)
        if node is None or not node.is_folder() or node._children is not None:
            return
        rows = list(self._load_children(node.path()))
        if not rows:
            node._children = []
            return
        self.beginInsertRows(parent, 0, len(rows) - 1)
        children: List[_Node] = []
        for i, (noun, name) in enumerate(rows):
            ch = _Node(node, i, noun, name)
            if ch.is_folder():
                ch._children = None
            else:
                ch._children = []
            children.append(ch)
        node._children = children
        self._reindex_children(node)
        self.endInsertRows()
