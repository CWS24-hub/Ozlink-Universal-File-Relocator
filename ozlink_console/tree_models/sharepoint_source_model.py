"""
QAbstractItemModel for the source SharePoint tree (v2 / QTreeView path).

Rows store the same payload dicts historically attached to QTreeWidgetItem UserRole.
Unexpanded folders use ``_children is None`` and report ``rowCount`` 0 so Qt draws an
expand affordance without materializing child rows.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

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
        # Case-insensitive lookup (planning memory vs Graph payload casing).
        self._path_to_node_ci: Dict[str, _Node] = {}
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

    def _path_register_node(self, key: str, node: _Node) -> None:
        if not key or self._source_index_key_fn is None:
            return
        self._path_to_node[key] = node
        self._path_to_node_ci[key.casefold()] = node

    def _path_unregister_node(self, key: str, node: _Node) -> None:
        if not key or self._source_index_key_fn is None:
            return
        if self._path_to_node.get(key) is node:
            del self._path_to_node[key]
        cf = key.casefold()
        if self._path_to_node_ci.get(cf) is node:
            del self._path_to_node_ci[cf]

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
            if k:
                self._path_unregister_node(k, n)

    def _register_subtree_paths(self, node: _Node) -> None:
        if self._source_index_key_fn is None:
            return
        for n in self._iter_subtree_nodes(node):
            if n.is_placeholder():
                continue
            k = self._path_key_for_payload(n.payload)
            if k:
                self._path_register_node(k, n)

    def _rebuild_path_index(self) -> None:
        self._path_to_node.clear()
        self._path_to_node_ci.clear()
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
        if node is None and canonical_key:
            node = self._path_to_node_ci.get(canonical_key.casefold())
        if node is None:
            return QModelIndex()
        return self._index_for_node(node)

    def clear(self) -> None:
        self.beginResetModel()
        self._invisible._children = []
        self._path_to_node.clear()
        self._path_to_node_ci.clear()
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
        if ctx.get("replace_reason"):
            log_info(
                "source_replace_all_children_destructive",
                reason=str(ctx.get("replace_reason") or ""),
                child_count=len(child_payloads),
                **{
                    k: ctx[k]
                    for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                    if k in ctx
                },
            )
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
            if old_key:
                self._path_unregister_node(old_key, node)
            if new_key and not node.is_placeholder():
                self._path_register_node(new_key, node)
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

    def count_snapshot_shell_nodes_depth_first(self) -> int:
        """All visible rows including placeholders (forensic shell size)."""
        return len(self.iter_depth_first())

    @staticmethod
    def _merge_root_row_path_key(pl: Dict[str, Any]) -> str:
        raw = str(pl.get("item_path") or pl.get("semantic_path") or pl.get("display_path") or "").strip()
        return raw.replace("/", "\\").casefold()

    @staticmethod
    def _normalize_source_drive_item_key(pl: Dict[str, Any], default_drive: str = "") -> Tuple[str, str]:
        did = str(pl.get("drive_id") or pl.get("library_id") or "").strip()
        if not did:
            did = str(default_drive or "").strip()
        iid = str(pl.get("id") or "").strip()
        return (did, iid)

    def _folder_child_path_fallback_key(self, pl: Dict[str, Any]) -> str:
        k = self._path_key_for_payload(pl)
        if k:
            return k.casefold()
        return self._merge_root_row_path_key(pl)

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
        self._bump_structure_generation()

    def _insert_root_child_at(self, row: int, pl: Dict[str, Any]) -> None:
        parent_node = self._invisible
        ch = list(parent_node._children or [])
        row = max(0, min(int(row), len(ch)))
        child_list: Optional[List[_Node]] = None if pl.get("is_folder") else []
        inv = QModelIndex()
        self.beginInsertRows(inv, row, row)
        node = _Node(parent_node, row, dict(pl), child_list)
        ch.insert(row, node)
        parent_node._children = ch
        self._reindex(parent_node)
        self.endInsertRows()
        self._register_subtree_paths(node)
        self._bump_structure_generation()

    @staticmethod
    def _root_graph_sort_key(pl: Dict[str, Any]) -> Tuple[bool, str]:
        return (not bool(pl.get("is_folder")), str(pl.get("name") or "").lower())

    def _root_graph_insertion_row(self, incoming_pl: Dict[str, Any]) -> int:
        inv = QModelIndex()
        sk = self._root_graph_sort_key(incoming_pl)
        n = self.rowCount(inv)
        for r in range(n):
            pl = self.index(r, 0, inv).data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            if self._root_graph_sort_key(pl) > sk:
                return r
        return n

    def mount_from_session_snapshot_roots(self, snapshot_roots: List[Dict[str, Any]]) -> int:
        """Replace the model with a recursive snapshot shell (Phase 1 metadata cache).

        Serialized folder rows with no ``children`` use ``_children is None`` unless
        ``children_loaded`` is true (materialized empty).
        """
        roots: List[_Node] = []
        total = 0
        for i, snap in enumerate(snapshot_roots or []):
            node = self._node_from_snapshot_branch(snap if isinstance(snap, dict) else {}, self._invisible, i)
            if node is not None:
                roots.append(node)
                total += self._count_subtree_nodes(node)
        self.beginResetModel()
        self._invisible._children = roots
        self._reindex(self._invisible)
        self.endResetModel()
        self._rebuild_path_index()
        self._bump_structure_generation()
        return total

    def _count_subtree_nodes(self, node: _Node) -> int:
        n = 1
        ch = node._children
        if not ch:
            return n
        for c in ch:
            n += self._count_subtree_nodes(c)
        return n

    def _node_from_snapshot_branch(self, snap: Dict[str, Any], parent: _Node, row: int) -> Optional[_Node]:
        data = dict((snap or {}).get("data") or {})
        if not data:
            return None
        is_ph = bool(data.get("placeholder"))
        if not is_ph:
            data["source_shell_provisional"] = True
        ch_snaps = list((snap or {}).get("children") or [])
        is_folder = bool(data.get("is_folder")) and not is_ph
        if not is_folder:
            return _Node(parent, row, data, [])
        if not ch_snaps:
            cl = bool(data.get("children_loaded"))
            inner: Optional[List[_Node]] = [] if cl else None
            return _Node(parent, row, data, inner)
        pl = dict(data)
        pl["children_loaded"] = True
        folder_node = _Node(parent, row, pl, [])
        children: List[_Node] = []
        for j, c_snap in enumerate(ch_snaps):
            cn = self._node_from_snapshot_branch(c_snap if isinstance(c_snap, dict) else {}, folder_node, j)
            if cn is not None:
                children.append(cn)
        folder_node._children = children
        return folder_node

    @staticmethod
    def _source_shell_row_removal_allowed(pl: Dict[str, Any], *, enrich_only: bool) -> bool:
        if enrich_only:
            return False
        if pl.get("source_shell_provisional"):
            return False
        return True

    def merge_sharepoint_source_root_graph_children(
        self,
        graph_payloads: List[Dict[str, Any]],
        *,
        enrich_only: bool = False,
        default_drive_id: str = "",
    ) -> Dict[str, int]:
        """Merge live Graph root children into the tree without resetting the model (startup shell).

        Rows match on ``(drive_id, item_id)`` first; path keys are a fallback when ids align after renames.
        Snapshot ``source_shell_provisional`` rows are retained when missing from the Graph listing unless
        ``enrich_only`` is used to suppress all structural removals (quiet overlay).
        """
        inv = QModelIndex()
        dd = str(default_drive_id or "").strip()
        before_count = self.count_snapshot_shell_nodes_depth_first()
        stats = {
            "updated": 0,
            "inserted": 0,
            "removed": 0,
            "matched": 0,
            "enrich_only": int(bool(enrich_only)),
        }
        incoming: List[Dict[str, Any]] = []
        for p in graph_payloads or []:
            if not isinstance(p, dict):
                continue
            pl = dict(p)
            iid = str(pl.get("id") or "").strip()
            if not iid:
                continue
            if dd and not str(pl.get("drive_id") or pl.get("library_id") or "").strip():
                pl["drive_id"] = dd
            incoming.append(pl)
        incoming.sort(key=self._root_graph_sort_key)
        incoming_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for p in incoming:
            k = self._normalize_source_drive_item_key(p, dd)
            if k[1]:
                incoming_by_key[k] = p
        incoming_by_path: Dict[str, Dict[str, Any]] = {}
        for p in incoming:
            pk = self._merge_root_row_path_key(p)
            if pk and pk not in incoming_by_path:
                incoming_by_path[pk] = p

        used_key: set[Tuple[str, str]] = set()
        for r in range(self.rowCount(inv)):
            ix = self.index(r, 0, inv)
            pl = ix.data(Qt.UserRole) or {}
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            ek = self._normalize_source_drive_item_key(pl, dd)
            inc = incoming_by_key.get(ek)
            if inc is None and ek[1] and dd:
                inc = incoming_by_key.get((dd, ek[1]))
            if inc is None:
                continue
            used_key.add(self._normalize_source_drive_item_key(inc, dd))
            prev_children_loaded = bool(pl.get("children_loaded")) if pl.get("is_folder") else False
            inc_copy = dict(inc)

            def mutator(payload: Dict[str, Any], _prev=prev_children_loaded, _inc=inc_copy) -> None:
                payload.update(_inc)
                payload.pop("source_shell_provisional", None)
                if payload.get("is_folder") and _prev:
                    payload["children_loaded"] = True

            self.update_payload_for_index(ix, mutator)
            stats["updated"] += 1
            stats["matched"] += 1

        for r in range(self.rowCount(inv) - 1, -1, -1):
            pl = self.index(r, 0, inv).data(Qt.UserRole) or {}
            if not isinstance(pl, dict):
                continue
            if pl.get("placeholder"):
                role = str(pl.get("placeholder_role") or "")
                if incoming and incoming_by_key and role in ("empty_library_message", "loading_in_progress", "terminal_empty"):
                    self._remove_root_row(r)
                    stats["removed"] += 1
                continue
            gid = str(pl.get("id") or "").strip()
            if not gid:
                if incoming_by_key and self._source_shell_row_removal_allowed(pl, enrich_only=enrich_only):
                    self._remove_root_row(r)
                    stats["removed"] += 1
                continue
            ek = self._normalize_source_drive_item_key(pl, dd)
            if ek in used_key:
                continue
            pk = self._merge_root_row_path_key(pl)
            inc_path = incoming_by_path.get(pk) if pk else None
            if inc_path is not None:
                ik = self._normalize_source_drive_item_key(inc_path, dd)
                if ik[1] and ik not in used_key:
                    ix = self.index(r, 0, inv)
                    prev_children_loaded = bool(pl.get("children_loaded")) if pl.get("is_folder") else False
                    inc_copy = dict(inc_path)

                    def mutator_path(
                        payload: Dict[str, Any], _prev=prev_children_loaded, _inc=inc_copy
                    ) -> None:
                        payload.update(_inc)
                        payload.pop("source_shell_provisional", None)
                        if payload.get("is_folder") and _prev:
                            payload["children_loaded"] = True

                    self.update_payload_for_index(ix, mutator_path)
                    used_key.add(ik)
                    stats["updated"] += 1
                    stats["matched"] += 1
                    continue
            if self._source_shell_row_removal_allowed(pl, enrich_only=enrich_only):
                self._remove_root_row(r)
                stats["removed"] += 1

        for inc in incoming:
            ik = self._normalize_source_drive_item_key(inc, dd)
            if not ik[1] or ik in used_key:
                continue
            row_ins = self._root_graph_insertion_row(inc)
            self._insert_root_child_at(row_ins, inc)
            used_key.add(ik)
            stats["inserted"] += 1

        self._rebuild_path_index()
        after_count = self.count_snapshot_shell_nodes_depth_first()
        log_info(
            "source_root_merge_forensic",
            before_count=int(before_count),
            after_count=int(after_count),
            matched=int(stats["matched"]),
            inserted=int(stats["inserted"]),
            updated=int(stats["updated"]),
            removed=int(stats["removed"]),
            enrich_only=int(bool(enrich_only)),
            default_drive_id_suffix=dd[-16:] if len(dd) > 16 else dd,
        )
        return stats

    def merge_sharepoint_folder_children(
        self,
        parent: QModelIndex,
        child_payloads: List[Dict[str, Any]],
        *,
        drive_id: str,
        log_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        """Merge Graph folder children into ``parent``, preserving existing subtree nodes for matched ids."""
        ctx = dict(log_context) if log_context else {}
        dd = str(drive_id or "").strip()
        stats = {"matched": 0, "inserted": 0, "updated": 0, "removed": 0, "before_count": 0, "after_count": 0}
        gen = self.structure_generation()
        if not self.is_index_live(parent):
            log_info(
                "source_folder_merge_skip_invalid_parent",
                model_generation=gen,
                child_count=len(child_payloads),
                **{
                    k: ctx[k]
                    for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                    if k in ctx
                },
            )
            return stats
        parent_node = self._node(parent)
        if parent_node is None:
            return stats
        parent_pl = parent_node.payload or {}
        parent_path = str(parent_pl.get("item_path") or parent_pl.get("display_path") or "")[:400]

        old_children = list(parent_node._children or [])
        stats["before_count"] = sum(1 for c in old_children if not c.is_placeholder())

        incoming: List[Dict[str, Any]] = []
        for p in child_payloads or []:
            if not isinstance(p, dict):
                continue
            pl = dict(p)
            if dd and not str(pl.get("drive_id") or pl.get("library_id") or "").strip():
                pl["drive_id"] = dd
            incoming.append(pl)
        incoming.sort(key=self._root_graph_sort_key)
        incoming_dedup: List[Dict[str, Any]] = []
        _seen_folder_inc: set[Tuple[str, str]] = set()
        for pl in incoming:
            nk = self._normalize_source_drive_item_key(pl, dd)
            if not nk[1] or nk in _seen_folder_inc:
                continue
            _seen_folder_inc.add(nk)
            incoming_dedup.append(pl)
        incoming = incoming_dedup

        incoming_by_path: Dict[str, Dict[str, Any]] = {}
        for p in incoming:
            pk = self._folder_child_path_fallback_key(p)
            if pk and pk not in incoming_by_path:
                incoming_by_path[pk] = p

        existing_by_key: Dict[Tuple[str, str], _Node] = {}
        existing_by_path: Dict[str, _Node] = {}
        for c in old_children:
            if c.is_placeholder():
                continue
            pl = c.payload or {}
            k = self._normalize_source_drive_item_key(pl, dd)
            if k[1]:
                existing_by_key[k] = c
            pk = self._folder_child_path_fallback_key(pl)
            if pk and pk not in existing_by_path:
                existing_by_path[pk] = c

        used_nodes: set[int] = set()
        used_incoming_keys: set[Tuple[str, str]] = set()
        final_nodes: List[_Node] = []

        def _apply_payload_to_node(node: _Node, inc_payload: Dict[str, Any]) -> None:
            prev_children_loaded = bool(node.payload.get("children_loaded")) if node.payload.get("is_folder") else False
            inc_copy = dict(inc_payload)

            def mutator(payload: Dict[str, Any], _prev=prev_children_loaded, _inc=inc_copy) -> None:
                payload.update(_inc)
                payload.pop("source_shell_provisional", None)
                if payload.get("is_folder") and _prev:
                    payload["children_loaded"] = True

            self.update_payload_for_index(self._index_for_node(node), mutator)

        for inc in incoming:
            nk = self._normalize_source_drive_item_key(inc, dd)
            if not nk[1]:
                continue
            match: Optional[_Node] = existing_by_key.get(nk)
            if match is None:
                pk = self._folder_child_path_fallback_key(inc)
                cand = existing_by_path.get(pk) if pk else None
                if cand is not None and id(cand) not in used_nodes:
                    match = cand
            if match is not None and id(match) not in used_nodes:
                _apply_payload_to_node(match, inc)
                final_nodes.append(match)
                used_nodes.add(id(match))
                used_incoming_keys.add(nk)
                stats["matched"] += 1
                stats["updated"] += 1
                continue
            row = len(final_nodes)
            ch_list: Optional[List[_Node]] = None if inc.get("is_folder") else []
            node = _Node(parent_node, row, dict(inc), ch_list)
            final_nodes.append(node)
            used_incoming_keys.add(nk)
            stats["inserted"] += 1

        for c in old_children:
            if c.is_placeholder():
                continue
            if id(c) in used_nodes:
                continue
            pl = c.payload or {}
            nk = self._normalize_source_drive_item_key(pl, dd)
            if nk[1] and nk in used_incoming_keys:
                continue
            if pl.get("source_shell_provisional"):
                final_nodes.append(c)
                continue
            if self._source_shell_row_removal_allowed(pl, enrich_only=False):
                stats["removed"] += 1
                continue
            final_nodes.append(c)

        final_nodes.sort(key=lambda n: self._root_graph_sort_key(n.payload or {}))

        if not final_nodes:
            empty_pl = {
                "placeholder": True,
                "placeholder_role": "terminal_empty",
                "base_display_label": "This folder is empty.",
                "tree_role": "source",
            }
            final_nodes = [_Node(parent_node, 0, empty_pl, [])]

        old_count = len(old_children)
        if old_count:
            for old_child in list(old_children):
                self._unregister_subtree_paths(old_child)
            self.beginRemoveRows(parent, 0, old_count - 1)
            parent_node._children = []
            self.endRemoveRows()
        n = len(final_nodes)
        self.beginInsertRows(parent, 0, n - 1)
        parent_node._children = final_nodes
        self._reindex(parent_node)
        self.endInsertRows()
        for c in final_nodes:
            self._register_subtree_paths(c)
        self._bump_structure_generation()
        stats["after_count"] = sum(1 for c in final_nodes if not c.is_placeholder())
        log_info(
            "source_folder_merge_forensic",
            parent_path_excerpt=parent_path,
            before_count=int(stats["before_count"]),
            after_count=int(stats["after_count"]),
            matched=int(stats["matched"]),
            inserted=int(stats["inserted"]),
            updated=int(stats["updated"]),
            removed=int(stats["removed"]),
            model_generation=self.structure_generation(),
            **{k: ctx[k] for k in ("worker_id", "drive_id", "item_id", "item_id_suffix") if k in ctx},
        )
        log_info(
            "source_replace_children_complete",
            model_generation=self.structure_generation(),
            child_count=n,
            mode="merged_payloads",
            **{
                k: ctx[k]
                for k in ("worker_id", "drive_id", "item_id", "item_id_suffix", "parent_path_excerpt")
                if k in ctx
            },
        )
        return stats
