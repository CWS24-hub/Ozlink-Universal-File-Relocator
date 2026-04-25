# UI map (Ozlink Universal File Relocator)

This document lists **where the user interface is implemented** and how the main window is assembled. The app is **PySide6**, mostly **code-built** (no Qt Designer `.ui` files for the main shell).

**Important:** `ozlink_console/main_window.py` is both **UI** and **application logic** (~84k lines). There is no clean “UI-only” split; this file is the single largest surface for screens, trees, delegates, and slots.

---

## 1. Application entry

| File | Purpose |
|------|---------|
| `app.py` | `QApplication`, session logging, `apply_application_branding`, constructs `MainWindow`, `exec()`. |

---

## 2. Main window shell assembly (`MainWindow.__init__`)

Construction order in `ozlink_console/main_window.py` (search near `setCentralWidget` / `build_custom_window_chrome`):

1. `self._apply_theme()`
2. `self.central = QWidget()` → `setCentralWidget(self.central)` → `root_layout = QVBoxLayout(self.central)`
3. `build_custom_window_chrome()` — custom frameless title/titlebar region
4. `build_top_bar()` — top chrome / session bar
5. `build_main_area()` — **sidebar + stacked pages** (Dashboard, Planning, Settings, etc.)
6. `build_bottom_status_bar()`
7. Timers (expand-all, background load, etc.), `switch_page("Dashboard")`, `apply_role_visibility()`

**Role-based nav** is defined on `self.nav_allowed_by_role` in the same `__init__` (guest / user / admin page sets).

---

## 3. Page / panel `build_*` entry points (main_window.py)

Use these as anchors when reading the file (line numbers are approximate; prefer **Find in file** in your editor).

| Method | Approx. line | Role |
|--------|----------------|------|
| `build_custom_window_chrome` | ~5444 | Frameless chrome, resize/drag |
| `build_top_bar` | ~5449 | Top bar |
| `build_main_area` | ~5524 | `build_left_nav` + `build_content_area` |
| `build_left_nav` | ~5534 | Side navigation |
| `build_content_area` | ~5587 | Stacked `QStackedWidget` / page host |
| `build_dashboard_page` | ~5623 | Dashboard |
| `build_planning_page` | ~5947 | **Planning Workspace** (source/destination trees, details, etc.) |
| `build_tree_panel` | ~7278 | Reusable tree panel (source/destination) |
| `build_requests_page` | ~6568 | Requests (admin) |
| `build_details_panel` | ~17114 | Item details |
| `build_preview_tab_panel` | ~17178 | File preview area |
| `build_planned_moves_panel` | ~17369 | Planned moves table / summary |
| `build_audit_page` | ~22893 | Audit |
| `build_execution_page` | ~22997 | Execution |
| `build_settings_page` | ~23176 | Settings |
| `build_bottom_status_bar` | ~23382 | Status line |
| `build_placeholder_page` | ~22870 | Placeholder pages |
| `_init_destination_drag_overlay_widgets` (on `DestinationPlanningTreeView`) | ~1009 | Drag/drop overlay helpers |

Smaller layout helpers: `build_metric_card`, `build_panel_box`, `build_workflow_table`, `build_suggestions_panel`, `build_needs_review_panel`, `build_suggestions_panel`, etc. (search `def build_` in `main_window.py`).

---

## 4. Custom widgets and delegates (same file unless noted)

| Symbol | Type | Role |
|--------|------|------|
| `DestinationPlanningTreeView` | `QTreeView` | Destination planning tree, manual drag, drop bands, scroll coalescing |
| `SourceTreeRelationshipDelegate` | `QStyledItemDelegate` | Source name + relationship column painting |
| `DestinationPlanningTreeDelegate` | `QStyledItemDelegate` | Destination planning columns |
| `DeviceFlowPromptDialog` | `QDialog` | Device-code sign-in |
| `ArrowComboBox` | `QComboBox` | Themed combo |
| `FramelessTitleBar` | `QWidget` | Custom titlebar buttons |
| `InlineReasonBlock` | `QFrame` | Inline explanatory blocks |
| `ConflictRowWidget` / `DuplicateGroupCard` | `QWidget` / `QFrame` | Duplicate / conflict UI |
| `SourceRelationDelegate` | in `ozlink_console/delegates.py` | Alternate / shared delegate pattern |

`QThread` **workers** in the same file are **not** “layout” but are listed next to UI in code (login, folder load, cache refresh, manifest run, etc.).

---

## 5. Qt models (tree data for views)

| Module | Class |
|--------|--------|
| `ozlink_console/tree_models/lazy_folder_tree_model.py` | `LazyFolderTreeModel` |
| `ozlink_console/tree_models/sharepoint_source_model.py` | `SharePointSourceTreeModel` |
| `ozlink_console/tree_models/destination_planning_model.py` | `DestinationPlanningTreeModel` |
| `ozlink_console/tree_models/explorer_columns.py` | Explorer column labels/icons (Name, Size, Type, Date) |

`ozlink_console/tree_models/__init__.py` re-exports the three models.

---

## 6. Branding and resources

| Path | Role |
|------|------|
| `ozlink_console/branding.py` | Application icon via `:/branding/...` |
| `ozlink_console/resources/ozlink_resources.qrc` | Qt resource file |
| `ozlink_console/ozlink_resources_rc.py` | Generated (compiled resources) — **do not hand-edit** |
| `scripts/generate_branding_assets.py` | Regenerate PNGs from SVGs |

---

## 7. Platform / window behavior

| File | Role |
|------|------|
| `ozlink_console/win32_qt_minmaxinfo.py` | Windows min/max client rectangle / sizing behavior with frameless window |

---

## 8. UX text / display helpers (not widgets)

| File | Role |
|------|------|
| `ozlink_console/draft_import/import_ux_messages.py` | User-facing import strings |
| `ozlink_console/recovered_planning_display.py` | Label text for legacy/recovered planning rows |
| `ozlink_console/planning_interaction_contract.py` | Product rules for local-first / overlays (no Qt widgets) |

---

## 9. How to use this in another project or in Cursor

- **Start from** `app.py` → `MainWindow` → `build_*` methods above.
- **For parity**, also expect dependencies on `memory`, Graph client, execution, and draft snapshot modules; the UI is not self-contained in isolation.
- **Search tip:** In `main_window.py`, search for `def build_`, `def handle_`, `def on_`, `def _on_`, and `connect(` to find signals/slots for a feature.

---

*Generated for navigation and refactoring; line numbers drift as the file changes.*
