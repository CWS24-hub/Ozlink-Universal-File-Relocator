# Progress log

Bounded passes on `ui-modernisation`. Full tree redesign deferred; changes stay within the current architecture.

## Pass 2 — Planning UI: unassign, cut/paste, drag/move, open file (2026-04-02)

**Commit:** Message `Fix: unassign, cut/paste, destination drag-move, and local Open File (Pass 2)` on branch `ui-modernisation` (use `git log -1` for the object id).

**Summary**

- **Unassign:** Added `_planned_move_index_for_source_node()` (path fallback when id/key matching fails). Unassign from **destination** selection mirrors removal flow. Planned-moves table uses **current row** when range selection is empty. Updated user hint text.
- **Cut/Paste:** Paste resolves target/source paths with `_canonical_destination_projection_path` / `normalize_memory_path` before lookup (model-view path alignment).
- **Drag/Move:** Destination tree `startDrag` no longer clears drag state in a `finally` before drop handling; invalid drag start bails early; ignored drops clear `_dragged_*`. **`_move_planned_destination_node`** now calls `refresh_planned_moves_table()` and `_schedule_deferred_destination_materialization("planned_item_moved")` so data moves show in UI.
- **Open File:** `_resolve_local_open_file_target()` enables Open File only for paths that exist on disk; selection-details and context-menu flows use it; `handle_open_selected_file` uses `fromLocalFile` only after `os.path.isfile`; clearer messages when local path is missing.

**Risks**

- Drag state if platform defers drop without `dropEvent` on the same view (mitigated: next `startDrag` overwrites).
- Unassign from destination on projected-only rows still depends on `find_planned_move_index_by_destination` resolving the row.

**Tests run:** `pytest tests/test_main_window_restore_guards.py tests/test_pilot_injection_strict_destination_match.py tests/test_on_tree_selection_changed_model_view.py` (12 passed).
