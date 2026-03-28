# Ozlink IT – SharePoint File Relocation Console (PySide6)

This is a Python/PySide6 rebuild of the PowerShell WinForms planning console.

## What is preserved

- Microsoft 365 / Graph-based site and library discovery
- Source and destination trees
- Lazy loading of tree children on expand
- Local APPDATA draft memory
- Backups / recovery / quarantine
- Import / export of draft memory bundles
- Auto-export on exit
- Planned Moves grid
- Allocation overlay logic:
  - source: `SourceName → DestinationLeaf`
  - inherited: `SourceName ↳ via ParentMappedName`
  - destination projected `[Allocated]` and `[Proposed]` nodes

## Memory location

Live memory is stored at:

`%LOCALAPPDATA%\OzlinkIT\SharePointRelocationConsole\Memory`

Exports are stored at:

`%USERPROFILE%\Documents\Ozlink File Relocation Console\Exports`

## Run from scratch

```bash
pip install -r requirements.txt
python app.py
```

## Packaging (PyInstaller, same layout as historical Codex builds)

The repo uses **`OzlinkConsole.spec`**: a **onedir** build (`OzlinkConsole.exe` plus an **`_internal`** folder). That is what your **Good Build.zip** sample contains under `dist_…/OzlinkConsole/` — **not** a single-file exe.

**Client delivery:** zip **only** `dist/OzlinkConsole/` (exe + `_internal`). You do **not** need to ship the `build/` folder inside the zip (it is intermediate output and bloat).

From repo root:

```powershell
pip install pyinstaller
.\scripts\package_release.ps1
```

Or manually: `pyinstaller --noconfirm OzlinkConsole.spec` then zip the `dist\OzlinkConsole` folder. The client extracts the zip and runs **`OzlinkConsole.exe`** in place (folder must stay intact).

## Client handoff: optional faster source tree (mapping / large libraries)

**Default:** the classic source tree (unchanged). **No action required** for normal deliveries.

If the library is large and the **source** pane feels sluggish when scrolling or expanding:

1. Open **Settings**.
2. Under **Planning workspace — faster source tree (optional)**, turn on **Use faster source tree (restart required)**.
3. **Fully quit** the app and start it again.

To go back: turn the option **off** and restart. The setting is stored per Windows user profile (Qt `QSettings`).

**Note:** In faster mode, **Source → Expand All** is not available yet; users expand folders as needed. IT can still force one launch with `OZLINK_SOURCE_QTREEVIEW=1` or `=0` if the environment variable is set.

## Important assumptions

- Graph auth uses the same client/tenant configuration found in the PowerShell script.
- Device code sign-in is used for the first cut because it is stable and easy to package.
- The current draft JSON structure is preserved intentionally so existing memory files remain readable.
- The request package format remains JSON-based.

## Known limitations of this first production Python pass

- The WinForms screen-by-screen layout is preserved functionally, not pixel-for-pixel.
- The destination projected overlay shows direct allocated nodes under the requested destination folder; it does not yet explode inherited descendants recursively as separate visual nodes.
- Import/export preserves the current JSON memory behavior rather than switching to SQLite.
