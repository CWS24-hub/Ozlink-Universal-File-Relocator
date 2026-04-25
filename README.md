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

**Client delivery:** ship a zip that contains **only** the `OzlinkConsole` folder (`OzlinkConsole.exe` + `_internal`). Do **not** put the PyInstaller `build/` tree inside the zip.

From repo root:

```powershell
pip install pyinstaller
.\scripts\package_release.ps1
```

`package_release.ps1` runs PyInstaller with **`dist`** and **`work`** under **`%TEMP%`** (avoids OneDrive locks on `dist\`) and writes **`OzlinkConsole_release_<timestamp>.zip`** next to the repo root. The client extracts that zip and runs **`OzlinkConsole.exe`** in place (folder must stay intact).

Or manually from repo root: `python -m PyInstaller --noconfirm OzlinkConsole.spec`, then zip **`dist\OzlinkConsole`** yourself.

## Client handoff: SharePoint source tree (planning)

The planning **source** pane for SharePoint uses a virtualized tree view backed by an in-memory model (not the legacy widget tree). **Source → Expand All** loads folder branches in the background (up to a few concurrent Graph requests), then expands materialized rows; very large libraries may take a while, and users can cancel by choosing **Expand All** again to collapse loaded branches.

## Important assumptions

- Graph auth uses the same client/tenant configuration found in the PowerShell script.
- Device code sign-in is used for the first cut because it is stable and easy to package.
- The current draft JSON structure is preserved intentionally so existing memory files remain readable.
- The request package format remains JSON-based.

## Known limitations of this first production Python pass

- The WinForms screen-by-screen layout is preserved functionally, not pixel-for-pixel.
- The destination projected overlay shows direct allocated nodes under the requested destination folder; it does not yet explode inherited descendants recursively as separate visual nodes.
- Import/export preserves the current JSON memory behavior rather than switching to SQLite.
