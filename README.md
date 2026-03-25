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

## Packaging as EXE later

```bash
pip install pyinstaller
pyinstaller --noconfirm --onefile --windowed app.py
```

## Important assumptions

- Graph auth uses the same client/tenant configuration found in the PowerShell script.
- Device code sign-in is used for the first cut because it is stable and easy to package.
- The current draft JSON structure is preserved intentionally so existing memory files remain readable.
- The request package format remains JSON-based.

## Known limitations of this first production Python pass

- The WinForms screen-by-screen layout is preserved functionally, not pixel-for-pixel.
- The destination projected overlay shows direct allocated nodes under the requested destination folder; it does not yet explode inherited descendants recursively as separate visual nodes.
- Import/export preserves the current JSON memory behavior rather than switching to SQLite.
