#!/usr/bin/env python3
"""Rasterize SVG branding to PNG + ICO (dev-time; outputs are committed for packaging without Pillow).

Requires: PySide6 (same as app). Optional: Pillow for multi-size ICO (``pip install Pillow``).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRAND = ROOT / "ozlink_console" / "resources" / "branding"
SVG_APP = BRAND / "ozlink_app_icon.svg"


def _render_pngs() -> dict[int, Path]:
    from PySide6.QtCore import QByteArray, Qt
    from PySide6.QtGui import QGuiApplication, QImage, QPainter
    from PySide6.QtSvg import QSvgRenderer

    # QSvgRenderer / raster path may require a Qt Gui application on some platforms.
    _app = QGuiApplication.instance() or QGuiApplication(sys.argv)

    data = SVG_APP.read_bytes()
    renderer = QSvgRenderer(QByteArray(data))
    if not renderer.isValid():
        raise RuntimeError(f"Invalid SVG: {SVG_APP}")

    out: dict[int, Path] = {}
    sizes = (16, 24, 32, 48, 64, 128, 256)
    for size in sizes:
        img = QImage(size, size, QImage.Format.Format_ARGB32_Premultiplied)
        img.fill(Qt.GlobalColor.transparent)
        p = QPainter(img)
        p.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        renderer.render(p)
        p.end()
        dest = BRAND / f"ozlink_app_icon_{size}.png"
        if not img.save(str(dest), "PNG"):
            raise RuntimeError(f"Failed to save {dest}")
        out[size] = dest
    return out


def _write_ico(png_paths: dict[int, Path]) -> Path | None:
    try:
        from PIL import Image
    except ImportError:
        return None
    # Windows shell: include standard sizes
    order = (16, 24, 32, 48, 64, 128, 256)
    ims = []
    for s in order:
        p = png_paths.get(s)
        if p and p.is_file():
            ims.append(Image.open(p).convert("RGBA"))
    if not ims:
        return None
    dest = BRAND / "ozlink_favicon.ico"
    ims[0].save(dest, format="ICO", sizes=[(im.width, im.height) for im in ims], append_images=ims[1:])
    return dest


def _variance_check(png_path: Path, min_std: float = 2.0) -> float:
    try:
        from PIL import Image
        import statistics

        im = Image.open(png_path).convert("L")
        px = list(im.get_flattened_data())
        if not px:
            return 0.0
        return float(statistics.pstdev(px))
    except ImportError:
        return -1.0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Regenerate PNG/ICO under ozlink_console/resources/branding/, then run: "
        "pyside6-rcc ozlink_console/resources/ozlink_resources.qrc -o ozlink_console/ozlink_resources_rc.py"
    )
    ap.add_argument("--check-only", action="store_true", help="Only verify 16/32 PNG contrast")
    args = ap.parse_args()

    if not SVG_APP.is_file():
        print(f"Missing {SVG_APP}", file=sys.stderr)
        return 1

    if args.check_only:
        for s in (16, 32):
            p = BRAND / f"ozlink_app_icon_{s}.png"
            if not p.is_file():
                print(f"Missing {p}", file=sys.stderr)
                return 1
            v = _variance_check(p)
            print(f"{p.name} luminance_stdev={v:.2f} (want > ~2 for visibility)")
        return 0

    pngs = _render_pngs()
    print("Wrote:", ", ".join(str(p) for p in pngs.values()))
    ico = _write_ico(pngs)
    if ico:
        print("Wrote:", ico)
    else:
        print("ICO skipped (install Pillow: pip install Pillow)", file=sys.stderr)

    for s in (16, 32):
        p = BRAND / f"ozlink_app_icon_{s}.png"
        v = _variance_check(p)
        print(f"Visibility check {p.name}: luminance_stdev={v:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
