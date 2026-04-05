"""Developer-only toggles. Production launches leave these unset.

``--dev`` also enables persisted MSAL token cache (see :mod:`ozlink_console.graph`).
"""

from __future__ import annotations

import os

_DEV_TRUTHY = frozenset({"1", "true", "yes", "on"})


def apply_cli_dev_flag(argv: list[str]) -> None:
    """If ``--dev`` appears in argv, set ``OZLINK_DEV`` so respawned children stay in dev mode."""
    if "--dev" in argv:
        os.environ["OZLINK_DEV"] = "1"


def is_dev_mode() -> bool:
    """True when ``OZLINK_DEV`` is a truthy string (after ``apply_cli_dev_flag`` when using ``--dev``)."""
    return os.environ.get("OZLINK_DEV", "").strip().lower() in _DEV_TRUTHY
