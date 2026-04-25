"""Errors for legacy backup import guard."""


class LegacyBackupDirectRestoreBlocked(Exception):
    """Raised when a bundle looks legacy-shaped and direct import is not allowed."""

    def __init__(self, message: str = "", *, reasons: list[str] | None = None) -> None:
        super().__init__(message or "Legacy backup requires migration before import.")
        self.reasons = list(reasons or [])
