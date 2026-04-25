from __future__ import annotations

from types import SimpleNamespace

from ozlink_console.main_window import MainWindow


class _Label:
    def __init__(self) -> None:
        self.text = ""

    def setText(self, value: str) -> None:
        self.text = str(value)


class _FakeWindow:
    def __init__(self) -> None:
        self.execution_status_label = _Label()
        self.fallback_calls: list[dict] = []

    def _start_legacy_manifest_worker(self, **kwargs):
        self.fallback_calls.append(dict(kwargs))


class _Toggle:
    def __init__(self, checked: bool) -> None:
        self._checked = bool(checked)

    def isChecked(self) -> bool:
        return self._checked


class _ToggleHost:
    def __init__(self, checked: bool = False) -> None:
        self._settings_execution_path_toggle = _Toggle(checked)

    def _resolve_draft_pipeline_toggle_state(self):
        return MainWindow._resolve_draft_pipeline_toggle_state(self)


def test_snapshot_primary_by_default(monkeypatch):
    monkeypatch.delenv("ENABLE_DRAFT_PIPELINE_EXECUTION", raising=False)
    assert MainWindow._draft_pipeline_execution_enabled(_ToggleHost(False)) is True


def test_env_true_forces_snapshot(monkeypatch):
    monkeypatch.setenv("ENABLE_DRAFT_PIPELINE_EXECUTION", "true")
    assert MainWindow._draft_pipeline_execution_enabled(_ToggleHost(False)) is True


def test_ui_toggle_legacy_when_checked(monkeypatch):
    monkeypatch.delenv("ENABLE_DRAFT_PIPELINE_EXECUTION", raising=False)
    fake = _ToggleHost(True)
    assert MainWindow._draft_pipeline_execution_enabled(fake) is False


def test_env_false_overrides_ui_legacy_checkbox(monkeypatch):
    monkeypatch.setenv("ENABLE_DRAFT_PIPELINE_EXECUTION", "false")
    fake = _ToggleHost(True)
    enabled, source = MainWindow._resolve_draft_pipeline_toggle_state(fake)
    assert enabled is False
    assert source == "env"


def test_draft_pipeline_success_does_not_fallback():
    win = _FakeWindow()
    result = SimpleNamespace(success=True, snapshot_id="snap-1", run_id="run-1", plan_id="plan-1")
    MainWindow._on_draft_pipeline_run_finished(win, result, {"path": "legacy"})
    assert win.fallback_calls == []
    assert "completed" in win.execution_status_label.text.lower()


def test_draft_pipeline_failure_falls_back_to_legacy():
    win = _FakeWindow()
    result = SimpleNamespace(
        success=False,
        snapshot_id="snap-2",
        run_id="run-2",
        plan_id="plan-2",
        bridge_summary={"outcome_counts": {"failed": 1}},
        compatibility_blocks=[],
    )
    fallback = {"path": "legacy", "dry_run": True}
    MainWindow._on_draft_pipeline_run_finished(win, result, fallback)
    assert len(win.fallback_calls) == 1
    assert win.fallback_calls[0] == fallback
    assert "falling back" in win.execution_status_label.text.lower()
