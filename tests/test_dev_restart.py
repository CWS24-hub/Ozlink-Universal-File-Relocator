import sys

import pytest

from ozlink_console import dev_mode
from ozlink_console.dev_restart import build_respawn_command


def test_build_respawn_command_script_mode(monkeypatch):
    monkeypatch.setattr(sys, "executable", r"C:\Python\python.exe")
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(sys, "argv", [r"D:\proj\app.py", "--dev", "x"])
    assert build_respawn_command() == [r"C:\Python\python.exe", r"D:\proj\app.py", "--dev", "x"]


def test_build_respawn_command_frozen_dedup_argv0(monkeypatch, tmp_path):
    exe = str(tmp_path / "Ozlink.exe")
    monkeypatch.setattr(sys, "executable", exe)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "argv", [exe, "--foo"])
    assert build_respawn_command() == [exe, "--foo"]


def test_build_respawn_command_frozen_mismatch_argv0(monkeypatch, tmp_path):
    exe = str(tmp_path / "Ozlink.exe")
    monkeypatch.setattr(sys, "executable", exe)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    other = str(tmp_path / "other.exe")
    monkeypatch.setattr(sys, "argv", [other, "a"])
    assert build_respawn_command() == [exe, "a"]


def test_apply_cli_and_is_dev_mode(monkeypatch):
    monkeypatch.delenv("OZLINK_DEV", raising=False)
    dev_mode.apply_cli_dev_flag(["app.py"])
    assert not dev_mode.is_dev_mode()
    dev_mode.apply_cli_dev_flag(["app.py", "--dev"])
    assert dev_mode.is_dev_mode()


def test_respawn_and_exit_noop_when_not_dev(monkeypatch):
    monkeypatch.delenv("OZLINK_DEV", raising=False)
    called = []

    def _track(*a, **k):
        called.append((a, k))

    monkeypatch.setattr("subprocess.Popen", _track)
    from ozlink_console.dev_restart import respawn_and_exit

    respawn_and_exit()
    assert called == []
