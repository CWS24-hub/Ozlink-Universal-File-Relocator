"""Source startup projection refresh and count-worker policy gates."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from ozlink_console.hybrid_destination_preview import hybrid_destination_preview_browse_first_enabled
from ozlink_console.main_window import MainWindow


class SourceStartupGatesTests(unittest.TestCase):
    def test_broad_deferred_projection_blocked_during_destination_startup(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_startup_phase_active = True  # type: ignore[attr-defined]
        mw._memory_restore_in_progress = False  # type: ignore[attr-defined]
        mw._memory_restore_background_trees = False  # type: ignore[attr-defined]
        mw._source_user_explicit_full_projection = False  # type: ignore[attr-defined]
        ok, reason = MainWindow._source_should_run_startup_projection_refresh(  # type: ignore[misc]  # noqa: E501
            mw,
            phase_name="source_projection_deferred_graph_ids_resolved_from_sharepoint_paths",
            path_count=20,
            root_path_count=7,
            subtree_scope="full",
        )
        self.assertFalse(ok)
        self.assertIn("startup", reason)

    def test_roots_only_bounded_allowed_during_startup(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_startup_phase_active = True  # type: ignore[attr-defined]
        mw._memory_restore_in_progress = True  # type: ignore[attr-defined]
        mw._source_user_explicit_full_projection = False  # type: ignore[attr-defined]
        ok, _ = MainWindow._source_should_run_startup_projection_refresh(  # type: ignore[misc]  # noqa: E501
            mw,
            phase_name="any",
            path_count=8,
            root_path_count=4,
            subtree_scope="roots_only",
        )
        self.assertTrue(ok)

    def test_count_worker_blocked_during_destination_startup_phase(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_startup_phase_active = True  # type: ignore[attr-defined]
        ok, reason = MainWindow._source_should_run_startup_count_worker(mw, "d1")  # type: ignore[misc]  # noqa: E501
        self.assertFalse(ok)
        self.assertIn("startup", reason.lower())

    @unittest.skipUnless(
        hybrid_destination_preview_browse_first_enabled(),
        "hybrid destination browse default-on; env OZLINK_HYBRID_DESTINATION_PREVIEW=off skips",
    )
    def test_hybrid_browse_defer_recursive_full_count_until_unlock(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_startup_phase_active = False  # type: ignore[attr-defined]
        mw._source_user_explicit_full_projection = False  # type: ignore[attr-defined]
        mw._source_hybrid_full_count_unlocked = False  # type: ignore[attr-defined]
        ok, reason = MainWindow._source_should_run_startup_count_worker(mw, "d1")  # type: ignore[misc]  # noqa: E501
        self.assertFalse(ok)
        self.assertIn("hybrid", reason.lower())
        mw._source_hybrid_full_count_unlocked = True  # type: ignore[attr-defined]
        ok2, _ = MainWindow._source_should_run_startup_count_worker(mw, "d1")  # type: ignore[misc]  # noqa: E501
        self.assertTrue(ok2)


if __name__ == "__main__":
    unittest.main()
