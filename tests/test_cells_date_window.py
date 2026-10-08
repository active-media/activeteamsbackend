"""
Unit tests for the /events/cells date-window helpers extracted from main.py:
resolve_cells_date_window and cells_max_weeks.

Run from the repo root:
    venv/bin/python -m unittest tests.test_cells_date_window -v
or:
    venv/bin/python -m unittest discover -s tests -p "test_cells_date*.py" -v
"""

import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from main import (
    MAX_RANGE_WEEKS,
    cells_max_weeks,
    resolve_cells_date_window,
)

TODAY = date(2026, 9, 30)


class TestResolveCellsDateWindow(unittest.TestCase):
    def test_start_date_only_keeps_legacy_lookback(self):
        # The endpoint used to receive only start_date; omitting end_date must
        # not switch on the new range-spanning behaviour.
        start, end, explicit = resolve_cells_date_window("2025-11-30", None, TODAY)
        self.assertEqual(start, date(2025, 11, 30))
        self.assertEqual(end, TODAY)
        self.assertFalse(explicit)

    def test_missing_start_date_falls_back_to_default(self):
        start, _, explicit = resolve_cells_date_window(None, None, TODAY)
        self.assertEqual(start, date(2025, 11, 30))
        self.assertFalse(explicit)

    def test_explicit_range_is_detected(self):
        start, end, explicit = resolve_cells_date_window(
            "2026-09-01", "2026-09-15", TODAY
        )
        self.assertEqual(start, date(2026, 9, 1))
        self.assertEqual(end, date(2026, 9, 15))
        self.assertTrue(explicit)

    def test_same_day_range_is_explicit(self):
        _, _, explicit = resolve_cells_date_window("2026-09-10", "2026-09-10", TODAY)
        self.assertTrue(explicit)

    def test_reversed_range_is_not_explicit(self):
        start, end, explicit = resolve_cells_date_window(
            "2026-09-15", "2026-09-01", TODAY
        )
        self.assertEqual(start, date(2026, 9, 15))
        self.assertEqual(end, date(2026, 9, 1))
        self.assertFalse(explicit)

    def test_unparseable_end_date_falls_back_to_today(self):
        # end_date is truthy, so an explicit flag is raised; the parsed value
        # still falls back to today rather than raising.
        _, end, explicit = resolve_cells_date_window("2026-09-01", "not-a-date", TODAY)
        self.assertEqual(end, TODAY)
        self.assertTrue(explicit)

    def test_unparseable_start_date_falls_back_to_default(self):
        start, _, _ = resolve_cells_date_window("garbage", None, TODAY)
        self.assertEqual(start, date(2025, 11, 30))


class TestCellsMaxWeeks(unittest.TestCase):
    def test_legacy_caps_without_explicit_range(self):
        self.assertEqual(cells_max_weeks("incomplete", False, date(2020, 1, 1), TODAY), 1)
        self.assertEqual(cells_max_weeks("complete", False, date(2020, 1, 1), TODAY), 4)
        self.assertEqual(cells_max_weeks(None, False, date(2020, 1, 1), TODAY), 4)

    def test_narrow_range_keeps_legacy_cap(self):
        # A three-day window needs fewer weeks than the legacy four-week cap,
        # so the cap must not shrink below what callers already relied on.
        self.assertEqual(
            cells_max_weeks("complete", True, date(2026, 9, 28), date(2026, 9, 30)), 4
        )

    def test_short_range_still_generates_at_least_one_week(self):
        # The +2 partial-week allowance means even a one-day range generates
        # two weeks; the extra instances are dropped by the date bounds.
        self.assertEqual(
            cells_max_weeks("incomplete", True, date(2026, 9, 29), date(2026, 9, 30)), 2
        )

    def test_range_wider_than_legacy_cap_widens_it(self):
        # Eight weeks of history could never be returned under the 4-week cap.
        weeks = cells_max_weeks("complete", True, date(2026, 8, 3), date(2026, 9, 30))
        self.assertEqual(weeks, (56 // 7) + 2)
        self.assertGreater(weeks, 4)

    def test_incomplete_widens_for_explicit_range(self):
        weeks = cells_max_weeks("incomplete", True, date(2026, 8, 3), date(2026, 9, 30))
        self.assertGreater(weeks, 1)

    def test_very_wide_range_is_clamped(self):
        weeks = cells_max_weeks("complete", True, date(1990, 1, 1), TODAY)
        self.assertEqual(weeks, MAX_RANGE_WEEKS)

    def test_partial_weeks_are_rounded_up(self):
        # 10 days spans two calendar weeks, so +2 covers both partial edges.
        # The legacy floor is 1 here, so the widened value is what survives.
        weeks = cells_max_weeks("incomplete", True, date(2026, 9, 20), date(2026, 9, 30))
        self.assertEqual(weeks, (10 // 7) + 2)


if __name__ == "__main__":
    unittest.main()
