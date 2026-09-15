"""
Unit tests for the dashboard-comprehensive helpers extracted from main.py:
parse_custom_period and build_dashboard_task_match.

Run from the repo root:
    venv/bin/python -m unittest tests.test_dashboard_filters -v
or:
    venv/bin/python -m unittest discover -s tests -p "test_dashboard*.py" -v
"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from main import (
    build_dashboard_task_match,
    parse_custom_period,
    get_period_range,
)


class TestBuildDashboardTaskMatch(unittest.TestCase):
    def test_default_all_returns_empty_match(self):
        self.assertEqual(build_dashboard_task_match("all", None, False), {})

    def test_completed(self):
        self.assertEqual(
            build_dashboard_task_match("completed", None, False),
            {"is_completed": True},
        )

    def test_incomplete(self):
        self.assertEqual(
            build_dashboard_task_match("incomplete", None, False),
            {"is_completed": False},
        )

    def test_overdue(self):
        self.assertEqual(
            build_dashboard_task_match("overdue", None, False),
            {"is_overdue": True},
        )

    def test_task_type_is_anchored_and_case_insensitive(self):
        match = build_dashboard_task_match("all", "Service Follow Up", False)
        # re.escape also escapes spaces; the escalated regex is a literal match
        self.assertEqual(
            match["task_type_label"],
            {"$regex": r"^Service\ Follow\ Up$", "$options": "i"},
        )

    def test_task_type_special_characters_are_escaped(self):
        match = build_dashboard_task_match("all", "a.b(c)", False)
        self.assertEqual(
            match["task_type_label"],
            {"$regex": r"^a\.b\(c\)$", "$options": "i"},
        )

    def test_consolidation_only(self):
        self.assertEqual(
            build_dashboard_task_match("all", None, True),
            {"is_consolidation_ish": True},
        )

    def test_combined_filters(self):
        match = build_dashboard_task_match("completed", "Cell Consolidation", True)
        self.assertEqual(match["is_completed"], True)
        self.assertEqual(
            match["task_type_label"],
            {"$regex": r"^Cell\ Consolidation$", "$options": "i"},
        )
        self.assertEqual(match["is_consolidation_ish"], True)


class TestParseCustomPeriod(unittest.TestCase):
    def test_valid_range_midnight_bounds(self):
        start, end = parse_custom_period("2026-09-01", "2026-09-05")
        self.assertEqual(start, datetime(2026, 9, 1, 0, 0, 0, 0))
        self.assertEqual(end, datetime(2026, 9, 5, 23, 59, 59, 999999))

    def test_z_suffix_accepted(self):
        start, end = parse_custom_period(
            "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"
        )
        self.assertEqual(start.hour, 0)
        self.assertEqual(end.day, 5)
        self.assertEqual(end.hour, 23)

    def test_invalid_dates_fall_back_to_today(self):
        expected = get_period_range("today")
        got = parse_custom_period("not-a-date", "also-bad")
        self.assertEqual(got[0].date(), expected[0].date())
        self.assertEqual(got[1].date(), expected[1].date())

    def test_missing_dates_fall_back_to_today(self):
        expected = get_period_range("today")
        got = parse_custom_period(None, None)
        self.assertEqual(got[0].date(), expected[0].date())
        self.assertEqual(got[1].date(), expected[1].date())


if __name__ == "__main__":
    unittest.main()