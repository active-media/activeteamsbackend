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
from datetime import datetime, timedelta

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
        # Boundaries are the Africa/Johannesburg calendar day, returned as the
        # naive-UTC datetimes Mongo stores (SAST is UTC+2, no daylight saving):
        # 2026-09-01 00:00:00 SAST == 2026-08-31 22:00:00 UTC
        # 2026-09-05 23:59:59.999999 SAST == 2026-09-05 21:59:59.999999 UTC
        start, end = parse_custom_period("2026-09-01", "2026-09-05")
        self.assertEqual(start, datetime(2026, 8, 31, 22, 0, 0, 0))
        self.assertEqual(end, datetime(2026, 9, 5, 21, 59, 59, 999999))

    def test_z_suffix_accepted(self):
        start, end = parse_custom_period(
            "2026-09-01T00:00:00Z", "2026-09-05T23:59:59Z"
        )
        # 2026-09-01T00:00:00Z is 2026-09-01 02:00 SAST -> the SAST day starts
        # at 2026-08-31 22:00 UTC. 2026-09-05T23:59:59Z is 2026-09-06 01:59 SAST
        # -> the end of that SAST day is 6 September 21:59:59 UTC.
        self.assertEqual(start.hour, 22)
        self.assertEqual(start.day, 31)
        self.assertEqual(end.day, 6)
        self.assertEqual(end.hour, 21)

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


class TestGetPeriodRangeSAST(unittest.TestCase):
    """Period boundaries must sit on the Africa/Johannesburg calendar."""

    @staticmethod
    def to_sast(dt):
        return dt + timedelta(hours=2)  # UTC+2, no daylight saving

    def test_this_week_starts_on_sast_monday_midnight(self):
        start, end = get_period_range("thisWeek")
        sast_start = self.to_sast(start)
        self.assertEqual(sast_start.weekday(), 0)  # Monday
        self.assertEqual(
            (sast_start.hour, sast_start.minute, sast_start.second), (0, 0, 0)
        )
        self.assertEqual(end - start, timedelta(days=7) - timedelta(microseconds=1))

    def test_today_spans_one_full_sast_day(self):
        start, end = get_period_range("today")
        self.assertEqual(end - start, timedelta(days=1) - timedelta(microseconds=1))
        # The next SAST day begins right after the period ends.
        self.assertEqual(end + timedelta(microseconds=1), start + timedelta(days=1))

    def test_previous_month_is_the_previous_calendar_month(self):
        start, end = get_period_range("previousMonth")
        sast_start = self.to_sast(start)
        sast_end_next = self.to_sast(end) + timedelta(microseconds=1)
        self.assertEqual((sast_start.hour, sast_start.minute), (0, 0))
        self.assertEqual(sast_start.day, 1)
        # First instant of the following month
        self.assertEqual((sast_end_next.hour, sast_end_next.minute, sast_end_next.day), (0, 0, 1))


if __name__ == "__main__":
    unittest.main()