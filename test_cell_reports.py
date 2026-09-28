from datetime import date

import pytest

from supabase_helpers.supabase_stats import (
    _cell_report_bucket_start,
    _cell_report_window,
    _person_key,
)
from supabase_helpers.cell_reports import (
    _comparison_window,
    _percentage_change,
    get_cell_report,
)


def test_custom_report_window_is_inclusive():
    start, end = _cell_report_window("monthly", "2026-01-10", "2026-02-02")

    assert start.date().isoformat() == "2026-01-10"
    assert end.date().isoformat() == "2026-02-02"
    assert end.hour == 23
    assert end.minute == 59


def test_report_window_rejects_incomplete_or_reversed_dates():
    with pytest.raises(ValueError, match="provided together"):
        _cell_report_window("monthly", start_date="2026-01-10")

    with pytest.raises(ValueError, match="before or equal"):
        _cell_report_window("monthly", "2026-02-02", "2026-01-10")


def test_report_buckets_support_requested_granularities():
    value = date(2026, 9, 14)

    assert _cell_report_bucket_start(value, "weekly") == date(2026, 9, 14)
    assert _cell_report_bucket_start(value, "monthly") == date(2026, 9, 1)
    assert _cell_report_bucket_start(value, "three_months") == date(2026, 7, 1)
    assert _cell_report_bucket_start(value, "six_months") == date(2026, 7, 1)
    assert _cell_report_bucket_start(value, "yearly") == date(2026, 1, 1)


def test_person_key_prefers_stable_identifier_and_normalizes_it():
    assert _person_key(
        {"mongo_person_id": " PERSON-42 ", "email": "person@example.com"},
        "mongo_person_id",
        "email",
    ) == "person-42"
    assert _person_key({}, "mongo_person_id", "email") is None


def test_comparison_window_uses_equal_custom_ranges():
    start, end = _comparison_window(
        "monthly", date(2026, 9, 10), date(2026, 9, 18), True
    )

    assert (start, end) == (date(2026, 9, 1), date(2026, 9, 9))


def test_comparison_window_uses_same_elapsed_days_for_month_to_date():
    start, end = _comparison_window(
        "monthly", date(2026, 9, 1), date(2026, 9, 18), False
    )

    assert (start, end) == (date(2026, 8, 1), date(2026, 8, 18))


def test_percentage_change_handles_zero_denominator():
    assert _percentage_change(0, 0) == 0.0
    assert _percentage_change(3, 0) is None
    assert _percentage_change(8, 5) == 60.0


def test_cell_report_includes_zero_safe_comparison_for_empty_period(monkeypatch):
    class DummyQuery:
        def select(self, *_args, **_kwargs):
            return self

        def in_(self, *_args, **_kwargs):
            return self

        def eq(self, *_args, **_kwargs):
            return self

        def gte(self, *_args, **_kwargs):
            return self

        def lte(self, *_args, **_kwargs):
            return self

        def execute(self):
            return type("Result", (), {"data": []})()

    class DummyAdmin:
        def table(self, *_args, **_kwargs):
            return DummyQuery()

    monkeypatch.setattr("supabase_helpers.cell_reports.supabase_admin", DummyAdmin())

    result = get_cell_report(
        period="monthly",
        start_date="2026-09-01",
        end_date="2026-09-18",
        scope="all",
        include_comparison=True,
    )

    assert "comparison" in result
    assert result["comparison"]["current_period"]["new_cells"] == 0
    assert result["comparison"]["previous_period"]["new_cells"] == 0
    assert result["comparison"]["percentage_change"]["new_cells"] == 0.0