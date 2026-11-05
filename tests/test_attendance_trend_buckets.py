from datetime import date, timedelta

import pytest

from supabase_helpers.weekly_buckets import build_week_buckets


def _assert_weekly_series(first_day: date, last_day: date) -> None:
    week_keys, canonical_by_week, labels = build_week_buckets(first_day, last_day)
    sessions = [
        (first_day + timedelta(days=offset), offset + 1)
        for offset in range((last_day - first_day).days + 1)
    ]
    totals = {key: 0 for key in week_keys}

    for session_day, attendance in sessions:
        week_start = session_day - timedelta(days=session_day.weekday())
        totals[canonical_by_week[week_start]] += attendance

    series_labels = [labels[key] for key in week_keys]
    assert all(first_day <= label <= last_day for label in series_labels)
    assert len(series_labels) == len(set(series_labels))
    assert all(
        (right - left).days >= 3
        for left, right in zip(series_labels, series_labels[1:])
    )
    assert sum(totals.values()) == sum(attendance for _, attendance in sessions)


@pytest.mark.parametrize(
    ("first_day", "last_day"),
    [
        (date(2026, 6, 1), date(2026, 6, 30)),
        (date(2026, 11, 1), date(2026, 11, 30)),
        (date(2026, 12, 1), date(2026, 12, 31)),
        (date(2026, 11, 2), date(2026, 11, 8)),  # thisWeek
        (date(2026, 10, 26), date(2026, 11, 1)),  # previousWeek
    ],
)
def test_weekly_trend_buckets_preserve_attendance(first_day, last_day):
    _assert_weekly_series(first_day, last_day)
