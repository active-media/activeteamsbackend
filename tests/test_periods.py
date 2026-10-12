from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

import reporting_core.periods as periods


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2025, 1, 1, 12, tzinfo=timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


def test_previous_month_crosses_year_boundary(monkeypatch):
    monkeypatch.setattr(periods, "datetime", FrozenDateTime)

    result = periods.resolve_period("previousMonth", None, None)

    assert result.start.isoformat() == "2024-12-01"
    assert result.end.isoformat() == "2024-12-31"


def test_week_crosses_december_into_january(monkeypatch):
    monkeypatch.setattr(periods, "datetime", FrozenDateTime)

    result = periods.resolve_period("thisWeek", None, None)

    assert result.start.isoformat() == "2024-12-30"
    assert result.end.isoformat() == "2025-01-05"
    assert periods.period_to_week_ids(result) == ["2025-W01"]


def test_leap_day_is_in_february_period():
    result = periods.resolve_period("2024-02", None, None)

    assert result.start.isoformat() == "2024-02-01"
    assert result.end.isoformat() == "2024-02-29"


def test_explicit_range_wins_over_period_token():
    result = periods.resolve_period("not-a-token", "2024-02-29", "2024-03-01")

    assert result.start.isoformat() == "2024-02-29"
    assert result.end.isoformat() == "2024-03-01"
    assert result.token == "custom"


@pytest.mark.parametrize(
    "period,start_date,end_date",
    [
        ("not-a-period", None, None),
        (None, "2024-02-30", "2024-03-01"),
        (None, "2024-01-01", None),
        (None, "2024-02-02", "2024-02-01"),
        ("2024-13", None, None),
        (None, "", ""),
    ],
)
def test_invalid_period_inputs_return_422(period, start_date, end_date):
    with pytest.raises(HTTPException) as error:
        periods.resolve_period(period, start_date, end_date)

    assert error.value.status_code == 422
    assert error.value.detail