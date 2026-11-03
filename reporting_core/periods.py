from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re

from fastapi import HTTPException


@dataclass(frozen=True)
class ResolvedPeriod:
    start: date
    end: date
    label: str
    token: str


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _parse_date(value: str, field: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise HTTPException(status_code=422, detail=f"{field} must use YYYY-MM-DD format")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"{field} is not a valid calendar date") from None


def resolve_period(
    period: str | None,
    start_date: str | None,
    end_date: str | None,
) -> ResolvedPeriod:
    """Resolve supported reporting tokens to inclusive calendar dates in UTC."""
    if (start_date is None) != (end_date is None):
        raise HTTPException(status_code=422, detail="start_date and end_date must be provided together")

    if start_date is not None and end_date is not None:
        start = _parse_date(start_date, "start_date")
        end = _parse_date(end_date, "end_date")
        if start > end:
            raise HTTPException(status_code=422, detail="start_date must be on or before end_date")
        return ResolvedPeriod(start, end, f"{start.isoformat()} to {end.isoformat()}", "custom")

    now = datetime.now(timezone.utc).date()
    token = period or "thisMonth"

    if re.fullmatch(r"\d{4}-\d{2}", token):
        try:
            year, month = (int(part) for part in token.split("-"))
            start = date(year, month, 1)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid period '{token}'; expected YYYY-MM") from None
        end = _month_end(year, month)
        return ResolvedPeriod(start, end, start.strftime("%B %Y"), token)

    if token == "today":
        start = end = now
        label = "Today"
    elif token in {"thisWeek", "previousWeek"}:
        this_monday = now - timedelta(days=now.weekday())
        start = this_monday if token == "thisWeek" else this_monday - timedelta(days=7)
        end = start + timedelta(days=6)
        label = "This week" if token == "thisWeek" else "Previous week"
    elif token in {"thisMonth", "previousMonth"}:
        year, month = now.year, now.month
        if token == "previousMonth":
            month -= 1
            if month == 0:
                year -= 1
                month = 12
        start = date(year, month, 1)
        end = _month_end(year, month)
        label = start.strftime("%B %Y")
    elif token == "previous7":
        end = now - timedelta(days=1)
        start = end - timedelta(days=6)
        label = "Previous 7 days"
    elif token == "thisQuarter":
        month = ((now.month - 1) // 3) * 3 + 1
        start = date(now.year, month, 1)
        end = _month_end(now.year, month + 2)
        label = f"Q{((month - 1) // 3) + 1} {now.year}"
    elif token == "yearly":
        start = date(now.year, 1, 1)
        end = date(now.year, 12, 31)
        label = str(now.year)
    else:
        allowed = "today, thisWeek, thisMonth, previous7, previousWeek, previousMonth, thisQuarter, yearly, or YYYY-MM"
        raise HTTPException(status_code=422, detail=f"Invalid period '{token}'; allowed values are {allowed}")

    return ResolvedPeriod(start, end, label, token)


def period_to_week_ids(start: date | ResolvedPeriod, end: date | None = None) -> list[str]:
    """Return ISO week identifiers (YYYY-Www) intersecting an inclusive range."""
    if isinstance(start, ResolvedPeriod):
        range_start, range_end = start.start, start.end
    else:
        if end is None:
            raise ValueError("end is required when start is a date")
        range_start, range_end = start, end
    if range_start > range_end:
        raise ValueError("start must be on or before end")

    week_start = range_start - timedelta(days=range_start.weekday())
    week_end = range_end - timedelta(days=range_end.weekday())
    identifiers = []
    while week_start <= week_end:
        iso_year, iso_week, _ = week_start.isocalendar()
        identifiers.append(f"{iso_year}-W{iso_week:02d}")
        week_start += timedelta(days=7)
    return identifiers