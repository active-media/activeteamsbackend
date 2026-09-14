from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from supabase_helpers.supabase_connection import supabase


def _period_range(period: str) -> tuple[datetime, datetime]:
    """Return (start, end) UTC-aware datetimes for the requested period."""
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period in ("today", "daily"):
        return today, today.replace(hour=23, minute=59, second=59, microsecond=999_999)

    if period in ("thisWeek", "weekly"):
        start = today - timedelta(days=today.weekday())  # Monday
        return start, start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    if period in ("thisMonth", "monthly"):
        start = today.replace(day=1)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
        else:
            end = datetime(today.year, today.month + 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
        return start, end

    if period == "previous7":
        end = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        start = (end - timedelta(days=6)).replace(hour=0, minute=0, second=0)
        return start, end

    if period == "previousWeek":
        last = today - timedelta(weeks=1)
        start = last - timedelta(days=last.weekday())
        return start, start + timedelta(days=6, hours=23, minutes=59, seconds=59)

    if period == "previousMonth":
        year, month = today.year, today.month - 1
        if month == 0:
            year, month = year - 1, 12
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
        else:
            end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(microseconds=1)
        return start, end

    raise ValueError(f"Unknown period: {period!r}")


def _iso(dt: datetime) -> str:
    """Return an ISO-8601 string with timezone offset (Supabase-compatible)."""
    return dt.isoformat()


def _count_leaders(tasks: list[dict], start: datetime, end: datetime) -> dict[str, int]:
    """Count tasks assigned to each leader that touch the supplied period."""
    start_iso, end_iso = _iso(start), _iso(end)
    counts: dict[str, int] = defaultdict(int)

    for task in tasks:
        touches_period = any(
            start_iso <= str(task.get(field) or "") <= end_iso
            for field in ("followup_date", "completedAt", "created_at")
        )
        if not touches_period:
            continue

        assigned_for = (task.get("assignedfor") or task.get("assigned_to_email") or "").strip().lower()
        if assigned_for:
            counts[assigned_for] += 1

    return dict(counts)


def sb_get_twelve_tasks_report(period: str = "thisWeek", org_filter: Optional[dict] = None) -> dict:
    """
    Fetch Twelve Tasks grouped by leader and compare it with the prior period.

    The existing ``leaders`` shape is retained for compatibility. Each leader
    also includes the previous count and week-over-week change.
    """
    start, end = _period_range(period)
    period_length = end - start + timedelta(microseconds=1)
    previous_start = start - period_length
    previous_end = start - timedelta(microseconds=1)

    # Fetch both periods in one bounded query; Python applies the individual
    # period boundaries because each task can have three relevant dates.
    task_q = (
        supabase.table("Tasks")
        .select(
            "_id, name, taskType, status, followup_date, completedAt, created_at, "
            "assignedfor, assigned_to_email"
        )
        .or_(
            f"followup_date.gte.{_iso(previous_start)},"
            f"completedAt.gte.{_iso(previous_start)},"
            f"created_at.gte.{_iso(previous_start)}"
        )
    )

    if org_filter:
        org_value = org_filter.get("Organization") or org_filter.get("organization")
        if org_value:
            task_q = task_q.eq("Organization", org_value)

    tasks = task_q.execute().data or []
    current_counts = _count_leaders(tasks, start, end)
    previous_counts = _count_leaders(tasks, previous_start, previous_end)

    leaders = []
    for name in set(current_counts) | set(previous_counts):
        current = current_counts.get(name, 0)
        previous = previous_counts.get(name, 0)
        change = current - previous
        leaders.append(
            {
                "name": name,
                "total_captured": current,
                "previous_total_captured": previous,
                "change": change,
                "change_percent": round((change / previous) * 100, 2) if previous else None,
            }
        )

    leaders.sort(key=lambda leader: (-leader["total_captured"], leader["name"]))
    return {
        "period": {"start": start.date().isoformat(), "end": end.date().isoformat()},
        "previous_period": {
            "start": previous_start.date().isoformat(),
            "end": previous_end.date().isoformat(),
        },
        "leaders": leaders,
    }