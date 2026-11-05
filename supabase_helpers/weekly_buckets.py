from datetime import date, timedelta


def build_week_buckets(
    first_day: date, last_day: date
) -> tuple[list[date], dict[date, date], dict[date, date]]:
    """Return canonical Monday keys, source-week mappings, and clipped Sunday labels."""
    week_starts = []
    cursor = first_day - timedelta(days=first_day.weekday())
    while cursor <= last_day:
        week_starts.append(cursor)
        cursor += timedelta(days=7)

    canonical_by_week = {week_start: week_start for week_start in week_starts}
    if len(week_starts) > 1 and last_day < week_starts[-1] + timedelta(days=6):
        canonical_by_week[week_starts[-1]] = week_starts[-2]
        week_starts.pop()

    labels = {
        week_start: min(week_start + timedelta(days=6), last_day)
        for week_start in week_starts
    }
    return week_starts, canonical_by_week, labels
