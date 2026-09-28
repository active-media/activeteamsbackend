from datetime import datetime, timezone

from supabase_helpers.supabase_stats import (
    _cells_graph_live_window,
    merge_cells_growth_metrics,
)


def test_merge_prefers_live_current_period_and_sorts():
    closed = [
        {
            "period": "2026-01",
            "total_cells": 4,
            "total_attendance": 20,
            "growth_rate": 0.1,
        },
        {
            "period": "2026-02",
            "total_cells": 5,
            "total_attendance": 25,
            "growth_rate": 0.25,
        },
    ]
    current = {
        "period": "2026-02",
        "total_cells": 6,
        "total_attendance": 30,
        "growth_rate": 0.2,
    }

    assert merge_cells_growth_metrics(closed, current) == [
        {
            "period": "2026-01",
            "total_cells": 4,
            "total_attendance": 20,
            "growth_rate": 0.1,
        },
        {
            "period": "2026-02",
            "total_cells": 6,
            "total_attendance": 30,
            "growth_rate": 0.2,
        },
    ]


def test_monthly_live_window_handles_december():
    period, start, end = _cells_graph_live_window(
        "monthly", datetime(2026, 12, 15, tzinfo=timezone.utc)
    )

    assert (period, start, end) == ("2026-12", "2026-12-01", "2026-12-31")


def test_yearly_live_window_uses_calendar_year():
    period, start, end = _cells_graph_live_window(
        "yearly", datetime(2026, 9, 3, tzinfo=timezone.utc)
    )

    assert (period, start, end) == ("2026", "2026-01-01", "2026-12-31")
