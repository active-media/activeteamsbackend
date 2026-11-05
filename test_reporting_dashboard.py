from datetime import datetime, timezone
from types import SimpleNamespace

from supabase_helpers import supabase_stats
from supabase_helpers.supabase_stats import _aggregate_reporting_dashboard, _period_range
from reporting_core.envelope import dashboard_envelope
from reporting_core.periods import resolve_period


def test_reporting_dashboard_aggregates_scoped_events_and_sessions():
    start, end = _period_range("2026-10")
    events = [
        {
            "event_id": "cell-1",
            "event_name": "North Cell",
            "event_type_name": "Cells",
            "event_leader": "Lead A",
            "campus_id": "campus-north",
            "location": "North",
            "status": "active",
            "is_active": True,
        },
        {
            "event_id": "cell-2",
            "event_name": "South Cell",
            "event_type_name": "Cells",
            "event_leader": "Lead A",
            "campus_id": "campus-south",
            "location": "South",
            "status": "active",
            "is_active": True,
        },
        {
            "event_id": "service-1",
            "event_type_name": "Sunday Service",
            "campus_id": "campus-north",
            "location": "North",
            "status": "active",
            "is_active": True,
        },
        {
            "event_id": "closed-cell",
            "event_type_name": "Cells",
            "event_leader": "Lead B",
            "location": "North",
            "status": "complete",
            "is_active": False,
            "campus_id": "campus-north",
        },
        {
            "event_id": "cell-no-session",
            "event_type_name": "Cells",
            "event_leader": "Lead B",
            "status": "active",
            "is_active": True,
            "campus_id": "campus-north",
        },
    ]
    sessions = [
        {"event_id": "cell-1", "session_date": "2026-10-02", "status": "complete", "checked_in_count": 4},
        {"event_id": "cell-1", "session_date": "2026-10-06", "status": "complete", "checked_in_count": 3},
        {"event_id": "cell-1", "session_date": "2026-09-30", "status": "complete", "checked_in_count": 100},
        {"event_id": "cell-2", "session_date": "2026-10-05", "status": "complete", "checked_in_count": 2},
        {"event_id": "cell-2", "session_date": "2026-10-07", "status": "scheduled", "checked_in_count": 50},
        {"event_id": "service-1", "session_date": "2026-10-09", "status": "complete", "checked_in_count": 10},
        {"event_id": "closed-cell", "session_date": "2026-10-09", "status": "complete", "checked_in_count": 20},
        {"event_id": "other-org", "session_date": "2026-10-09", "status": "complete", "checked_in_count": 50},
    ]
    previous_sessions = [{"event_id": "cell-1", "checked_in_count": 5}]

    result = _aggregate_reporting_dashboard(
        "2026-10", start, end, events, sessions, previous_sessions, total_people=20
    )

    assert result["total_attendance"] == 39
    assert result["growth_rate"] == 680.0
    assert result["total_people"] == 20
    assert result["active_cells"] == 2
    assert result["active_leaders"] == 1
    assert result["leaders"][0]["attendance"] == 9
    assert result["leaders"][0]["cells"] == 2
    assert result["attendance_by_campus"] == {"campus-north": 37, "campus-south": 2}
    assert result["campus_comparison"] == [
        {"campus_id": "campus-north", "value": 37},
        {"campus_id": "campus-south", "value": 2},
    ]
    assert sum(result["attendance_trend"]["values"]) == 39
    assert sum(result["attendance_trend_by_campus"]["campus-north"]) == 37
    assert sum(result["attendance_trend_by_campus"]["campus-south"]) == 2


def test_reporting_dashboard_period_ranges():
    month_start, month_end = _period_range("2026-10")
    quarter_start, quarter_end = _period_range("thisQuarter")

    assert month_start == datetime(2026, 10, 1, tzinfo=timezone.utc)
    assert month_end.date().isoformat() == "2026-10-31"
    assert quarter_start.month in (1, 4, 7, 10)
    assert quarter_start.day == 1
    assert quarter_end >= quarter_start


def test_ministry_rows_use_available_attendance_and_leave_completion_unknown(monkeypatch):
    class Query:
        def __init__(self, rows):
            self.rows = rows

        def select(self, _columns):
            return self

        def in_(self, _column, _values):
            return self

        def range(self, _start, _end):
            return self

        def execute(self):
            return SimpleNamespace(data=self.rows)

    class Client:
        def table(self, table_name):
            assert table_name == "event_attendees"
            return Query([
                {"event_id": "life-1", "mongo_person_id": "person-1", "is_persistent": True},
                {"event_id": "life-2", "mongo_person_id": "person-1", "is_persistent": False},
                {"event_id": "life-1", "mongo_person_id": "person-2", "is_persistent": False},
            ])

    monkeypatch.setattr(supabase_stats, "supabase", Client())
    start, end = _period_range("2026-10")
    events = [
        {"event_id": "life-1", "event_type_name": "Life Class", "event_date": "2026-10-04"},
        {"event_id": "life-2", "event_type_name": "Life Class", "event_date": "2026-10-11"},
        {"event_id": "school-cell", "event_type_name": "School Cell", "is_school_cell": True},
    ]
    sessions = [
        {"event_id": "life-1", "session_date": "2026-10-04", "status": "complete", "checked_in_count": 1},
        {"event_id": "school-cell", "session_date": "2026-10-04", "status": "complete", "checked_in_count": 12},
    ]

    rows = supabase_stats._ministry_rows(events, sessions, start, end)
    by_name = {row["ministry"]: row for row in rows}

    assert by_name["Life Class"] == {
        "ministry": "Life Class",
        "registered": 2,
        "attended": 1,
        "completed": None,
        "attendance_percent": 50.0,
        "completion_percent": None,
    }
    assert by_name["School Cell"]["registered"] is None
    assert by_name["School Cell"]["attended"] == 12
    assert by_name["School Cell"]["completed"] is None
    assert by_name["School of Leaders"]["attended"] is None
    assert by_name["Plan 40"]["attended"] is None


def test_dashboard_envelope_uses_campus_directory_and_row_metrics_for_kpis():
    ministry_rows = [
        {
            "ministry": name,
            "registered": registered,
            "attended": attended,
            "completed": None,
            "attendance_percent": percent,
            "completion_percent": None,
        }
        for name, registered, attended, percent in [
            ("Life Class", 20, 16, 80.0),
            ("School of Leaders", None, None, None),
            ("Plan 40", None, None, None),
            ("School Cell", None, 12, None),
        ]
    ]
    envelope = dashboard_envelope(
        {
            "total_attendance": 24,
            "growth_rate": 10,
            "active_cells": 2,
            "active_cell_sessions": 4,
            "new_visitors": 3,
            "campuses": [{"campus_id": "campus-north", "label": "North"}],
            "campus_comparison": [{"campus_id": "campus-north", "value": 24}],
            "ministry_performance": ministry_rows,
        },
        resolve_period("2026-10", None, None),
        "campus-north",
        {"total_target": 1600},
        {"rows": [{"target": 6, "this_week": 4}]},
    )
    kpis = {item.key: item for item in envelope.kpis}
    table = next(table for table in envelope.tables if table.key == "ministry_performance")
    rows = {row["ministry"]: row for row in table.rows}

    assert envelope.campuses[0].campus_id == "campus-north"
    assert envelope.series[1].points[0].x == "North"
    assert envelope.series[1].points[0].group == "campus-north"
    assert kpis["life_class"].value == rows["Life Class"]["attended"] == 16
    assert rows["School Cell"]["attended"] == 12
    assert rows["Life Class"]["completed"] is None
    assert rows["Life Class"]["completion_percent"] is None
    assert kpis["total_attendance"].hint == "3 new visitors"
    assert kpis["active_cells"].hint == "4 completed sessions"
    assert kpis["service_target"].hint == "Target 1,600"
    assert kpis["twelve_tasks"].hint == "Target 6"
    assert all(item.hint for item in envelope.kpis)
