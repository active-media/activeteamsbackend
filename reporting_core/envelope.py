from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .periods import ResolvedPeriod
from .schemas import DashboardEnvelope, ReportEnvelope


def _meta(report_type: str, title: str, period: ResolvedPeriod, campus: str | None) -> dict:
    return {
        "report_type": report_type,
        "title": title,
        "period": {
            "start": period.start.isoformat(),
            "end": period.end.isoformat(),
            "label": period.label,
            "token": period.token,
        },
        "campus": campus,
        "generated_at": datetime.now(timezone.utc),
    }


def _number(value: Any) -> int | float | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _known(value: Any) -> Any:
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"", "unknown", "unassigned", "unnamed cell", "cell", "—", "–", "-"}:
            return None
    return value


def _clean_row(row: dict) -> dict:
    return {key: _known(value) if isinstance(value, str) else value for key, value in row.items()}


def dashboard_envelope(
    data: dict,
    period: ResolvedPeriod,
    campus: str | None = None,
    service_target: dict | None = None,
    twelve_tasks: dict | None = None,
) -> DashboardEnvelope:
    rows = data.get("cells") or []
    attendance = _number(data.get("total_attendance"))
    active_cells = _number(data.get("active_cells"))
    target_total = _number((service_target or {}).get("total_target"))
    task_rows = (twelve_tasks or {}).get("rows") or []
    task_total = sum(_number(row.get("this_week")) or 0 for row in task_rows)
    task_value = task_total if twelve_tasks is not None else None
    ministry_rows = data.get("ministry_performance") or [
        {
            "ministry": name,
            "registered": None,
            "attended": None,
            "completed": None,
            "attendance_percent": None,
            "completion_percent": None,
        }
        for name in ("Life Class", "School of Leaders", "Plan 40", "School Cell")
    ]
    ministry_by_name = {row.get("ministry"): row for row in ministry_rows}
    campus_options = data.get("campuses") or []
    campus_labels = {str(row["campus_id"]): row["label"] for row in campus_options}
    targets = [row.get("target") for row in task_rows if row.get("target") is not None]
    target_hint = f"Target {target_total:,.0f}" if target_total is not None else "Target unavailable"
    tasks_hint = f"Target {sum(targets):,.0f}" if targets else "Targets unavailable"
    new_visitors = data.get("new_visitors")
    attendance_hint = f"{new_visitors:,} new visitors" if new_visitors is not None else "Attendance in selected period"
    active_cell_sessions = data.get("active_cell_sessions")
    active_cells_hint = (
        f"{active_cell_sessions:,} completed sessions"
        if active_cell_sessions is not None
        else "With completed sessions in selected period"
    )

    labels = (data.get("attendance_trend") or {}).get("labels") or []
    values = (data.get("attendance_trend") or {}).get("values") or []
    campus_rows = data.get("campus_comparison") or []
    weekly_cells = data.get("active_cells_by_week") or []

    return DashboardEnvelope.model_validate({
        "meta": _meta("dashboard", "Reporting Dashboard", period, campus),
        "campuses": campus_options,
        "kpis": [
            {"key": "total_attendance", "label": "Total Attendance", "value": attendance, "delta": _number(data.get("growth_rate")), "format": "number", "hint": attendance_hint},
            {"key": "active_cells", "label": "Active Cells", "value": active_cells, "format": "number", "hint": active_cells_hint},
            {"key": "life_class", "label": "Life Class", "value": _ministry_kpi(ministry_by_name, "Life Class"), "format": "number", "hint": _ministry_hint(ministry_by_name, "Life Class")},
            {"key": "school_of_leaders", "label": "School of Leaders", "value": _ministry_kpi(ministry_by_name, "School of Leaders"), "format": "number", "hint": _ministry_hint(ministry_by_name, "School of Leaders")},
            {"key": "plan_40", "label": "Plan 40", "value": _ministry_kpi(ministry_by_name, "Plan 40"), "format": "number", "hint": _ministry_hint(ministry_by_name, "Plan 40")},
            {"key": "service_target", "label": "Service Target", "value": target_total, "format": "number", "hint": target_hint},
            {"key": "twelve_tasks", "label": "Twelve Tasks", "value": task_value, "format": "number", "hint": tasks_hint},
            {"key": "staff_interns_youth", "label": "Staff/Interns/Youth", "value": None, "format": "number", "hint": "No verified source data"},
        ],
        "tables": [
            {
                "key": "ministry_performance",
                "title": "Ministry Performance",
                "columns": [
                    {"key": "ministry", "label": "Ministry"},
                    {"key": "registered", "label": "Registered", "format": "number"},
                    {"key": "attended", "label": "Attended", "format": "number"},
                    {"key": "completed", "label": "Completed", "format": "number"},
                    {"key": "attendance_percent", "label": "Attendance %", "format": "percent"},
                    {"key": "completion_percent", "label": "Completion %", "format": "percent"},
                ],
                "rows": ministry_rows,
            },
            {
                "key": "cells",
                "title": "Cells",
                "columns": [
                    {"key": "name", "label": "Cell"},
                    {"key": "leader", "label": "Leader"},
                    {"key": "attendance", "label": "Attendance", "format": "number"},
                    {"key": "location", "label": "Campus"},
                ],
                "rows": [
                    {
                        **_clean_row(row),
                        "location": campus_labels.get(str(row.get("campus_id"))),
                    }
                    for row in rows
                ],
            },
        ],
        "series": [
            {
                "key": "attendance_trend",
                "title": "Attendance Trend",
                "type": "line",
                "points": [
                    {"x": label, "y": _number(values[index]) if index < len(values) else None}
                    for index, label in enumerate(labels)
                ],
            },
            {
                "key": "campus_comparison",
                "title": "Campus Comparison",
                "type": "bar",
                "points": [
                    {
                        "x": campus_labels.get(str(row.get("campus_id")), str(row.get("campus_id") or "")),
                        "y": _number(row.get("value")),
                        "group": str(row.get("campus_id")) if row.get("campus_id") is not None else None,
                    }
                    for row in campus_rows
                ],
            },
            {
                "key": "active_cells_trend",
                "title": "Active Cells Trend",
                "type": "line",
                "points": [
                    {"x": labels[index], "y": _number(weekly_cells[index]) if index < len(weekly_cells) else None}
                    for index in range(len(labels))
                ],
            },
        ],
    })


def _ministry_kpi(ministry_by_name: dict, ministry: str):
    return ministry_by_name.get(ministry, {}).get("attended")


def _ministry_hint(ministry_by_name: dict, ministry: str) -> str:
    row = ministry_by_name.get(ministry, {})
    registered = row.get("registered")
    attended = row.get("attended")
    if registered is not None and attended is not None:
        return f"{registered:,} registered"
    if attended is not None:
        return f"{attended:,} attended; registration unavailable"
    return "No verified source data"


def twelve_tasks_envelope(data: dict, period: ResolvedPeriod) -> ReportEnvelope:
    rows = data.get("rows") or []
    total = sum(_number(row.get("this_week")) or 0 for row in rows)
    return ReportEnvelope.model_validate({
        "meta": _meta("twelve_tasks", "Twelve Tasks", period, None),
        "kpis": [
            {"key": "completed_tasks", "label": "Completed Tasks", "value": total, "format": "number"},
            {"key": "tracked_people", "label": "Tracked People", "value": len(rows), "format": "number"},
        ],
        "tables": [{
            "key": "twelve_tasks",
            "title": "Twelve Tasks",
            "columns": [
                {"key": "name", "label": "Name"},
                {"key": "last_week", "label": "Last Week", "format": "number"},
                {"key": "target", "label": "Target", "format": "number"},
                {"key": "this_week", "label": "This Week", "format": "number"},
            ],
            "rows": [_clean_row(row) for row in rows],
        }],
        "series": [],
    })


def school_cell_envelope(data: dict, period: ResolvedPeriod) -> ReportEnvelope:
    weekly = data.get("weeklyData") or []
    leaders = data.get("leaders") or []
    captured = sum(_number(row.get("total_captured")) or 0 for row in leaders)
    return ReportEnvelope.model_validate({
        "kpis": [
            {"key": "captured", "label": "Cells Captured", "value": captured, "format": "number"},
            {"key": "leaders", "label": "Leaders", "value": len(leaders), "format": "number"},
        ],
        "tables": [
            {
                "key": "weekly_summary",
                "title": "Weekly Summary",
                "columns": [
                    {"key": "week_identifier", "label": "Week"},
                    {"key": "event_name", "label": "Cell"},
                    {"key": "cell_count", "label": "Cells", "format": "number"},
                    {"key": "attendance", "label": "Attendance", "format": "number"},
                ],
                "rows": [
                    _clean_row(row)
                    for row in weekly
                ],
            },
            {
                "key": "leaders",
                "columns": [
                    {"key": "name", "label": "Leader"},
                    {"key": "total_captured", "label": "Captured", "format": "number"},
                    {"key": "unique_cells", "label": "Unique Cells", "format": "number"},
                ],
                "rows": [_clean_row(row) for row in leaders],
            },
        ],
        "series": [{
            "key": "weekly_attendance",
            "title": "Weekly Attendance",
            "type": "line",
            "points": [{"x": row.get("week_identifier") or "", "y": _number(row.get("attendance"))} for row in weekly],
        }],
    })


def service_target_envelope(data: dict, period: ResolvedPeriod) -> ReportEnvelope:
    targets = data.get("targets") or []
    return ReportEnvelope.model_validate({
        "meta": _meta("service_target", "Service Target", period, None),
        "kpis": [
            {"key": "total_target", "label": "Total Target", "value": _number(data.get("total_target")), "format": "number"},
            {"key": "leaders", "label": "Leaders", "value": _number(data.get("total_targets")), "format": "number"},
        ],
        "tables": [{
            "key": "targets",
            "title": "Service Targets",
            "columns": [
                {"key": "leader_name", "label": "Leader"},
                {"key": "week_identifier", "label": "Week"},
                {"key": "target_count", "label": "Target", "format": "number"},
                {"key": "actual", "label": "Actual", "format": "number"},
            ],
            "rows": [{**_clean_row(row), "actual": None} for row in targets],
        }],
        "series": [],
    })


def cells_graph_envelope(data: dict, period: ResolvedPeriod, campus: str | None = None) -> ReportEnvelope:
    result = dashboard_envelope(data, period, campus)
    result.meta.report_type = "cells_graph"
    result.meta.title = "Cells Graph"
    result.kpis = result.kpis[:2]
    result.tables = [table for table in result.tables if table.key == "cells"]
    result.series = [series for series in result.series if series.key in {"attendance_trend", "active_cells_trend"}]
    return result