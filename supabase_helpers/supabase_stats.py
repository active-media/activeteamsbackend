"""
supabase_helpers/stats_queries.py
==================================
Pure synchronous Supabase query functions for the four stats/dashboard
endpoints.  FastAPI async endpoints call these via asyncio.to_thread().

Tables consumed (per Stats_ServiceCheckin_Tables.txt):
  events               – event_id, event_name, event_type_name, event_leader,
                         event_leader_email, location, event_date, status,
                         Organization, org_id, is_active
  event_sessions       – session_id, event_id, session_date, status,
                         checked_in_count, total_headcounts
  Tasks                – _id, name, taskType, status, followup_date,
                         completedAt, created_at, assignedfor,
                         assigned_to_email, Organization, org_id,
                         contacted_person_name/email/phone,
                         is_consolidation_task, consolidation_source,
                         source_display, person_name, person_surname,
                         decision_display_name, priority
  Task Types           – _id, name, Organization, org_id
  Users                – _id, name, surname, email, Organization, org_id
  people               – _id, Name, Surname, Email, InvitedBy, Organization

Key field-name notes
--------------------
* `Tasks` keeps MongoDB-style camelCase column names (taskType, followup_date,
  completedAt, created_at, assignedfor) — confirmed from the sample row in
  Stats_ServiceCheckin_Tables.txt.
* `events` uses snake_case Supabase columns (event_type_name, event_leader,
  event_date) — confirmed from the sample row.
* `Task Types` uses `name` and `Organization` (capital O) — confirmed.
* `Users` uses `Organization` (capital O) — confirmed.
* `people` uses `Organization` (capital O) — confirmed.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Sequence

from supabase_helpers.supabase_connection import supabase, supabase_admin

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCLUDED_TASK_TYPES: list[str] = ["no answer", "Awaiting Call"]
_STATUS_COMPLETED: tuple[str, ...] = ("completed", "done", "closed", "finished")

# Supabase .not_.in_() requires a list
_STATUS_COMPLETED_LIST = list(_STATUS_COMPLETED)

# The three cell-type values we match against events.event_type_name
_CELL_TYPE_VALUES = ["Cells", "cells", "CELLS"]
_CELL_GRAPH_ENTITY_TYPES = {"leader1", "leader12", "leader144"}
_CELL_GRAPH_PERIOD_TYPES = {"monthly", "yearly"}
_CELL_REPORT_PERIODS = {"weekly", "monthly", "three_months", "six_months", "yearly"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _period_range(period: str) -> tuple[datetime, datetime]:
    """Return (start, end) UTC-aware datetimes for the requested period."""
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period in ("today", "daily"):
        return today, today.replace(hour=23, minute=59, second=59, microsecond=999_999)

    if period in ("thisWeek", "weekly"):
        start = today - timedelta(days=today.weekday())          # Monday
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

    if period == "yearly":
        start = today.replace(month=1, day=1)
        end = today.replace(month=12, day=31, hour=23, minute=59, second=59)
        return start, end

    raise ValueError(f"Unknown period: {period!r}")


def _iso(dt: datetime) -> str:
    """Return an ISO-8601 string with timezone offset (Supabase-compatible)."""
    return dt.isoformat()


def _apply_org(query, org_filter: Optional[dict]):
    """
    Append organisation filtering to a Supabase query builder.

    org_filter can be:
      None / {}                            → no restriction (super-admin)
    {"Organization": "Active Church"}    → filter on the table's org column
    {"organization": "Active Church"}    → same, lower-case key accepted
    """
    if not org_filter:
        return query
    # Normalise key: both casing variants are accepted from callers.
    org_value = org_filter.get("Organization") or org_filter.get("organization")
    if org_value:
        # Tasks / Task Types / Users / people use the capitalized column.
        query = query.eq("Organization", org_value)
    return query


def _apply_org_events(query, org_filter: Optional[dict]):
    """Apply the organization filter to the lowercase events.organization column."""
    if not org_filter:
        return query
    org_value = org_filter.get("Organization") or org_filter.get("organization")
    if org_value:
        query = query.eq("organization", org_value)
    return query


def _is_completed_flag(status: str, task_type: str) -> bool:
    """Return True if the task counts as completed (not excluded by type)."""
    return (status or "").lower() in _STATUS_COMPLETED and task_type not in EXCLUDED_TASK_TYPES


def _cells_graph_period_value(period_type: str, value: datetime) -> str:
    """Format a period using the public Cells Graph contract."""
    if period_type == "monthly":
        return value.strftime("%Y-%m")
    return value.strftime("%Y")


def _validate_cells_graph_dimensions(entity_type: str, period_type: str) -> None:
    if entity_type not in _CELL_GRAPH_ENTITY_TYPES:
        raise ValueError("entity_type must be leader1, leader12, or leader144")
    if period_type not in _CELL_GRAPH_PERIOD_TYPES:
        raise ValueError("period_type must be monthly or yearly")


def _validate_cells_graph_period(period: Optional[str], period_type: str) -> None:
    if not period:
        return
    try:
        parsed = datetime.strptime(period, "%Y-%m" if period_type == "monthly" else "%Y")
    except ValueError as exc:
        expected = "YYYY-MM" if period_type == "monthly" else "YYYY"
        raise ValueError(f"period must use {expected} format") from exc
    if _cells_graph_period_value(period_type, parsed) != period:
        raise ValueError("period is not a valid calendar period")


def _cells_graph_row(row: dict) -> dict:
    return {
        "period": str(row.get("period", "")),
        "total_cells": int(row.get("total_cells") or 0),
        "total_attendance": int(row.get("total_attendance") or 0),
        "growth_rate": float(row.get("growth_rate") or 0),
    }


def get_closed_cells_growth_metrics(
    user_id: str,
    entity_type: str,
    period_type: str,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    visible_user_ids: Optional[Sequence[str]] = None,
) -> list[dict]:
    """Return chart-ready, closed growth metrics for one visible leader."""
    _validate_cells_graph_dimensions(entity_type, period_type)
    _validate_cells_graph_period(start_period, period_type)
    _validate_cells_graph_period(end_period, period_type)
    if start_period and end_period and start_period > end_period:
        raise ValueError("start_period must be before or equal to end_period")
    if visible_user_ids is not None and user_id not in visible_user_ids:
        return []

    query = (
        supabase_admin.table("growth_metrics")
        .select("period, total_cells, total_attendance, growth_rate")
        .eq("user_id", user_id)
        .eq("entity_type", entity_type)
        .eq("period_type", period_type)
    )
    if start_period:
        query = query.gte("period", start_period)
    if end_period:
        query = query.lte("period", end_period)

    rows = [_cells_graph_row(row) for row in (query.order("period").execute().data or [])]
    return sorted(rows, key=lambda row: row["period"])


def _cells_graph_live_window(period_type: str, as_of: Optional[datetime]) -> tuple[str, str, str]:
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    period = _cells_graph_period_value(period_type, now)
    if period_type == "monthly":
        start = now.replace(day=1).date()
        if now.month == 12:
            end = now.replace(year=now.year + 1, month=1, day=1).date() - timedelta(days=1)
        else:
            end = now.replace(month=now.month + 1, day=1).date() - timedelta(days=1)
    else:
        start = now.replace(month=1, day=1).date()
        end = now.replace(month=12, day=31).date()
    return period, start.isoformat(), end.isoformat()


def get_growth_visible_user_ids(current_user: dict) -> set[str]:
    """Resolve the authenticated user's self and recursive Users hierarchy."""
    current_id = str(current_user.get("user_id") or current_user.get("_id") or "")
    if not current_id:
        return set()
    if current_user.get("is_supreme_admin") or current_user.get("role") in {"admin", "super_admin"}:
        return set()

    rows = (
        supabase.table("Users")
        .select("_id, leader12, leader144, leader1728")
        .execute()
        .data or []
    )
    visible = {current_id}
    changed = True
    while changed:
        changed = False
        for row in rows:
            row_id = str(row.get("_id") or "")
            leaders = {str(row.get(field)) for field in ("leader12", "leader144", "leader1728") if row.get(field)}
            if row_id and row_id not in visible and leaders & visible:
                visible.add(row_id)
                changed = True
    return visible


def get_current_cells_growth_metric(
    user_id: str,
    entity_type: str,
    period_type: str,
    as_of: Optional[datetime] = None,
    org_filter: Optional[dict] = None,
) -> dict:
    """Compute the in-progress period from live Cells event sessions."""
    _validate_cells_graph_dimensions(entity_type, period_type)
    period, start_date, end_date = _cells_graph_live_window(period_type, as_of)
    user_query = supabase_admin.table("Users").select("_id, email, name, surname").eq("_id", user_id).limit(1)
    user_rows = user_query.execute().data or []
    target = user_rows[0] if user_rows else {}
    target_email = (target.get("email") or "").strip().lower()
    target_name = " ".join(filter(None, [target.get("name"), target.get("surname")])).strip().lower()

    response = (
        supabase_admin.table("event_sessions")
        .select(
            "event_id, session_date, checked_in_count, is_did_not_meet, "
            "events!inner(event_type_name, event_leader, event_leader_email, organization)"
        )
        .gte("session_date", start_date)
        .lte("session_date", end_date)
        .eq("status", "complete")
        .execute()
    )
    event_ids: set[str] = set()
    attendance = 0
    org_value = str((org_filter or {}).get("organization") or (org_filter or {}).get("Organization") or "").lower()
    for row in response.data or []:
        if row.get("is_did_not_meet"):
            continue
        event = row.get("events") or {}
        if (event.get("event_type_name") or "").strip().lower() != "cells":
            continue
        if org_value and (event.get("organization") or "").strip().lower() != org_value:
            continue
        leader_email = (event.get("event_leader_email") or "").strip().lower()
        leader_name = (event.get("event_leader") or "").strip().lower()
        if target_email and leader_email != target_email and target_name != leader_name:
            continue
        event_id = str(row.get("event_id") or "")
        if event_id:
            event_ids.add(event_id)
        attendance += int(row.get("checked_in_count") or 0)

    current_date = (as_of or datetime.now(timezone.utc)).date()
    if period_type == "monthly":
        previous_period = (current_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    else:
        previous_period = str(current_date.year - 1)
    previous = (
        supabase_admin.table("growth_metrics")
        .select("total_cells")
        .eq("user_id", user_id)
        .eq("entity_type", entity_type)
        .eq("period_type", period_type)
        .eq("period", previous_period)
        .limit(1)
        .execute()
        .data or []
    )
    previous_cells = int(previous[0].get("total_cells") or 0) if previous else 0
    total_cells = len(event_ids)
    growth_rate = ((total_cells - previous_cells) / previous_cells) if previous_cells else (1.0 if total_cells else 0.0)
    return {
        "period": period,
        "total_cells": total_cells,
        "total_attendance": attendance,
        "growth_rate": round(growth_rate, 4),
    }


def merge_cells_growth_metrics(closed_rows: Sequence[dict], current_row: dict) -> list[dict]:
    """Merge history with the live bucket, preferring live data on collision."""
    merged = {row["period"]: _cells_graph_row(row) for row in closed_rows if row.get("period")}
    if current_row.get("period"):
        merged[current_row["period"]] = _cells_graph_row(current_row)
    return [merged[key] for key in sorted(merged)]


def get_cells_growth_timeseries(
    user_id: str,
    entity_type: str,
    period_type: str,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    visible_user_ids: Optional[Sequence[str]] = None,
    org_filter: Optional[dict] = None,
) -> list[dict]:
    """Return closed history merged with the live current period."""
    closed = get_closed_cells_growth_metrics(
        user_id,
        entity_type,
        period_type,
        start_period,
        end_period,
        visible_user_ids,
    )
    current = get_current_cells_growth_metric(user_id, entity_type, period_type, org_filter=org_filter)
    return merge_cells_growth_metrics(closed, current)


def _cell_report_window(
    period: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> tuple[datetime, datetime]:
    """Resolve report presets or explicit inclusive UTC dates."""
    if start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided together")
        try:
            start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(end_date).replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError("start_date and end_date must use YYYY-MM-DD format") from exc
    else:
        today = datetime.now(timezone.utc).date()
        if period == "weekly":
            start_date_value = today - timedelta(days=today.weekday())
        elif period == "monthly":
            start_date_value = today.replace(day=1)
        elif period == "three_months":
            start_date_value = today - timedelta(days=89)
        elif period == "six_months":
            start_date_value = today - timedelta(days=182)
        elif period == "yearly":
            start_date_value = today.replace(month=1, day=1)
        else:
            raise ValueError(f"period must be one of: {', '.join(sorted(_CELL_REPORT_PERIODS))}")
        start = datetime.combine(start_date_value, datetime.min.time(), tzinfo=timezone.utc)
        end = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)

    if start > end:
        raise ValueError("start_date must be before or equal to end_date")
    return start, end


def _cell_report_bucket_start(value: date, period: str) -> date:
    if period == "weekly":
        return value - timedelta(days=value.weekday())
    if period == "monthly":
        return value.replace(day=1)
    if period == "yearly":
        return value.replace(month=1, day=1)
    if period in {"three_months", "six_months"}:
        months = 3 if period == "three_months" else 6
        month = ((value.month - 1) // months) * months + 1
        return value.replace(month=month, day=1)
    return value


def _report_date(value: object) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _person_key(row: dict, *fields: str) -> Optional[str]:
    for field in fields:
        value = str(row.get(field) or "").strip().lower()
        if value:
            return value
    return None


def get_cell_report(
    period: str = "monthly",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    scope: str = "all",
    leader_id: Optional[str] = None,
    cell_id: Optional[str] = None,
    org_filter: Optional[dict] = None,
    visible_user_ids: Optional[Sequence[str]] = None,
) -> dict:
    """Aggregate hierarchical cell metrics from the Supabase check-in tables."""
    start, end = _cell_report_window(period, start_date, end_date)
    if scope not in {"all", "leader1", "leader12", "leader144", "leader1728"}:
        raise ValueError("scope must be all, leader1, leader12, leader144, or leader1728")
    if scope != "all" and not leader_id:
        raise ValueError("leader_id is required when scope is not all")
    if visible_user_ids is not None and leader_id and leader_id not in visible_user_ids:
        raise PermissionError("leader is outside the permitted hierarchy")

    users = supabase_admin.table("Users").select(
        "_id, name, surname, email, leader12, leader144, leader1728"
    ).execute().data or []
    selected_user_ids = set()
    selected_users = []
    if scope == "all":
        selected_user_ids = {str(row.get("_id")) for row in users if row.get("_id")}
        if visible_user_ids is not None:
            selected_user_ids &= {str(user_id) for user_id in visible_user_ids}
        selected_users = users
        if visible_user_ids is not None:
            selected_users = [row for row in users if str(row.get("_id") or "") in selected_user_ids]
    else:
        selected_user_ids.add(str(leader_id))
        changed = True
        while changed:
            changed = False
            for row in users:
                row_id = str(row.get("_id") or "")
                leaders = {str(row.get(field)) for field in ("leader12", "leader144", "leader1728") if row.get(field)}
                if row_id and leaders & selected_user_ids and row_id not in selected_user_ids:
                    selected_user_ids.add(row_id)
                    changed = True
        selected_users = [row for row in users if str(row.get("_id") or "") in selected_user_ids]

    leader_keys = set()
    for row in selected_users:
        for field in ("email",):
            value = str(row.get(field) or "").strip().lower()
            if value:
                leader_keys.add(value)
        name = " ".join(filter(None, [row.get("name"), row.get("surname")])).strip().lower()
        if name:
            leader_keys.add(name)

    event_query = supabase_admin.table("events").select(
        "event_id, event_name, event_leader, event_leader_email, event_date, event_type_name, organization"
    ).in_("event_type_name", _CELL_TYPE_VALUES)
    event_query = _apply_org_events(event_query, org_filter)
    events = event_query.execute().data or []
    scoped_events = []
    for event in events:
        event_id = str(event.get("event_id") or "")
        if not event_id:
            continue
        if scope != "all" or visible_user_ids is not None:
            event_leader = str(event.get("event_leader_email") or event.get("event_leader") or "").strip().lower()
            if event_leader not in leader_keys:
                continue
        if cell_id and event_id != str(cell_id):
            continue
        scoped_events.append(event)

    event_ids = [str(event["event_id"]) for event in scoped_events]
    empty = {"new_cells": 0, "new_people": 0, "lives_given": 0, "unique_attendees": 0, "attendance_visits": 0}
    if not event_ids:
        return {"period": period, "date_range": {"start": start.date().isoformat(), "end": end.date().isoformat()}, "scope": {"type": scope, "leader_id": leader_id, "cell_id": cell_id}, "summary": empty, "buckets": []}

    new_cells = {str(event["event_id"]) for event in scoped_events if start.date() <= (_report_date(event.get("event_date")) or date.min) <= end.date()}
    session_rows = supabase_admin.table("event_sessions").select(
        "session_id, event_id, session_date, status, is_did_not_meet, checked_in_count"
    ).in_("event_id", event_ids).gte("session_date", start.date().isoformat()).lte("session_date", end.date().isoformat()).execute().data or []
    session_rows = [row for row in session_rows if str(row.get("status") or "").lower() == "complete" and not row.get("is_did_not_meet")]
    session_ids = [str(row["session_id"]) for row in session_rows if row.get("session_id")]
    attendees = []
    if session_ids:
        attendees = supabase_admin.table("event_session_attendees").select(
            "session_id, event_id, mongo_person_id, email, full_name, is_checked_in"
        ).in_("session_id", session_ids).execute().data or []
    attendees = [row for row in attendees if row.get("is_checked_in", True)]
    new_people = supabase_admin.table("event_new_people").select(
        "event_id, mongo_id, email, phone, added_at"
    ).in_("event_id", event_ids).execute().data or []
    new_people = [row for row in new_people if start.date() <= (_report_date(row.get("added_at")) or date.min) <= end.date()]
    consolidations = supabase_admin.table("event_consolidations").select(
        "event_id, mongo_person_id, person_email, person_phone, created_at, decision_type"
    ).in_("event_id", event_ids).eq("decision_type", "first_time").execute().data or []
    consolidations = [row for row in consolidations if start.date() <= (_report_date(row.get("created_at")) or date.min) <= end.date()]

    summary = {
        "new_cells": len(new_cells),
        "new_people": len({_person_key(row, "mongo_id", "email", "phone") for row in new_people} - {None}),
        "lives_given": len({_person_key(row, "mongo_person_id", "person_email", "person_phone") for row in consolidations} - {None}),
        "unique_attendees": len({_person_key(row, "mongo_person_id", "email", "full_name") for row in attendees} - {None}),
        "attendance_visits": len(attendees),
    }
    bucket_dates = {}

    def bucket_for(value: Optional[date]) -> dict:
        bucket = _cell_report_bucket_start(value or start.date(), period)
        return bucket_dates.setdefault(
            bucket,
            {
                "new_cells": set(),
                "new_people": set(),
                "lives_given": set(),
                "unique_attendees": set(),
                "attendance_visits": 0,
            },
        )

    for event in scoped_events:
        event_date = _report_date(event.get("event_date"))
        if event_date and start.date() <= event_date <= end.date():
            bucket_for(event_date)["new_cells"].add(str(event["event_id"]))
    for row in new_people:
        bucket_for(_report_date(row.get("added_at")))["new_people"].add(
            _person_key(row, "mongo_id", "email", "phone")
        )
    for row in consolidations:
        bucket_for(_report_date(row.get("created_at")))["lives_given"].add(
            _person_key(row, "mongo_person_id", "person_email", "person_phone")
        )
    for row in attendees:
        session = next((item for item in session_rows if str(item.get("session_id")) == str(row.get("session_id"))), None)
        bucket = bucket_for(_report_date(session.get("session_date")) if session else None)
        bucket["attendance_visits"] += 1
        attendee_key = _person_key(row, "mongo_person_id", "email", "full_name")
        if attendee_key:
            bucket["unique_attendees"].add(attendee_key)
    buckets = []
    for key, value in sorted(bucket_dates.items()):
        buckets.append({
            "period": key.isoformat(),
            "new_cells": len(value["new_cells"]),
            "new_people": len(value["new_people"] - {None}),
            "lives_given": len(value["lives_given"] - {None}),
            "unique_attendees": len(value["unique_attendees"]),
            "attendance_visits": value["attendance_visits"],
        })
    return {"period": period, "date_range": {"start": start.date().isoformat(), "end": end.date().isoformat()}, "scope": {"type": scope, "leader_id": leader_id, "cell_id": cell_id}, "summary": summary, "buckets": buckets}


# ---------------------------------------------------------------------------
# 1.  /stats/overview
# ---------------------------------------------------------------------------

def sb_get_stats_overview(
    period: str = "monthly",
    org_filter: Optional[dict] = None,
) -> dict:
    """
    Supabase replacement for GET /stats/overview.

    Uses event_sessions for attendance figures (sum of checked_in_count)
    instead of iterating raw event documents.  This is accurate for both
    recurring (cells) and one-off events once sessions are recorded.
    """
    start, end = _period_range(period)
    start_iso, end_iso = _iso(start), _iso(end)
    start_date = start.date().isoformat()
    end_date   = end.date().isoformat()

    # ── Outstanding cell events (Cells-type, not complete/closed) ──────────
    cell_q = (
        supabase.table("events")
        .select("event_id", count="exact")
        .in_("event_type_name", _CELL_TYPE_VALUES)
        .not_.in_("status", ["Complete", "complete", "closed", "did_not_meet"])
    )
    cell_q = _apply_org_events(cell_q, org_filter)
    outstanding_cells = cell_q.execute().count or 0

    # ── Outstanding Tasks ───────────────────────────────────────────────────
    task_q = (
        supabase.table("Tasks")
        .select("_id", count="exact")
        .not_.in_("status", _STATUS_COMPLETED_LIST)
    )
    task_q = _apply_org(task_q, org_filter)
    outstanding_Tasks = task_q.execute().count or 0

    # ── Total people ────────────────────────────────────────────────────────
    ppl_q = supabase.table("people").select("_id", count="exact")
    if org_filter:
        org_value = org_filter.get("Organization") 
        if org_value:
            ppl_q = ppl_q.eq("Organization", org_value)
    total_people = ppl_q.execute().count or 0

    # ── Attendance in period via event_sessions ──────────────────────────────
    # session_date is a DATE column in Supabase (YYYY-MM-DD)
    sess_q = (
        supabase.table("event_sessions")
        .select("checked_in_count, session_date")
        .gte("session_date", start_date)
        .lte("session_date", end_date)
        .eq("status", "complete")
    )
    sessions = sess_q.execute().data or []
    total_attendance = sum(int(s.get("checked_in_count") or 0) for s in sessions)

    # ── Previous period attendance for growth rate ──────────────────────────
    delta     = end - start
    prev_end  = start - timedelta(microseconds=1)
    prev_start = prev_end - delta

    prev_sess_q = (
        supabase.table("event_sessions")
        .select("checked_in_count")
        .gte("session_date", prev_start.date().isoformat())
        .lte("session_date", prev_end.date().isoformat())
        .eq("status", "complete")
    )
    prev_sessions  = prev_sess_q.execute().data or []
    prev_attendance = sum(int(s.get("checked_in_count") or 0) for s in prev_sessions)

    if prev_attendance > 0:
        growth_rate = round(((total_attendance - prev_attendance) / prev_attendance) * 100, 1)
    else:
        growth_rate = 100.0 if total_attendance > 0 else 0.0

    # ── Attendance breakdown ─────────────────────────────────────────────────
    attendance_breakdown: dict[str, int] = {}
    for sess in sessions:
        date_str = sess.get("session_date", "")
        if not date_str:
            continue
        try:
            sess_date = datetime.fromisoformat(date_str).date()
        except ValueError:
            continue

        if period in ("today", "daily"):
            key = date_str[:10]
        elif period in ("thisWeek", "weekly"):
            key = datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%A")
        else:
            days_back = sess_date.weekday()
            week_start = sess_date - timedelta(days=days_back)
            key = week_start.isoformat()

        attendance_breakdown[key] = attendance_breakdown.get(key, 0) + int(
            sess.get("checked_in_count") or 0
        )

    return {
        "outstanding_cells":     outstanding_cells,
        "outstanding_Tasks":     outstanding_Tasks,
        "total_people":          total_people,
        "total_attendance":      total_attendance,
        "growth_rate":           growth_rate,
        "attendance_breakdown":  attendance_breakdown,
        "period":                period,
    }


# ---------------------------------------------------------------------------
# 2.  /stats/outstanding-items
# ---------------------------------------------------------------------------

def sb_get_outstanding_items(org_filter: Optional[dict] = None) -> dict:
    """
    Supabase replacement for GET /stats/outstanding-items.
    Returns cells and Tasks that are not yet complete.
    """
    # ── Cells ────────────────────────────────────────────────────────────────
    cell_q = (
        supabase.table("events")
        .select(
            "event_id, event_name, event_leader, location, event_date, status"
        )
        .in_("event_type_name", _CELL_TYPE_VALUES)
        .not_.in_("status", ["Complete", "complete", "closed", "did_not_meet"])
        .limit(200)
    )
    cell_q = _apply_org_events(cell_q, org_filter)
    raw_cells = cell_q.execute().data or []

    cells_data = [
        {
            "name":     c.get("event_leader", "Unknown Leader"),
            "location": c.get("location", "Unknown Location"),
            "title":    c.get("event_name", "Untitled Cell"),
            "date":     c.get("event_date"),
            "status":   c.get("status", "pending"),
        }
        for c in raw_cells
    ]

    # ── Tasks ────────────────────────────────────────────────────────────────
    task_q = (
        supabase.table("Tasks")
        .select(
            "_id, name, assignedfor, assigned_to_email, "
            "followup_date, status, taskType"
        )
        .not_.in_("status", _STATUS_COMPLETED_LIST)
        .limit(300)
    )
    task_q = _apply_org(task_q, org_filter)
    raw_Tasks = task_q.execute().data or []

    Tasks_data = [
        {
            "name":    t.get("assignedfor", "Unassigned"),
            "email":   t.get("assigned_to_email", ""),
            "title":   t.get("name", "Untitled Task"),
            "count":   1,
            "dueDate": t.get("followup_date"),
            "status":  t.get("status", "pending"),
        }
        for t in raw_Tasks
    ]

    return {
        "outstanding_cells": cells_data,
        "outstanding_Tasks": Tasks_data,
    }


# ---------------------------------------------------------------------------
# 3.  /stats/dashboard-quick
# ---------------------------------------------------------------------------

def sb_get_dashboard_quick(
    period: str = "today",
    org_filter: Optional[dict] = None,
) -> dict:
    """
    Supabase replacement for GET /stats/dashboard-quick.

    Replicates every count_documents / aggregate call from the MongoDB version.
    The task-type breakdown is computed in Python from a single broad fetch
    (avoids running ~10 separate count queries against the Tasks table).
    """
    start, end = _period_range(period)
    start_iso, end_iso = _iso(start), _iso(end)

    # ── Fetch all Tasks touching the period in ONE query ─────────────────────
    # We use .or_() with a broad date window, then filter in Python.
    # Supabase .or_() format: "col.op.value,col.op.value"
    period_Tasks_q = (
        supabase.table("Tasks")
        .select(
            "_id, taskType, status, followup_date, completedAt, created_at"
        )
        .or_(
            f"followup_date.gte.{start_iso},"
            f"completedAt.gte.{start_iso},"
            f"created_at.gte.{start_iso}"
        )
    )
    period_Tasks_q = _apply_org(period_Tasks_q, org_filter)
    period_Tasks_raw = period_Tasks_q.execute().data or []

    # Python-side filtering + counting — mirrors the MongoDB aggregation pipeline
    total_Tasks_in_period = 0
    Tasks_due_in_period   = 0
    Tasks_comp_in_period  = 0
    task_type_stats: dict[str, dict] = {}

    for t in period_Tasks_raw:
        tt           = t.get("taskType") or "Uncategorized"
        status_lower = (t.get("status") or "").lower()
        is_excluded  = tt in EXCLUDED_TASK_TYPES
        is_completed = status_lower in _STATUS_COMPLETED and not is_excluded

        due_iso  = t.get("followup_date") or ""
        comp_iso = t.get("completedAt")   or ""
        cre_iso  = t.get("created_at")     or ""

        # "touches the period" check (mirrors the $or in the MongoDB query)
        touches = (
            (due_iso  and start_iso <= due_iso  <= end_iso) or
            (comp_iso and start_iso <= comp_iso <= end_iso) or
            (cre_iso  and start_iso <= cre_iso  <= end_iso)
        )
        if not touches:
            continue

        total_Tasks_in_period += 1

        is_due         = bool(due_iso  and start_iso <= due_iso  <= end_iso)
        is_comp_period = bool(
            is_completed and comp_iso and start_iso <= comp_iso <= end_iso
        )

        # "due in period and not yet complete"
        if is_due and not is_completed:
            Tasks_due_in_period += 1

        if is_comp_period:
            Tasks_comp_in_period += 1

        # Per-type stats
        if tt not in task_type_stats:
            task_type_stats[tt] = {
                "total": 0, "completed": 0,
                "completed_in_period": 0, "due_in_period": 0,
                "is_excluded": is_excluded,
            }
        task_type_stats[tt]["total"] += 1
        if is_completed:
            task_type_stats[tt]["completed"] += 1
        if is_comp_period:
            task_type_stats[tt]["completed_in_period"] += 1
        if is_due:
            task_type_stats[tt]["due_in_period"] += 1

    # Compute rates
    for stats in task_type_stats.values():
        t = stats["total"]
        d = stats["due_in_period"]
        stats["completion_rate"] = round(stats["completed"] / t * 100, 2) if t else 0
        stats["completion_rate_in_period"] = (
            round(stats["completed_in_period"] / d * 100, 2) if d else 0
        )

    # ── Overall completed (all time, not excluded) ────────────────────────────
    total_comp_q = (
        supabase.table("Tasks")
        .select("_id", count="exact")
        .in_("status", _STATUS_COMPLETED_LIST)
        .not_.in_("taskType", EXCLUDED_TASK_TYPES)
    )
    total_comp_q = _apply_org(total_comp_q, org_filter)
    total_completed = total_comp_q.execute().count or 0

    # ── Consolidation-specific counts ─────────────────────────────────────────
    cons_total_q = (
        supabase.table("Tasks")
        .select("_id", count="exact")
        .eq("taskType", "consolidation")
    )
    cons_total_q = _apply_org(cons_total_q, org_filter)
    total_consolidation = cons_total_q.execute().count or 0

    cons_done_q = (
        supabase.table("Tasks")
        .select("_id", count="exact")
        .eq("taskType", "consolidation")
        .in_("status", _STATUS_COMPLETED_LIST)
    )
    cons_done_q = _apply_org(cons_done_q, org_filter)
    total_consolidation_completed = cons_done_q.execute().count or 0

    cons_period_q = (
        supabase.table("Tasks")
        .select("_id", count="exact")
        .eq("taskType", "consolidation")
        .in_("status", _STATUS_COMPLETED_LIST)
        .gte("completedAt", start_iso)
        .lte("completedAt", end_iso)
    )
    cons_period_q = _apply_org(cons_period_q, org_filter)
    consolidation_completed_in_period = cons_period_q.execute().count or 0

    # ── Overdue cells ──────────────────────────────────────────────────────────
    overdue_q = (
        supabase.table("events")
        .select("event_id", count="exact")
        .in_("event_type_name", _CELL_TYPE_VALUES)
        .lte("event_date", end_iso)
        .not_.in_("status", ["Complete", "complete", "closed", "did_not_meet"])
    )
    overdue_q = _apply_org_events(overdue_q, org_filter)
    overdue_cells = overdue_q.execute().count or 0

    return {
        "period": period,
        "date_range": {
            "start": start.date().isoformat(),
            "end":   end.date().isoformat(),
        },
        "taskCount":                     total_Tasks_in_period,
        "TasksDueInPeriod":              Tasks_due_in_period,
        "TasksCompletedInPeriod":        Tasks_comp_in_period,
        "totalCompletedTasks":           total_completed,
        "consolidationTasks":            total_consolidation,
        "consolidationCompleted":        total_consolidation_completed,
        "consolidationCompletedInPeriod": consolidation_completed_in_period,
        "consolidationCompletionRate": (
            round(total_consolidation_completed / total_consolidation * 100, 2)
            if total_consolidation else 0
        ),
        "overdueCells": overdue_cells,
        "completionRateDueTasks": (
            round(Tasks_comp_in_period / Tasks_due_in_period * 100, 2)
            if Tasks_due_in_period else 0
        ),
        "overallCompletionRate": (
            round(total_completed / total_Tasks_in_period * 100, 2)
            if total_Tasks_in_period else 0
        ),
        "taskTypeBreakdown":      task_type_stats,
        "totalTaskTypesFound":    len(task_type_stats),
        "excludedTaskTypes":      EXCLUDED_TASK_TYPES,
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "note": (
            "'no answer' and 'Awaiting Call' task types are excluded "
            "from completed counts"
        ),
    }


# ---------------------------------------------------------------------------
# 4.  /stats/dashboard-comprehensive
# ---------------------------------------------------------------------------

def sb_get_dashboard_comprehensive(
    period: str = "today",
    limit: int = 100,
    org_filter: Optional[dict] = None,
) -> dict:
    """
    Supabase replacement for GET /stats/dashboard-comprehensive.

    The MongoDB version used a large aggregation pipeline to group Tasks by
    assignedfor.  Here we fetch all Tasks touching the period in one query
    and do the grouping in Python — same output shape, no pipeline needed.
    """
    start, end = _period_range(period)
    start_iso, end_iso = _iso(start), _iso(end)

    # ── Overdue cells ─────────────────────────────────────────────────────────
    cell_q = (
        supabase.table("events")
        .select(
            "event_id, event_name, event_leader, event_leader_email, "
            "location, event_date, status, event_type_name, organization"
        )
        .in_("event_type_name", _CELL_TYPE_VALUES)
        .lte("event_date", end_iso)
        .not_.in_("status", ["Complete", "complete", "closed", "did_not_meet"])
        .limit(200)
    )
    cell_q = _apply_org_events(cell_q, org_filter)
    overdue_cells_raw = cell_q.execute().data or []

    overdue_cells = [
        {
            "_id":              c.get("event_id", ""),
            "eventName":        c.get("event_name", ""),
            "eventType":        "Cells",
            "eventLeaderName":  c.get("event_leader", ""),
            "eventLeaderEmail": c.get("event_leader_email", ""),
            "location":         c.get("location", ""),
            "date":             c.get("event_date", ""),
            "status":           (c.get("status") or "incomplete").lower(),
            "_is_overdue":      True,
        }
        for c in overdue_cells_raw
    ]

    # ── Fetch all Tasks touching the period ───────────────────────────────────
    task_q = (
        supabase.table("Tasks")
        .select(
            "_id, name, taskType, followup_date, completedAt, created_at, "
            "status, assignedfor, assigned_to_email, type, "
            "contacted_person_name, contacted_person_email, contacted_person_phone, "
            "is_consolidation_task, consolidation_source, source_display, "
            "person_name, person_surname, decision_display_name, priority"
        )
        .or_(
            f"followup_date.gte.{start_iso},"
            f"completedAt.gte.{start_iso},"
            f"created_at.gte.{start_iso}"
        )
        .limit(5000)
    )
    task_q = _apply_org(task_q, org_filter)
    all_Tasks_raw = task_q.execute().data or []

    # ── Group Tasks by assignedfor (mirrors MongoDB $group stage) ─────────────
    groups: dict[str, list[dict]] = defaultdict(list)
    task_type_stats: dict[str, dict] = {}

    global_total        = 0
    global_completed    = 0
    global_comp_period  = 0
    global_due_period   = 0
    global_inc_due      = 0

    for t in all_Tasks_raw:
        tt           = t.get("taskType") or "Uncategorized"
        status_lower = (t.get("status") or "").lower()
        is_excluded  = tt in EXCLUDED_TASK_TYPES
        is_completed = status_lower in _STATUS_COMPLETED and not is_excluded

        due_iso  = t.get("followup_date") or ""
        comp_iso = t.get("completedAt")   or ""
        cre_iso  = t.get("created_at")     or ""

        # Restrict to Tasks that actually touch this period
        touches = (
            (due_iso  and start_iso <= due_iso  <= end_iso) or
            (comp_iso and start_iso <= comp_iso <= end_iso) or
            (cre_iso  and start_iso <= cre_iso  <= end_iso)
        )
        if not touches:
            continue

        is_due         = bool(due_iso  and start_iso <= due_iso  <= end_iso)
        is_comp_period = bool(
            is_completed and comp_iso and start_iso <= comp_iso <= end_iso
        )

        clean = {
            "_id":           str(t.get("_id") or ""),
            "name":          t.get("name", "Unnamed Task"),
            "taskType":      tt,
            "task_type_label": tt,
            "followup_date": due_iso,
            "due_date":      due_iso,
            "completedAt":   comp_iso,
            "created_at":     cre_iso,
            "status":        t.get("status", "Open"),
            "assignedfor":   t.get("assignedfor", ""),
            "type":          t.get("type", "call"),
            "contacted_person": {
                "name":  t.get("contacted_person_name", ""),
                "email": t.get("contacted_person_email", ""),
                "phone": t.get("contacted_person_phone", ""),
            },
            "isRecurring":           False,
            "priority":              t.get("priority", ""),
            "is_completed":          is_completed,
            "is_due_in_period":      is_due,
            "completed_in_period":   is_comp_period,
            "is_excluded_type":      is_excluded,
            "is_consolidation_task": bool(t.get("is_consolidation_task")),
            "consolidation_source":  t.get("consolidation_source", "manual"),
            "source_display":        t.get("source_display", "Manual"),
            "person_name":           t.get("person_name", ""),
            "person_surname":        t.get("person_surname", ""),
            "decision_display_name": t.get("decision_display_name", ""),
            "description":           "",
        }
        groups[t.get("assignedfor") or "unassigned"].append(clean)

        # Global aggregates
        global_total += 1
        if is_completed:
            global_completed += 1
        if is_comp_period:
            global_comp_period += 1
        if is_due:
            global_due_period += 1
        if is_due and not is_completed:
            global_inc_due += 1

        # Per-type aggregates
        if tt not in task_type_stats:
            task_type_stats[tt] = {
                "total": 0, "completed": 0,
                "completed_in_period": 0, "due_in_period": 0,
                "incomplete_due": 0, "is_excluded": is_excluded,
            }
        task_type_stats[tt]["total"] += 1
        if is_completed:
            task_type_stats[tt]["completed"] += 1
        if is_comp_period:
            task_type_stats[tt]["completed_in_period"] += 1
        if is_due:
            task_type_stats[tt]["due_in_period"] += 1
        if is_due and not is_completed:
            task_type_stats[tt]["incomplete_due"] += 1

    # ── Fetch users for name lookup ───────────────────────────────────────────
    users_q = (
        supabase.table("Users")
        .select("_id, email, name, surname")
        .limit(limit)
    )
    if org_filter:
        org_value = org_filter.get("Organization")
        if org_value:
            users_q = users_q.eq("Organization", org_value)
    users_raw = users_q.execute().data or []

    user_map: dict[str, dict] = {}
    for u in users_raw:
        email = (u.get("email") or "").lower()
        name  = (u.get("name") or "").strip()
        surn  = (u.get("surname") or "").strip()
        full  = f"{name} {surn}".strip() or (email.split("@")[0] if "@" in email else email)
        info  = {"_id": str(u.get("_id", "")), "email": email, "fullName": full}
        if email:
            user_map[email] = info

    # ── Build grouped-task list ───────────────────────────────────────────────
    grouped_Tasks: list[dict] = []
    all_Tasks_list: list[dict] = []

    for email, Tasks_list in groups.items():
        user_info = user_map.get(email.lower()) or {
            "_id":      f"unknown_{email}",
            "email":    email,
            "fullName": email.split("@")[0] if "@" in email else email,
        }
        total_u      = len(Tasks_list)
        comp_u       = sum(1 for t in Tasks_list if t["is_completed"])
        incomplete_u = total_u - comp_u
        due_u        = sum(1 for t in Tasks_list if t["is_due_in_period"])
        comp_period_u = sum(1 for t in Tasks_list if t["completed_in_period"])
        inc_due_u    = sum(1 for t in Tasks_list if t["is_due_in_period"] and not t["is_completed"])

        grouped_Tasks.append({
            "user":                       user_info,
            "Tasks":                      Tasks_list,
            "totalCount":                 total_u,
            "completedCount":             comp_u,
            "incompleteCount":            incomplete_u,
            "dueInPeriodCount":           due_u,
            "completedInPeriodCount":     comp_period_u,
            "incompleteDueInPeriodCount": inc_due_u,
            "taskTypes": list({t["taskType"] for t in Tasks_list}),
        })
        all_Tasks_list.extend(Tasks_list)

    grouped_Tasks.sort(key=lambda x: x["user"]["fullName"].lower())

    # ── Available task types from Task Types table ────────────────────────────
    tt_q = supabase.table("Task Types").select("name")
    if org_filter:
        org_value = org_filter.get("Organization")
        if org_value:
            tt_q = tt_q.eq("Organization", org_value)
    all_task_types = [
        r["name"] for r in (tt_q.execute().data or []) if r.get("name")
    ]

    # ── Compute rates ─────────────────────────────────────────────────────────
    comp_rate_due     = round(global_comp_period / global_due_period * 100, 2) if global_due_period else 0
    comp_rate_overall = round(global_completed / global_total * 100, 2) if global_total else 0
    cons_stats        = task_type_stats.get("consolidation", {})

    overview = {
        "total_attendance":                  sum(len(c.get("attendees", [])) for c in overdue_cells),
        "outstanding_cells":                 len(overdue_cells),
        "outstanding_Tasks":                 global_inc_due,
        "Tasks_due_in_period":               global_due_period,
        "Tasks_completed_in_period":         global_comp_period,
        "total_Tasks_in_period":             global_total,
        "total_Tasks_completed":             global_completed,
        "total_Tasks_incomplete":            global_total - global_completed,
        "consolidation_Tasks":               cons_stats.get("total", 0),
        "consolidation_completed":           cons_stats.get("completed", 0),
        "consolidation_completed_in_period": cons_stats.get("completed_in_period", 0),
        "people_behind":                     sum(1 for g in grouped_Tasks if g["incompleteDueInPeriodCount"] > 0),
        "total_users":                       len(users_raw),
        "completion_rate_due_Tasks":         comp_rate_due,
        "completion_rate_overall":           comp_rate_overall,
        "consolidation_completion_rate": (
            round(cons_stats.get("completed", 0) / cons_stats.get("total", 1) * 100, 2)
            if cons_stats.get("total") else 0
        ),
        "task_type_breakdown":    task_type_stats,
        "users_with_Tasks":       len(grouped_Tasks),
        "users_without_Tasks":    len(users_raw) - len(grouped_Tasks),
        "available_task_types":   all_task_types,
        "task_types_found":       list(task_type_stats.keys()),
        "excluded_task_types":    EXCLUDED_TASK_TYPES,
        "total_unique_task_types": len(task_type_stats),
        "note": (
            "'no answer' and 'Awaiting Call' task types are excluded "
            "from completed counts"
        ),
    }

    all_users_list = [
        {
            "_id":      u["_id"],
            "email":    u["email"],
            "name":     u["fullName"].split()[0] if u["fullName"].split() else "",
            "surname":  " ".join(u["fullName"].split()[1:]) if len(u["fullName"].split()) > 1 else "",
            "fullName": u["fullName"],
        }
        for u in user_map.values()
        if not u["_id"].startswith("unknown_")
    ]

    return {
        "overview":             overview,
        "overdueCells":         overdue_cells,
        "groupedTasks":         grouped_Tasks,
        "allTasks":             all_Tasks_list,
        "allUsers":             all_users_list,
        "period":               period,
        "date_range": {
            "start": start.date().isoformat(),
            "end":   end.date().isoformat(),
        },
        "task_type_stats":      task_type_stats,
        "available_task_types": all_task_types,
        "task_types_found":     list(task_type_stats.keys()),
        "excluded_task_types":  EXCLUDED_TASK_TYPES,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# 5.  /stats/people-with-Tasks  (capture stats)
# ---------------------------------------------------------------------------

def sb_get_people_capture_stats(org_filter: Optional[dict] = None) -> dict:
    """
    Supabase replacement for GET /stats/people-with-Tasks.

    The old MongoDB version relied on a `captured_by` field that was
    inconsistently populated.  This version groups by `InvitedBy` in the
    people table — the canonical field set at signup / import.
    """
    ppl_q = supabase.table("people").select("InvitedBy, Name, Surname, Email")
    if org_filter:
        org_value = org_filter.get("Organization") 
        if org_value:
            ppl_q = ppl_q.eq("Organization", org_value)

    people_rows = ppl_q.execute().data or []

    groups: dict[str, list[dict]] = defaultdict(list)
    for p in people_rows:
        inviter = (p.get("InvitedBy") or "").strip()
        if inviter:
            name = f"{p.get('Name','').strip()} {p.get('Surname','').strip()}".strip()
            groups[inviter].append({"name": name, "email": p.get("Email", "")})

    stats = sorted(
        [
            {
                "capturer_name":        name,
                "capturer_email":       "",   # email not stored on InvitedBy string
                "people_captured_count": len(people),
                "captured_people":      people,
            }
            for name, people in groups.items()
        ],
        key=lambda x: x["people_captured_count"],
        reverse=True,
    )

    total_captured = sum(s["people_captured_count"] for s in stats)

    if not stats:
        return {
            "capture_stats":        [],
            "total_capturers":       0,
            "total_people_captured": 0,
            "message":               "No capture data found",
        }

    return {
        "capture_stats":        stats,
        "total_capturers":       len(stats),
        "total_people_captured": total_captured,
        "message": (
            f"Found {len(stats)} team members who captured "
            f"{total_captured} people total"
        ),
    }