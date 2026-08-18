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
from datetime import datetime, timedelta, timezone
from typing import Optional

from supabase_helpers.supabase_connection import supabase

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXCLUDED_TASK_TYPES: list[str] = ["no answer", "Awaiting Call"]
_STATUS_COMPLETED: tuple[str, ...] = ("completed", "done", "closed", "finished")

# Supabase .not_.in_() requires a list
_STATUS_COMPLETED_LIST = list(_STATUS_COMPLETED)

# The three cell-type values we match against events.event_type_name
_CELL_TYPE_VALUES = ["Cells", "cells", "CELLS"]


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
      {"Organization": "Active Church"}    → filter on capital-O column
      {"Organization": "Active Church"}    → same, lower-case key accepted
    """
    if not org_filter:
        return query
    # Normalise key: both "Organization" and "Organization" are accepted
    org_value = org_filter.get("Organization")
    if org_value:
        # Tasks / Task Types / Users / people all use "Organization" (capital O)
        # events uses "Organization" (lower-case) — we try both via ilike-fallback
        # but .eq() on the actual column name is what matters; callers must pass
        # the right key for the right table.  We expose two helpers below.
        query = query.eq("Organization", org_value)
    return query


def _apply_org_events(query, org_filter: Optional[dict]):
    """Same as _apply_org but for the `events` table which uses lower-case `Organization`."""
    if not org_filter:
        return query
    org_value = org_filter.get("Organization")
    if org_value:
        query = query.eq("Organization", org_value)
    return query


def _is_completed_flag(status: str, task_type: str) -> bool:
    """Return True if the task counts as completed (not excluded by type)."""
    return (status or "").lower() in _STATUS_COMPLETED and task_type not in EXCLUDED_TASK_TYPES


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
            "location, event_date, status, event_type_name, Organization"
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