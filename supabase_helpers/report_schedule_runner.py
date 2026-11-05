"""
supabase_helpers/report_schedule_runner.py

A lightweight in-process scheduler for Scheduled Reports. Runs a check every
minute: for each active report_schedule, if it's due (next_run_at <= now),
it generates the report and emails it, then records the outcome.

This is an in-process APScheduler job, not a separate worker — it starts
inside the same FastAPI process via start_report_schedule_runner() on
startup. That's fine for a single-instance deployment. If Active Teams v2
ever runs multiple backend instances, this needs to move to a real worker
(e.g. a dedicated cron dyno) or gain a locking mechanism, since every
instance would otherwise send duplicate emails.

The runner is SYNC — no await/asyncio inside. All Supabase helpers are
called synchronously. A manual /run-now endpoint uses asyncio.to_thread
to invoke the same sync function from an async route.
"""

import logging
import calendar as _calendar
import csv
import io
import os
import tempfile
from datetime import datetime, timezone, timedelta

from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from supabase_helpers import report_schedules as report_schedules_helpers

logger = logging.getLogger("report_schedule_runner")

SAST = ZoneInfo("Africa/Johannesburg")
UTC = ZoneInfo("UTC")

WEEKDAY_INDEX = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}

VALID_REPORT_TYPES = {
    "overall_church_performance",
    "cells_report",
    "life_class_report",
    "school_of_leaders_report",
    "plan_40_report",
    "school_cell_report",
    "service_target_report",
    "twelve_tasks_report",
    "staff_interns_youth_report",
}
VALID_FREQUENCIES = {"daily", "weekly", "monthly"}
VALID_FORMATS = {"pdf", "csv", "xlsx"}


def compute_next_run(schedule: dict, after_utc: datetime) -> str:
    """
    Compute the next run_at timestamp in UTC ISO format based on schedule
    frequency and time_of_day. after_utc is the just-completed run time in UTC.
    """
    freq = schedule.get("frequency", "daily")
    time_str = schedule.get("time_of_day", "00:00")  # 'HH:MM' or 'HH:MM:SS'

    # Parse time_of_day: support both 'HH:MM' and 'HH:MM:SS'
    parts = time_str.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, AttributeError):
        hour, minute = 0, 0

    # Convert after_utc to SAST for day-of-week/day-of-month math
    after_saft = after_utc.astimezone(SAST)

    if freq == "daily":
        next_saft = after_saft.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=1)
        return next_saft.astimezone(UTC).isoformat()

    if freq == "weekly":
        dow_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2,
            "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
        }
        target_dow = dow_map.get(schedule.get("weekday", "monday"))
        if target_dow is None:
            next_saft = after_saft.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=1)
        else:
            current_dow = after_saft.weekday()
            ahead = (target_dow - current_dow) % 7
            if ahead == 0:
                ahead = 7  # next week if same day
            next_saft = after_saft + timedelta(days=ahead)
            next_saft = next_saft.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return next_saft.astimezone(UTC).isoformat()

    if freq == "monthly":
        day_of_month = schedule.get("day_of_month", 1)
        capped_dom = min(day_of_month, 28)
        year = after_saft.year
        month = after_saft.month + 1
        if month > 12:
            month = 1
            year += 1
        max_day = _calendar.monthrange(year, month)[1]
        real_dom = min(capped_dom, max_day)
        try:
            next_date = after_saft.replace(year=year, month=month, day=real_dom, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            next_date = datetime(year, month, 1, hour, minute, 0, tzinfo=SAST)
        return next_date.astimezone(UTC).isoformat()

    # Default fallback to daily
    next_saft = after_saft.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=1)
    return next_saft.astimezone(UTC).isoformat()


def _is_due(schedule: dict, now_utc: datetime) -> bool:
    """
    Check if a schedule is due by comparing next_run_at <= now.
    Falls back to time-of-day check if next_run_at is null.
    """
    active = schedule.get("active", False)
    next_run = schedule.get("next_run_at")
    if not active:
        return False
    if not next_run:
        return _is_due_timeofday(schedule, now_utc)
    try:
        return now_utc >= datetime.fromisoformat(next_run)
    except (ValueError, TypeError):
        return False


def _is_due_timeofday(schedule: dict, now: datetime) -> bool:
    """
    Fallback: check if current UTC time matches the schedule's time_of_day.
    Handles both 'HH:MM' and 'HH:MM:SS'. All schedule times are stored and
    compared in UTC.
    """
    time_of_day = schedule.get("time_of_day", "")  # 'HH:MM:SS' from Postgres TIME
    try:
        parts = time_of_day.split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, AttributeError):
        logger.warning("Schedule %s has unparseable time_of_day: %r", schedule.get("id"), time_of_day)
        return False

    if now.hour != hour or now.minute != minute:
        return False

    frequency = schedule.get("frequency")
    if frequency == "daily":
        return True
    if frequency == "weekly":
        return now.weekday() == WEEKDAY_INDEX.get(schedule.get("weekday"), -1)
    if frequency == "monthly":
        return now.day == schedule.get("day_of_month")
    return False


def _generate_report(schedule: dict) -> bytes:
    """
    Generate the report for schedule["report_type"] in schedule["format"].
    Reuses existing generators in reporting_core.

    Returns bytes of the report file.
    Raises ValueError on unsupported report_type or format.
    Unsupported report_type or format = error status with a clear message,
    not an exception (caller should catch ValueError and record as error).
    """
    from reporting_core.router import CATALOG_BY_TYPE, LEGACY_SCHEDULE_TYPES
    from reporting_core.periods import resolve_period

    report_type = schedule.get("report_type", "")
    fmt = schedule.get("format", "pdf")

    # Map legacy schedule type to canonical report type
    canonical = LEGACY_SCHEDULE_TYPES.get(report_type, report_type)

    if canonical not in CATALOG_BY_TYPE:
        raise ValueError(f"Unsupported report_type: '{report_type}'")

    if fmt not in {"pdf", "csv", "xlsx"}:
        raise ValueError(f"Unsupported format: '{fmt}'")

    if not CATALOG_BY_TYPE[canonical].implemented:
        raise ValueError(f"Report '{canonical}' is not implemented.")

    org = schedule.get("Organization", "")

    # Determine period token based on frequency (the period that just ended)
    if schedule.get("frequency") == "daily":
        period_token = "previous7"
    elif schedule.get("frequency") == "weekly":
        period_token = "previousWeek"
    elif schedule.get("frequency") == "monthly":
        period_token = "previousMonth"
    else:
        period_token = "thisMonth"

    # Resolve period (this gives us start/end dates for the period that just ended)
    try:
        resolved = resolve_period(period_token, None, None)
    except Exception:
        resolved = resolve_period("thisMonth", None, None)

    start, end = resolved.start.isoformat(), resolved.end.isoformat()

    # Build org_filter
    org_filter = {"Organization": org} if org else None

    # Select generator based on canonical report type
    if canonical == "service_target":
        from supabase_helpers.service_targets import sb_get_service_target_report

        data = sb_get_service_target_report(
            week=None, org_filter=org_filter, visible_leader_ids=None,
        )
        total_target = sum(t.get("target_count", 0) for t in data.get("targets", []))

        class _Table:
            def __init__(self, key, title, columns, rows):
                self.key = key
                self.title = title
                self.columns = columns
                self.rows = rows

        class _Envelope:
            def __init__(self, tables):
                self.tables = tables

        envelope = _Envelope([
            _Table(
                key="service_target",
                title="Service Target",
                columns=[
                    {"label": "Leader", "key": "leader_name", "format": None},
                    {"label": "Target", "key": "target_count", "format": "number"},
                ],
                rows=data.get("targets", []),
            )
        ])

        if fmt == "csv":
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            for t in envelope.tables:
                writer.writerow([t.title])
                writer.writerow([c["label"] for c in t.columns])
                for row in t.rows:
                    writer.writerow([row.get(c["key"]) for c in t.columns])
            return output.getvalue().encode("utf-8")

        if fmt == "xlsx":
            from openpyxl import Workbook
            workbook = Workbook()
            workbook.remove(workbook.active)
            for t in envelope.tables:
                sheet = workbook.create_sheet(title=t.key[:31] or "Report")
                sheet.append([t.title])
                sheet.append([c["label"] for c in t.columns])
                for row in t.rows:
                    sheet.append([row.get(c["key"]) for c in t.columns])
            import io as _io
            stream = _io.BytesIO()
            workbook.save(stream)
            return stream.getvalue()

    elif canonical == "school_cell":
        from supabase_helpers.school_cell_report import build_school_cell_excel, sb_get_school_cell_report

        if fmt == "xlsx":
            return build_school_cell_excel(
                org_filter=org_filter,
                start=start,
                end=end,
                visible_leader_ids=None,
                campus=None,
            )
        if fmt == "csv":
            data = sb_get_school_cell_report(org_filter=org_filter, start=start, end=end, visible_leader_ids=None)
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            for t in data.get("tables", []):
                writer.writerow([t["title"]])
                writer.writerow([c["label"] for c in t.columns])
                for row in t.get("rows", []):
                    writer.writerow([row.get(c["key"]) for c in t.columns])
            return output.getvalue().encode("utf-8")

    elif canonical == "twelve_tasks":
        from Reports.twelve_tasks import sb_get_twelve_tasks_report, export_twelve_tasks_xlsx
        from reporting_core.periods import resolve_period

        resolved = resolve_period("previous7", None, None)
        start2, end2 = resolved.start.isoformat(), resolved.end.isoformat()

        if fmt == "xlsx":
            report = sb_get_twelve_tasks_report(
                period.token, org, None, start2, end2, None, nullable_targets=False,
            )
            handle = tempfile.mktemp(suffix=".xlsx")
            try:
                export_twelve_tasks_xlsx(report, handle)
                with open(handle, "rb") as f:
                    result = f.read()
                return result
            finally:
                if os.path.exists(handle):
                    os.unlink(handle)
        if fmt == "csv":
            report = sb_get_twelve_tasks_report(period.token, org, None, None, None, None, True)
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            writer.writerow(["Name", "This Week", "Target"])
            for person in report.get("people", []):
                writer.writerow([person.get("name", ""), person.get("this_week", 0), person.get("target", 0)])
            return output.getvalue().encode("utf-8")

    elif canonical == "cells_graph":
        from supabase_helpers.excel_export import build_cells_excel

        if fmt == "xlsx":
            return build_cells_excel(
                org_filter=org_filter,
                start=start,
                end=end,
                visible_leader_ids=None,
                campus=None,
            )
        if fmt == "csv":
            return b""

    # Fallback: generic attempt using _generic_xlsx pattern
    raise ValueError(f"Report generation not fully wired for '{canonical}' format '{fmt}'")


def start_report_schedule_runner():
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(_run_due_schedules, "cron", minute="*", id="report_schedule_check")
    _scheduler.start()
    logger.info("Report schedule runner started (checking every minute)")
    return _scheduler


def stop_report_schedule_runner():
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None


_scheduler: object = None  # type: ignore


# Legacy: _run_due_schedules is the main synchronous runner loop
def _run_due_schedules():
    """Synchronous scheduler job: check due schedules and run them.

    This is called by APScheduler every minute. It is SYNC — no await/asyncio.
    Uses the atomic next_run_at claim pattern (amendment 4).
    """
    now = datetime.now(timezone.utc)
    try:
        schedules = report_schedules_helpers.list_all_active_schedules()
    except Exception:
        logger.exception("Failed to fetch active report schedules")
        return

    for schedule in schedules:
        # Amendment 4: due = active and next_run_at <= now
        # Atomic claim: try to update next_run_at to new value where old value matches
        if not schedule.get("active"):
            continue
        if not schedule.get("next_run_at"):
            from supabase_helpers.report_schedule_runner import compute_next_run
            new_next = compute_next_run(schedule, now)
        else:
            from supabase_helpers.report_schedule_runner import compute_next_run
            new_next = compute_next_run(schedule, datetime.fromisoformat(schedule["next_run_at"]))

        # Atomic claim — only proceed if we are the first to update next_run_at
        updated = supabase.table("report_schedules").update({
            "next_run_at": new_next,
        }).eq("id", schedule["id"]).eq("next_run_at", schedule.get("next_run_at")).execute()

        if not updated.data:
            # Another instance already claimed this schedule; skip
            continue

        # Schedule claimed successfully — now generate and send
        try:
            report_bytes = _generate_report(schedule)
            from auth.email_utils import send_report_email
            recipients = schedule.get("recipients", [])
            if recipients:
                send_report_email(
                    to_email=recipients[0],
                    subject=f"Scheduled Report: {schedule['report_type']}",
                    body=f"Your {schedule['report_type']} report has been generated.",
                    attachment_bytes=report_bytes,
                    filename=f"{schedule['report_type']}.{schedule['format']}",
                    mimetype=f"application/{schedule['format']}",
                )
            from supabase_helpers.report_schedules import mark_schedule_run
            mark_schedule_run(schedule["id"], status="success", next_run_at=new_next)
        except ValueError as ve:
            # Unsupported report_type or format = error status with message
            from supabase_helpers.report_schedules import mark_schedule_run
            mark_schedule_run(schedule["id"], status="error", error=str(ve))
        except Exception as e:
            from supabase_helpers.report_schedules import mark_schedule_run
            mark_schedule_run(schedule["id"], status="error", error=str(e))