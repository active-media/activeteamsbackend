"""
supabase_helpers/report_schedule_runner.py

A lightweight in-process scheduler for Scheduled Reports. Runs a check every
minute: for each active report_schedule, if it's due (matches the current
UTC time to the minute), it generates the report and emails it, then records
the outcome.

This is an in-process APScheduler job, not a separate worker — it starts
inside the same FastAPI process via start_report_schedule_runner() on
startup. That's fine for a single-instance deployment. If Active Teams v2
ever runs multiple backend instances, this needs to move to a real worker
(e.g. a dedicated cron dyno) or gain a locking mechanism, since every
instance would otherwise send duplicate emails.

TODO before this is functional — two stub points below:
  1. `_generate_report()` — wire up to whatever already produces each
     report's PDF/CSV/XLSX (the Reports page "Generate" buttons must already
     call something like this — reuse it).
  2. `_send_report_email()` — wire up to your existing email-sending
     mechanism (the one used for password reset emails, if there's a shared
     SMTP/provider client, is a good place to look).
"""

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from supabase_helpers import report_schedules as report_schedules_helpers

logger = logging.getLogger("report_schedule_runner")

_scheduler: BackgroundScheduler | None = None

WEEKDAY_INDEX = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def _is_due(schedule: dict, now: datetime) -> bool:
    """
    All schedule times are stored and compared in UTC for now (see note in
    the migration). If schedules need to respect each org's local timezone,
    store a timezone alongside `time_of_day` and convert `now` per-org here.
    """
    time_of_day = schedule.get("time_of_day", "")  # 'HH:MM:SS' from Postgres TIME
    try:
        hour, minute = (int(p) for p in time_of_day.split(":")[:2])
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
    STUB — replace with a call to the existing report-generation logic for
    schedule["report_type"], producing bytes in schedule["format"].
    Raise an exception on failure; the caller records it via mark_schedule_run.
    """
    raise NotImplementedError(
        f"Wire _generate_report() up to the existing generator for "
        f"'{schedule['report_type']}' (format: {schedule['format']})."
    )


def _send_report_email(schedule: dict, report_bytes: bytes) -> None:
    """
    STUB — replace with a call to the existing email-sending mechanism.
    """
    raise NotImplementedError(
        f"Wire _send_report_email() up to send to {schedule['recipients']}."
    )


def _run_due_schedules():
    now = datetime.now(timezone.utc)
    try:
        schedules = report_schedules_helpers.list_all_active_schedules()
    except Exception:
        logger.exception("Failed to fetch active report schedules")
        return

    for schedule in schedules:
        if not _is_due(schedule, now):
            continue

        schedule_id = schedule["id"]
        logger.info("Running due schedule %s (%s)", schedule_id, schedule["report_type"])
        try:
            report_bytes = _generate_report(schedule)
            _send_report_email(schedule, report_bytes)
            report_schedules_helpers.mark_schedule_run(schedule_id, status="success")
        except Exception as e:
            logger.exception("Schedule %s failed", schedule_id)
            report_schedules_helpers.mark_schedule_run(schedule_id, status="failed", error=str(e))


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