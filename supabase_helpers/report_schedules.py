"""
supabase_helpers/report_schedules.py

CRUD helpers for the report_schedules table (Scheduled Reports feature).

Follows the project convention: plain synchronous functions here, called
from FastAPI routes via asyncio.to_thread(). No SDK filtering on joined
tables is used (there are none for this table), so this module doesn't
need the post-fetch-filter workaround described in technical-notes.
"""

from datetime import datetime, timezone
from typing import Optional

from supabase_helpers.supabase_connection import supabase_admin as supabase  # service_role client, bypasses RLS

TABLE = "report_schedules"

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
VALID_WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
VALID_FORMATS = {"pdf", "csv", "xlsx"}


class ReportScheduleValidationError(ValueError):
    """Raised when incoming schedule data fails validation."""
    pass


def _validate_schedule_payload(payload: dict, partial: bool = False) -> None:
    """
    Validate a create/update payload. When partial=True (used for PATCH-style
    updates like toggling `active`), only validate the fields present.
    """
    def field_present(key):
        return key in payload and payload[key] is not None

    if (not partial or field_present("report_type")) and payload.get("report_type") not in VALID_REPORT_TYPES:
        raise ReportScheduleValidationError(f"Invalid report_type: {payload.get('report_type')}")

    if not partial or field_present("frequency"):
        frequency = payload.get("frequency")
        if frequency not in VALID_FREQUENCIES:
            raise ReportScheduleValidationError(f"Invalid frequency: {frequency}")

        if frequency == "weekly" and payload.get("weekday") not in VALID_WEEKDAYS:
            raise ReportScheduleValidationError("weekday is required and must be valid when frequency is 'weekly'")

        if frequency == "monthly":
            day = payload.get("day_of_month")
            if not isinstance(day, int) or not (1 <= day <= 28):
                raise ReportScheduleValidationError("day_of_month must be an integer between 1 and 28 when frequency is 'monthly'")

    if (not partial or field_present("format")) and payload.get("format") not in VALID_FORMATS:
        raise ReportScheduleValidationError(f"Invalid format: {payload.get('format')}")

    if not partial or field_present("recipients"):
        recipients = payload.get("recipients")
        if not recipients or not isinstance(recipients, list):
            raise ReportScheduleValidationError("recipients must be a non-empty list of email addresses")


def list_report_schedules(organization: str) -> list[dict]:
    """Return all schedules for an organization, newest first."""
    res = (
        supabase.table(TABLE)
        .select("*")
        .eq("Organization", organization)
        .order("created_at", desc=True)
        .execute()
    )
    return res.data or []


def get_report_schedule(schedule_id: str, organization: str) -> Optional[dict]:
    res = (
        supabase.table(TABLE)
        .select("*")
        .eq("id", schedule_id)
        .eq("Organization", organization)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def create_report_schedule(payload: dict, organization: str, created_by: str) -> dict:
    _validate_schedule_payload(payload, partial=False)

    row = {
        "Organization": organization,
        "report_type": payload["report_type"],
        "frequency": payload["frequency"],
        "weekday": payload.get("weekday") if payload["frequency"] == "weekly" else None,
        "day_of_month": payload.get("day_of_month") if payload["frequency"] == "monthly" else None,
        "time_of_day": payload["time"],  # 'HH:MM'
        "format": payload["format"],
        "recipients": payload["recipients"],
        "active": True,
        "created_by": created_by,
    }

    res = supabase.table(TABLE).insert(row).execute()
    return res.data[0]


def update_report_schedule(schedule_id: str, payload: dict, organization: str) -> Optional[dict]:
    """
    Partial update — used both for full edits from the modal and for the
    active/pause toggle (which only sends {"active": bool}).
    """
    existing = get_report_schedule(schedule_id, organization)
    if not existing:
        return None

    _validate_schedule_payload(payload, partial=True)

    update_fields = {}
    for key in ("report_type", "frequency", "weekday", "day_of_month", "format", "recipients", "active"):
        if key in payload and payload[key] is not None:
            update_fields[key] = payload[key]
    if "time" in payload and payload["time"] is not None:
        update_fields["time_of_day"] = payload["time"]

    # Clear the field that no longer applies when frequency changes.
    frequency = update_fields.get("frequency", existing["frequency"])
    if frequency == "weekly":
        update_fields["day_of_month"] = None
    elif frequency == "monthly":
        update_fields["weekday"] = None

    if not update_fields:
        return existing

    res = (
        supabase.table(TABLE)
        .update(update_fields)
        .eq("id", schedule_id)
        .eq("Organization", organization)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def delete_report_schedule(schedule_id: str, organization: str) -> bool:
    res = (
        supabase.table(TABLE)
        .delete()
        .eq("id", schedule_id)
        .eq("Organization", organization)
        .execute()
    )
    return bool(res.data)


def mark_schedule_run(schedule_id: str, status: str, error: Optional[str] = None) -> None:
    """Called by the runner after each attempted send."""
    supabase.table(TABLE).update({
        "last_run_at": datetime.now(timezone.utc).isoformat(),
        "last_run_status": status,
        "last_run_error": error,
    }).eq("id", schedule_id).execute()


def list_all_active_schedules() -> list[dict]:
    """Used by the runner — not org-scoped, pulls every active schedule across orgs."""
    res = supabase.table(TABLE).select("*").eq("active", True).execute()
    return res.data or []