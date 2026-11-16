from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from supabase_helpers.supabase_connection import supabase_admin as supabase
from supabase_helpers.weekly_buckets import build_week_buckets

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task assignees are reconciled to the user IDs used by Admin target rows.
# ---------------------------------------------------------------------------
PERSON_KEY_FIELDS = ("assignedfor", "assigned_to_email")
SAST = ZoneInfo("Africa/Johannesburg")
COMPLETED_STATUSES = frozenset({"completed", "done", "closed", "finished"})

# ---------------------------------------------------------------------------
# Indicator colors — must match the legend in the exported report.
# These fills are applied to the "Last week" and "Actual this week"
# columns when comparing each row's completed count against that row's target.
# ---------------------------------------------------------------------------
FILL_ON_TARGET = PatternFill("solid", fgColor="FFC000")     # orange — count meets target exactly
FILL_LESS_THAN_TARGET = PatternFill("solid", fgColor="FF0000")  # red — count below target
FILL_MORE_THAN_TARGET = PatternFill("solid", fgColor="00B050")  # green — count above target

HEADER_FONT = Font(bold=True)


def _sast_midnight_utc(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=SAST).astimezone(timezone.utc)


def _iso_week_range(week_identifier: str) -> tuple[datetime, datetime]:
    try:
        year_text, week_text = week_identifier.split("-W", 1)
        monday = date.fromisocalendar(int(year_text), int(week_text), 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ISO week identifier: {week_identifier!r}") from exc
    return _sast_midnight_utc(monday), _sast_midnight_utc(monday + timedelta(days=7))


def _period_range(period: str) -> tuple[datetime, datetime]:
    """Return the requested period as UTC-aware [start, end) bounds from SAST dates."""
    today = datetime.now(SAST).date()
    if period in ("today", "daily"):
        return _sast_midnight_utc(today), _sast_midnight_utc(today + timedelta(days=1))

    if period in ("thisWeek", "weekly"):
        monday = today - timedelta(days=today.weekday())
        return _sast_midnight_utc(monday), _sast_midnight_utc(monday + timedelta(days=7))

    if period in ("thisMonth", "monthly"):
        month_start = today.replace(day=1)
        if today.month == 12:
            next_month = date(today.year + 1, 1, 1)
        else:
            next_month = date(today.year, today.month + 1, 1)
        return _sast_midnight_utc(month_start), _sast_midnight_utc(next_month)

    if period == "previousWeek":
        current_monday = today - timedelta(days=today.weekday())
        previous_monday = current_monday - timedelta(days=7)
        return _sast_midnight_utc(previous_monday), _sast_midnight_utc(current_monday)

    if period == "previousMonth":
        current_month = today.replace(day=1)
        current_month_start = _sast_midnight_utc(current_month)
        if current_month.month == 1:
            previous_month = date(current_month.year - 1, 12, 1)
        else:
            previous_month = date(current_month.year, current_month.month - 1, 1)
        return _sast_midnight_utc(previous_month), current_month_start

    raise ValueError(f"Unknown period: {period!r}")


def _iso(dt: datetime) -> str:
    """Return an ISO-8601 string with timezone offset (Supabase-compatible).

    Supabase expects ISO-8601 formatted timestamps with UTC offset
    for filtering queries (gte/lte on completedAt).
    """
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Roster (Column A) — persisted, admin-managed list of tracked people.
# Independent of live LeaderPath/hierarchy so it only changes when an admin
# explicitly adds/removes someone. Removing someone stops future tracking
# but never deletes their historical rows (soft delete via `active`).
# ---------------------------------------------------------------------------

def sb_get_tracked_people(
    leader12_id: Optional[str] = None,
    org_filter: Optional[dict] = None,
    visible_leader_ids: Optional[set[str]] = None,
) -> list[dict]:
    """Return the persisted, admin-curated roster for the Twelve Tasks report.

    Only rows with active=True are returned — this is what should populate
    Column A. Historical report rows for removed people are preserved
    elsewhere (task history), just not re-included in future roster pulls.
    """
    query = (
        supabase.table("twelve_tasks_tracked_people")
        .select("person_id, leader12_id, org, display_name, added_at")
        .eq("active", True)
    )
    if leader12_id:
        query = query.eq("leader12_id", leader12_id)

    rows = query.execute().data or []

    # Supabase SDK rule: never filter on joined-table columns with .eq()/.ilike();
    # fetch and filter Organization in Python instead.
    # Join with People table to get gender
    if rows:
        person_ids = [str(r.get("person_id") or "") for r in rows if r.get("person_id")]
        people_query = (
            supabase.table("People")
            .select("_id, Gender")
            .in_("_id", person_ids)
        )
        people_result = people_query.execute().data or []
        people_by_id = {str(p.get("_id")): p.get("Gender") or "" for p in people_result if p.get("_id")}
        for row in rows:
            pid = str(row.get("person_id") or "")
            row["gender"] = people_by_id.get(pid, "")
    else:
        people_by_id = {}

    if org_filter:
        org_value = org_filter.get("Organization") or org_filter.get("organization")
        if org_value:
            rows = [r for r in rows if r.get("org") == org_value]
    if visible_leader_ids is not None:
        rows = [row for row in rows if str(row.get("person_id") or "") in visible_leader_ids]

    return rows


def sb_add_tracked_person(person_id: str, leader12_id: str, display_name: str, org: str) -> dict:
    """Admin action: add a person to the tracked roster.

    Starts at 0/0 — caller does not need to seed history rows.
    If the person was previously removed (active=False), this re-activates
    them (sets active=True) rather than inserting a duplicate row.
    """
    existing = (
        supabase.table("twelve_tasks_tracked_people")
        .select("person_id")
        .eq("person_id", person_id)
        .eq("leader12_id", leader12_id)
        .execute()
        .data
    )
    if existing:
        # Re-activating someone previously removed — never insert a duplicate row.
        return (
            supabase.table("twelve_tasks_tracked_people")
            .update({"active": True, "display_name": display_name, "org": org})
            .eq("person_id", person_id)
            .eq("leader12_id", leader12_id)
            .execute()
            .data
        )

    return (
        supabase.table("twelve_tasks_tracked_people")
        .insert(
            {
                "person_id": person_id,
                "leader12_id": leader12_id,
                "display_name": display_name,
                "org": org,
                "active": True,
                "added_at": _iso(datetime.now(timezone.utc)),
            }
        )
        .execute()
        .data
    )


def sb_remove_tracked_person(person_id: str, leader12_id: str) -> dict:
    """Admin action: soft-remove a person from the tracked roster.

    Only stops future tracking — their completed/task history remains
    intact in past report snapshots/exports. The row stays in the
    table with active=False, so historical data is never deleted.
    """
    return (
        supabase.table("twelve_tasks_tracked_people")
        .update({"active": False})
        .eq("person_id", person_id)
        .eq("leader12_id", leader12_id)
        .execute()
        .data
    )


# ---------------------------------------------------------------------------
# Targets (Column C) — per-person, admin/leader-editable, persists until
# explicitly changed. Independent of any other report's target field.
# ---------------------------------------------------------------------------

def sb_get_admin_task_targets(
    org_filter: Optional[dict],
    week_identifiers: list[str],
) -> list[dict]:
    """Return the Admin-defined Twelve Tasks targets for the selected org/weeks."""
    org = (org_filter or {}).get("Organization") or (org_filter or {}).get("organization")
    if not org or not week_identifiers:
        return []
    result = (
        supabase.table("twelve_tasks_targets")
        .select("person_id, target, week_identifier, organization, is_default")
        .eq("organization", org)
        .in_("week_identifier", week_identifiers)
        .execute()
    )
    return [
        row for row in (result.data or [])
        if row.get("person_id") or row.get("is_default")
    ]


def _fetch_target_people(person_ids: list[str]) -> dict[str, dict]:
    if not person_ids:
        return {}
    user_rows = (
        supabase.table("Users")
        .select("_id, name, surname, gender, email")
        .in_("_id", person_ids)
        .execute()
        .data
        or []
    )
    people = {
        str(row["_id"]): {
            "Name": row.get("name"),
            "Surname": row.get("surname"),
            "Gender": row.get("gender"),
            "Email": row.get("email"),
        }
        for row in user_rows
        if row.get("_id") is not None
    }

    missing_ids = [person_id for person_id in person_ids if person_id not in people]
    if missing_ids:
        legacy_rows = (
            supabase.table("People")
            .select("_id, Name, Surname, Gender, Email")
            .in_("_id", missing_ids)
            .execute()
            .data
            or []
        )
        people.update(
            {
                str(row["_id"]): row
                for row in legacy_rows
                if row.get("_id") is not None
            }
        )
    return people


def sb_set_target(person_id: str, target: int, updated_by: Optional[str] = None) -> dict:
    """Admin/leader action: set or change a person's target. Persists as-is
    until this is called again — never reset automatically."""
    payload = {
        "person_id": person_id,
        "target": target,
        "updated_at": _iso(datetime.now(timezone.utc)),
    }
    if updated_by:
        payload["updated_by"] = updated_by

    existing = (
        supabase.table("twelve_tasks_targets").select("person_id").eq("person_id", person_id).execute().data
    )
    if existing:
        return (
            supabase.table("twelve_tasks_targets")
            .update(payload)
            .eq("person_id", person_id)
            .execute()
            .data
        )
    return supabase.table("twelve_tasks_targets").insert(payload).execute().data


# ---------------------------------------------------------------------------
# Task counting — fixed to only count actually-COMPLETED tasks, using
# completedAt as the sole date field. Previously this also matched on
# followup_date (due date) and created_at, which inflated counts with
# tasks that were merely due or newly created, not completed.
# ---------------------------------------------------------------------------

def _count_completed_by_person(
    tasks: list[dict],
    start: datetime,
    end: datetime,
    person_aliases: Optional[dict[str, str]] = None,
) -> dict[str, int]:
    """Count completed tasks per person within the given period.

    Completion is determined by completedAt only. Bounds are timezone-aware
    UTC datetimes and use an inclusive start, exclusive end.
    """
    counts: dict[str, int] = defaultdict(int)

    for task in tasks:
        if str(task.get("status") or "").strip().casefold() not in COMPLETED_STATUSES:
            continue

        raw_completed_at = task.get("completedAt")
        if not raw_completed_at:
            continue
        if isinstance(raw_completed_at, datetime):
            completed_at = raw_completed_at
        else:
            completed_at_text = str(raw_completed_at).strip()
            if completed_at_text.endswith("+00"):
                completed_at_text += ":00"
            elif completed_at_text.endswith("Z"):
                completed_at_text = completed_at_text[:-1] + "+00:00"
            try:
                completed_at = datetime.fromisoformat(completed_at_text)
            except ValueError:
                continue
        if completed_at.tzinfo is None:
            completed_at = completed_at.replace(tzinfo=timezone.utc)
        else:
            completed_at = completed_at.astimezone(timezone.utc)
        if not (start <= completed_at < end):
            continue

        person_key = None
        for field in PERSON_KEY_FIELDS:
            value = str(task.get(field) or "").strip()
            if value:
                mapped_person = (person_aliases or {}).get(value.casefold())
                if mapped_person:
                    person_key = mapped_person
                    break
                if person_aliases is None:
                    person_key = value
                    break
        if person_key:
            counts[person_key] += 1

    return dict(counts)


def _fetch_completed_tasks(start: datetime, end: datetime, org_filter: Optional[dict] = None) -> list[dict]:
    """Fetch tasks with completed status spellings and completion times in [start, end)."""

    task_q = (
        supabase.table("Tasks")
        .select("_id, name, taskType, status, completedAt, assignedfor, assigned_to_email")
        .gte("completedAt", _iso(start))
        .lt("completedAt", _iso(end))
        .or_(
            "status.ilike.completed,status.ilike.done,"
            "status.ilike.closed,status.ilike.finished"
        )
    )

    if org_filter:
        org_value = org_filter.get("Organization") or org_filter.get("organization")
        if org_value:
            task_q = task_q.eq("Organization", org_value)

    return task_q.execute().data or []


def sb_get_completed_task_actuals(
    org_filter: Optional[dict],
    date_ranges: dict[str, tuple[datetime, datetime]],
    person_ids: list[str],
) -> dict[str, dict[str, int]]:
    """Return completed-task totals per person for each named UTC date range."""
    if not date_ranges or not person_ids:
        return {key: {} for key in date_ranges}

    people = _fetch_target_people(person_ids)
    person_aliases = {}
    for person_id in person_ids:
        person_id = str(person_id)
        person_aliases[person_id.casefold()] = person_id
        person = people.get(person_id, {})
        email = str(person.get("Email") or "").strip().casefold()
        if email:
            person_aliases[email] = person_id

    fetch_start = min(start for start, _ in date_ranges.values())
    fetch_end = max(end for _, end in date_ranges.values())
    tasks = _fetch_completed_tasks(fetch_start, fetch_end, org_filter=org_filter)
    return {
        key: _count_completed_by_person(tasks, start, end, person_aliases)
        for key, (start, end) in date_ranges.items()
    }


# ---------------------------------------------------------------------------
# Report assembly — Admin weekly target rows are the source of truth.
# ---------------------------------------------------------------------------

def sb_get_twelve_tasks_report(
    period: str = "thisWeek",
    org_filter: Optional[dict] = None,
    leader12_id: Optional[str] = None,
    start_date=None,
    end_date=None,
    visible_leader_ids: Optional[set[str]] = None,
    nullable_targets: bool = False,
) -> dict:
    """Build the Twelve Tasks report dictionary.

    Rows are driven only by the per-person target rows created on the Admin
    page for the requested organization and ISO week(s) in the selected period.
    Target rows from all overlapping ISO weeks are aggregated into one row
    per person. An organization default remains one Admin-defined row; it is
    not expanded into fabricated person rows.

    Parameters:
      leader12_id:  if set, only return people assigned to this leader
      org_filter:   if set, only include tasks/people from this organization

    Returns period, previous_period, rows and a period-aware empty_state
    message when no Admin targets are defined for the selected organization
    and any overlapping week.
    """
    if start_date and end_date:
        period_start = _sast_midnight_utc(start_date)
        period_end = _sast_midnight_utc(end_date + timedelta(days=1))
    else:
        period_start, period_end = _period_range(period)

    period_first_sast_day = period_start.astimezone(SAST).date()
    period_last_sast_day = period_end.astimezone(SAST).date() - timedelta(days=1)
    _, canonical_by_week, _ = build_week_buckets(
        period_first_sast_day,
        period_last_sast_day,
    )
    source_week_starts = list(canonical_by_week)
    week_ranges = []
    for cursor in source_week_starts:
        iso_year, iso_week, _ = cursor.isocalendar()
        week_identifier = f"{iso_year}-W{iso_week:02d}"
        previous_start = _sast_midnight_utc(cursor - timedelta(days=7))
        previous_end = _sast_midnight_utc(cursor)
        canonical_start = canonical_by_week[cursor]
        canonical_year, canonical_week, _ = canonical_start.isocalendar()
        canonical_identifier = f"{canonical_year}-W{canonical_week:02d}"
        week_ranges.append((
            week_identifier,
            canonical_identifier,
            previous_start,
            previous_end,
        ))

    try:
        targets = sb_get_admin_task_targets(
            org_filter,
            [week_identifier for week_identifier, *_ in week_ranges],
        )
        if leader12_id:
            leader_roster = sb_get_tracked_people(
                leader12_id=leader12_id,
                org_filter=org_filter,
                visible_leader_ids=visible_leader_ids,
            )
            leader_person_ids = {str(person["person_id"]) for person in leader_roster}
            targets = [
                row for row in targets
                if row.get("is_default") or str(row["person_id"]) in leader_person_ids
            ]
        if visible_leader_ids is not None:
            targets = [
                row for row in targets
                if row.get("is_default") or str(row["person_id"]) in visible_leader_ids
            ]
        person_ids = list(
            dict.fromkeys(str(row["person_id"]) for row in targets if row.get("person_id"))
        )
        people = _fetch_target_people(person_ids)
        actual_ranges = {"period": (period_start, period_end)}
        actual_ranges.update(
            {
                f"previous:{week_identifier}": (previous_start, previous_end)
                for week_identifier, _canonical_identifier, previous_start, previous_end in week_ranges
            }
        )
        actuals = sb_get_completed_task_actuals(org_filter, actual_ranges, person_ids)
    except Exception:
        logger.exception("Unable to build twelve_tasks report data")
        raise

    previous_period_start = period_start - (period_end - period_start)
    previous_period_end = period_start
    canonical_by_identifier = {
        week_identifier: canonical_identifier
        for week_identifier, canonical_identifier, _, _ in week_ranges
    }
    aggregate_by_person = {}
    for target in targets:
        person_id = str(target.get("person_id") or "")
        is_default = bool(target.get("is_default"))
        key = ("default", "") if is_default and not person_id else ("person", person_id)
        week_identifier = str(target.get("week_identifier") or "")
        if not is_default and not person_id:
            continue
        if key not in aggregate_by_person:
            aggregate_by_person[key] = {
                "person_id": person_id,
                "target": 0,
                "last_week": 0,
                "weeks": set(),
                "is_default": is_default,
            }
        aggregate = aggregate_by_person[key]
        aggregate["target"] += target.get("target") or 0
        aggregate["last_week"] += actuals.get(
            f"previous:{week_identifier}", {}
        ).get(person_id, 0)
        if week_identifier in canonical_by_identifier:
            aggregate["weeks"].add(canonical_by_identifier[week_identifier])

    rows = []
    for aggregate in aggregate_by_person.values():
        pid = aggregate["person_id"]
        person = people.get(pid, {})
        name = "Leader default" if aggregate["is_default"] else " ".join(
            str(person.get(field) or "").strip()
            for field in ("Name", "Surname")
            if str(person.get(field) or "").strip()
        )
        rows.append(
            {
                "person_id": pid,
                "name": name or pid,
                "gender": person.get("Gender") or "",
                "week_identifier": ", ".join(sorted(aggregate["weeks"])),
                "last_week": aggregate["last_week"],
                "target": aggregate["target"],
                "this_week": actuals.get("period", {}).get(pid, 0),
            }
        )

    empty_message = (
        "No tasks set for this week"
        if period in {"thisWeek", "previousWeek"}
        else "No tasks set for this period"
    )
    return {
        "period": {
            "start": period_first_sast_day.isoformat(),
            "end": period_last_sast_day.isoformat(),
        },
        "previous_period": {
            "start": previous_period_start.astimezone(SAST).date().isoformat(),
            "end": (previous_period_end.astimezone(SAST).date() - timedelta(days=1)).isoformat(),
        },
        "rows": rows,
        "empty_state": empty_message if not rows else None,
    }


# ---------------------------------------------------------------------------
# XLSX export — Weeks / Name / Prior-week actuals / Targets / Actual in period, plus
# the Key legend and orange/red/green indicator coloring on both the "Last week" and
# "Actual this week" columns (compared against that row's target).
# ---------------------------------------------------------------------------

def _indicator_fill(value: int, target: int) -> Optional[PatternFill]:
    """Return the appropriate fill color based on completed count vs target.

    - If target is 0 (not yet set): return None — leave cell uncolored.
    - If value == target: on-target fill (orange).
    - If value < target: below-target fill (red).
    - If value > target: above-target fill (green).
    """
    if target == 0:
        return None  # no target set yet — leave uncolored rather than guessing
    if value == target:
        return FILL_ON_TARGET
    if value < target:
        return FILL_LESS_THAN_TARGET
    return FILL_MORE_THAN_TARGET


def export_twelve_tasks_xlsx(report: dict, path: str) -> str:
    """Export the Twelve Tasks report to an XLSX file matching the mockup layout.

    Layout:
      - Row 1: headers — Weeks / Name / Prior-week actuals / Targets / Actual in period
        (bold font). Columns F-G contain a Key/legend block.
      - Data rows (starting row 2): each Admin-defined target row.
        - Column C (Prior-week actuals) : count + conditional fill (orange/red/green
          vs. that row's target).
        - Column D (Targets)    : persisted target value.
        - Column E (Actual in period) : count + conditional fill vs. target.
      - Columns F-G: Key legend rows with colored squares and labels.

    The function applies indicator fills via _indicator_fill so that
    each cell's color reflects its relationship to that row's target.

    Parameters:
      report:   dict from sb_get_twelve_tasks_report (period, previous_period, rows)
      path:     file path to write the .xlsx to

    Returns the saved file path.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    headers = ["Weeks", "Name", "Prior-week actuals", "Targets", "Actual in period"]
    for col, text in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col, value=text)
        cell.font = HEADER_FONT

    for row_idx, row in enumerate(report["rows"], start=2):
        ws.cell(row=row_idx, column=1, value=row.get("week_identifier"))
        ws.cell(row=row_idx, column=2, value=row["name"])

        last_week_cell = ws.cell(row=row_idx, column=3, value=row["last_week"])
        ws.cell(row=row_idx, column=4, value=row["target"])
        this_week_cell = ws.cell(row=row_idx, column=5, value=row["this_week"])

        fill = _indicator_fill(row["last_week"], row["target"])
        if fill:
            last_week_cell.fill = fill

        fill = _indicator_fill(row["this_week"], row["target"])
        if fill:
            this_week_cell.fill = fill

    # Key / legend block, matching the mockup (columns F/G)
    ws.cell(row=1, column=6, value="Key")
    legend = [
        (FILL_ON_TARGET, "On target"),
        (FILL_LESS_THAN_TARGET, "Less than target"),
        (FILL_MORE_THAN_TARGET, "More than target"),
    ]
    for i, (fill, label) in enumerate(legend, start=2):
        ws.cell(row=i, column=6).fill = fill
        ws.cell(row=i, column=7, value=label)

    for col, width in zip("ABCDEFG", (12, 20, 12, 10, 16, 6, 20)):
        ws.column_dimensions[col].width = width

    wb.save(path)
    return path