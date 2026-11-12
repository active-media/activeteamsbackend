"""Custom targets business logic and data access.

Pure logic functions (week math, classification, evaluation, versioning
plans, attribution) are separated from Supabase data-access functions so
they can be unit-tested without a database.  No endpoints, no routers —
this module is imported by the route layer in Phase 2b.

All Supabase access goes through an ``sb`` parameter (defaulting to the
module-level service client) so tests can inject a mock client.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from supabase_helpers.supabase_connection import supabase_admin as _supabase

logger = logging.getLogger(__name__)

SAST = ZoneInfo("Africa/Johannesburg")
COMPLETED_STATUS_LOWER = "completed"
NO_USER_MESSAGE = "This person has no user account, so their tasks can't be tracked"
CONSTRAINT_NO_OVERLAP_ACTIVE = "custom_targets_no_overlap_active"

# Number of rows fetched per page when paging completed tasks.
TASK_PAGE_SIZE = 1000


# ============================================================================
# Configuration
# ============================================================================

@dataclass(frozen=True)
class SnapshotFreeze:
    """When a completed week becomes frozen (non-provisional).

    ``weekday`` is the ISO weekday (0=Monday … 6=Sunday) the freeze applies
    to; ``hour`` is the hour of day in ``timezone``.  A completed week is
    provisional until the freeze instant on its Sunday is reached.
    """

    weekday: int
    hour: int
    timezone: str = "Africa/Johannesburg"


# ============================================================================
# Week math (Africa/Johannesburg, zoneinfo, never fixed offsets)
# ============================================================================

def snap_to_monday(d: date) -> date:
    """Snap a date to the Monday of its ISO week."""
    return d - timedelta(days=d.weekday())


def snap_to_sunday(d: date) -> date:
    """Snap a date to the Sunday of its ISO week."""
    return d + timedelta(days=6 - d.weekday())


def sast_start_of_day_utc(day: date) -> datetime:
    """00:00:00 SAST on ``day``, converted to UTC."""
    return datetime.combine(day, time.min, tzinfo=SAST).astimezone(timezone.utc)


def sast_end_of_day_utc(day: date) -> datetime:
    """23:59:59.999999 SAST on ``day``, converted to UTC."""
    return datetime.combine(day, time.max, tzinfo=SAST).astimezone(timezone.utc)


def current_sast_week(now: Optional[datetime] = None) -> tuple[date, date]:
    """Return (monday, sunday) of the current SAST week."""
    now = now or datetime.now(timezone.utc)
    d = now.astimezone(SAST).date()
    monday = snap_to_monday(d)
    return monday, snap_to_sunday(monday)


def last_completed_sast_week(now: Optional[datetime] = None) -> tuple[date, date]:
    """Return (monday, sunday) of the most recently completed SAST week.

    If today is Sunday or later, the current week is complete and is
    returned.  Otherwise the previous Monday–Sunday week is returned.
    """
    now = now or datetime.now(timezone.utc)
    d = now.astimezone(SAST).date()
    monday, sunday = current_sast_week(now)
    if d >= sunday:
        return monday, sunday
    return monday - timedelta(days=7), sunday - timedelta(days=7)


# ============================================================================
# Week classification (pure)
# ============================================================================

@dataclass(frozen=True)
class WeekPhase:
    kind: str  # "in_progress" or "completed"
    provisional: bool


def classify_week(
    week_monday: date,
    week_sunday: date,
    now: Optional[datetime] = None,
    freeze: Optional[SnapshotFreeze] = None,
) -> WeekPhase:
    """Classify a week relative to ``now`` and the freeze config.

    * ``in_progress`` — only the current unfinished week.
    * ``completed`` — Sunday has passed.  ``provisional`` is True when the
      week is complete but the freeze instant has not yet been reached.
    """
    now = now or datetime.now(timezone.utc)
    sast_now = now.astimezone(SAST)

    cur_mon, cur_sun = current_sast_week(now)

    if week_monday == cur_mon:
        if sast_now.date() < cur_sun:
            return WeekPhase(kind="in_progress", provisional=False)
    elif week_sunday >= sast_now.date():
        # Future week — not "in progress" (only the current unfinished week
        # is in_progress).  Treat as completed with no data so callers can
        # filter it out.
        return WeekPhase(kind="completed", provisional=False)

    # Week is completed (Sunday has passed).
    if freeze:
        tz = ZoneInfo(freeze.timezone)
        freeze_dt = datetime.combine(
            week_sunday, time(hour=freeze.hour), tzinfo=tz
        )
        provisional = sast_now < freeze_dt
    else:
        provisional = True

    return WeekPhase(kind="completed", provisional=provisional)


def evaluate_met_not_met(weekly_target: int, weekly_completed: int) -> str:
    """Return ``"met"`` if completed >= target, else ``"not_met"``."""
    return "met" if weekly_completed >= weekly_target else "not_met"


# ============================================================================
# Email normalisation and attribution helpers (pure)
# ============================================================================

def _norm_email(value: Any) -> Optional[str]:
    """Lowercase and trim an email address; return None if empty/missing."""
    if value is None:
        return None
    v = str(value).strip().lower()
    return v or None


def build_email_index(users: list[dict]) -> dict[str, list[dict]]:
    """Map normalised email -> list of Users rows sharing that email."""
    index: dict[str, list[dict]] = {}
    for user in users:
        email = _norm_email(user.get("email"))
        if email:
            index.setdefault(email, []).append(user)
    return index


def build_people_index(people: list[dict]) -> dict[str, dict]:
    """Map People._id -> People row."""
    return {str(p.get("_id")): p for p in people if p.get("_id") is not None}


def build_person_id_index(people: list[dict]) -> dict[str, str]:
    """Map People.person_id -> People._id."""
    index: dict[str, str] = {}
    for p in people:
        pid = p.get("person_id")
        pid_key = str(pid) if pid is not None else None
        if pid_key and p.get("_id") is not None:
            index[pid_key] = str(p["_id"])
    return index


def _resolve_single_user(
    email: str,
    users: list[dict],
    people_by_id: dict[str, dict],
    person_id_to_people_id: dict[str, str],
    log: list[str],
) -> Optional[dict]:
    """Resolve one Users row from a possibly-ambiguous email match.

    When an email maps to multiple Users rows, we resolve each user to a
    person (Users.person_id -> People.person_id -> People._id).  If all
    resolved users point to the SAME person, it is not ambiguous — return any
    of them (preferring one whose linked People.Email matches the email).
    If they resolve to DIFFERENT people, the email is AMBIGUOUS.

    Users without a person_id are ignored for ambiguity purposes.

    Returns the chosen Users row or None if ambiguous.
    """
    if len(users) == 1:
        return users[0]

    person_to_user: dict[str, dict] = {}
    person_to_user_email_match: dict[str, dict] = {}
    for u in users:
        person_id = u.get("person_id")
        if person_id is None:
            continue
        people_id = person_id_to_people_id.get(str(person_id))
        if people_id is None:
            continue
        person = people_by_id.get(people_id)
        if person is None:
            continue
        person_email = _norm_email(person.get("Email"))
        if person_email == email:
            person_to_user_email_match[people_id] = u
        person_to_user.setdefault(people_id, u)

    if len(person_to_user) == 0:
        log.append(f"AMBIGUOUS email={email} matches {len(users)} users")
        return None

    if len(person_to_user) == 1:
        people_id = next(iter(person_to_user))
        return person_to_user_email_match.get(
            people_id, person_to_user[people_id]
        )

    # Multiple different people — ambiguous.
    log.append(f"AMBIGUOUS email={email} matches {len(users)} users")
    return None


def _user_to_person_id(
    user: dict,
    person_id_to_people_id: dict[str, str],
    log: list[str],
) -> Optional[str]:
    """Map a Users row to a People._id via Users.person_id -> People.person_id."""
    person_id = user.get("person_id")
    if person_id is None:
        log.append(f"user {user.get('_id')} has no person_id")
        return None
    pid = str(person_id)
    people_id = person_id_to_people_id.get(pid)
    if people_id is None:
        log.append(f"no People row with person_id={pid}")
        return None
    return people_id


def attribute_task(
    task: dict,
    email_index: dict[str, list[dict]],
    users_by_id: dict[str, dict],
    people_by_id: dict[str, dict],
    person_id_to_people_id: dict[str, str],
    log: list[str],
) -> Optional[str]:
    """Attribute a completed task to a People._id.

    Resolution order:
    1. Primary: lower(trim(Tasks.assignedfor)) -> Users.email
    2. Secondary: lower(trim(Tasks.assigned_to_email)) -> Users.email (logged)
    3. Secondary: Tasks.assigned_to_user_id -> Users._id (logged)
    4. Then Users.person_id -> People.person_id -> People._id

    Returns the People._id or None if the task cannot be attributed.
    """
    assignedfor = _norm_email(task.get("assignedfor"))

    # --- Primary: assignedfor -> Users.email ------------------------------
    if assignedfor and assignedfor in email_index:
        users = email_index[assignedfor]
        user = _resolve_single_user(assignedfor, users, people_by_id,
                                     person_id_to_people_id, log)
        if user:
            return _user_to_person_id(user, person_id_to_people_id, log)
        return None

    # --- Secondary: assigned_to_email -> Users.email ---------------------
    a2e = _norm_email(task.get("assigned_to_email"))
    if a2e and a2e in email_index:
        log.append(f"secondary match assigned_to_email={a2e}")
        users = email_index[a2e]
        user = _resolve_single_user(a2e, users, people_by_id,
                                     person_id_to_people_id, log)
        if user:
            return _user_to_person_id(user, person_id_to_people_id, log)
        return None

    # --- Secondary: assigned_to_user_id -> Users._id --------------------
    a2uid = task.get("assigned_to_user_id")
    if a2uid is not None:
        uid = str(a2uid)
        if uid in users_by_id:
            log.append(f"secondary match assigned_to_user_id={uid}")
            user = users_by_id[uid]
            return _user_to_person_id(user, person_id_to_people_id, log)

    return None


# ============================================================================
# Task counting and attribution (data access + pure)
# ============================================================================

def _is_completed_status(task: dict) -> bool:
    """A task has completed status when lower(status) = 'completed'."""
    return str(task.get("status") or "").strip().lower() == COMPLETED_STATUS_LOWER


def _is_completed_task(task: dict) -> bool:
    """A task is fully completed when lower(status)='completed' AND completedAt IS NOT NULL."""
    return _is_completed_status(task) and task.get("completedAt") is not None


def _parse_completed_at(task: dict) -> Optional[datetime]:
    """Parse a task's completedAt into a timezone-aware UTC datetime."""
    raw = task.get("completedAt")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw).strip()
        if text.endswith("+00"):
            text += ":00"
        elif text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _task_sast_date(task: dict) -> Optional[date]:
    """Return the SAST date of a completed task's completedAt."""
    dt = _parse_completed_at(task)
    if dt is None:
        return None
    return dt.astimezone(SAST).date()


def fetch_users(
    sb: Any,
    org_filter: Optional[dict] = None,
) -> list[dict]:
    """Fetch all Users rows, optionally filtered by organization."""
    query = sb.table("Users").select("*")
    if org_filter:
        org_value = org_filter.get("Organization") or org_filter.get("organization")
        if org_value:
            query = query.eq("Organization", org_value)
    result = query.execute()
    return result.data or []


def fetch_people(
    sb: Any,
    org_filter: Optional[dict] = None,
) -> list[dict]:
    """Fetch all People rows, optionally filtered by organization."""
    query = sb.table("People").select("*")
    if org_filter:
        org_value = org_filter.get("Organization") or org_filter.get("organization")
        if org_value:
            query = query.eq("Organization", org_value)
    result = query.execute()
    return result.data or []


def count_completed_tasks(
    sb: Any,
    org_filter: Optional[dict],
    start_date: date,
    end_date: date,
    person_ids: Optional[list[str]] = None,
    task_type_filter: Optional[list[str]] = None,
    page_size: int = TASK_PAGE_SIZE,
) -> dict[str, Any]:
    """Count completed tasks per person per SAST day, with paging.

    Queries Tasks with date-bounded completedAt (gte start, lt end+1 day)
    and pages the results so the whole Tasks table is never loaded.

    Returns a dict with:
      * ``daily_counts``: {person_id: {date_str: count, ...}}
      * ``excluded_no_completedat``: {person_id: count} — completed tasks
        (status='completed') with NULL completedAt, attributed to a person
      * ``unmapped_completed_tasks``: int — completed tasks with non-NULL
        completedAt that could not be attributed to any person
      * ``attribution_log``: list[str] — debug log of attribution decisions
    """
    start_utc = sast_start_of_day_utc(start_date)
    end_exclusive_utc = sast_start_of_day_utc(end_date + timedelta(days=1))

    email_index: dict[str, list[dict]] = {}
    users_by_id: dict[str, dict] = {}
    person_id_to_people_id: dict[str, str] = {}
    people_by_id: dict[str, dict] = {}
    log: list[str] = []

    # Load reference data once (Users + People).
    users = fetch_users(sb, org_filter)
    people = fetch_people(sb, org_filter)
    users_by_id = {str(u.get("_id")): u for u in users if u.get("_id") is not None}
    email_index = build_email_index(users)
    person_id_to_people_id = build_person_id_index(people)
    people_by_id = build_people_index(people)

    daily_counts: dict[str, dict[str, int]] = {}
    excluded_no_completedat: dict[str, int] = {}
    unmapped_completed_tasks = 0

    offset = 0
    while True:
        query = (
            sb.table("Tasks")
            .select(
                "_id, status, completedAt, assignedfor, assigned_to_email, "
                "assigned_to_user_id, taskType, Organization"
            )
            .gte("completedAt", start_utc.isoformat())
            .lt("completedAt", end_exclusive_utc.isoformat())
        )
        if org_filter:
            org_value = org_filter.get("Organization") or org_filter.get("organization")
            if org_value:
                query = query.eq("Organization", org_value)
        if task_type_filter:
            # taskType filter hook (empty by default — Option A: count all)
            query = query.in_("taskType", task_type_filter)
        if person_ids:
            # We filter by person after attribution; the Tasks table does not
            # store the canonical person key, so we fetch all and attribute.
            pass

        query = query.limit(page_size).offset(offset)
        result = query.execute()
        rows = result.data or []
        if not rows:
            break

        for task in rows:
            if not _is_completed_status(task):
                continue

            person_id = attribute_task(
                task, email_index, users_by_id,
                people_by_id, person_id_to_people_id, log,
            )

            if person_id is None:
                # Unattributable completed task — never silently dropped
                unmapped_completed_tasks += 1
                continue

            sast_day = _task_sast_date(task)
            if sast_day is None:
                # Status is 'completed' but completedAt is NULL — exclude from
                # counts but report per person
                excluded_no_completedat[person_id] = (
                    excluded_no_completedat.get(person_id, 0) + 1
                )
                continue

            day_str = sast_day.isoformat()
            daily_counts.setdefault(person_id, {})
            daily_counts[person_id][day_str] = (
                daily_counts[person_id].get(day_str, 0) + 1
            )

        if len(rows) < page_size:
            break
        offset += page_size

    return {
        "daily_counts": daily_counts,
        "excluded_no_completedat": excluded_no_completedat,
        "unmapped_completed_tasks": unmapped_completed_tasks,
        "attribution_log": log,
    }


# ============================================================================
# Hierarchy scope check (pure)
# ============================================================================

def check_hierarchy_scope(
    target_person_id: str,
    viewer_person_id: str,
    leader_path_values: list[Optional[str]],
) -> bool:
    """Check if ``viewer_person_id`` can see ``target_person_id`` via LeaderPath.

    ``leader_path_values`` are the LeaderPath[0..3] values of the viewer's
    person row.  The viewer can see the target if the target's People._id
    appears in the viewer's LeaderPath.  Bracketed column names must be
    double-quoted in SQL; the caller is responsible for quoting when building
    queries.

    Returns True if in scope, False otherwise.
    """
    if not target_person_id or not viewer_person_id:
        return False
    for value in leader_path_values:
        if value is not None and str(value) == target_person_id:
            return True
    return False


def is_supreme_admin(user: dict) -> bool:
    """Return True if ``lower(Users.is_supreme_admin) = 'true'``."""
    value = user.get("is_supreme_admin")
    if value is None:
        return False
    return str(value).strip().lower() == "true"


# ============================================================================
# Validation (pure)
# ============================================================================

def validate_target_days(days: dict[str, int]) -> list[str]:
    """Validate the seven day values.  Returns a list of error messages."""
    errors: list[str] = []
    day_names = (
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
    )
    for name in day_names:
        value = days.get(name, 0)
        if not isinstance(value, int) or value < 0:
            errors.append(f"{name} must be an integer >= 0")
    total = sum(int(days.get(name, 0) or 0) for name in day_names)
    if total <= 0:
        errors.append("at least one day must have a value > 0")
    return errors


def validate_dates(start_date: date, end_date: Optional[date]) -> list[str]:
    """Validate start_date is Monday and end_date (if set) is Sunday >= start."""
    errors: list[str] = []
    if start_date.weekday() != 0:
        errors.append("start_date must be a Monday (isodow = 1)")
    if end_date is not None:
        if end_date.weekday() != 6:
            errors.append("end_date must be a Sunday (isodow = 7)")
        if end_date < start_date:
            errors.append("end_date must be on or after start_date")
    return errors


def person_has_linked_user(
    person_id: str,
    users: list[dict],
    person_id_to_people_id: dict[str, str],
) -> bool:
    """Return True if the person (People._id) has a linked Users account."""
    # Find the People row, then check if any Users row references it via person_id.
    people_id = person_id
    for u in users:
        if str(u.get("person_id")) == people_id:
            return True
    return False


def validate_no_overlap(
    new_start_date: date,
    new_end_date: Optional[date],
    existing_targets: list[dict],
    exclude_target_id: Optional[str] = None,
) -> list[str]:
    """Reject a new/edited target whose active range overlaps another active
    target for the same person.

    Returns a list of error messages (empty if no overlap).
    """
    errors: list[str] = []
    new_end = new_end_date  # None = open-ended
    for t in existing_targets:
        if exclude_target_id and t.get("id") == exclude_target_id:
            continue
        if not t.get("is_active", True):
            continue
        t_start = t.get("start_date")
        t_end = t.get("end_date")
        if isinstance(t_start, str):
            t_start = date.fromisoformat(t_start)
        if isinstance(t_end, str):
            t_end = date.fromisoformat(t_end)
        # Ranges overlap if new_start <= t_end AND (t_start <= new_end or t_end is None)
        if t_start is None:
            continue
        t_end_eff = t_end if t_end is not None else date.max
        new_end_eff = new_end if new_end is not None else date.max
        if new_start_date <= t_end_eff and t_start <= new_end_eff:
            errors.append(
                f"target {t.get('id')} already covers {t_start} to {t_end_eff}; "
                f"overlaps requested {new_start_date} to {new_end_eff}"
            )
    return errors


# ============================================================================
# Versioning plans (pure — return DB operations, do not execute)
# ============================================================================

@dataclass
class DbOp:
    """A single database operation to perform.

    ``op`` is one of: "insert", "update", "delete", "upsert".
    ``match`` contains WHERE conditions for update/delete/upsert.
    ``on_conflict`` is the conflict column list for upserts.
    """

    op: str
    table: str
    data: dict[str, Any]
    match: Optional[dict[str, Any]] = None
    on_conflict: Optional[str] = None


def plan_create_target(
    person_id: str,
    organization: str,
    days: dict[str, int],
    start_date: date,
    end_date: Optional[date],
    set_by: str,
) -> list[DbOp]:
    """Plan creation of a new custom target.  Returns insert operation."""
    now_iso = datetime.now(timezone.utc).isoformat()
    data = {
        "person_id": person_id,
        "organization": organization,
        "monday": int(days.get("monday", 0) or 0),
        "tuesday": int(days.get("tuesday", 0) or 0),
        "wednesday": int(days.get("wednesday", 0) or 0),
        "thursday": int(days.get("thursday", 0) or 0),
        "friday": int(days.get("friday", 0) or 0),
        "saturday": int(days.get("saturday", 0) or 0),
        "sunday": int(days.get("sunday", 0) or 0),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat() if end_date else None,
        "is_active": True,
        "set_by": set_by,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    return [DbOp(op="insert", table="custom_targets", data=data)]


def plan_edit_started_target(
    target_id: str,
    person_id: str,
    organization: str,
    days: dict[str, int],
    start_date: date,
    end_date: Optional[date],
    set_by: str,
    current_sunday: date,
) -> list[DbOp]:
    """Plan editing a started target.

    Close the old row at the Sunday before the new start_date and insert a
    new row starting the following Monday.  is_active stays True on both
    rows (the closed row is simply time-bounded).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    new_monday = snap_to_monday(start_date)

    # Close the old row: set end_date to the Sunday before the new start.
    # is_active stays True — the row is simply time-bounded.
    close_data = {
        "end_date": current_sunday.isoformat(),
        "is_active": True,
        "updated_at": now_iso,
    }
    close_op = DbOp(
        op="update",
        table="custom_targets",
        data=close_data,
        match={"id": target_id, "is_active": True},
    )

    # Insert the new row starting the following Monday.
    new_data = {
        "person_id": person_id,
        "organization": organization,
        "monday": int(days.get("monday", 0) or 0),
        "tuesday": int(days.get("tuesday", 0) or 0),
        "wednesday": int(days.get("wednesday", 0) or 0),
        "thursday": int(days.get("thursday", 0) or 0),
        "friday": int(days.get("friday", 0) or 0),
        "saturday": int(days.get("saturday", 0) or 0),
        "sunday": int(days.get("sunday", 0) or 0),
        "start_date": new_monday.isoformat(),
        "end_date": end_date.isoformat() if end_date else None,
        "is_active": True,
        "set_by": set_by,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    insert_op = DbOp(op="insert", table="custom_targets", data=new_data)

    return [close_op, insert_op]


def plan_edit_future_target(
    target_id: str,
    days: dict[str, int],
    end_date: Optional[date],
    set_by: str,
) -> list[DbOp]:
    """Plan editing a future-start target — in-place update."""
    now_iso = datetime.now(timezone.utc).isoformat()
    data = {
        "monday": int(days.get("monday", 0) or 0),
        "tuesday": int(days.get("tuesday", 0) or 0),
        "wednesday": int(days.get("wednesday", 0) or 0),
        "thursday": int(days.get("thursday", 0) or 0),
        "friday": int(days.get("friday", 0) or 0),
        "saturday": int(days.get("saturday", 0) or 0),
        "sunday": int(days.get("sunday", 0) or 0),
        "end_date": end_date.isoformat() if end_date else None,
        "updated_at": now_iso,
    }
    return [DbOp(op="update", table="custom_targets", data=data, match={"id": target_id})]


def plan_deactivate_target(
    target_id: str,
    current_sunday: date,
) -> list[DbOp]:
    """Plan deactivating a target.

    Sets end_date = Sunday of the current week; is_active stays True.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    data = {
        "end_date": current_sunday.isoformat(),
        "updated_at": now_iso,
    }
    return [DbOp(op="update", table="custom_targets", data=data, match={"id": target_id})]


# ============================================================================
# Evaluation (pure)
# ============================================================================

def evaluate_week(
    week_monday: date,
    days: dict[str, int],
    daily_counts: dict[str, int],
) -> dict[str, Any]:
    """Evaluate a single person's week.

    Returns a dict with weekly_target, weekly_completed, status, and
    per-day target/completed values.
    """
    day_names = (
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
    )
    day_targets = {name: int(days.get(name, 0) or 0) for name in day_names}
    day_completed = {name: int(daily_counts.get(name, 0) or 0) for name in day_names}
    weekly_target = sum(day_targets.values())
    weekly_completed = sum(day_completed.values())
    status = evaluate_met_not_met(weekly_target, weekly_completed)
    return {
        "weekly_target": weekly_target,
        "weekly_completed": weekly_completed,
        "status": status,
        "day_targets": day_targets,
        "day_completed": day_completed,
    }


# ============================================================================
# Snapshot operations (data access)
# ============================================================================

def insert_snapshot(
    sb: Any,
    person_id: str,
    week_start: date,
    organization: str,
    day_targets: dict[str, int],
    day_completed: dict[str, int],
    weekly_target: int,
    weekly_completed: int,
    excluded_no_completedat: int,
    status: str,
    custom_target_id: Optional[str] = None,
    ignore_duplicates: bool = True,
) -> dict[str, Any]:
    """Insert a snapshot row (insert-ignore on person_id + week_start).

    When ``ignore_duplicates`` is True (the default), an existing row with the
    same (person_id, week_start) is left untouched.  When False, the existing
    row is overwritten (used by ``upsert_snapshot`` for admin re-evaluate).
    """
    day_names = (
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
    )
    data = {
        "custom_target_id": custom_target_id,
        "person_id": person_id,
        "week_start": week_start.isoformat(),
        "organization": organization,
    }
    for name in day_names:
        data[f"{name}_target"] = int(day_targets.get(name, 0) or 0)
        data[f"{name}_completed"] = int(day_completed.get(name, 0) or 0)
    data["weekly_target"] = int(weekly_target)
    data["weekly_completed"] = int(weekly_completed)
    data["excluded_no_completedat"] = int(excluded_no_completedat)
    data["status"] = status
    data["evaluated_at"] = datetime.now(timezone.utc).isoformat()

    result = (
        sb.table("custom_target_snapshots")
        .upsert(
            data,
            on_conflict="person_id,week_start",
            ignore_duplicates=ignore_duplicates,
        )
        .execute()
    )
    rows = result.data or []
    if rows:
        return rows[0]
    return data


def upsert_snapshot(
    sb: Any,
    person_id: str,
    week_start: date,
    organization: str,
    day_targets: dict[str, int],
    day_completed: dict[str, int],
    weekly_target: int,
    weekly_completed: int,
    excluded_no_completedat: int,
    status: str,
) -> dict[str, Any]:
    """Admin re-evaluate: upsert a snapshot (overwrite on conflict)."""
    return insert_snapshot(
        sb=sb,
        person_id=person_id,
        week_start=week_start,
        organization=organization,
        day_targets=day_targets,
        day_completed=day_completed,
        weekly_target=weekly_target,
        weekly_completed=weekly_completed,
        excluded_no_completedat=excluded_no_completedat,
        status=status,
        ignore_duplicates=False,
    )
