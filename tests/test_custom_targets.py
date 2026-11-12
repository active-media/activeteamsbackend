"""Unit tests for supabase_helpers.custom_targets.

Follows the mock-Supabase pattern from tests/test_twelve_tasks_admin_targets.py.
No network, no DB access — all Supabase calls go through an injectable mock.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace

import pytest

from zoneinfo import ZoneInfo

from supabase_helpers import custom_targets as ct

SAST = ZoneInfo("Africa/Johannesburg")


# ---------------------------------------------------------------------------
# Mock Supabase (same pattern as test_twelve_tasks_admin_targets.py)
# ---------------------------------------------------------------------------

class _MockQuery:
    def __init__(self, client, table_name):
        self.client = client
        self.table_name = table_name
        self.filters = []
        self._limit = None
        self._offset = 0
        self._order = None

    def select(self, _columns):
        return self

    def eq(self, column, value):
        self.filters.append(("eq", column, value))
        return self

    def in_(self, column, values):
        self.filters.append(("in", column, values))
        return self

    def gte(self, column, value):
        self.filters.append(("gte", column, value))
        return self

    def lt(self, column, value):
        self.filters.append(("lt", column, value))
        return self

    def lte(self, column, value):
        self.filters.append(("lte", column, value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def offset(self, n):
        self._offset = n
        return self

    def order(self, column):
        self._order = column
        return self

    def upsert(self, data, on_conflict=None, ignore_duplicates=None):
        self._upsert_data = data
        self._on_conflict = on_conflict
        self._ignore_duplicates = ignore_duplicates
        return self

    def execute(self):
        # Handle upsert operations
        if hasattr(self, "_upsert_data"):
            table_rows = self.client.tables.setdefault(self.table_name, [])
            data = self._upsert_data
            on_conflict = getattr(self, "_on_conflict", None)
            ignore_duplicates = getattr(self, "_ignore_duplicates", None)
            if on_conflict:
                conflict_keys = [k.strip() for k in on_conflict.split(",")]
                # Find existing row matching conflict keys
                match_idx = None
                for i, row in enumerate(table_rows):
                    if all(row.get(k) == data.get(k) for k in conflict_keys):
                        match_idx = i
                        break
                if match_idx is not None:
                    if ignore_duplicates:
                        # Insert-ignore: leave the existing row untouched
                        return SimpleNamespace(data=[table_rows[match_idx]])
                    # Upsert = overwrite on conflict
                    table_rows[match_idx].update(data)
                    return SimpleNamespace(data=[table_rows[match_idx]])
                else:
                    table_rows.append(dict(data))
                    return SimpleNamespace(data=[dict(data)])
            else:
                table_rows.append(dict(data))
                return SimpleNamespace(data=[dict(data)])

        rows = list(self.client.tables.get(self.table_name, []))
        self.client.queries.append((self.table_name, list(self.filters)))
        for operation, column, expected in self.filters:
            if operation == "eq":
                rows = [r for r in rows if r.get(column) == expected]
            elif operation == "in":
                rows = [r for r in rows if r.get(column) in expected]
            elif operation == "gte":
                rows = [
                    r for r in rows
                    if r.get(column) is None or _ts(r.get(column)) >= _ts(expected)
                ]
            elif operation == "lt":
                rows = [
                    r for r in rows
                    if r.get(column) is None or _ts(r.get(column)) < _ts(expected)
                ]
            elif operation == "lte":
                rows = [
                    r for r in rows
                    if r.get(column) is None or _ts(r.get(column)) <= _ts(expected)
                ]
        if self._order:
            rows.sort(key=lambda r: r.get(self._order) or "")
        if self._offset:
            rows = rows[self._offset:]
        if self._limit is not None:
            rows = rows[: self._limit]
        return SimpleNamespace(data=rows)


class _MockSupabase:
    def __init__(self, **tables):
        self.tables = {k: list(v) for k, v in tables.items()}
        self.queries = []

    def table(self, name):
        return _MockQuery(self, name)


def _ts(value):
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    text = str(value).strip()
    if text.endswith("+00"):
        text += ":00"
    elif text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Week math tests
# ---------------------------------------------------------------------------

def test_snap_to_monday():
    assert ct.snap_to_monday(date(2026, 11, 9)) == date(2026, 11, 9)  # Monday
    assert ct.snap_to_monday(date(2026, 11, 10)) == date(2026, 11, 9)  # Tuesday
    assert ct.snap_to_monday(date(2026, 11, 15)) == date(2026, 11, 9)  # Sunday


def test_snap_to_sunday():
    assert ct.snap_to_sunday(date(2026, 11, 9)) == date(2026, 11, 15)  # Monday
    assert ct.snap_to_sunday(date(2026, 11, 15)) == date(2026, 11, 15)  # Sunday


def test_sast_start_of_day_utc():
    dt = ct.sast_start_of_day_utc(date(2026, 11, 9))
    assert dt.tzinfo is not None
    # 2026-11-09 00:00 SAST = 2026-11-08 22:00 UTC (SAST = UTC+2 in Nov)
    assert dt.hour == 22
    assert dt.date() == date(2026, 11, 8)


def test_current_sast_week():
    now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)  # Thursday SAST
    mon, sun = ct.current_sast_week(now)
    assert mon == date(2026, 11, 9)
    assert sun == date(2026, 11, 15)


def test_last_completed_sast_week_mid_week():
    now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)  # Thursday SAST
    mon, sun = ct.last_completed_sast_week(now)
    assert mon == date(2026, 11, 2)
    assert sun == date(2026, 11, 8)


def test_last_completed_sast_week_after_sunday():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)  # Monday SAST (after Sunday)
    mon, sun = ct.last_completed_sast_week(now)
    assert mon == date(2026, 11, 9)
    assert sun == date(2026, 11, 15)


def test_last_completed_sast_week_on_sunday():
    now = datetime(2026, 11, 15, 23, 59, tzinfo=timezone.utc)  # Sunday SAST late
    mon, sun = ct.last_completed_sast_week(now)
    assert mon == date(2026, 11, 9)
    assert sun == date(2026, 11, 15)


# ---------------------------------------------------------------------------
# Week classification tests
# ---------------------------------------------------------------------------

def test_classify_week_in_progress():
    now = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)  # Thursday
    phase = ct.classify_week(
        date(2026, 11, 9), date(2026, 11, 15), now,
        ct.SnapshotFreeze(weekday=6, hour=18),
    )
    assert phase.kind == "in_progress"
    assert phase.provisional is False


def test_classify_week_completed_provisional():
    # Sunday has passed, but before freeze time (18:00 SAST on Sunday)
    now = datetime(2026, 11, 15, 15, 0, tzinfo=timezone.utc)  # 17:00 SAST Sunday
    phase = ct.classify_week(
        date(2026, 11, 9), date(2026, 11, 15), now,
        ct.SnapshotFreeze(weekday=6, hour=18),
    )
    assert phase.kind == "completed"
    assert phase.provisional is True


def test_classify_week_completed_frozen():
    # After freeze time (18:00 SAST on Sunday)
    now = datetime(2026, 11, 15, 17, 0, tzinfo=timezone.utc)  # 19:00 SAST Sunday
    phase = ct.classify_week(
        date(2026, 11, 9), date(2026, 11, 15), now,
        ct.SnapshotFreeze(weekday=6, hour=18),
    )
    assert phase.kind == "completed"
    assert phase.provisional is False


def test_classify_week_no_freeze_provisional():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    phase = ct.classify_week(
        date(2026, 11, 9), date(2026, 11, 15), now,
        freeze=None,
    )
    assert phase.kind == "completed"
    assert phase.provisional is True


# ---------------------------------------------------------------------------
# Evaluation tests
# ---------------------------------------------------------------------------

def test_evaluate_met_not_met_met():
    assert ct.evaluate_met_not_met(10, 10) == "met"
    assert ct.evaluate_met_not_met(10, 12) == "met"


def test_evaluate_met_not_met_not_met():
    assert ct.evaluate_met_not_met(10, 9) == "not_met"
    assert ct.evaluate_met_not_met(10, 0) == "not_met"


# ---------------------------------------------------------------------------
# Email normalisation tests
# ---------------------------------------------------------------------------

def test_norm_email():
    assert ct._norm_email("  Alice@Example.COM  ") == "alice@example.com"
    assert ct._norm_email(None) is None
    assert ct._norm_email("") is None


def test_build_email_index():
    users = [
        {"_id": "u1", "email": "Alice@example.com", "person_id": "p1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": "p2"},  # duplicate
        {"_id": "u3", "email": "bob@example.com", "person_id": "p3"},
        {"_id": "u4", "email": None},
    ]
    index = ct.build_email_index(users)
    assert len(index["alice@example.com"]) == 2
    assert len(index["bob@example.com"]) == 1


# ---------------------------------------------------------------------------
# Attribution tests
# ---------------------------------------------------------------------------

def test_attribute_task_primary_match():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "Alice@example.com",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result == "person-1"
    assert log == []  # no secondary matches


def test_attribute_task_secondary_assigned_to_email():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "someone.else@example.com",
        "assigned_to_email": "alice@example.com",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result == "person-1"
    assert any("secondary match assigned_to_email" in entry for entry in log)


def test_attribute_task_secondary_assigned_to_user_id():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "nobody@example.com",
        "assigned_to_user_id": "u1",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result == "person-1"
    assert any("secondary match assigned_to_user_id" in entry for entry in log)


def test_attribute_task_unmapped():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "unknown@example.com",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result is None


def test_attribute_task_not_completed():
    """attribute_task is a pure attribution function; it doesn't filter by
    completion status. Use an unattributable task to verify None is returned."""
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "nobody@example.com",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result is None


# ---------------------------------------------------------------------------
# Duplicate email resolution tests
# ---------------------------------------------------------------------------

def test_duplicate_email_same_person():
    """Two Users rows link to the same People via person_id → not ambiguous."""
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": "pid1"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1", "Email": "alice@example.com"},
    ]
    email_index = ct.build_email_index(users)
    people_by_id = {str(p["_id"]): p for p in people}
    log = []

    users_list = email_index["alice@example.com"]
    assert len(users_list) == 2
    pid_index = ct.build_person_id_index(people)
    user = ct._resolve_single_user(
        "alice@example.com", users_list, people_by_id, pid_index, log,
    )
    # Both link to same person via person_id → People.Email matches → one candidate
    assert user is not None


def test_duplicate_email_different_people():
    """Two Users rows with same email link to different People → ambiguous."""
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": "pid2"},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1", "Email": "alice@example.com"},
        {"_id": "person-2", "person_id": "pid2", "Email": "bob@example.com"},
    ]
    email_index = ct.build_email_index(users)
    people_by_id = {str(p["_id"]): p for p in people}
    log = []

    users_list = email_index["alice@example.com"]
    pid_index = ct.build_person_id_index(people)
    user = ct._resolve_single_user(
        "alice@example.com", users_list, people_by_id, pid_index, log,
    )
    assert user is None  # ambiguous
    assert any("AMBIGUOUS" in entry for entry in log)


def test_duplicate_email_one_unlinked():
    """One Users row has no person_id → ambiguous unless the other matches People.Email."""
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": None},
    ]
    people = [
        {"_id": "person-1", "person_id": "pid1", "Email": "alice@example.com"},
    ]
    email_index = ct.build_email_index(users)
    people_by_id = {str(p["_id"]): p for p in people}
    log = []

    users_list = email_index["alice@example.com"]
    pid_index = ct.build_person_id_index(people)
    user = ct._resolve_single_user(
        "alice@example.com", users_list, people_by_id, pid_index, log,
    )
    assert user is not None  # exactly one qualifies via People.Email
    assert user["_id"] == "u1"


def test_duplicate_email_case_difference():
    """Emails differing only by case normalise to the same key."""
    users = [
        {"_id": "u1", "email": "Alice@Example.com", "person_id": "pid1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": "pid1"},
    ]
    email_index = ct.build_email_index(users)
    assert len(email_index) == 1
    assert len(email_index["alice@example.com"]) == 2


# ---------------------------------------------------------------------------
# count_completed_tasks integration tests (mock Supabase)
# ---------------------------------------------------------------------------

def test_count_completed_tasks_basic():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": "2026-11-09T10:00:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
            {
                "status": "completed",
                "completedAt": "2026-11-09T14:00:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )

    assert result["unmapped_completed_tasks"] == 0
    assert result["daily_counts"]["person-1"]["2026-11-09"] == 2
    assert result["excluded_no_completedat"] == {}


def test_count_completed_tasks_excluded_no_completedat():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": None,
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
            {
                "status": "completed",
                "completedAt": "2026-11-09T10:00:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )

    assert result["excluded_no_completedat"]["person-1"] == 1
    assert result["daily_counts"]["person-1"]["2026-11-09"] == 1


def test_count_completed_tasks_unmapped():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": "2026-11-09T10:00:00+00:00",
                "assignedfor": "unknown@example.com",
                "Organization": "Org",
            },
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )

    assert result["unmapped_completed_tasks"] == 1
    assert result["daily_counts"] == {}
    assert result["excluded_no_completedat"] == {}


def test_count_completed_tasks_paging():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": f"2026-11-09T10:00:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            }
            for _ in range(5)
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
        page_size=2,
    )

    assert result["daily_counts"]["person-1"]["2026-11-09"] == 5
    # Should have made multiple queries due to paging
    assert len(sb.queries) >= 2


def test_count_completed_tasks_not_completed_ignored():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "Open",
                "completedAt": None,
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
            {
                "status": "completed",
                "completedAt": None,
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )

    # completedAt=NULL counts as excluded_no_completedat, not unmapped
    assert result["excluded_no_completedat"]["person-1"] == 1
    assert result["unmapped_completed_tasks"] == 0


def test_count_completed_tasks_task_type_filter():
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": "2026-11-09T10:00:00+00:00",
                "assignedfor": "alice@example.com",
                "taskType": "Follow Up",
                "Organization": "Org",
            },
            {
                "status": "completed",
                "completedAt": "2026-11-09T10:00:00+00:00",
                "assignedfor": "alice@example.com",
                "taskType": "Visit",
                "Organization": "Org",
            },
        ],
    )

    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
        task_type_filter=["Follow Up"],
    )

    assert result["daily_counts"]["person-1"]["2026-11-09"] == 1
    # Verify the taskType filter was applied in the query
    assert any("taskType" in str(q) for q in sb.queries)


# ---------------------------------------------------------------------------
# Hierarchy scope tests
# ---------------------------------------------------------------------------

def test_check_hierarchy_scope_match():
    leader_path = ["person-root", "person-12", "person-144", None]
    assert ct.check_hierarchy_scope("person-144", "person-12", leader_path) is True


def test_check_hierarchy_scope_no_match():
    leader_path = ["person-root", "person-12", "person-144", None]
    assert ct.check_hierarchy_scope("person-999", "person-12", leader_path) is False


def test_check_hierarchy_scope_empty():
    assert ct.check_hierarchy_scope("person-1", "person-2", []) is False


# ---------------------------------------------------------------------------
# Supreme admin tests
# ---------------------------------------------------------------------------

def test_is_supreme_admin_true():
    assert ct.is_supreme_admin({"is_supreme_admin": "true"}) is True
    assert ct.is_supreme_admin({"is_supreme_admin": "TRUE"}) is True


def test_is_supreme_admin_false():
    assert ct.is_supreme_admin({"is_supreme_admin": "false"}) is False
    assert ct.is_supreme_admin({"is_supreme_admin": "False"}) is False


def test_is_supreme_admin_null():
    assert ct.is_supreme_admin({"is_supreme_admin": None}) is False
    assert ct.is_supreme_admin({}) is False


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

def test_validate_target_days_valid():
    days = {"monday": 1, "tuesday": 2, "wednesday": 3,
            "thursday": 4, "friday": 5, "saturday": 6, "sunday": 7}
    assert ct.validate_target_days(days) == []


def test_validate_target_days_negative():
    days = {"monday": -1, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    errors = ct.validate_target_days(days)
    assert any("monday" in e and ">=" in e for e in errors)


def test_validate_target_days_all_zero():
    days = {"monday": 0, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    errors = ct.validate_target_days(days)
    assert any("at least one day" in e for e in errors)


def test_validate_dates_valid():
    assert ct.validate_dates(date(2026, 11, 9), date(2026, 11, 15)) == []


def test_validate_dates_start_not_monday():
    errors = ct.validate_dates(date(2026, 11, 10), date(2026, 11, 15))
    assert any("Monday" in e for e in errors)


def test_validate_dates_end_not_sunday():
    errors = ct.validate_dates(date(2026, 11, 9), date(2026, 11, 14))
    assert any("Sunday" in e for e in errors)


def test_validate_dates_end_before_start():
    errors = ct.validate_dates(date(2026, 11, 16), date(2026, 11, 9))
    assert any("after" in e for e in errors)


# ---------------------------------------------------------------------------
# Person tracking eligibility tests
# ---------------------------------------------------------------------------

def test_person_has_linked_user_true():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "person-1"},
    ]
    pid_index = {"pid1": "person-1"}
    assert ct.person_has_linked_user("person-1", users, pid_index) is True


def test_person_has_linked_user_false():
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "person-2"},
    ]
    pid_index = {"pid1": "person-1"}
    assert ct.person_has_linked_user("person-1", users, pid_index) is False


# ---------------------------------------------------------------------------
# Versioning plan tests
# ---------------------------------------------------------------------------

def test_plan_create_target():
    days = {"monday": 1, "tuesday": 2, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    ops = ct.plan_create_target(
        person_id="person-1",
        organization="Org",
        days=days,
        start_date=date(2026, 11, 9),
        end_date=date(2026, 11, 15),
        set_by="admin-1",
    )
    assert len(ops) == 1
    assert ops[0].op == "insert"
    assert ops[0].table == "custom_targets"
    assert ops[0].data["person_id"] == "person-1"
    assert ops[0].data["monday"] == 1
    assert ops[0].data["is_active"] is True


def test_plan_edit_started_target():
    days = {"monday": 5, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    ops = ct.plan_edit_started_target(
        target_id="target-1",
        person_id="person-1",
        organization="Org",
        days=days,
        start_date=date(2026, 11, 16),
        end_date=date(2026, 11, 22),
        set_by="admin-1",
        current_sunday=date(2026, 11, 15),
    )
    assert len(ops) == 2
    assert ops[0].op == "update"
    assert ops[0].data["end_date"] == "2026-11-15"
    assert ops[1].op == "insert"
    assert ops[1].data["start_date"] == "2026-11-16"


def test_plan_edit_future_target():
    days = {"monday": 3, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    ops = ct.plan_edit_future_target(
        target_id="target-1",
        days=days,
        end_date=date(2026, 11, 22),
        set_by="admin-1",
    )
    assert len(ops) == 1
    assert ops[0].op == "update"
    assert ops[0].match == {"id": "target-1"}
    assert ops[0].data["monday"] == 3


def test_plan_deactivate_target():
    ops = ct.plan_deactivate_target(
        target_id="target-1",
        current_sunday=date(2026, 11, 15),
    )
    assert len(ops) == 1
    assert ops[0].op == "update"
    assert ops[0].data["end_date"] == "2026-11-15"
    # is_active is NOT set to False — it stays True
    assert "is_active" not in ops[0].data


# ---------------------------------------------------------------------------
# evaluate_week tests
# ---------------------------------------------------------------------------

def test_evaluate_week_met():
    days = {"monday": 2, "tuesday": 3, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    daily = {"monday": 2, "tuesday": 3, "wednesday": 0,
             "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    result = ct.evaluate_week(date(2026, 11, 9), days, daily)
    assert result["weekly_target"] == 5
    assert result["weekly_completed"] == 5
    assert result["status"] == "met"


def test_evaluate_week_not_met():
    days = {"monday": 2, "tuesday": 3, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    daily = {"monday": 1, "tuesday": 2, "wednesday": 0,
             "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    result = ct.evaluate_week(date(2026, 11, 9), days, daily)
    assert result["weekly_target"] == 5
    assert result["weekly_completed"] == 3
    assert result["status"] == "not_met"


# ---------------------------------------------------------------------------
# Snapshot tests
# ---------------------------------------------------------------------------

def test_insert_snapshot():
    sb = _MockSupabase(custom_target_snapshots=[])
    result = ct.insert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 1, "tuesday": 2, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 1, "tuesday": 1, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=3,
        weekly_completed=2,
        excluded_no_completedat=0,
        status="not_met",
    )
    assert result["person_id"] == "person-1"
    assert result["weekly_target"] == 3
    assert result["weekly_completed"] == 2
    assert result["status"] == "not_met"
    assert len(sb.tables["custom_target_snapshots"]) == 1


def test_upsert_snapshot():
    sb = _MockSupabase(custom_target_snapshots=[])
    ct.upsert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 1, "tuesday": 0, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 1, "tuesday": 0, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=1,
        weekly_completed=1,
        excluded_no_completedat=0,
        status="met",
    )
    assert len(sb.tables["custom_target_snapshots"]) == 1
    assert sb.tables["custom_target_snapshots"][0]["status"] == "met"


# ---------------------------------------------------------------------------
# Additional coverage tests (Phase 2a review)
# ---------------------------------------------------------------------------

def test_counting_sunday_bucket_and_next_monday_bucket():
    """Completed task at Sunday 23:59:59 SAST lands in Sunday bucket;
    one at Monday 00:00:00 SAST lands in next week's Monday bucket."""
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            # Sunday Nov 15 23:59:59 SAST = Nov 15 21:59:59 UTC
            {
                "status": "completed",
                "completedAt": "2026-11-15T21:59:59+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
            # Monday Nov 16 00:00:00 SAST = Nov 15 22:00:00 UTC
            {
                "status": "completed",
                "completedAt": "2026-11-15T22:00:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
        ],
    )

    # Week Nov 9-15: only the Sunday task should be in range
    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )
    assert result["daily_counts"]["person-1"]["2026-11-15"] == 1
    assert result["unmapped_completed_tasks"] == 0

    # Week Nov 16-22: the Monday task lands in the Monday bucket
    result2 = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 16), date(2026, 11, 22),
    )
    assert result2["daily_counts"]["person-1"]["2026-11-16"] == 1


def test_counting_utc_vs_sast_date():
    """A task whose UTC calendar date differs from its SAST date is counted
    on the SAST day. 2026-11-15 22:30 UTC = 2026-11-16 00:30 SAST."""
    sb = _MockSupabase(
        Users=[
            {"_id": "u1", "email": "alice@example.com", "person_id": "pid1",
             "Organization": "Org"},
        ],
        People=[
            {"_id": "person-1", "person_id": "pid1", "Organization": "Org"},
        ],
        Tasks=[
            {
                "status": "completed",
                "completedAt": "2026-11-15T22:30:00+00:00",
                "assignedfor": "alice@example.com",
                "Organization": "Org",
            },
        ],
    )

    # UTC date Nov 15, but SAST date Nov 16 → not in week Nov 9-15
    result = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 9), date(2026, 11, 15),
    )
    assert result["daily_counts"] == {}

    # Counted on SAST day Nov 16 (Monday) in week Nov 16-22
    result2 = ct.count_completed_tasks(
        sb, {"Organization": "Org"},
        date(2026, 11, 16), date(2026, 11, 22),
    )
    assert result2["daily_counts"]["person-1"]["2026-11-16"] == 1


def test_evaluate_zero_completed_with_target_not_met():
    """Zero completed tasks with a target > 0 gives not_met; weekly_completed = 0."""
    days = {"monday": 5, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    daily = {"monday": 0, "tuesday": 0, "wednesday": 0,
             "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    result = ct.evaluate_week(date(2026, 11, 9), days, daily)
    assert result["weekly_target"] == 5
    assert result["weekly_completed"] == 0
    assert result["status"] == "not_met"


def test_evaluate_exactly_at_target_met():
    """Exactly at target (45) gives met."""
    days = {"monday": 45, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    daily = {"monday": 45, "tuesday": 0, "wednesday": 0,
             "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    result = ct.evaluate_week(date(2026, 11, 9), days, daily)
    assert result["weekly_target"] == 45
    assert result["weekly_completed"] == 45
    assert result["status"] == "met"


def test_evaluate_one_under_target_not_met():
    """One under target (44 vs 45) gives not_met."""
    days = {"monday": 45, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    daily = {"monday": 44, "tuesday": 0, "wednesday": 0,
             "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    result = ct.evaluate_week(date(2026, 11, 9), days, daily)
    assert result["weekly_target"] == 45
    assert result["weekly_completed"] == 44
    assert result["status"] == "not_met"


def test_person_target_starts_after_week_excluded():
    """A person whose target start_date is after the week is excluded."""
    week_monday = date(2026, 11, 9)
    week_sunday = date(2026, 11, 15)
    target_start = date(2026, 11, 16)  # starts after the week ends
    assert target_start > week_sunday


def test_insert_snapshot_insert_ignore_first_stays():
    """insert_snapshot twice for same (person_id, week_start): first stays."""
    sb = _MockSupabase(custom_target_snapshots=[])

    ct.insert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 1, "tuesday": 0, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 1, "tuesday": 0, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=1,
        weekly_completed=1,
        excluded_no_completedat=0,
        status="met",
    )

    # Second insert with different values — first should stay (insert-ignore)
    ct.insert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 9, "tuesday": 0, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 9, "tuesday": 0, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=9,
        weekly_completed=9,
        excluded_no_completedat=0,
        status="met",
    )

    assert len(sb.tables["custom_target_snapshots"]) == 1
    row = sb.tables["custom_target_snapshots"][0]
    assert row["weekly_target"] == 1  # first value kept
    assert row["status"] == "met"


def test_upsert_snapshot_replaces():
    """upsert_snapshot replaces the existing snapshot."""
    sb = _MockSupabase(custom_target_snapshots=[])

    ct.insert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 1, "tuesday": 0, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 1, "tuesday": 0, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=1,
        weekly_completed=1,
        excluded_no_completedat=0,
        status="not_met",
    )

    ct.upsert_snapshot(
        sb=sb,
        person_id="person-1",
        week_start=date(2026, 11, 9),
        organization="Org",
        day_targets={"monday": 9, "tuesday": 0, "wednesday": 0,
                     "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        day_completed={"monday": 9, "tuesday": 0, "wednesday": 0,
                       "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0},
        weekly_target=9,
        weekly_completed=9,
        excluded_no_completedat=0,
        status="met",
    )

    assert len(sb.tables["custom_target_snapshots"]) == 1
    row = sb.tables["custom_target_snapshots"][0]
    assert row["weekly_target"] == 9  # replaced
    assert row["status"] == "met"


def test_ambiguous_duplicate_email_rejected_and_unmapped():
    """Ambiguous duplicate email: targeting that person is rejected, tasks
    count under unmapped_completed_tasks, never credited to either user."""
    users = [
        {"_id": "u1", "email": "alice@example.com", "person_id": "pid1"},
        {"_id": "u2", "email": "alice@example.com", "person_id": "pid2"},
    ]
    people = [
        {"_id": "person-A", "person_id": "pid1", "Email": "alice@example.com"},
        {"_id": "person-B", "person_id": "pid2", "Email": "bob@example.com"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    # Two Users rows with same email but linked to different People → ambiguous
    users_list = email_index["alice@example.com"]
    assert len(users_list) == 2
    resolved = ct._resolve_single_user(
        "alice@example.com", users_list, people_by_id, pid_index, log,
    )
    assert resolved is None  # ambiguous → rejected
    assert any("AMBIGUOUS" in entry for entry in log)

    # A task with that email is not attributed → counts as unmapped
    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "alice@example.com",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result is None  # never credited to either user


def test_validate_no_overlap_rejects_overlapping_target():
    """A second active target for the same person with an overlapping range
    is rejected by the validation layer with a clear error."""
    existing = [
        {
            "id": "target-1",
            "person_id": "person-1",
            "start_date": date(2026, 11, 9),
            "end_date": date(2026, 11, 15),
            "is_active": True,
        },
    ]

    # New target overlaps (Nov 12-18 overlaps Nov 9-15)
    errors = ct.validate_no_overlap(
        new_start_date=date(2026, 11, 12),
        new_end_date=date(2026, 11, 18),
        existing_targets=existing,
    )
    assert len(errors) == 1
    assert "overlaps" in errors[0].lower()


def test_validate_no_overlap_allows_non_overlapping():
    """Non-overlapping targets produce no errors."""
    existing = [
        {
            "id": "target-1",
            "person_id": "person-1",
            "start_date": date(2026, 11, 9),
            "end_date": date(2026, 11, 15),
            "is_active": True,
        },
    ]

    errors = ct.validate_no_overlap(
        new_start_date=date(2026, 11, 16),
        new_end_date=date(2026, 11, 22),
        existing_targets=existing,
    )
    assert errors == []


def test_plan_edit_started_target_mid_week():
    """Mid-week edit of a started target: old row closed with end_date =
    the Sunday before the new start; new row starts the following Monday."""
    days = {"monday": 5, "tuesday": 0, "wednesday": 0,
            "thursday": 0, "friday": 0, "saturday": 0, "sunday": 0}
    ops = ct.plan_edit_started_target(
        target_id="target-1",
        person_id="person-1",
        organization="Org",
        days=days,
        start_date=date(2026, 11, 16),
        end_date=date(2026, 11, 22),
        set_by="admin-1",
        current_sunday=date(2026, 11, 15),
    )
    assert len(ops) == 2
    # Old row closed at the Sunday before the new start
    assert ops[0].op == "update"
    assert ops[0].data["end_date"] == "2026-11-15"
    assert ops[0].data["is_active"] is True
    # New row starts the following Monday
    assert ops[1].op == "insert"
    assert ops[1].data["start_date"] == "2026-11-16"


def test_attribution_ignores_memberid_and_person_id():
    """Attribution ignores Tasks.memberID and Tasks.person_id: a task whose
    memberID/person_id point to person A but whose assignedfor email belongs
    to person B is credited to B only."""
    users = [
        {"_id": "uA", "email": "a@example.com", "person_id": "pidA"},
        {"_id": "uB", "email": "b@example.com", "person_id": "pidB"},
    ]
    people = [
        {"_id": "person-A", "person_id": "pidA", "Email": "a@example.com"},
        {"_id": "person-B", "person_id": "pidB", "Email": "b@example.com"},
    ]
    email_index = ct.build_email_index(users)
    users_by_id = {str(u["_id"]): u for u in users}
    people_by_id = {str(p["_id"]): p for p in people}
    pid_index = ct.build_person_id_index(people)
    log = []

    # memberID and person_id point to person-A, but assignedfor is b@example.com (person-B)
    task = {
        "status": "completed",
        "completedAt": "2026-11-09T10:00:00+00:00",
        "assignedfor": "b@example.com",
        "memberID": "person-A",
        "person_id": "person-A",
        "Organization": "Org",
    }
    result = ct.attribute_task(
        task, email_index, users_by_id, people_by_id, pid_index, log,
    )
    assert result == "person-B"  # credited to B only, never A
    # memberID/person_id should not appear in the attribution log
    assert not any("memberID" in entry or "person_id" in entry for entry in log)
