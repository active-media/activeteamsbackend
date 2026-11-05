import asyncio
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
import main
from Reports import twelve_tasks


class MockSupabaseQuery:
    def __init__(self, client, table):
        self.client = client
        self.table_name = table
        self.filters = []

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

    def lte(self, column, value):
        self.filters.append(("lte", column, value))
        return self

    def lt(self, column, value):
        self.filters.append(("lt", column, value))
        return self

    def or_(self, expression):
        self.filters.append(("or", expression))
        return self

    @staticmethod
    def _timestamp(value):
        if not isinstance(value, str):
            return value
        normalized = value.replace(" ", "T", 1)
        if normalized.endswith("+00"):
            normalized += ":00"
        elif normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed

    def execute(self):
        rows = self.client.tables.get(self.table_name, [])
        self.client.queries.append((self.table_name, list(self.filters)))
        for filter_item in self.filters:
            operation = filter_item[0]
            if operation == "or":
                expected = filter_item[1]
                alternatives = [term.split(".", 2) for term in expected.split(",")]
                rows = [
                    row for row in rows
                    if any(
                        len(term) == 3
                        and term[1] == "ilike"
                        and str(row.get(term[0]) or "").casefold() == term[2].casefold()
                        for term in alternatives
                    )
                ]
                continue
            _, column, expected = filter_item
            if operation == "eq":
                rows = [row for row in rows if row.get(column) == expected]
            elif operation == "in":
                rows = [row for row in rows if row.get(column) in expected]
            elif operation == "gte":
                rows = [row for row in rows if self._timestamp(row.get(column) or "") >= self._timestamp(expected)]
            elif operation == "lte":
                rows = [row for row in rows if self._timestamp(row.get(column) or "") <= self._timestamp(expected)]
            elif operation == "lt":
                rows = [row for row in rows if self._timestamp(row.get(column) or "") < self._timestamp(expected)]
        return SimpleNamespace(data=rows)


class MockSupabase:
    def __init__(self, targets, people=(), users=(), tasks=()):
        self.tables = {
            "twelve_tasks_targets": list(targets),
            "People": list(people),
            "Users": list(users),
            "Tasks": list(tasks),
        }
        self.queries = []

    def table(self, name):
        return MockSupabaseQuery(self, name)


ORG = "Active Church"
WEEK_START = date(2026, 11, 2)
WEEK_END = date(2026, 11, 8)
MONTH_START = date(2026, 11, 1)
MONTH_END = date(2026, 11, 30)


def target(person_id, value, week="2026-W45", organization=ORG, is_default=False):
    return {
        "person_id": person_id,
        "target": value,
        "week_identifier": week,
        "organization": organization,
        "is_default": is_default,
    }


def person(person_id):
    return {
        "_id": person_id,
        "Name": f"Name {person_id}",
        "Surname": "Person",
        "Gender": "Female",
        "Email": f"{person_id}@example.test",
    }


def admin_user(person_id, name, surname, email):
    return {
        "_id": person_id,
        "name": name,
        "surname": surname,
        "gender": "Female",
        "email": email,
    }


def completed(person_id, completed_at, organization=ORG):
    return {
        "status": "completed",
        "completedAt": completed_at,
        "assignedfor": person_id,
        "Organization": organization,
    }


def report_for(monkeypatch, supabase, period, start, end, org=ORG):
    monkeypatch.setattr(twelve_tasks, "supabase", supabase)
    return twelve_tasks.sb_get_twelve_tasks_report(
        period=period,
        org_filter={"Organization": org},
        start_date=start,
        end_date=end,
    )


def test_this_week_returns_three_admin_targets(monkeypatch):
    supabase = MockSupabase(
        targets=[
            target("person-1", 10),
            target("person-2", 40),
            target("person-3", 10),
        ],
        people=[person("person-1"), person("person-2"), person("person-3")],
    )

    report = report_for(monkeypatch, supabase, "thisWeek", WEEK_START, WEEK_END)

    assert len(report["rows"]) == 3
    assert {row["person_id"]: row["target"] for row in report["rows"]} == {
        "person-1": 10,
        "person-2": 40,
        "person-3": 10,
    }
    target_query = next(filters for table, filters in supabase.queries if table == "twelve_tasks_targets")
    assert ("in", "week_identifier", ["2026-W45"]) in target_query


def test_this_month_includes_admin_targets_from_overlapping_weeks(monkeypatch):
    supabase = MockSupabase(
        targets=[
            target("person-1", 10),
            target("person-2", 40),
            target("person-3", 10),
        ],
        people=[person("person-1"), person("person-2"), person("person-3")],
    )

    report = report_for(monkeypatch, supabase, "thisMonth", MONTH_START, MONTH_END)

    assert len(report["rows"]) == 3
    assert {row["person_id"]: row["target"] for row in report["rows"]} == {
        "person-1": 10,
        "person-2": 40,
        "person-3": 10,
    }
    target_query = next(filters for table, filters in supabase.queries if table == "twelve_tasks_targets")
    assert (
        "in",
        "week_identifier",
        ["2026-W44", "2026-W45", "2026-W46", "2026-W47", "2026-W48", "2026-W49"],
    ) in target_query


def test_month_sums_multiple_week_targets_and_actuals_per_person(monkeypatch):
    supabase = MockSupabase(
        targets=[
            target("person-1", 8, week="2026-W45"),
            target("person-1", 12, week="2026-W46"),
        ],
        people=[person("person-1")],
        tasks=[
            completed("person-1", "2026-11-03T10:00:00+00:00"),
            completed("person-1", "2026-11-12T10:00:00+00:00"),
        ],
    )

    report = report_for(monkeypatch, supabase, "thisMonth", MONTH_START, MONTH_END)

    assert len(report["rows"]) == 1
    assert report["rows"][0]["person_id"] == "person-1"
    assert report["rows"][0]["target"] == 20
    assert report["rows"][0]["this_week"] == 2
    assert report["rows"][0]["week_identifier"] == "2026-W45, 2026-W46"


def test_admin_user_ids_resolve_names_and_email_assigned_actuals(monkeypatch):
    supabase = MockSupabase(
        targets=[target("admin-user-1", 10)],
        users=[admin_user("admin-user-1", "Gloire", "Tsina", "gloire@example.test")],
        tasks=[
            {
                "status": "completed",
                "completedAt": "2026-11-03T10:00:00+00:00",
                "assigned_to_email": "gloire@example.test",
                "Organization": ORG,
            },
        ],
    )

    report = report_for(monkeypatch, supabase, "thisWeek", WEEK_START, WEEK_END)

    assert report["rows"] == [
        {
            "person_id": "admin-user-1",
            "name": "Gloire Tsina",
            "gender": "Female",
            "week_identifier": "2026-W45",
            "last_week": 0,
            "target": 10,
            "this_week": 1,
        },
    ]


def actuals_for_tasks(monkeypatch, tasks, ranges=None):
    supabase = MockSupabase(
        targets=[],
        people=[person("person-1")],
        tasks=tasks,
    )
    monkeypatch.setattr(twelve_tasks, "supabase", supabase)
    if ranges is None:
        ranges = {"week": twelve_tasks._iso_week_range("2026-W45")}
    actuals = twelve_tasks.sb_get_completed_task_actuals(
        {"Organization": ORG},
        ranges,
        ["person-1"],
    )
    return actuals, supabase


def task_at_sast(status, local_timestamp, **fields):
    local = datetime.fromisoformat(local_timestamp).replace(tzinfo=twelve_tasks.SAST)
    return {
        "status": status,
        "completedAt": local.astimezone(timezone.utc).isoformat(),
        "assignedfor": "person-1",
        "Organization": ORG,
        **fields,
    }


def test_monday_0030_sast_counts_in_new_week_only(monkeypatch):
    ranges = {
        "previous": twelve_tasks._iso_week_range("2026-W44"),
        "current": twelve_tasks._iso_week_range("2026-W45"),
    }
    actuals, _ = actuals_for_tasks(
        monkeypatch,
        [task_at_sast("completed", "2026-11-02T00:30:00")],
        ranges,
    )

    assert actuals["previous"].get("person-1", 0) == 0
    assert actuals["current"]["person-1"] == 1


def test_sunday_final_fractional_second_counts_in_week(monkeypatch):
    actuals, _ = actuals_for_tasks(
        monkeypatch,
        [task_at_sast("completed", "2026-11-08T23:59:59.500000")],
    )

    assert actuals["week"]["person-1"] == 1


def test_monday_at_week_end_is_excluded(monkeypatch):
    actuals, _ = actuals_for_tasks(
        monkeypatch,
        [task_at_sast("completed", "2026-11-09T00:00:00")],
    )

    assert actuals["week"].get("person-1", 0) == 0


def test_naive_utc_and_postgres_utc_offset_timestamps_count(monkeypatch):
    actuals, _ = actuals_for_tasks(
        monkeypatch,
        [
            {
                "status": "completed",
                "completedAt": "2026-11-08 21:59:59.500000",
                "assignedfor": "person-1",
                "Organization": ORG,
            },
            {
                "status": "completed",
                "completedAt": "2026-11-08 21:59:59.828923+00",
                "assignedfor": "person-1",
                "Organization": ORG,
            },
        ],
    )

    assert actuals["week"]["person-1"] == 2


def test_status_matching_is_case_insensitive_for_completed_spellings(monkeypatch):
    statuses = ["completed", "Completed", "Done", "CLOSED", "finished"]
    tasks = [
        {
            "status": status,
            "completedAt": "2026-11-03T10:00:00+00:00",
            "assignedfor": "person-1",
            "Organization": ORG,
        }
        for status in statuses
    ]
    actuals, _ = actuals_for_tasks(monkeypatch, tasks)

    assert actuals["week"]["person-1"] == len(statuses)


@pytest.mark.parametrize(
    ("period", "expected_start", "expected_end"),
    [
        ("today", "2026-11-04T22:00:00+00:00", "2026-11-05T22:00:00+00:00"),
        ("thisWeek", "2026-11-01T22:00:00+00:00", "2026-11-08T22:00:00+00:00"),
        ("previousWeek", "2026-10-25T22:00:00+00:00", "2026-11-01T22:00:00+00:00"),
        ("thisMonth", "2026-10-31T22:00:00+00:00", "2026-11-30T22:00:00+00:00"),
        ("previousMonth", "2026-09-30T22:00:00+00:00", "2026-10-31T22:00:00+00:00"),
    ],
)
def test_period_range_uses_sast_midnight_and_exclusive_end(
    monkeypatch, period, expected_start, expected_end
):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 5, 12, tzinfo=tz)

    monkeypatch.setattr(twelve_tasks, "datetime", FrozenDateTime)
    start, end = twelve_tasks._period_range(period)

    assert start.isoformat() == expected_start
    assert end.isoformat() == expected_end


def test_admin_target_actual_matches_report_for_person_and_week(monkeypatch):
    supabase = MockSupabase(
        targets=[target("admin-user-1", 10)],
        users=[admin_user("admin-user-1", "Gloire", "Tsina", "gloire@example.test")],
        tasks=[
            {
                "status": "Completed",
                "completedAt": "2026-11-03 10:00:00+00",
                "assigned_to_email": "GLOIRE@example.test",
                "Organization": ORG,
            },
        ],
    )
    monkeypatch.setattr(main, "supabase", supabase)
    monkeypatch.setattr(twelve_tasks, "supabase", supabase)

    admin_result = asyncio.run(
        main.get_twelve_tasks_targets(
            week="2026-W45",
            person_id=None,
            org=ORG,
            current_user={"role": "admin", "Organization": ORG},
        )
    )
    report = report_for(monkeypatch, supabase, "thisWeek", WEEK_START, WEEK_END)

    assert admin_result["targets"], supabase.queries
    assert set(admin_result["targets"][0]) == {
        "person_id",
        "target",
        "week_identifier",
        "organization",
        "is_default",
        "updated_by",
        "updated_at",
        "actual",
    }
    assert admin_result["targets"][0]["actual"] == 1
    assert admin_result["targets"][0]["target"] == report["rows"][0]["target"]
    assert admin_result["targets"][0]["actual"] == report["rows"][0]["this_week"]


def test_targets_outside_month_return_period_empty_state(monkeypatch):
    supabase = MockSupabase(
        targets=[target("person-1", 22, week="2026-W43")],
        people=[person("person-1")],
    )

    report = report_for(monkeypatch, supabase, "thisMonth", MONTH_START, MONTH_END)

    assert report["rows"] == []
    assert report["empty_state"] == "No tasks set for this period"


def test_targets_from_other_organizations_are_isolated(monkeypatch):
    supabase = MockSupabase(
        targets=[
            target("person-other", 99, organization="Other Org"),
        ],
        people=[person("person-other")],
    )

    report = report_for(monkeypatch, supabase, "thisWeek", WEEK_START, WEEK_END)

    assert report["rows"] == []
    assert report["empty_state"] == "No tasks set for this week"
    target_query = next(filters for table, filters in supabase.queries if table == "twelve_tasks_targets")
    assert ("eq", "organization", ORG) in target_query
