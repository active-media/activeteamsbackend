from datetime import date
from types import SimpleNamespace

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

    def execute(self):
        rows = self.client.tables.get(self.table_name, [])
        self.client.queries.append((self.table_name, list(self.filters)))
        for operation, column, expected in self.filters:
            if operation == "eq":
                rows = [row for row in rows if row.get(column) == expected]
            elif operation == "in":
                rows = [row for row in rows if row.get(column) in expected]
            elif operation == "gte":
                rows = [row for row in rows if str(row.get(column) or "") >= expected]
            elif operation == "lte":
                rows = [row for row in rows if str(row.get(column) or "") <= expected]
        return SimpleNamespace(data=rows)


class MockSupabase:
    def __init__(self, targets, people=(), tasks=()):
        self.tables = {
            "twelve_tasks_targets": list(targets),
            "People": list(people),
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
