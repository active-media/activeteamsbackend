from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import uuid

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from auth.utils import get_current_user
from main import app
from reporting_core import router as reporting_routes
from reporting_core.schemas import ReportEnvelope
from reporting_core.scope import ReportScope, get_report_scope
from supabase_helpers import excel_export


@pytest.fixture
def client(monkeypatch):
    original_startup = app.router.on_startup
    original_shutdown = app.router.on_shutdown
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def empty_lifespan(application):
        yield

    app.router.on_startup = []
    app.router.on_shutdown = []
    app.router.lifespan_context = empty_lifespan
    app.dependency_overrides.clear()
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    app.router.on_startup = original_startup
    app.router.on_shutdown = original_shutdown
    app.router.lifespan_context = original_lifespan


@pytest.fixture
def admin_scope():
    return ReportScope({"Organization": "Example Org"}, False, "admin", "admin-1", None)


def set_scope(scope):
    app.dependency_overrides[get_report_scope] = lambda: scope


def dashboard_helper(period, org_filter, campus, visible_ids, start_date, end_date):
    return {
        "total_attendance": 24,
        "growth_rate": 20.0,
        "active_cells": 2,
        "active_cells_by_week": [2],
        "attendance_trend": {"labels": [start_date.isoformat()], "values": [24]},
        "campus_comparison": [{"campus": campus or "North", "value": 24}],
        "cells": [{"name": "Central Cell", "leader": "—", "attendance": 24, "location": "North"}],
    }


def mock_calculators(monkeypatch, dashboard_calls=None):
    def dashboard(*args):
        if dashboard_calls is not None:
            dashboard_calls.append(args)
        return dashboard_helper(*args)

    def twelve(*args):
        return {"rows": [{"person_id": "person-1", "name": "Alex Leader", "last_week": 2, "target": None, "this_week": 4}]}

    def service(week, org_filter, visible_ids=None):
        return {"targets": [], "total_target": 0, "total_targets": 0}

    monkeypatch.setattr(reporting_routes, "sb_get_reporting_dashboard", dashboard)
    monkeypatch.setattr(reporting_routes, "sb_get_twelve_tasks_report", twelve)
    monkeypatch.setattr(reporting_routes, "sb_get_service_target_report", service)


def test_catalog_has_eight_types_and_implementation_flags(client, admin_scope):
    set_scope(admin_scope)

    response = client.get("/reporting/catalog")

    assert response.status_code == 200
    items = {item["report_type"]: item for item in response.json()}
    assert len(items) == 8
    assert {name for name, item in items.items() if item["implemented"]} == {
        "twelve_tasks", "cells_graph", "school_cell", "service_target"
    }


def test_dashboard_and_twelve_tasks_are_valid_envelopes_and_nulls_are_real_nulls(
    client, admin_scope, monkeypatch
):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)

    dashboard = client.get("/reporting/dashboard?period=2025-02&campus=North")
    twelve = client.get("/reporting/reports/twelve_tasks?period=2025-02")

    assert dashboard.status_code == twelve.status_code == 200
    dashboard_model = ReportEnvelope.model_validate(dashboard.json())
    twelve_model = ReportEnvelope.model_validate(twelve.json())
    assert len(dashboard_model.kpis) == 8
    assert dashboard_model.kpis[2].value is None
    assert dashboard_model.tables[1].rows[0]["leader"] is None
    assert twelve_model.tables[0].rows[0]["target"] is None
    assert "—" not in dashboard.text + twelve.text


@pytest.mark.parametrize(
    ("section", "helper_name", "kpi_index", "other_kpi_index", "other_value"),
    [
        ("Service Target", "sb_get_service_target_report", 5, 6, 4),
        ("Twelve Tasks", "sb_get_twelve_tasks_report", 6, 5, 0),
    ],
)
def test_dashboard_optional_source_failure_returns_null_kpi(
    client, admin_scope, monkeypatch, caplog, section, helper_name,
    kpi_index, other_kpi_index, other_value,
):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)

    def fail(*args):
        raise RuntimeError("report table unavailable")

    monkeypatch.setattr(reporting_routes, helper_name, fail)
    with caplog.at_level(logging.ERROR, logger=reporting_routes.__name__):
        response = client.get("/reporting/dashboard?period=2025-02")

    assert response.status_code == 200
    payload = response.json()
    assert payload["kpis"][0]["value"] == 24
    assert payload["kpis"][kpi_index]["value"] is None
    assert payload["kpis"][other_kpi_index]["value"] == other_value
    assert any(
        section in record.message and record.exc_info
        for record in caplog.records
    )


def test_dashboard_core_source_failure_returns_500(client, admin_scope, monkeypatch):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)

    def fail(*args):
        raise RuntimeError("attendance data unavailable")

    monkeypatch.setattr(reporting_routes, "sb_get_reporting_dashboard", fail)

    response = client.get("/reporting/dashboard?period=2025-02")

    assert response.status_code == 500
    assert response.json()["detail"] == "Unable to calculate reporting dashboard"


@pytest.mark.parametrize(
    "report_type",
    ["life_class", "school_of_leaders", "plan_40", "staff_interns_youth"],
)
@pytest.mark.parametrize("operation", ["report", "csv_export", "generate", "pdf_export"])
def test_unimplemented_reports_have_exact_501_shape(
    client, admin_scope, report_type, operation
):
    set_scope(admin_scope)
    if operation == "report":
        response = client.get(f"/reporting/reports/{report_type}")
        message = f"The {report_type} report is not implemented."
    elif operation == "generate":
        response = client.post(f"/reporting/reports/{report_type}/generate")
        message = f"The {report_type} report is not implemented."
    else:
        format_value = "pdf" if operation == "pdf_export" else "csv"
        response = client.get(f"/reporting/reports/{report_type}/export?format={format_value}")
        message = f"The {report_type} report is not implemented."

    assert response.status_code == 501
    assert response.json() == {
        "detail": {
            "code": "not_implemented",
            "report_type": report_type,
            "message": message,
        }
    }


@pytest.mark.parametrize("role,organization", [("admin", None), ("user", "Example Org"), ("registrant", "Example Org")])
@pytest.mark.parametrize(
    "method,path",
    [
        ("get", "/reporting/catalog"),
        ("get", "/reporting/dashboard"),
        ("get", "/reporting/reports/twelve_tasks"),
        ("get", "/reporting/reports/life_class"),
        ("get", "/reporting/reports/life_class/export?format=csv"),
        ("post", "/reporting/reports/twelve_tasks/generate"),
        ("get", "/reporting/history"),
        ("get", "/reporting/history/not-a-run"),
    ],
)
def test_reporting_routes_deny_missing_org_and_nonreporting_roles(
    client, role, organization, method, path
):
    user = {"role": role, "user_id": "test-user"}
    if organization:
        user["Organization"] = organization
    app.dependency_overrides[get_current_user] = lambda: user

    response = getattr(client, method)(path)

    assert response.status_code == 403, (role, path, response.text)


def test_dashboard_and_cells_graph_use_identical_resolved_helper_params(
    client, admin_scope, monkeypatch
):
    set_scope(admin_scope)
    calls = []
    mock_calculators(monkeypatch, calls)

    dashboard = client.get("/reporting/dashboard?period=2025-03&campus=North")
    cells_graph = client.get("/reporting/reports/cells_graph?period=2025-03&campus=North")

    assert dashboard.status_code == cells_graph.status_code == 200
    assert calls[0] == calls[1]
    assert calls[0][0:3] == ("2025-03", {"Organization": "Example Org"}, "North")
    assert calls[0][4].isoformat() == "2025-03-01"
    assert calls[0][5].isoformat() == "2025-03-31"
    assert dashboard.json()["kpis"][0]["value"] == cells_graph.json()["kpis"][0]["value"]


def test_dashboard_and_twelve_tasks_use_identical_helper_params_and_values(
    client, admin_scope, monkeypatch
):
    set_scope(admin_scope)
    calls = []

    def twelve(*args):
        calls.append(args)
        return {"rows": [{"person_id": "person-1", "name": "Alex Leader", "last_week": 2, "target": None, "this_week": 4}]}

    mock_calculators(monkeypatch)
    monkeypatch.setattr(reporting_routes, "sb_get_twelve_tasks_report", twelve)

    dashboard = client.get("/reporting/dashboard?period=2025-03&campus=North")
    report = client.get("/reporting/reports/twelve_tasks?period=2025-03&campus=North")

    assert dashboard.status_code == report.status_code == 200
    assert calls[0] == calls[1]
    assert dashboard.json()["kpis"][6]["value"] == report.json()["kpis"][0]["value"] == 4


def test_csv_export_has_content_type_and_filename(client, admin_scope, monkeypatch):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)

    response = client.get("/reporting/reports/twelve_tasks/export?format=csv&period=2025-02")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "twelve_tasks_2025-02-01_2025-02-28.csv" in response.headers["content-disposition"]


def test_cells_graph_xlsx_receives_campus_and_resolved_dates(client, admin_scope, monkeypatch):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)
    calls = []

    def builder(*args):
        calls.append(args)
        return b"xlsx-bytes"

    monkeypatch.setattr(reporting_routes, "build_cells_excel", builder)
    response = client.get(
        "/reporting/reports/cells_graph/export?format=xlsx&start_date=2025-02-01&end_date=2025-02-28&campus=North"
    )

    assert response.status_code == 200
    assert response.content == b"xlsx-bytes"
    assert calls == [({"Organization": "Example Org"}, "2025-02-01", "2025-02-28", None, "North")]
    assert response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert "cells_graph_2025-02-01_2025-02-28.xlsx" in response.headers["content-disposition"]


def test_cells_export_fetcher_applies_campus_and_dates(monkeypatch):
    class Query:
        def __init__(self, rows):
            self.rows = rows
            self.filters = {}

        def select(self, *_args):
            return self

        def gte(self, key, value):
            self.filters["gte"] = (key, value)
            return self

        def lte(self, key, value):
            self.filters["lte"] = (key, value)
            return self

        def execute(self):
            return FakeResult(self.rows)

    rows = [
        {"session_date": "2025-02-03", "week_identifier": "2025-W06", "checked_in_count": 10, "event_id": "north", "events": {"event_type_name": "Cells", "Organization": "Example Org", "location": "North", "event_leader": "Alex Leader"}},
        {"session_date": "2025-02-03", "week_identifier": "2025-W06", "checked_in_count": 20, "event_id": "south", "events": {"event_type_name": "Cells", "Organization": "Example Org", "location": "South", "event_leader": "Alex Leader"}},
        {"session_date": "2025-02-03", "week_identifier": "2025-W06", "checked_in_count": 30, "event_id": "other-org", "events": {"event_type_name": "Cells", "Organization": "Other Org", "location": "North", "event_leader": "Alex Leader"}},
    ]

    class Supabase:
        def table(self, _name):
            return Query(rows)

    monkeypatch.setattr("supabase_helpers.supabase_connection.supabase", Supabase())

    result = excel_export._get_weekly_cells_data(
        {"Organization": "Example Org"}, "2025-02-01", "2025-02-28", campus="North"
    )

    assert [row["event_id"] for row in result] == ["north"]


class FakeResult:
    def __init__(self, data):
        self.data = data


class FakeHistoryQuery:
    def __init__(self, history_client):
        self.history_client = history_client
        self.filters = {}
        self.insert_value = None

    def select(self, *_args):
        return self

    def insert(self, value):
        self.insert_value = value
        return self

    def eq(self, key, value):
        self.filters[key] = value
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        if self.insert_value is not None:
            row = {
                **self.insert_value,
                "id": str(uuid.uuid4()),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.history_client.rows.append(row)
            return FakeResult([row])
        self.history_client.read_filters.append(dict(self.filters))
        rows = [
            row for row in self.history_client.rows
            if all(row.get(key) == value for key, value in self.filters.items())
        ]
        return FakeResult(rows)


class FakeHistoryClient:
    def __init__(self):
        self.rows = []
        self.read_filters = []

    def table(self, _name):
        return FakeHistoryQuery(self)


def test_generated_history_is_saved_scoped_and_fetchable(client, admin_scope, monkeypatch):
    set_scope(admin_scope)
    mock_calculators(monkeypatch)
    fake_history = FakeHistoryClient()
    monkeypatch.setattr(reporting_routes, "supabase_admin", fake_history)

    generated = client.post("/reporting/reports/twelve_tasks/generate?period=2025-02")

    assert generated.status_code == 200
    assert len(fake_history.rows) == 1
    run_id = fake_history.rows[0]["id"]
    assert generated.json()["id"] == run_id
    assert fake_history.rows[0]["org"] == "Example Org"
    assert client.get("/reporting/history").status_code == 200
    assert client.get(f"/reporting/history/{run_id}").status_code == 200
    assert fake_history.read_filters == [
        {"org": "Example Org"},
        {"org": "Example Org", "id": run_id},
    ]

    set_scope(ReportScope({"Organization": "Other Org"}, False, "admin", "admin-2", None))
    other_org_response = client.get(f"/reporting/history/{run_id}")
    assert other_org_response.status_code == 404
    assert fake_history.read_filters[-1] == {"org": "Other Org", "id": run_id}


def test_history_list_preserves_cross_organization_403(client, admin_scope):
    set_scope(admin_scope)

    response = client.get("/reporting/history?organization=Other%20Org")

    assert response.status_code == 403
    assert response.json()["detail"] == "Cannot access report history for another organization"