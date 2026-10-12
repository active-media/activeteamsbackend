from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from main import app
from reporting_core import router as reporting_routes
from reporting_core.scope import ReportScope


class Result:
    def __init__(self, data):
        self.data = data


class HistoryQuery:
    def __init__(self, store):
        self.store = store
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
                "created_at": datetime(2026, 10, 5, 12, tzinfo=timezone.utc).isoformat(),
            }
            self.store.append(row)
            return Result([row])
        return Result([
            row for row in self.store
            if all(row.get(key) == value for key, value in self.filters.items())
        ])


class HistoryClient:
    def __init__(self):
        self.rows = []

    def table(self, _name):
        return HistoryQuery(self.rows)


def json_block(value) -> str:
    return "```json\n" + json.dumps(value, indent=2, ensure_ascii=False) + "\n```"


def dashboard_helper(period, org_filter, campus, visible_ids, start_date, end_date):
    return {
        "total_attendance": 24,
        "growth_rate": 20.0,
        "active_cells": 2,
        "active_cells_by_week": [2],
        "attendance_trend": {"labels": [start_date.isoformat()], "values": [24]},
        "campus_comparison": [{"campus": campus or "North", "value": 24}],
        "cells": [{"name": "Central Cell", "leader": "Alex Leader", "attendance": 24, "location": campus or "North"}],
    }


def twelve_helper(*_args):
    return {
        "rows": [{
            "person_id": "person-1",
            "name": "Alex Leader",
            "last_week": 2,
            "target": 3,
            "this_week": 4,
        }]
    }


def service_helper(week, _org_filter, _visible_ids=None):
    return {
        "targets": [{
            "leader_name": "Alex Leader",
            "week_identifier": week,
            "target_count": 6,
            "email": "alex@example.test",
        }],
        "total_target": 6,
        "total_targets": 1,
    }


def build_contract() -> str:
    @asynccontextmanager
    async def empty_lifespan(application):
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = empty_lifespan
    app.dependency_overrides.clear()
    scope = ReportScope({"Organization": "Example Org"}, False, "admin", "admin-1", None)
    app.dependency_overrides[reporting_routes.get_report_scope] = lambda: scope
    history = HistoryClient()

    try:
        with (
            patch.object(reporting_routes, "sb_get_reporting_dashboard", dashboard_helper),
            patch.object(reporting_routes, "sb_get_twelve_tasks_report", twelve_helper),
            patch.object(reporting_routes, "sb_get_service_target_report", service_helper),
            patch.object(reporting_routes, "supabase_admin", history),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            catalog = client.get("/reporting/catalog").json()
            dashboard_response = client.get("/reporting/dashboard?period=2025-02&campus=North")
            dashboard = dashboard_response.json()
            report_response = client.get("/reporting/reports/twelve_tasks?period=2025-02")
            report = report_response.json()
            error = client.get("/reporting/reports/life_class").json()
            csv_response = client.get("/reporting/reports/twelve_tasks/export?format=csv&period=2025-02")
            generated_response = client.post("/reporting/reports/twelve_tasks/generate?period=2025-02")
            generated = generated_response.json()
            history_list = client.get("/reporting/history").json()
            history_item = client.get(f"/reporting/history/{generated['id']}").json()
    finally:
        app.dependency_overrides.clear()
        app.router.lifespan_context = original_lifespan

    assert dashboard_response.status_code == report_response.status_code == 200
    assert generated_response.status_code == 200
    return f'''# Reporting API Contract

All endpoints are served by FastAPI under `/reporting` and require the existing bearer token. Allowed roles are `admin`, `super_admin`, `leader`, and `leaderAt12`. A non-super user must have an Organization. Refer to Scope rules below.

## Period tokens

Weeks use Monday-Sunday and dates are inclusive UTC calendar dates. Today's worked example is **2026-10-05**:

| Token | Resolved range on 2026-10-05 |
| --- | --- |
| `today` | 2026-10-05 to 2026-10-05 |
| `thisWeek` | 2026-10-05 to 2026-10-11 |
| `thisMonth` | 2026-10-01 to 2026-10-31 |
| `previous7` | 2026-09-28 to 2026-10-04 |
| `previousWeek` | 2026-09-28 to 2026-10-04 |
| `previousMonth` | 2026-09-01 to 2026-09-30 |
| `thisQuarter` | 2026-10-01 to 2026-12-31 |
| `yearly` | 2026-01-01 to 2026-12-31 |
| `YYYY-MM` | Calendar month, e.g. `2026-09` is 2026-09-01 to 2026-09-30 |
| explicit dates | `start_date` and `end_date` together override `period`; `period` metadata token is `custom` |

Invalid dates, one-sided explicit ranges, or reversed ranges return `422`.

## Scope rules

- `super_admin` or `is_supreme_admin=true`: all organizations; `org_filter=null`.
- `admin`: all reporting data in the user's Organization.
- `leader` and `leaderAt12`: own person record and descendants resolved from `LeaderId`, `LeaderPath`, and hierarchy leader fields, within their Organization.
- Missing Organization for any non-super user, unresolved leader hierarchy, `user`, and `registrant`: `403`.

## GET /reporting/catalog

**Params:** none. **Roles:** all reporting roles above. **Success:** list of all 8 report types, their params, supported exports, and `implemented` flag.

Example request: `GET /reporting/catalog`

Example success response generated from the route:

{json_block(catalog)}

**Errors:** `401` invalid/missing token; `403` denied scope; `422` N/A; `501` N/A; `500` unexpected catalog failure.

## GET /reporting/dashboard

**Params:** `period` optional (`today`, `thisWeek`, `thisMonth`, `previous7`, `previousWeek`, `previousMonth`, `thisQuarter`, `yearly`, `YYYY-MM`); `start_date` and `end_date` optional together (`YYYY-MM-DD`); `campus` optional non-empty location, max 120 characters. Explicit dates take precedence. **Roles:** all reporting roles above.

Example request: `GET /reporting/dashboard?period=2025-02&campus=North`

Example success response generated from mocked helper data and validated by `ReportEnvelope`:

{json_block(dashboard)}

**Errors:** `401`, `403`, `422`; `501` N/A; `500` calculation failure.

## GET /reporting/reports/{{report_type}}

**Params:** `report_type` is `twelve_tasks`, `cells_graph`, `life_class`, `school_of_leaders`, `plan_40`, `staff_interns_youth`, `school_cell`, or `service_target`; shared `period`, `start_date`, `end_date`, and optional `campus` as above. **Roles:** all reporting roles above.

Example request: `GET /reporting/reports/twelve_tasks?period=2025-02`

Example success response generated from mocked helper data and validated by `ReportEnvelope`:

{json_block(report)}

Example `501` response for a known unimplemented report:

{json_block(error)}

**Errors:** `401`, `403`, `422`, `501` known report not implemented, `500` calculation failure; `404` unknown report type.

## GET /reporting/reports/{{report_type}}/export

**Params:** `report_type` as above; required `format` is `csv`, `xlsx`, or `pdf`; shared `period`, `start_date`, `end_date`, optional `campus`. **Roles:** all reporting roles above.

Example request: `GET /reporting/reports/twelve_tasks/export?format=csv&period=2025-02`

Example CSV success body generated from the CSV route:

```csv
{csv_response.text.rstrip()}
```

CSV responses use `text/csv` and a `Content-Disposition` filename; XLSX responses use the Office Open XML spreadsheet media type. PDF is not implemented.

**Errors:** `401`, `403`, `422`, `501` known report or PDF export not implemented, `500` export failure; `404` unknown report type.

## POST /reporting/reports/{{report_type}}/generate

**Params:** `report_type` and shared `period`, `start_date`, `end_date`, optional `campus`; optional `organization` is accepted only for a super-admin and is required for that role. No request body. **Roles:** all reporting roles above. Persists the envelope to `report_runs` and returns the saved record, including its `id`.

Example request: `POST /reporting/reports/twelve_tasks/generate?period=2025-02`

Example success response generated from the route:

{json_block(generated)}

**Errors:** `401`, `403`, `422`, `501` known report not implemented, `500` calculation/persistence failure; `404` unknown report type.

## GET /reporting/history

**Params:** optional `limit`, integer 1-200, default 50; optional `organization`, required for super-admin and must equal the caller's Organization for everyone else. All queries filter by the resolved organization. **Roles:** all reporting roles above.

Example request: `GET /reporting/history?limit=10`

Example success response generated from the route:

{json_block(history_list)}

**Errors:** `401`, `403`, `422` invalid limit, `501` N/A, `500` history query failure.

## GET /reporting/history/{{run_id}}

**Params:** `run_id` is a report run UUID; optional `organization`, required for super-admin and must equal the caller's Organization for everyone else. All queries filter by the resolved organization. **Roles:** all reporting roles above; organization-scoped users can only retrieve their Organization's runs.

Example request: `GET /reporting/history/{generated['id']}`

Example success response generated from the route:

{json_block(history_item)}

**Errors:** `401`, `403`, `422` invalid id format not enforced (missing IDs return `404`), `501` N/A, `500` history query failure; `404` missing or out-of-scope run.

## Compatibility and Setup

Existing `/stats/*` contracts and `/report-schedules` remain available. Schedule IDs are translated to the established stored report type names. Apply `migration_reporting_core.sql` in the Supabase project's **SQL Editor** (or run it with `psql` against that same project's database) before calling generate/history. The migration is idempotent and is not applied by this generator.
'''


if __name__ == "__main__":
    destination = ROOT / "docs" / "REPORTING_API_CONTRACT.md"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(build_contract(), encoding="utf-8")
    print(destination)