"""Read-only KPI audit using the backend's Supabase reporting helpers."""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BACKEND_DIR = Path(__file__).resolve().parents[1]
KPI_KEYS = (
    "total_attendance",
    "active_cells",
    "life_class",
    "school_of_leaders",
    "plan_40",
    "service_target",
    "twelve_tasks",
    "staff_interns_youth",
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--period",
        choices=("thisMonth", "thisWeek", "previousWeek"),
        default="thisMonth",
    )
    parser.add_argument("--org", help="Limit direct Supabase reads to this organization")
    return parser.parse_args()


def _credentials() -> tuple[str, str, bool]:
    url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    anon_key = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY")
    if not url:
        raise RuntimeError("SUPABASE_URL is required")
    if service_key:
        return url, service_key, False
    if anon_key:
        return url, anon_key, True
    raise RuntimeError(
        "Set SUPABASE_SERVICE_ROLE_KEY, or provide SUPABASE_ANON_KEY/SUPABASE_KEY"
    )


def _load_reporting_helpers(url: str, key: str):
    os.environ["SUPABASE_URL"] = url
    os.environ["SUPABASE_KEY"] = key
    os.environ["SUPABASE_SERVICE_ROLE_KEY"] = key
    os.environ["PYTHON_DOTENV_DISABLED"] = "true"
    if str(BACKEND_DIR) not in sys.path:
        sys.path.insert(0, str(BACKEND_DIR))

    # The backend connection module prints connection diagnostics at import time.
    # Keep those diagnostics, including the project URL, out of audit output.
    with redirect_stdout(StringIO()):
        from Reports.twelve_tasks import sb_get_twelve_tasks_report
        from reporting_core.envelope import dashboard_envelope
        from reporting_core.periods import period_to_week_ids, resolve_period
        from supabase_helpers.service_targets import sb_get_service_target_report
        from supabase_helpers.supabase_connection import supabase
        from supabase_helpers.supabase_stats import sb_get_reporting_dashboard

    return (
        supabase,
        sb_get_reporting_dashboard,
        sb_get_service_target_report,
        sb_get_twelve_tasks_report,
        dashboard_envelope,
        period_to_week_ids,
        resolve_period,
    )


def _fetch_all(query_factory) -> list[dict]:
    rows = []
    offset = 0
    page_size = 1000
    while True:
        page = query_factory().range(offset, offset + page_size - 1).execute().data or []
        rows.extend(page)
        if len(page) < page_size:
            return rows
        offset += page_size


def _events_for_org(supabase, org_filter: dict | None) -> list[dict]:
    def query():
        result = supabase.table("events").select(
            "event_id, event_type_name, is_active, Organization"
        )
        if org_filter:
            result = result.eq("Organization", org_filter["Organization"])
        return result

    return _fetch_all(query)


def _sessions_for_period(supabase, start, end) -> list[dict]:
    def query():
        return (
            supabase.table("event_sessions")
            .select("event_id, session_date, status")
            .gte("session_date", start.isoformat())
            .lte("session_date", end.isoformat())
        )

    return _fetch_all(query)


def _print_active_cells_funnel(events: list[dict], sessions: list[dict]) -> None:
    event_types = Counter(str(row.get("event_type_name") or "(missing)") for row in events)
    active_states = Counter(
        "missing" if row.get("is_active") is None else str(row["is_active"]).lower()
        for row in events
    )
    session_statuses = Counter(str(row.get("status") or "(missing)") for row in sessions)

    cell_events = [
        row for row in events
        if str(row.get("event_type_name") or "").strip().casefold() == "cells"
    ]
    active_cell_events = [row for row in cell_events if row.get("is_active") is not False]
    active_event_ids = {
        str(row.get("event_id"))
        for row in active_cell_events
        if row.get("event_id") is not None
    }
    completed_event_ids = {
        str(row.get("event_id"))
        for row in sessions
        if str(row.get("status") or "complete").lower() == "complete"
        and row.get("event_id") is not None
    }
    with_completed_sessions = active_event_ids & completed_event_ids

    steps = [
        ("Organization-scoped events", len(events)),
        ("Cells event type", len(cell_events)),
        ("Active Cells events", len(active_cell_events)),
        ("Active Cells with completed sessions in period", len(with_completed_sessions)),
    ]
    print("\nActive Cells funnel")
    print("  Events by event_type_name:")
    for label, count in sorted(event_types.items(), key=lambda item: item[0].casefold()):
        print(f"    {label}: {count}")
    print("  Events by is_active:")
    for label, count in sorted(active_states.items()):
        print(f"    {label}: {count}")
    print("  Sessions by status in selected period:")
    for label, count in sorted(session_statuses.items(), key=lambda item: item[0].casefold()):
        print(f"    {label}: {count}")

    print("  Step-by-step event counts:")
    removals = []
    for index, (label, count) in enumerate(steps):
        removed = steps[index - 1][1] - count if index else 0
        print(f"    {label}: {count} (removed {removed})")
        if index:
            removals.append((label, removed))
    if removals:
        largest_step, largest_count = max(removals, key=lambda item: item[1])
        print(f"  Largest drop: {largest_step} ({largest_count} events)")


def _kpi_values(payload: dict) -> dict:
    kpis = payload.get("kpis") or []
    if isinstance(kpis, dict):
        return {
            key: value.get("value") if isinstance(value, dict) else value
            for key, value in kpis.items()
        }
    return {
        row["key"]: row.get("value")
        for row in kpis
        if isinstance(row, dict) and row.get("key")
    }


def _fetch_dashboard_api(period: str) -> tuple[dict | None, str | None]:
    token = os.getenv("AUDIT_BEARER_TOKEN")
    base_url = os.getenv("AUDIT_BACKEND_URL") or os.getenv("VITE_BACKEND_URL")
    if not token:
        return None, "skipped (AUDIT_BEARER_TOKEN is not set)"
    if not base_url:
        return None, "skipped (set AUDIT_BACKEND_URL or VITE_BACKEND_URL)"

    url = f"{base_url.rstrip('/')}/reporting/dashboard?{urlencode({'period': period})}"
    request = Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8")), None
    except HTTPError as error:
        return None, f"HTTP {error.code}"
    except URLError as error:
        return None, f"network error ({type(error.reason).__name__})"


def _same_value(left, right) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return left == right


def audit(args: argparse.Namespace) -> bool:
    url, key, using_anon = _credentials()
    if using_anon:
        print("Using an anon key; Supabase RLS may hide rows from this audit.")
    if args.org:
        org_filter = {"Organization": args.org}
    else:
        org_filter = None

    (
        supabase,
        sb_get_reporting_dashboard,
        sb_get_service_target_report,
        sb_get_twelve_tasks_report,
        dashboard_envelope,
        period_to_week_ids,
        resolve_period,
    ) = _load_reporting_helpers(url, key)

    period = resolve_period(args.period, None, None)
    data = sb_get_reporting_dashboard(
        args.period,
        org_filter,
        start_date=period.start,
        end_date=period.end,
    )

    service_rows = []
    for week_identifier in period_to_week_ids(period):
        report = sb_get_service_target_report(week_identifier, org_filter)
        service_rows.extend(report.get("targets") or [])
    service_data = {
        "targets": service_rows,
        "total_target": sum(row.get("target_count") or 0 for row in service_rows),
        "total_targets": len(service_rows),
    }
    twelve_data = sb_get_twelve_tasks_report(
        period.token,
        org_filter,
        None,
        period.start,
        period.end,
        None,
        True,
    )

    dashboard = dashboard_envelope(data, period, None, service_data, twelve_data)
    dashboard_values = _kpi_values(dashboard.model_dump(mode="json"))
    ministry_values = {
        row.get("ministry"): row.get("attended")
        for row in data.get("ministry_performance") or []
    }
    task_total = sum(row.get("this_week") or 0 for row in twelve_data.get("rows") or [])
    direct_values = {
        "total_attendance": data.get("total_attendance"),
        "active_cells": data.get("active_cells"),
        "life_class": ministry_values.get("Life Class"),
        "school_of_leaders": ministry_values.get("School of Leaders"),
        "plan_40": ministry_values.get("Plan 40"),
        "service_target": service_data["total_target"],
        "twelve_tasks": task_total,
        "staff_interns_youth": None,
    }

    print(f"KPI data audit ({args.period}; org filter: {args.org or 'none'})")
    direct_matches = True
    for key_name in KPI_KEYS:
        direct = direct_values.get(key_name)
        expected = dashboard_values.get(key_name)
        matches = _same_value(direct, expected)
        direct_matches = direct_matches and matches
        status = "MATCH" if matches else "MISMATCH"
        print(f"  {key_name}: {direct if direct is not None else 'n/a'} "
              f"(dashboard function: {expected if expected is not None else 'n/a'}; {status})")
    print(f"  Service Target rows: {len(service_rows)}")

    current_week = resolve_period("thisWeek", None, None)
    if args.period == "thisWeek":
        this_week_data = twelve_data
    else:
        this_week_data = sb_get_twelve_tasks_report(
            "thisWeek",
            org_filter,
            None,
            current_week.start,
            current_week.end,
            None,
            True,
        )
    this_week_rows = this_week_data.get("rows") or []
    people_covered = sum(1 for row in this_week_rows if (row.get("this_week") or 0) > 0)
    this_week_total = sum(row.get("this_week") or 0 for row in this_week_rows)
    print(
        "  Twelve Tasks thisWeek: "
        f"{len(this_week_rows)} rows, {people_covered} people covered, {this_week_total} total"
    )

    events = _events_for_org(supabase, org_filter)
    sessions = _sessions_for_period(supabase, period.start, period.end)
    _print_active_cells_funnel(events, sessions)

    api_payload, api_error = _fetch_dashboard_api(args.period)
    if api_error:
        print(f"\nDashboard API comparison: {api_error}")
    else:
        api_values = _kpi_values(api_payload or {})
        print("\nDashboard API comparison (dashboard value vs direct value)")
        if args.org:
            print("  Note: API scope comes from the bearer account; --org applies only to direct reads.")
        api_matches = True
        for key_name in KPI_KEYS:
            api_value = api_values.get(key_name)
            direct = direct_values.get(key_name)
            matches = _same_value(api_value, direct)
            api_matches = api_matches and matches
            print(
                f"  {key_name}: {api_value if api_value is not None else 'n/a'} vs "
                f"{direct if direct is not None else 'n/a'} "
                f"({'MATCH' if matches else 'MISMATCH'})"
            )
        direct_matches = direct_matches and api_matches
    return direct_matches


def main() -> None:
    args = _arguments()
    try:
        matches = audit(args)
    except RuntimeError as error:
        print(f"KPI data audit: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    if not matches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
