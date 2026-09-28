"""Independent cell reporting queries.

This module intentionally has no dependency on the stats/dashboard module.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Sequence

from supabase_helpers.supabase_connection import supabase_admin

_CELL_TYPES = ["Cells", "cells", "CELLS"]
_SCOPES = {"all", "leader1", "leader12", "leader144", "leader1728"}
_PERIODS = {
    "weekly", "previous_week", "previousWeek", "monthly", "previous_month",
    "previousMonth", "three_months", "six_months", "yearly",
}


def _date_value(value: object) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _window(period: str, start_date: Optional[str], end_date: Optional[str]) -> tuple[date, date]:
    if start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date must be provided together")
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("dates must use YYYY-MM-DD format") from exc
    else:
        today = datetime.now(timezone.utc).date()
        if period in {"weekly", "previous_week", "previousWeek"}:
            start = today - timedelta(days=today.weekday())
            if period in {"previous_week", "previousWeek"}:
                start -= timedelta(days=7)
            end = start + timedelta(days=6)
        elif period in {"previous_month", "previousMonth"}:
            current_month_start = today.replace(day=1)
            end = current_month_start - timedelta(days=1)
            start = end.replace(day=1)
        elif period == "monthly":
            start = today.replace(day=1)
            end = today
        elif period == "three_months":
            start = today.replace(month=((today.month - 1) // 3) * 3 + 1, day=1)
            end = today
        elif period == "six_months":
            start = today.replace(month=1 if today.month <= 6 else 7, day=1)
            end = today
        elif period == "yearly":
            start = today.replace(month=1, day=1)
            end = today
        else:
            raise ValueError(f"period must be one of: {', '.join(sorted(_PERIODS))}")
    if start > end:
        raise ValueError("start_date must be before or equal to end_date")
    return start, end


def _bucket_start(value: date, period: str) -> date:
    if period in {"weekly", "previous_week", "previousWeek"}:
        return value - timedelta(days=value.weekday())
    if period in {"monthly", "previous_month", "previousMonth"}:
        return value.replace(day=1)
    if period == "yearly":
        return value.replace(month=1, day=1)
    months = 3 if period == "three_months" else 6
    return value.replace(month=((value.month - 1) // months) * months + 1, day=1)


def _shift_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year, month = divmod(month_index, 12)
    day = min(value.day, calendar.monthrange(year, month + 1)[1])
    return value.replace(year=year, month=month + 1, day=day)


def _comparison_window(
    period: str,
    start: date,
    end: date,
    custom_range: bool,
) -> tuple[date, date]:
    """Return the period immediately before the requested period."""
    if custom_range:
        length = end - start
        previous_end = start - timedelta(days=1)
        return previous_end - length, previous_end
    if period in {"weekly", "previous_week", "previousWeek"}:
        return start - timedelta(days=7), end - timedelta(days=7)
    if period in {"monthly", "previous_month", "previousMonth"}:
        if period in {"previous_month", "previousMonth"}:
            return _shift_months(start, -1), _shift_months(end, -1)
        previous_start = _shift_months(start, -1)
        previous_end = _shift_months(end, -1)
        last_day = previous_start.replace(
            day=calendar.monthrange(previous_start.year, previous_start.month)[1]
        )
        return previous_start, min(previous_end, last_day)
    months = {"three_months": 3, "six_months": 6, "yearly": 12}[period]
    previous_start = _shift_months(start, -months)
    return previous_start, previous_start + (end - start)


def _percentage_change(current: int, previous: int) -> Optional[float]:
    if previous == 0:
        return 0.0 if current == 0 else None
    return round((current - previous) * 100 / previous, 2)


def _key(row: dict, *fields: str) -> Optional[str]:
    for field in fields:
        value = str(row.get(field) or "").strip().lower()
        if value:
            return value
    return None


def _org_events(query, org_filter: Optional[dict]):
    if org_filter:
        value = org_filter.get("organization") or org_filter.get("Organization")
        if value:
            query = query.eq("organization", value)
    return query


def _fetch_in_chunks(table: str, columns: str, column: str, values: Sequence[str], **filters) -> list[dict]:
    """Read large UUID sets in bounded requests so PostgREST URLs stay valid."""
    rows = []
    for offset in range(0, len(values), 100):
        query = supabase_admin.table(table).select(columns).in_(column, list(values[offset:offset + 100]))
        for field, value in filters.items():
            if isinstance(value, tuple) and value[0] == "gte":
                query = query.gte(field, value[1])
            elif isinstance(value, tuple) and value[0] == "lte":
                query = query.lte(field, value[1])
            elif isinstance(value, tuple) and value[0] == "eq":
                query = query.eq(field, value[1])
        rows.extend(query.execute().data or [])
    return rows


def _visible_leaders(leader_id: Optional[str], scope: str, visible_user_ids: Optional[Sequence[str]]) -> set[str]:
    users = supabase_admin.table("Users").select(
        "_id, name, surname, email, leader12, leader144, leader1728"
    ).execute().data or []
    allowed = {str(value) for value in visible_user_ids} if visible_user_ids is not None else None
    if scope == "all":
        selected = {str(row["_id"]) for row in users if row.get("_id")}
        if allowed is not None:
            selected &= allowed
    else:
        selected = {str(leader_id)}
        changed = True
        while changed:
            changed = False
            for row in users:
                row_id = str(row.get("_id") or "")
                leaders = {str(row.get(field)) for field in ("leader12", "leader144", "leader1728") if row.get(field)}
                if row_id and row_id not in selected and leaders & selected:
                    selected.add(row_id)
                    changed = True
        if allowed is not None:
            selected &= allowed
    keys = set()
    for row in users:
        if str(row.get("_id") or "") in selected:
            keys.update(filter(None, [str(row.get("email") or "").strip().lower(), " ".join(filter(None, [row.get("name"), row.get("surname")] )).strip().lower()]))
    return keys


def get_cell_report(
    period: str = "monthly",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    scope: str = "all",
    leader_id: Optional[str] = None,
    cell_id: Optional[str] = None,
    org_filter: Optional[dict] = None,
    visible_user_ids: Optional[Sequence[str]] = None,
    include_comparison: bool = True,
) -> dict:
    """Return independent cell reporting data, separate from dashboard stats."""
    if scope not in _SCOPES:
        raise ValueError("scope must be all, leader1, leader12, leader144, or leader1728")
    if scope != "all" and not leader_id:
        raise ValueError("leader_id is required when scope is not all")
    if leader_id and visible_user_ids is not None and str(leader_id) not in {str(value) for value in visible_user_ids}:
        raise PermissionError("leader is outside the permitted hierarchy")

    start, end = _window(period, start_date, end_date)
    leader_keys = _visible_leaders(leader_id, scope, visible_user_ids)
    query = supabase_admin.table("events").select(
        "event_id, event_leader, event_leader_email, event_date, event_type_name, organization"
    ).in_("event_type_name", _CELL_TYPES)
    events = _org_events(query, org_filter).execute().data or []
    scoped_events = []
    for event in events:
        leader = str(event.get("event_leader_email") or event.get("event_leader") or "").strip().lower()
        if (scope != "all" or visible_user_ids is not None) and leader not in leader_keys:
            continue
        if cell_id and str(event.get("event_id")) != str(cell_id):
            continue
        scoped_events.append(event)

    event_ids = [str(row["event_id"]) for row in scoped_events if row.get("event_id")]
    empty = {"new_cells": 0, "new_people": 0, "lives_given": 0, "unique_attendees": 0, "attendance_visits": 0}
    if not event_ids:
        result = {"period": period, "date_range": {"start": start.isoformat(), "end": end.isoformat()}, "scope": {"type": scope, "leader_id": leader_id, "cell_id": cell_id}, "summary": empty, "buckets": []}
    else:
        sessions = _fetch_in_chunks(
            "event_sessions",
            "session_id, event_id, session_date, status, is_did_not_meet",
            "event_id",
            event_ids,
            session_date=("gte", start.isoformat()),
        )
        sessions = [row for row in sessions if (_date_value(row.get("session_date")) or date.min) <= end]
        sessions = [row for row in sessions if str(row.get("status") or "").lower() == "complete" and not row.get("is_did_not_meet")]
        session_ids = [str(row["session_id"]) for row in sessions if row.get("session_id")]
        attendees = []
        if session_ids:
            attendees = _fetch_in_chunks(
                "event_session_attendees",
                "session_id, mongo_person_id, email, full_name, is_checked_in",
                "session_id",
                session_ids,
            )
        attendees = [row for row in attendees if row.get("is_checked_in", True)]
        new_people = _fetch_in_chunks(
            "event_new_people",
            "mongo_id, email, phone, added_at",
            "event_id",
            event_ids,
        )
        new_people = [row for row in new_people if start <= (_date_value(row.get("added_at")) or date.min) <= end]
        decisions = _fetch_in_chunks(
            "event_consolidations",
            "mongo_person_id, person_email, person_phone, created_at, decision_type",
            "event_id",
            event_ids,
            decision_type=("eq", "first_time"),
        )
        decisions = [row for row in decisions if start <= (_date_value(row.get("created_at")) or date.min) <= end]

        new_cells = {str(row["event_id"]) for row in scoped_events if start <= (_date_value(row.get("event_date")) or date.min) <= end}
        summary = {
            "new_cells": len(new_cells),
            "new_people": len({_key(row, "mongo_id", "email", "phone") for row in new_people} - {None}),
            "lives_given": len({_key(row, "mongo_person_id", "person_email", "person_phone") for row in decisions} - {None}),
            "unique_attendees": len({_key(row, "mongo_person_id", "email", "full_name") for row in attendees} - {None}),
            "attendance_visits": len(attendees),
        }
        buckets = {}
        def bucket(value: Optional[date]) -> dict:
            key = _bucket_start(value or start, period)
            return buckets.setdefault(key, {"new_cells": set(), "new_people": set(), "lives_given": set(), "unique_attendees": set(), "attendance_visits": 0})
        for row in scoped_events:
            value = _date_value(row.get("event_date"))
            if value and start <= value <= end:
                bucket(value)["new_cells"].add(str(row["event_id"]))
        for row in new_people:
            bucket(_date_value(row.get("added_at")))["new_people"].add(_key(row, "mongo_id", "email", "phone"))
        for row in decisions:
            bucket(_date_value(row.get("created_at")))["lives_given"].add(_key(row, "mongo_person_id", "person_email", "person_phone"))
        session_dates = {str(row["session_id"]): _date_value(row.get("session_date")) for row in sessions if row.get("session_id")}
        for row in attendees:
            item = bucket(session_dates.get(str(row.get("session_id"))))
            item["attendance_visits"] += 1
            key = _key(row, "mongo_person_id", "email", "full_name")
            if key:
                item["unique_attendees"].add(key)
        result_buckets = [{"period": key.isoformat(), "new_cells": len(value["new_cells"]), "new_people": len(value["new_people"] - {None}), "lives_given": len(value["lives_given"] - {None}), "unique_attendees": len(value["unique_attendees"]), "attendance_visits": value["attendance_visits"]} for key, value in sorted(buckets.items())]
        result = {"period": period, "date_range": {"start": start.isoformat(), "end": end.isoformat()}, "scope": {"type": scope, "leader_id": leader_id, "cell_id": cell_id}, "summary": summary, "buckets": result_buckets}

    if include_comparison:
        previous_start, previous_end = _comparison_window(
            period,
            start,
            end,
            bool(start_date or end_date),
        )
        previous = get_cell_report(
            period=period,
            start_date=previous_start.isoformat(),
            end_date=previous_end.isoformat(),
            scope=scope,
            leader_id=leader_id,
            cell_id=cell_id,
            org_filter=org_filter,
            visible_user_ids=visible_user_ids,
            include_comparison=False,
        )
        if "summary" in result:
            summary = result["summary"]
            change = {key: summary[key] - previous["summary"][key] for key in summary}
            result["comparison"] = {
                "current_period": {**result["date_range"], **summary},
                "previous_period": {**previous["date_range"], **previous["summary"]},
                "change": change,
                "percentage_change": {
                    key: _percentage_change(summary[key], previous["summary"][key])
                    for key in summary
                },
            }

    return result

    sessions = _fetch_in_chunks(
        "event_sessions",
        "session_id, event_id, session_date, status, is_did_not_meet",
        "event_id",
        event_ids,
        session_date=("gte", start.isoformat()),
    )
    sessions = [row for row in sessions if (_date_value(row.get("session_date")) or date.min) <= end]
    sessions = [row for row in sessions if str(row.get("status") or "").lower() == "complete" and not row.get("is_did_not_meet")]
    session_ids = [str(row["session_id"]) for row in sessions if row.get("session_id")]
    attendees = []
    if session_ids:
        attendees = _fetch_in_chunks(
            "event_session_attendees",
            "session_id, mongo_person_id, email, full_name, is_checked_in",
            "session_id",
            session_ids,
        )
    attendees = [row for row in attendees if row.get("is_checked_in", True)]
    new_people = _fetch_in_chunks(
        "event_new_people",
        "mongo_id, email, phone, added_at",
        "event_id",
        event_ids,
    )
    new_people = [row for row in new_people if start <= (_date_value(row.get("added_at")) or date.min) <= end]
    decisions = _fetch_in_chunks(
        "event_consolidations",
        "mongo_person_id, person_email, person_phone, created_at, decision_type",
        "event_id",
        event_ids,
        decision_type=("eq", "first_time"),
    )
    decisions = [row for row in decisions if start <= (_date_value(row.get("created_at")) or date.min) <= end]

    new_cells = {str(row["event_id"]) for row in scoped_events if start <= (_date_value(row.get("event_date")) or date.min) <= end}
    summary = {
        "new_cells": len(new_cells),
        "new_people": len({_key(row, "mongo_id", "email", "phone") for row in new_people} - {None}),
        "lives_given": len({_key(row, "mongo_person_id", "person_email", "person_phone") for row in decisions} - {None}),
        "unique_attendees": len({_key(row, "mongo_person_id", "email", "full_name") for row in attendees} - {None}),
        "attendance_visits": len(attendees),
    }
    buckets = {}
    def bucket(value: Optional[date]) -> dict:
        key = _bucket_start(value or start, period)
        return buckets.setdefault(key, {"new_cells": set(), "new_people": set(), "lives_given": set(), "unique_attendees": set(), "attendance_visits": 0})
    for row in scoped_events:
        value = _date_value(row.get("event_date"))
        if value and start <= value <= end:
            bucket(value)["new_cells"].add(str(row["event_id"]))
    for row in new_people:
        bucket(_date_value(row.get("added_at")))["new_people"].add(_key(row, "mongo_id", "email", "phone"))
    for row in decisions:
        bucket(_date_value(row.get("created_at")))["lives_given"].add(_key(row, "mongo_person_id", "person_email", "person_phone"))
    session_dates = {str(row["session_id"]): _date_value(row.get("session_date")) for row in sessions if row.get("session_id")}
    for row in attendees:
        item = bucket(session_dates.get(str(row.get("session_id"))))
        item["attendance_visits"] += 1
        key = _key(row, "mongo_person_id", "email", "full_name")
        if key:
            item["unique_attendees"].add(key)
    result_buckets = [{"period": key.isoformat(), "new_cells": len(value["new_cells"]), "new_people": len(value["new_people"] - {None}), "lives_given": len(value["lives_given"] - {None}), "unique_attendees": len(value["unique_attendees"]), "attendance_visits": value["attendance_visits"]} for key, value in sorted(buckets.items())]
    result = {"period": period, "date_range": {"start": start.isoformat(), "end": end.isoformat()}, "scope": {"type": scope, "leader_id": leader_id, "cell_id": cell_id}, "summary": summary, "buckets": result_buckets}
    if include_comparison:
        previous_start, previous_end = _comparison_window(
            period,
            start,
            end,
            bool(start_date or end_date),
        )
        previous = get_cell_report(
            period=period,
            start_date=previous_start.isoformat(),
            end_date=previous_end.isoformat(),
            scope=scope,
            leader_id=leader_id,
            cell_id=cell_id,
            org_filter=org_filter,
            visible_user_ids=visible_user_ids,
            include_comparison=False,
        )
        change = {key: result["summary"][key] - previous["summary"][key] for key in result["summary"]}
        result["comparison"] = {
            "current_period": {**result["date_range"], **result["summary"]},
            "previous_period": {**previous["date_range"], **previous["summary"]},
            "change": change,
            "percentage_change": {
                key: _percentage_change(result["summary"][key], previous["summary"][key])
                for key in result["summary"]
            },
        }
    return result
