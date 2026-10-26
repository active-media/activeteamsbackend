"""Data + response builders for the Cells report drill-down.

Cells are `events` rows (event_type_name = "cells"); their meetings are
`event_sessions` rows. A cell is identified by event_id. The cell's leader is
stored on the event as `event_leader`, which this module resolves to a People
`_id` (see build_leader_index).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from supabase_helpers.supabase_connection import supabase_admin

PAGE_SIZE = 1000
EVENT_SELECT = (
    "*, events!inner(event_name, event_type_name, Organization, event_leader, "
    "is_school_cell, location)"
)


def _normal(value: Any) -> str:
    return str(value or "").strip().casefold()


def person_name(person: dict | None) -> str:
    if not person:
        return ""
    return " ".join(
        str(person.get(key) or "").strip() for key in ("Name", "Surname") if str(person.get(key) or "").strip()
    )


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

def fetch_cell_sessions(
    org_filter: dict | None,
    start: str | None = None,
    end: str | None = None,
    event_id: str | None = None,
) -> list[dict]:
    """All cell sessions in [start, end] (or all time if omitted), paginated."""
    rows: list[dict] = []
    offset = 0
    while True:
        query = (
            supabase_admin.table("event_sessions")
            .select(EVENT_SELECT)
            .eq("events.event_type_name", "cells")
        )
        organization = (org_filter or {}).get("Organization")
        if organization:
            query = query.eq("events.Organization", organization)
        if event_id:
            query = query.eq("event_id", event_id)
        if start:
            query = query.gte("session_date", start)
        if end:
            query = query.lte("session_date", end)
        batch = (
            query.order("session_date").order("event_id")
            .range(offset, offset + PAGE_SIZE - 1).execute().data or []
        )
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


# --------------------------------------------------------------------------
# Leader resolution (event_leader text -> People._id)
# --------------------------------------------------------------------------

def build_leader_index(people: list[dict]) -> dict[str, set[str]]:
    """Lookup of normalised id / email / full name -> set of matching person ids."""
    index: dict[str, set[str]] = defaultdict(set)
    for person in people:
        person_id = str(person.get("_id") or "")
        if not person_id:
            continue
        for key in (person_id, person.get("Email") or person.get("email"), person_name(person)):
            normal = _normal(key)
            if normal:
                index[normal].add(person_id)
    return index


def resolve_leader(event_leader: Any, index: dict[str, set[str]]) -> tuple[str | None, set[str]]:
    """(leader_id, candidates). leader_id is set only when the match is unique."""
    candidates = set(index.get(_normal(event_leader), set()))
    return (next(iter(candidates)), candidates) if len(candidates) == 1 else (None, candidates)


# --------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------

def _attendance(session: dict) -> int:
    if session.get("is_did_not_meet"):
        return 0
    try:
        return int(session.get("checked_in_count") or 0)
    except (TypeError, ValueError):
        return 0


def group_cells(sessions: list[dict], people: list[dict]) -> dict[str, dict]:
    index = build_leader_index(people)
    cells: dict[str, dict] = {}
    for session in sessions:
        event_id = str(session.get("event_id") or "")
        if not event_id:
            continue
        event = session.get("events") or {}
        cell = cells.get(event_id)
        if cell is None:
            leader_id, candidates = resolve_leader(event.get("event_leader"), index)
            cell = cells[event_id] = {
                "event_id": event_id,
                "name": event.get("event_name") or "Unnamed cell",
                "location": event.get("location") or "",
                "leader_raw": event.get("event_leader") or "",
                "leader_id": leader_id,
                "candidates": candidates,
                "sessions": [],
            }
        cell["sessions"].append(session)
    return cells


def _held(cell_sessions: list[dict]) -> list[dict]:
    return [s for s in cell_sessions if not s.get("is_did_not_meet")]


def _summary(cells: list[dict]) -> dict:
    all_sessions = [s for c in cells for s in c["sessions"]]
    held = _held(all_sessions)
    total = sum(_attendance(s) for s in held)
    return {
        "cells": len(cells),
        "leaders": len({c["leader_id"] for c in cells if c["leader_id"]}),
        "meetings_held": len(held),
        "did_not_meet": len(all_sessions) - len(held),
        "total_attendance": total,
        "average_attendance": round(total / len(held), 1) if held else None,
    }


def _trend(sessions: list[dict], key: str, title: str) -> dict:
    buckets: dict[str, dict] = {}
    for s in sessions:
        bucket_key = str(s.get("week_identifier") or s.get("session_date") or "")
        if not bucket_key:
            continue
        date = str(s.get("session_date") or "")[:10]
        bucket = buckets.setdefault(bucket_key, {"x": date, "y": 0})
        if date and (not bucket["x"] or date < bucket["x"]):
            bucket["x"] = date
        bucket["y"] += _attendance(s)
    points = sorted(buckets.values(), key=lambda p: p["x"])
    return {"key": key, "title": title, "points": points}


def _period_meta(period) -> dict:
    return {"token": period.token, "start": period.start.isoformat(), "end": period.end.isoformat()}


def leader_payload(
    leader_id: str,
    visible_ids: set[str],
    people: list[dict],
    sessions: list[dict],
    view: str,
    period,
) -> dict:
    people_by_id = {str(p.get("_id")): p for p in people if p.get("_id")}
    cells = group_cells(sessions, people)
    # A cell is shown only when its leader resolves to exactly one person who is
    # inside the viewer's downline - never by an ambiguous name match.
    included = [c for c in cells.values() if c["leader_id"] in visible_ids]
    ambiguous = sum(
        1 for c in cells.values()
        if c["leader_id"] is None and len(c["candidates"]) > 1 and c["candidates"] & visible_ids
    )

    cell_rows = []
    for c in included:
        held = _held(c["sessions"])
        total = sum(_attendance(s) for s in held)
        cell_rows.append({
            "id": c["event_id"], "event_id": c["event_id"], "name": c["name"],
            "leader": person_name(people_by_id.get(c["leader_id"])), "leader_id": c["leader_id"],
            "meetings": len(held), "attendance": total,
            "average": round(total / len(held), 1) if held else None,
            "location": c["location"],
        })
    cell_rows.sort(key=lambda r: (-r["attendance"], r["name"].casefold()))

    by_leader: dict[str, list[dict]] = defaultdict(list)
    for c in included:
        by_leader[c["leader_id"]].append(c)
    leader_rows = []
    for lid, lcells in by_leader.items():
        held = [s for c in lcells for s in _held(c["sessions"])]
        total = sum(_attendance(s) for s in held)
        leader_rows.append({
            "id": lid, "leader_id": lid, "leader": person_name(people_by_id.get(lid)),
            "cells": len(lcells), "meetings": len(held), "attendance": total,
            "average": round(total / len(held), 1) if held else None,
        })
    leader_rows.sort(key=lambda r: (-r["attendance"], r["leader"].casefold()))

    return {
        "meta": {
            "title": person_name(people_by_id.get(leader_id)) or "Leader",
            "leader_id": leader_id, "scope": view, "period": _period_meta(period),
            "ambiguous_cells": ambiguous,
        },
        "summary": _summary(included),
        "series": [_trend([s for c in included for s in c["sessions"]], "downline_trend", "Attendance by Week")],
        "tables": [
            {"key": "leaders", "rows": leader_rows},
            {"key": "cells", "rows": cell_rows},
        ],
    }


def cell_payload(
    event_id: str,
    all_sessions: list[dict],
    people: list[dict],
    start: str,
    end: str,
    period,
) -> dict:
    """Stats for one cell. `all_sessions` is the cell's full history (used to resolve its leader)."""
    people_by_id = {str(p.get("_id")): p for p in people if p.get("_id")}
    cell = group_cells(all_sessions, people)[str(event_id)]
    in_range = sorted(
        (s for s in cell["sessions"] if start <= str(s.get("session_date") or "")[:10] <= end),
        key=lambda s: str(s.get("session_date") or ""),
    )
    held = _held(in_range)
    total = sum(_attendance(s) for s in held)
    rows = [{
        "id": str(s.get("id") or f"{event_id}-{i}"),
        "date": str(s.get("session_date") or "")[:10],
        "week": s.get("week_identifier") or "",
        "attendance": None if s.get("is_did_not_meet") else _attendance(s),
        "status": "Did not meet" if s.get("is_did_not_meet") else "Met",
    } for i, s in enumerate(reversed(in_range))]

    return {
        "meta": {
            "title": cell["name"], "event_id": str(event_id), "location": cell["location"],
            "leader_id": cell["leader_id"],
            "parent_title": person_name(people_by_id.get(cell["leader_id"])) or cell["leader_raw"],
            "period": _period_meta(period),
        },
        "summary": {
            "meetings_held": len(held), "did_not_meet": len(in_range) - len(held),
            "total_attendance": total,
            "average_attendance": round(total / len(held), 1) if held else None,
            "latest_attendance": _attendance(held[-1]) if held else None,
        },
        "series": [_trend(in_range, "cell_trend", "Attendance by Week")],
        "tables": [{"key": "sessions", "rows": rows}],
    }