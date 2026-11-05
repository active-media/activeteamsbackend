"""Staff & Intern Statistics — weekly Friday/Sunday service targets.

Classification (``staffStatus``) lives on the MongoDB ``People`` document; the
per-person, per-service, per-week targets live in the Supabase
``service_targets`` table (see ``migrations/001_service_targets_staff_intern.sql``).

Rows created here are tagged with ``service IN ('friday','sunday')`` so they do
not collide with the legacy leader-target rows (which leave ``service`` NULL).

Access is restricted to ``admin`` and ``leader@12`` (plus supreme admin).
"""

import re
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List

import pytz
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Body

from auth.utils import get_current_user, SUPREME_ADMIN_EMAIL
from supabase_client import get_supabase
from supabase_errors import handle_supabase_error
from database import people_collection, events_collection

router = APIRouter(prefix="/staff-intern", tags=["staff-intern"])

SAST = pytz.timezone("Africa/Johannesburg")
SERVICE_WEEKDAYS = {"friday": 4, "sunday": 6}
SERVICE_LABELS = {"friday": "Friday Service", "sunday": "Sunday Service"}
SERVICES = ("friday", "sunday")
TABLE = "service_targets"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def is_authorized(user: dict) -> bool:
    """admin / leader@12 / supreme admin may view and edit targets."""
    role_text = (user.get("role") or "").lower().strip()
    email_text = (user.get("email") or "").lower().strip()
    is_supreme = bool(user.get("is_supreme_admin")) or email_text == SUPREME_ADMIN_EMAIL.lower()
    return bool(
        is_supreme
        or role_text == "admin"
        or "admin" in role_text
        or role_text == "leaderat12"
        or "leaderat12" in role_text
        or "leader at 12" in role_text
        or "leader@12" in role_text
        or role_text == "leader12"
        or "leader12" in role_text
    )


def current_week_identifier(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(SAST)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def parse_week_identifier(week_id: str) -> date:
    """Return the Monday of an ``YYYY-Www`` week identifier."""
    match = re.match(r"^(\d{4})-W(\d{2})$", (week_id or "").strip())
    if not match:
        raise HTTPException(status_code=400, detail="week_identifier must look like YYYY-Www")
    year, week = int(match.group(1)), int(match.group(2))
    try:
        return date.fromisocalendar(year, week, 1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid week_identifier")


def service_date(week_id: str, service: str) -> date:
    return parse_week_identifier(week_id) + timedelta(days=SERVICE_WEEKDAYS[service])


def service_has_passed(week_id: str, service: str, now: Optional[datetime] = None) -> bool:
    """A service counts as passed at the end of its calendar day (SAST)."""
    now = now or datetime.now(SAST)
    end_of_day = SAST.localize(datetime.combine(service_date(week_id, service), time(23, 59, 59)))
    return now > end_of_day


def _require_service(service: str) -> str:
    service = (service or "").strip().lower()
    if service not in SERVICES:
        raise HTTPException(status_code=400, detail="service must be one of: friday, sunday")
    return service


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HTTPException(status_code=400, detail=f"{field} must be a non-negative integer")
    return value


async def _person_identity(person_id: str) -> Dict[str, str]:
    """Name / email / organisation for a person, best effort from Mongo."""
    identity = {"leader_name": "", "email": "", "organization": ""}
    if not ObjectId.is_valid(person_id):
        return identity
    try:
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
    except Exception:
        person = None
    if not person:
        return identity
    name = " ".join(
        part for part in [
            person.get("Name") or person.get("name") or "",
            person.get("Surname") or person.get("surname") or "",
        ] if part
    ).strip()
    identity["leader_name"] = name
    identity["email"] = person.get("Email") or person.get("email") or ""
    identity["organization"] = person.get("Organization") or person.get("organization") or ""
    return identity


async def _match_service_event(service: str, target_date: date) -> Optional[Dict[str, Any]]:
    """Best-effort match of a real Friday/Sunday service event for the date."""
    weekday = target_date.strftime("%A")
    try:
        query = {
            "$and": [
                {
                    "$or": [
                        {"isGlobal": True},
                        {"is_global": True},
                        {"eventTypeName": "Church Service"},
                        {"event_type_name": "Church Service"},
                        {"Event Type": "Church Service"},
                    ]
                },
                {
                    "$or": [
                        {"recurring_day": {"$regex": service, "$options": "i"}},
                        {"recurring_days": {"$regex": service, "$options": "i"}},
                    ]
                },
            ]
        }
        docs = await events_collection.find(query).limit(25).to_list(length=25)
    except Exception:
        return None
    for event in docs:
        name = event.get("Event Name") or event.get("eventName") or ""
        if "service" not in name.lower() and weekday.lower() not in name.lower():
            continue
        return {
            "event_id": str(event.get("_id", "")),
            "event_name": name,
            "uuid": event.get("UUID") or event.get("uuid") or "",
        }
    return None


def _service_info(week_id: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(SAST)
    info: Dict[str, Any] = {}
    for service in SERVICES:
        d = service_date(week_id, service)
        info[service] = {
            "label": SERVICE_LABELS[service],
            "date": d.isoformat(),
            "passed": service_has_passed(week_id, service, now),
            "event": None,
        }
    return info


async def _service_info_with_events(week_id: str, now: Optional[datetime] = None) -> Dict[str, Any]:
    info = _service_info(week_id, now)
    for service in SERVICES:
        event = await _match_service_event(service, service_date(week_id, service))
        if event:
            info[service]["event"] = event
    return info


def week_identifier_for_date(d: date) -> str:
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"


def service_dates_in_range(
    start: date, end: date, service_filter: Optional[str] = None
) -> List[tuple]:
    """Every Friday/Sunday service date in ``[start, end]``, oldest first."""
    services = (service_filter,) if service_filter else SERVICES
    occurrences: List[tuple] = []
    for service in services:
        weekday = SERVICE_WEEKDAYS[service]
        delta = (weekday - start.weekday()) % 7
        current = start + timedelta(days=delta)
        while current <= end:
            occurrences.append((service, current))
            current += timedelta(days=7)
    occurrences.sort(key=lambda item: (item[1], item[0]))
    return occurrences


def _parse_date(value: str, field: str) -> date:
    try:
        return datetime.strptime((value or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD")


def _safe_service_date(week_id: Optional[str], service: Optional[str]) -> Optional[str]:
    try:
        return service_date(week_id, service).isoformat()
    except Exception:
        return None


def _serialize(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "person_id": row.get("person_id"),
        "service": row.get("service"),
        "week_identifier": row.get("week_identifier"),
        "date": _safe_service_date(row.get("week_identifier"), row.get("service")),
        "target": row.get("target_count") or 0,
        "actual": row.get("actual") or 0,
        "met": bool(row.get("met")),
        "closed": bool(row.get("closed")),
        "closed_at": row.get("closed_at"),
        "closed_by": row.get("closed_by"),
        "leader_name": row.get("leader_name") or "",
        "email": row.get("email") or "",
        "organization": row.get("organization") or "",
    }


def _fetch(week_id: str) -> List[dict]:
    try:
        res = (
            get_supabase()
            .table(TABLE)
            .select("*")
            .eq("week_identifier", week_id)
            .in_("service", list(SERVICES))
            .execute()
        )
        return res.data or []
    except Exception as e:
        raise handle_supabase_error(e, not_found_msg="Could not load service targets")


def _fetch_range(start_week: str, end_week: str) -> List[dict]:
    """All targets whose week falls within ``[start_week, end_week]`` (paginated)."""
    rows: List[dict] = []
    page_size = 1000
    offset = 0
    try:
        while True:
            res = (
                get_supabase()
                .table(TABLE)
                .select("*")
                .in_("service", list(SERVICES))
                .gte("week_identifier", start_week)
                .lte("week_identifier", end_week)
                .order("week_identifier", desc=True)
                .order("service")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = res.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows
    except Exception as e:
        raise handle_supabase_error(e, not_found_msg="Could not load service targets")


def _auto_close(rows: List[dict]) -> None:
    """Lazily lock any open target whose service day has already passed."""
    for row in rows:
        service = row.get("service")
        week_id = row.get("week_identifier")
        if service not in SERVICES or row.get("closed"):
            continue
        try:
            passed = service_has_passed(week_id, service)
        except HTTPException:
            continue
        if not passed:
            continue
        target = row.get("target_count") or 0
        actual = row.get("actual") or 0
        patch = {
            "closed": True,
            "met": actual >= target,
            "closed_at": datetime.now(SAST).isoformat(),
            "closed_by": "system:auto",
        }
        try:
            updated = _update(row.get("id"), patch)
            row.update(updated or patch)
        except HTTPException:
            row.update(patch)  # still report the computed state to the client


def _fetch_one(person_id: str, week_id: str, service: str) -> Optional[dict]:
    try:
        res = (
            get_supabase()
            .table(TABLE)
            .select("*")
            .eq("person_id", person_id)
            .eq("week_identifier", week_id)
            .eq("service", service)
            .limit(1)
            .execute()
        )
        return (res.data or [None])[0]
    except Exception as e:
        raise handle_supabase_error(e, not_found_msg="Could not load service target")


def _update(row_id: Any, patch: dict) -> dict:
    try:
        res = get_supabase().table(TABLE).update(patch).eq("id", row_id).execute()
        return (res.data or [{}])[0]
    except Exception as e:
        raise handle_supabase_error(e, not_found_msg="Service target not found")


def _insert(payload: dict) -> dict:
    try:
        res = get_supabase().table(TABLE).insert(payload).execute()
        return (res.data or [{}])[0]
    except Exception as e:
        raise handle_supabase_error(e, bad_request_msg="Could not save service target")


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@router.get("/targets")
async def list_targets(
    week: Optional[str] = Query(None, description="Week identifier (YYYY-Www). Defaults to current week."),
    start_date: Optional[str] = Query(None, description="Range start (YYYY-MM-DD). Requires end_date."),
    end_date: Optional[str] = Query(None, description="Range end (YYYY-MM-DD). Requires start_date."),
    service: Optional[str] = Query(None, description="Optional service filter: friday or sunday."),
    current_user: dict = Depends(get_current_user),
):
    """List Friday/Sunday targets.

    - No params: the current week (with upcoming service dates).
    - ``start_date`` + ``end_date``: every target in the range, for history.

    Targets whose service day has passed are lazily auto-closed (locked) and
    their ``met`` flag is finalised.
    """
    if not is_authorized(current_user):
        raise HTTPException(status_code=403, detail="Not authorized to view staff/intern targets")

    service_filter = _require_service(service) if service else None

    # ── Date-range (history) mode ──────────────────────────────────────────
    if start_date or end_date:
        if not (start_date and end_date):
            raise HTTPException(status_code=400, detail="Both start_date and end_date are required")
        start = _parse_date(start_date, "start_date")
        end = _parse_date(end_date, "end_date")
        if end < start:
            raise HTTPException(status_code=400, detail="end_date must be on or after start_date")

        rows = _fetch_range(week_identifier_for_date(start), week_identifier_for_date(end))
        # Keep only targets whose service date actually falls inside the range
        # (the week filter is intentionally coarser than day granularity).
        start_iso, end_iso = start.isoformat(), end.isoformat()
        rows = [
            row for row in rows
            if (_safe_service_date(row.get("week_identifier"), row.get("service")) or "") and
            start_iso <= (_safe_service_date(row.get("week_identifier"), row.get("service")) or "") <= end_iso
        ]
        if service_filter:
            rows = [row for row in rows if row.get("service") == service_filter]
        _auto_close(rows)
        targets = [_serialize(row) for row in rows]

        rows_by_service_week: Dict[tuple, List[dict]] = {}
        for row in rows:
            key = (row.get("service"), row.get("week_identifier"))
            rows_by_service_week.setdefault(key, []).append(row)

        services_in_range = []
        for service, service_day in service_dates_in_range(start, end, service_filter):
            week_id = week_identifier_for_date(service_day)
            matching = rows_by_service_week.get((service, week_id), [])
            services_in_range.append({
                "service": service,
                "label": SERVICE_LABELS[service],
                "date": service_day.isoformat(),
                "week_identifier": week_id,
                "passed": service_has_passed(week_id, service),
                "has_targets": bool(matching),
                "targets_recorded": len(matching),
                "target": sum((row.get("target_count") or 0) for row in matching),
                "actual": sum((row.get("actual") or 0) for row in matching),
                "met": sum(1 for row in matching if row.get("met")),
            })

        response = {
            "start_date": start_iso,
            "end_date": end_iso,
            "count": len(targets),
            "service_count": len(services_in_range),
            "services": services_in_range,
            "targets": targets,
        }
        if not targets:
            suffix = f" for {SERVICE_LABELS[service_filter]}" if service_filter else ""
            if services_in_range:
                response["message"] = (
                    f"No targets were recorded between {start_iso} and {end_iso}{suffix}; "
                    "the services are still listed."
                )
            else:
                response["message"] = (
                    f"No Friday/Sunday services fall between {start_iso} and {end_iso}{suffix}."
                )
        return response

    # ── Single-week mode ───────────────────────────────────────────────────
    week_id = (week or current_week_identifier()).strip()
    parse_week_identifier(week_id)  # validate

    rows = _fetch(week_id)
    if service_filter:
        rows = [row for row in rows if row.get("service") == service_filter]
    _auto_close(rows)

    return {
        "week_identifier": week_id,
        "services": await _service_info_with_events(week_id),
        "targets": [_serialize(row) for row in rows],
    }


@router.put("/targets/{person_id}")
async def upsert_target(
    person_id: str,
    payload: dict = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """Create or update one person's target for a service in a week.

    Locked (closed) targets cannot be edited; reopen first.
    """
    if not is_authorized(current_user):
        raise HTTPException(status_code=403, detail="Not authorized to update staff/intern targets")

    service = _require_service(payload.get("service"))
    target = _non_negative_int(payload.get("target"), "target")
    actual = _non_negative_int(payload.get("actual", 0), "actual")
    week_id = (payload.get("week_identifier") or current_week_identifier()).strip()
    parse_week_identifier(week_id)

    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID")

    existing = _fetch_one(person_id, week_id, service)
    if existing and existing.get("closed"):
        raise HTTPException(status_code=409, detail="Target is closed; reopen it before editing")

    met = actual >= target
    now_iso = datetime.now(SAST).isoformat()

    if existing:
        patch = {
            "target_count": target,
            "actual": actual,
            "met": met,
            "updated_at": now_iso,
        }
        row = _update(existing["id"], patch)
        row = row or {**existing, **patch}
    else:
        identity = await _person_identity(person_id)
        organization = (
            current_user.get("organization")
            or current_user.get("Organization")
            or identity.get("organization")
            or ""
        )
        insert_payload = {
            "person_id": person_id,
            "service": service,
            "week_identifier": week_id,
            "target_count": target,
            "actual": actual,
            "met": met,
            "closed": False,
            "leader_name": identity.get("leader_name") or "",
            "email": identity.get("email") or "",
            "organization": organization,
        }
        row = _insert(insert_payload)
        row = row or insert_payload

    return {"success": True, "target": _serialize(row), "week_identifier": week_id}


@router.post("/targets/{person_id}/close")
async def close_target(
    person_id: str,
    payload: dict = Body(default={}),
    current_user: dict = Depends(get_current_user),
):
    """Close (lock) a target, finalising its met/not-met state."""
    if not is_authorized(current_user):
        raise HTTPException(status_code=403, detail="Not authorized to close staff/intern targets")

    service = _require_service(payload.get("service"))
    week_id = (payload.get("week_identifier") or current_week_identifier()).strip()
    parse_week_identifier(week_id)

    existing = _fetch_one(person_id, week_id, service)
    if not existing:
        raise HTTPException(status_code=404, detail="Target not found")

    actual = existing.get("actual") or 0
    target = existing.get("target_count") or 0
    patch = {
        "closed": True,
        "met": actual >= target,
        "closed_at": datetime.now(SAST).isoformat(),
        "closed_by": current_user.get("email") or current_user.get("user_id") or "",
    }
    row = _update(existing["id"], patch)
    return {"success": True, "target": _serialize(row or {**existing, **patch})}


@router.post("/targets/{person_id}/reopen")
async def reopen_target(
    person_id: str,
    payload: dict = Body(default={}),
    current_user: dict = Depends(get_current_user),
):
    """Reopen a closed target so it can be edited again."""
    if not is_authorized(current_user):
        raise HTTPException(status_code=403, detail="Not authorized to reopen staff/intern targets")

    service = _require_service(payload.get("service"))
    week_id = (payload.get("week_identifier") or current_week_identifier()).strip()
    parse_week_identifier(week_id)

    existing = _fetch_one(person_id, week_id, service)
    if not existing:
        raise HTTPException(status_code=404, detail="Target not found")

    patch = {"closed": False, "closed_at": None, "closed_by": None}
    row = _update(existing["id"], patch)
    return {"success": True, "target": _serialize(row or {**existing, **patch})}
