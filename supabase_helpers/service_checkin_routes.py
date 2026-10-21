"""
service_checkin_routes.py
=========================
Drop-in replacement for all /service-checkin/* and the related
/events/{event_id}/toggle-status endpoints.

HOW TO WIRE INTO main.py
-------------------------
1. Copy this file into  supabase_helpers/service_checkin_routes.py
2. Add at the top of main.py (with the other supabase imports):

    from supabase_helpers.service_checkin_routes import router as service_checkin_router

3. Add after  app.include_router(supreme_admin_router):

    app.include_router(service_checkin_router)

4. Remove (or comment out) the old @app.get/post/put/delete/patch blocks
   for the following paths in main.py:
     - GET  /service-checkin/real-time-data
     - GET  /service-checkin/validate-removal
     - POST /service-checkin/checkin
     - DELETE /service-checkin/remove
     - PUT  /service-checkin/update
     - POST /service-checkin/create-consolidation
     - DELETE /service-checkin/remove-consolidation
     - POST /service-checkin/migrate-consolidations
     - GET  /service-checkin/debug-consolidations
     - PATCH /events/{event_id}/toggle-status

DATABASE TABLES USED
---------------------
Supabase (PostgreSQL):
  - events                (uuid event_id, text organization, text recurring_day,
                           text status, text event_name / event_leader, …)
  - event_sessions        (uuid session_id, uuid event_id, date session_date,
                           text status, bool is_did_not_meet, …)
  - event_session_attendees (uuid id, uuid session_id, uuid event_id,
                             text mongo_person_id, text full_name, text email,
                             text phone, bool is_checked_in, …)
  - event_new_people      (uuid id, uuid event_id, text mongo_id, text name,
                           text surname, text email, text phone, …)
  - event_consolidations  (uuid id, uuid event_id, text mongo_person_id,
                           text person_name, text person_surname,
                           text person_email, text person_phone,
                           text decision_type, text decision_display_name,
                           text assigned_to, text assigned_to_email,
                           text status, text notes, timestamptz created_at)
  - Tasks                 (text _id, text taskType, text Organization, …)

MongoDB (still used for events & people lookup via existing motor client):
  - events_collection  — read-only lookups for event existence / recurring flag
  - people_collection  — read-only person lookups by ObjectId
  - tasks_collection   — write for consolidation task creation / deletion

The strategy: all *writes* for sessions / attendees / new_people / consolidations
go to Supabase.  MongoDB event & person lookups remain via the existing motor
client so nothing upstream in main.py has to change.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime
from typing import Optional

import pytz
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Query

# ── Shared dependencies already set up in main.py ────────────────────────────
# These imports work because this file is placed inside the same package as
# main.py.  Adjust the import paths if your project structure differs.
from auth.utils import get_current_user
from database import events_collection, people_collection, tasks_collection, db
from supabase_helpers.supabase_connection import supabase  # synchronous Supabase client

logger = logging.getLogger(__name__)

router = APIRouter(tags=["service-checkin"])

SAST = pytz.timezone("Africa/Johannesburg")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _today_sast() -> str:
    """Return today's date in Africa/Johannesburg as an ISO string (YYYY-MM-DD)."""
    return datetime.now(SAST).date().isoformat()


def _parse_event_id(event_id: str) -> tuple[str, Optional[str]]:
    """
    Split a composite event_id like '<mongo_id>_<date>' into its components.
    Returns (base_mongo_id, instance_date_or_None).
    """
    parts = event_id.split("_", 1)
    return parts[0], parts[1] if len(parts) > 1 else None

import uuid
async def _get_event_or_404(base_event_id: str):
    """Fetch a MongoDB event document or raise 404."""
    def is_valid_uuid(val):
        try:
            # Try to parse the string as a UUID
            uuid_obj = uuid.UUID(val)
            # Ensure the string representation matches to avoid lenient parsing quirks
            return str(uuid_obj) == val.lower()
        except (ValueError, AttributeError):
            return False
        
    if not is_valid_uuid(base_event_id):
        raise HTTPException(status_code=400, detail="Invalid event ID")
    
    event = supabase.table('events').select('*').eq('event_id', base_event_id).limit(1).execute()
    if not event.data:
        raise HTTPException(status_code=404, detail="Event not found")
    return event.data[0]


def _get_or_create_session(event_id_uuid: str, session_date: str) -> dict:
    """
    Look up an event_session row for the given event + date.
    Creates one if it does not exist yet.
    Returns the session row as a dict.
    """
    resp = (
        supabase.table("event_sessions")
        .select("*")
        .eq("event_id", event_id_uuid)
        .eq("session_date", session_date)
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]

    # Create a new session row
    new_session = {
        "event_id": event_id_uuid,
        "session_date": session_date,
        "week_identifier": datetime.strptime(session_date, "%Y-%m-%d").strftime("%G-W%V"),
        "status": "incomplete",
        "is_did_not_meet": False,
        "checked_in_count": 0,
        "total_headcounts": 0,
        "decisions_first_time": 0,
        "decisions_recommitment": 0,
        "decisions_total": 0,
        "total_associated": 0,
        "captured_by_leader_at_12": False,
    }
    insert_resp = supabase.table("event_sessions").insert(new_session).execute()
    if not insert_resp.data:
        raise HTTPException(status_code=500, detail="Failed to create event session")
    return insert_resp.data[0]


def _supabase_event_id(event_id: str) -> Optional[str]:
    """
    Resolve a MongoDB ObjectId string to the Supabase events.event_id (UUID).
    Returns None if no matching row is found.
    """
    resp = (
        supabase.table("events")
        .select("event_id")
        .eq("event_id", event_id)
        .limit(1)
        .execute()
    )
    if resp.data:
        return resp.data[0]["event_id"]
    return None


async def _log_activity(user_id: str, action: str, details: str):
    """Best-effort activity log (same helper logic as in main.py)."""
    try:
        await db.activity_logs.insert_one(
            {
                "user_id": user_id,
                "action": action,
                "details": details,
                "timestamp": datetime.utcnow(),
            }
        )
    except Exception as exc:
        logger.warning(f"Activity log failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# GET /service-checkin/real-time-data
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/service-checkin/real-time-data")
async def get_service_checkin_real_time_data(
    event_id: str = Query(...),
    # current_user: dict = Depends(get_current_user),
):
    """
    Return the live attendees, new people, and consolidations for an event /
    event-session from Supabase.

    For recurring events the caller passes  event_id as '<mongo_id>_<date>'.
    For non-recurring events, just the plain mongo_id is sufficient.
    """
    try:
        print(f"Fetching real-time data for event: {event_id}")
        # base_mongo_id, instance_date = _parse_event_id(event_id)
        event = await _get_event_or_404(event_id)

        is_recurring = bool(event.get("recurring_day"))
        if is_recurring:
            instance_date = _today_sast()

        # sb_event_id = _supabase_event_id(base_mongo_id)
        # if not sb_event_id:
        #     # Event not yet in Supabase — return empty but valid response
        #     return {
        #         "success": True,
        #         "event_id": event_id,
        #         "event_name": event.get("event_name") or event.get("eventName", "Unknown Event"),
        #         "present_attendees": [],
        #         "new_people": [],
        #         "consolidations": [],
        #         "present_count": 0,
        #         "new_people_count": 0,
        #         "consolidation_count": 0,
        #         "total_attendance": 0,
        #         "refreshed_at": datetime.utcnow().isoformat(),
        #     }

        # ── Fetch attendees ───────────────────────────────────────────────────
        if is_recurring and instance_date:
            session_resp = (
                supabase.table("event_sessions")
                .select("session_id")
                .eq("event_id", event_id)
                .eq("session_date", instance_date)
                .limit(1)
                .execute()
            )
            if session_resp.data:
                session_id = session_resp.data[0]["session_id"]
                att_resp = (
                    supabase.table("event_session_attendees")
                    .select("*")
                    .eq("session_id", session_id)
                    .execute()
                )
                attendees = att_resp.data or []
            else:
                attendees = []
        else:
            att_resp = (
                supabase.table("event_attendees")
                .select("*")
                .eq("event_id", event_id)
                .execute()
            )
            attendees = att_resp.data or []

        # ── Fetch new people ─────────────────────────────────────────────────
        np_query = supabase.table("event_new_people").select("*").eq("event_id", event_id)
        if is_recurring and instance_date:
            np_query = np_query.eq("session_date", instance_date)
        np_resp = np_query.execute()
        new_people = np_resp.data or []

        # ── Fetch consolidations ─────────────────────────────────────────────
        cons_query = (
            supabase.table("event_consolidations")
            .select("*")
            .eq("event_id", event_id)
        )
        if is_recurring and instance_date:
            cons_query = cons_query.eq("session_date", instance_date)
        cons_resp = cons_query.execute()
        consolidations = cons_resp.data or []

        return {
            "success": True,
            "event_id": event_id,
            "event_name": event.get("event_name") or event.get("eventName", "Unknown Event"),
            "present_attendees": attendees,
            "new_people": new_people,
            "consolidations": consolidations,
            "present_count": len(attendees),
            "new_people_count": len(new_people),
            "consolidation_count": len(consolidations),
            "total_attendance": len(attendees),
            "refreshed_at": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error in real-time-data: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# GET /service-checkin/validate-removal
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/service-checkin/validate-removal")
async def validate_removal(
    event_id: str = Query(..., description="Event ID"),
    consolidation_id: Optional[str] = Query(None, description="Consolidation UUID"),
    person_id: Optional[str] = Query(None, description="Person ID"),
    current_user: dict = Depends(get_current_user),
):
    """Describe what will be affected by a removal before it happens."""
    try:
        if not consolidation_id and not person_id:
            raise HTTPException(
                status_code=400,
                detail="Either consolidation_id or person_id is required",
            )

        warnings: list[str] = []
        affected_tasks: list[dict] = []

        if consolidation_id:
            # Look up associated task in MongoDB Tasks collection
            task = await tasks_collection.find_one({"consolidation_id": consolidation_id})
            if not task and ObjectId.is_valid(consolidation_id):
                task = await tasks_collection.find_one({"_id": ObjectId(consolidation_id)})

            if task:
                person_name = task.get("contacted_person", {}).get("name", "Unknown")
                warnings.append(f"Task for {person_name} will be deleted")
                task["_id"] = str(task["_id"])
                affected_tasks.append(task)

        return {
            "success": True,
            "validation": {
                "warnings": warnings,
                "affected_tasks": affected_tasks,
                "affected_tasks_count": len(affected_tasks),
            },
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Validation error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# POST /service-checkin/checkin
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/service-checkin/checkin")
async def service_checkin_person(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """
    Check in an existing person (type='attendee') or add a new visitor
    (type='new_person') to a service event.

    For attendees the record lands in event_session_attendees (recurring) or
    event_attendees (non-recurring).
    For new visitors the record goes into event_new_people.
    """
    try:
        raw_event_id: str = checkin_data.get("event_id", "")
        person_data: dict = checkin_data.get("person_data", {})
        checkin_type: str = checkin_data.get("type", "attendee")
        session_id:str = checkin_data.get('session_id')


        event = await _get_event_or_404(raw_event_id)
        is_recurring = bool(event.get("recurring_day"))
        now_iso = datetime.utcnow().isoformat()

        sb_event_id = _supabase_event_id(raw_event_id)
        if not sb_event_id:
            raise HTTPException(
                status_code=404,
                detail="Event not found in Supabase. Ensure the event has been synced.",
            )

        instance_date = _today_sast() if is_recurring else None
        att_table = "event_service_attendees" if not event.get('is_ticketed') else 'event_ticketed_attendees'
        # ── Attendee check-in ────────────────────────────────────────────────
        if checkin_type == "attendee":
            full_name = f"{person_data.get('Name', '')} {person_data.get('Surname', '')}".strip()

            
            session = supabase.table("event_sessions").select("*").eq("session_id", session_id).execute().data[0]

            # Prevent duplicate check-in
            dup = (
                supabase.table(att_table)
                .select("id")
                .eq("session_id", session_id)
                .eq("person_id",person_data.get("person_id"))
                .limit(1)
                .execute()
            )
            if dup.data:
                raise HTTPException(
                    status_code=400,
                    detail=f"{full_name} is already checked in",
                )

            insert_resp = (
                supabase.table(att_table)
                .insert(
                    {
                        "session_id": session_id,
                        "event_id": sb_event_id,
                        "full_name": full_name,
                        "email": person_data.get("email", ""),
                        "phone": person_data.get("number", ""),
                        "is_checked_in": True,
                        "check_in_date": now_iso,
                        "person_id": person_data.get("person_id")
                    }
                )
                .execute()
            )
            attendee_record = insert_resp.data[0] if insert_resp.data else {}
            print(f'Attendee record created: {attendee_record}')

            # Update session counter
            supabase.table("event_sessions").update(
                {"checked_in_count": session.get("checked_in_count", 0) + 1}
            ).eq("session_id", session_id).execute()
            
            present_count_resp = (
                supabase.table(att_table)
                .select("id", count="exact")
                .eq("session_id", session_id)
                .execute()
            )
            present_count = present_count_resp.count or 0
                

            return {
                "message": f"{full_name} checked in",
                "type": "attendee",
                "attendee": attendee_record,
                "present_count": present_count,
                "success": True,
            }

        # ── New visitor ──────────────────────────────────────────────────────
        elif checkin_type == "new_person":
            new_person_id = f"new_{secrets.token_urlsafe(8)}"
            new_person_payload = {
                "event_id": sb_event_id,
                "mongo_id": new_person_id,
                "name": person_data.get("name", ""),
                "surname": person_data.get("surname", ""),
                "email": person_data.get("email", ""),
                "phone": person_data.get("phone", ""),
                "gender": person_data.get("gender", ""),
                "invited_by": person_data.get("invitedBy", ""),
                "notes": person_data.get("notes", ""),
                "is_checked_in": True,
                "needs_db_entry": True,
                "added_at": now_iso,
            }
            if is_recurring and instance_date:
                new_person_payload["session_date"] = instance_date

            insert_resp = (
                supabase.table("event_new_people")
                .insert(new_person_payload)
                .execute()
            )
            new_person_record = insert_resp.data[0] if insert_resp.data else new_person_payload

            count_q = (
                supabase.table("event_new_people")
                .select("id", count="exact")
                .eq("event_id", sb_event_id)
            )
            if is_recurring and instance_date:
                count_q = count_q.eq("session_date", instance_date)
            count = (count_q.execute().count) or 0

            return {
                "message": "Visitor added to event",
                "type": "new_person",
                "new_person": new_person_record,
                "new_people_count": count,
                "success": True,
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid type — must be 'attendee' or 'new_person'",
            )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Check-in error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Check-in failed")


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /service-checkin/remove
# ─────────────────────────────────────────────────────────────────────────────

@router.delete("/service-checkin/remove")
async def service_checkin_remove_person(
    remove_data: dict = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """
    Undo a check-in: remove an existing person (type='attendee') or a
    new visitor (type='new_person') from a service event.

    For attendees the record is deleted from event_service_attendees
    (or event_ticketed_attendees for ticketed events) and the session's
    checked_in_count is decremented.
    For new visitors the record is deleted from event_new_people.

    Expected body:
      attendee:   {"event_id", "session_id", "type": "attendee", "person_id"}
      new_person: {"event_id", "type": "new_person", "mongo_id", "session_id"?}
    """
    try:
        raw_event_id: str = remove_data.get("event_id", "")
        checkin_type: str = remove_data.get("type", "attendee")
        session_id: str = remove_data.get("session_id")

        event = await _get_event_or_404(raw_event_id)
        is_recurring = bool(event.get("recurring_day"))

        sb_event_id = _supabase_event_id(raw_event_id)
        if not sb_event_id:
            raise HTTPException(
                status_code=404,
                detail="Event not found in Supabase. Ensure the event has been synced.",
            )

        att_table = (
            "event_service_attendees"
            if not event.get("is_ticketed")
            else "event_ticketed_attendees"
        )

        # ── Remove attendee ──────────────────────────────────────────────────
        if checkin_type == "attendee":
            person_id = remove_data.get("person_id")
            if not session_id or not person_id:
                raise HTTPException(
                    status_code=400,
                    detail="session_id and person_id are required for attendee removal",
                )

            session_resp = (
                supabase.table("event_sessions")
                .select("*")
                .eq("session_id", session_id)
                .limit(1)
                .execute()
            )
            if not session_resp.data:
                raise HTTPException(status_code=404, detail="Session not found")
            session = session_resp.data[0]

            # Make sure the person is actually checked in
            existing = (
                supabase.table(att_table)
                .select("id, full_name")
                .eq("session_id", session_id)
                .eq("person_id", person_id)
                .limit(1)
                .execute()
            )
            if not existing.data:
                raise HTTPException(
                    status_code=404,
                    detail="Person is not checked in to this session",
                )
            full_name = existing.data[0].get("full_name", "") or "Person"

            # Delete the check-in record(s)
            delete_resp = (
                supabase.table(att_table)
                .delete()
                .eq("session_id", session_id)
                .eq("person_id", person_id)
                .execute()
            )
            removed_record = delete_resp.data[0] if delete_resp.data else {}
            print(f"Attendee record removed: {removed_record}")

            # Update session counter (never below zero)
            supabase.table("event_sessions").update(
                {"checked_in_count": max(0, (session.get("checked_in_count") or 0) - 1)}
            ).eq("session_id", session_id).execute()

            present_count_resp = (
                supabase.table(att_table)
                .select("id", count="exact")
                .eq("session_id", session_id)
                .execute()
            )
            present_count = present_count_resp.count or 0

            return {
                "message": f"{full_name} removed from check-in",
                "type": "attendee",
                "removed": removed_record,
                "present_count": present_count,
                "success": True,
            }

        # ── Remove new visitor ───────────────────────────────────────────────
        elif checkin_type == "new_person":
            mongo_id = remove_data.get("mongo_id")
            if not mongo_id:
                raise HTTPException(
                    status_code=400,
                    detail="mongo_id is required for new person removal",
                )

            existing = (
                supabase.table("event_new_people")
                .select("id, name, surname, session_date")
                .eq("event_id", sb_event_id)
                .eq("mongo_id", mongo_id)
                .limit(1)
                .execute()
            )
            if not existing.data:
                raise HTTPException(status_code=404, detail="Visitor not found")
            visitor = existing.data[0]
            session_date = visitor.get("session_date")

            delete_resp = (
                supabase.table("event_new_people")
                .delete()
                .eq("event_id", sb_event_id)
                .eq("mongo_id", mongo_id)
                .execute()
            )
            removed_record = delete_resp.data[0] if delete_resp.data else visitor

            count_q = (
                supabase.table("event_new_people")
                .select("id", count="exact")
                .eq("event_id", sb_event_id)
            )
            if is_recurring and session_date:
                count_q = count_q.eq("session_date", session_date)
            count = (count_q.execute().count) or 0

            return {
                "message": "Visitor removed from event",
                "type": "new_person",
                "removed": removed_record,
                "new_people_count": count,
                "success": True,
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid type — must be 'attendee' or 'new_person'",
            )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Check-in removal error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to remove check-in")

# ─────────────────────────────────────────────────────────────────────────────
# PUT /service-checkin/update
# ─────────────────────────────────────────────────────────────────────────────

@router.put("/service-checkin/update")
async def update_service_checkin_person(
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """Update scalar fields on an attendee, new person, or consolidation row."""
    try:
        raw_event_id: str = update_data.get("event_id", "")
        person_id: str = update_data.get("person_id", "")
        data_type: str = update_data.get("type", "")
        update_fields: dict = update_data.get("update_fields", {})

        if not raw_event_id or not ObjectId.is_valid(raw_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = {"attendees", "new_people", "consolidations"}
        if data_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Type must be one of: {sorted(valid_types)}",
            )

        event = await _get_event_or_404(raw_event_id)
        is_recurring = bool(event.get("recurring_day"))
        instance_date = _today_sast() if is_recurring else None

        sb_event_id = _supabase_event_id(raw_event_id)
        if not sb_event_id:
            raise HTTPException(status_code=404, detail="Event not found in Supabase")

        if not update_fields:
            return {"success": True, "message": "No fields to update"}

        if data_type == "attendees":
            if is_recurring and instance_date:
                session_resp = (
                    supabase.table("event_sessions")
                    .select("session_id")
                    .eq("event_id", sb_event_id)
                    .eq("session_date", instance_date)
                    .limit(1)
                    .execute()
                )
                if not session_resp.data:
                    raise HTTPException(status_code=404, detail="Session not found")
                session_id = session_resp.data[0]["session_id"]
                supabase.table("event_session_attendees").update(update_fields).eq(
                    "session_id", session_id
                ).eq("mongo_person_id", person_id).execute()
            else:
                supabase.table("event_attendees").update(update_fields).eq(
                    "event_id", sb_event_id
                ).eq("mongo_person_id", person_id).execute()

        elif data_type == "new_people":
            supabase.table("event_new_people").update(update_fields).eq(
                "event_id", sb_event_id
            ).eq("mongo_id", person_id).execute()

        elif data_type == "consolidations":
            supabase.table("event_consolidations").update(update_fields).eq(
                "id", person_id
            ).execute()

        return {"success": True, "message": f"Person updated in {data_type} successfully"}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Update error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating person: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# POST /service-checkin/create-consolidation
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/service-checkin/create-consolidation")
async def create_service_checkin_consolidation(
    consolidation_data: dict = Body(...),
    current_user: dict = Depends(get_current_user),
):
    """
    Record a decision (first-time commitment or recommitment) made at a service.
    Writes to:
      - Supabase  event_consolidations
      - MongoDB   Tasks  (consolidation follow-up task)
    """
    try:
        raw_event_id: str = consolidation_data.get("event_id", "")
        person_data: dict = consolidation_data.get("person_data", {})
        decision_type: str = consolidation_data.get("decision_type", "Commitment")
        assigned_to: str = consolidation_data.get("assigned_to", "")
        assigned_to_email: str = consolidation_data.get("assigned_to_email", "")
        notes: str = consolidation_data.get("notes", "")

        if not raw_event_id:
            raise HTTPException(status_code=400, detail="Event ID is required")

        base_mongo_id, instance_date = _parse_event_id(raw_event_id)
        event = await _get_event_or_404(base_mongo_id)
        is_recurring = bool(event.get("recurring_day"))
        if is_recurring and not instance_date:
            instance_date = _today_sast()

        sb_event_id = _supabase_event_id(base_mongo_id)
        if not sb_event_id:
            raise HTTPException(
                status_code=404,
                detail="Event not found in Supabase. Sync the event first.",
            )

        person_name: str = person_data.get("name", "")
        person_surname: str = person_data.get("surname", "")
        person_email: str = person_data.get("email", "")
        person_phone: str = person_data.get("phone", "") or person_data.get("number", "")
        mongo_person_id: str = person_data.get("id", "")

        now_iso = datetime.utcnow().isoformat()
        org = current_user.get("Organization") or current_user.get("organization", "")

        # ── Create follow-up task in MongoDB ──────────────────────────────────
        task_payload = {
            "taskType": "consolidation",
            "contacted_person": {
                "name": f"{person_name} {person_surname}".strip(),
                "phone": person_phone,
                "email": person_email,
            },
            "followup_date": now_iso,
            "status": "Open",
            "type": "consolidation",
            "assignedfor": assigned_to_email or current_user.get("email", ""),
            "assigned_to_email": assigned_to_email,
            "is_consolidation_task": True,
            "leader_assigned": assigned_to,
            "leader_name": assigned_to,
            "decision_display_name": decision_type,
            "source_display": "Service",
            "consolidation_source": "service_consolidation",
            "person_name": person_name,
            "person_surname": person_surname,
            "person_email": person_email,
            "person_phone": person_phone,
            "person_id": mongo_person_id,
            "Organization": org,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        task_result = await tasks_collection.insert_one(task_payload)
        task_id = str(task_result.inserted_id)

        # ── Write consolidation row to Supabase ──────────────────────────────
        cons_payload: dict = {
            "event_id": sb_event_id,
            "mongo_person_id": mongo_person_id or None,
            "person_name": person_name,
            "person_surname": person_surname,
            "person_email": person_email,
            "person_phone": person_phone,
            "decision_type": decision_type.lower().replace(" ", "_"),
            "decision_display_name": decision_type,
            "assigned_to": assigned_to,
            "assigned_to_email": assigned_to_email,
            "status": "active",
            "notes": notes,
            "mongo_task_id": task_id,
        }
        if is_recurring and instance_date:
            cons_payload["session_date"] = instance_date

        insert_resp = (
            supabase.table("event_consolidations").insert(cons_payload).execute()
        )
        if not insert_resp.data:
            # Rollback task
            await tasks_collection.delete_one({"_id": task_result.inserted_id})
            raise HTTPException(
                status_code=500, detail="Failed to save consolidation to Supabase"
            )
        cons_record = insert_resp.data[0]

        # ── Update session counters if recurring ─────────────────────────────
        if is_recurring and instance_date:
            session_resp = (
                supabase.table("event_sessions")
                .select("session_id, decisions_first_time, decisions_recommitment, decisions_total")
                .eq("event_id", sb_event_id)
                .eq("session_date", instance_date)
                .limit(1)
                .execute()
            )
            if session_resp.data:
                sess = session_resp.data[0]
                dt_lower = decision_type.lower()
                first_delta = 1 if "first" in dt_lower else 0
                recommit_delta = 1 if "recommit" in dt_lower else 0
                supabase.table("event_sessions").update(
                    {
                        "decisions_first_time": sess.get("decisions_first_time", 0) + first_delta,
                        "decisions_recommitment": sess.get("decisions_recommitment", 0) + recommit_delta,
                        "decisions_total": sess.get("decisions_total", 0) + 1,
                    }
                ).eq("session_id", sess["session_id"]).execute()

        # ── Count updated consolidations ─────────────────────────────────────
        cons_count_q = (
            supabase.table("event_consolidations")
            .select("id", count="exact")
            .eq("event_id", sb_event_id)
        )
        if is_recurring and instance_date:
            cons_count_q = cons_count_q.eq("session_date", instance_date)
        cons_count = cons_count_q.execute().count or 0

        await _log_activity(
            user_id=current_user.get("email", ""),
            action="CONSOLIDATION_CREATED",
            details=(
                f"Created consolidation for '{person_name} {person_surname}' "
                f"in event '{event.get('event_name') or event.get('eventName', 'Unknown')}'"
            ),
        )

        return {
            "success": True,
            "message": "Consolidation created successfully",
            "consolidation": cons_record,
            "task_id": task_id,
            "event_id": raw_event_id,
            "event_name": event.get("event_name") or event.get("eventName", "Unknown Event"),
            "updated_statistics": {"consolidations_count": cons_count},
            "timestamp": now_iso,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Create consolidation error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /service-checkin/remove-consolidation
# ─────────────────────────────────────────────────────────────────────────────

@router.delete("/service-checkin/remove-consolidation")
async def remove_consolidation(
    event_id: str = Query(...),
    consolidation_id: str = Query(...),
    keep_person_in_attendees: bool = Query(True),
    current_user: dict = Depends(get_current_user),
):
    """
    Remove a consolidation row from Supabase and delete the linked MongoDB task.
    """
    try:
        base_mongo_id, instance_date = _parse_event_id(event_id)
        event = await _get_event_or_404(base_mongo_id)
        is_recurring = bool(event.get("recurring_day"))
        if is_recurring and not instance_date:
            instance_date = _today_sast()

        sb_event_id = _supabase_event_id(base_mongo_id)
        if not sb_event_id:
            raise HTTPException(status_code=404, detail="Event not found in Supabase")

        # ── Fetch the consolidation row to get the linked task id ─────────────
        cons_resp = (
            supabase.table("event_consolidations")
            .select("*")
            .eq("id", consolidation_id)
            .limit(1)
            .execute()
        )
        if not cons_resp.data:
            raise HTTPException(status_code=404, detail="Consolidation not found")

        cons_row = cons_resp.data[0]
        person_name = cons_row.get("person_name", "")
        person_surname = cons_row.get("person_surname", "")
        mongo_task_id = cons_row.get("mongo_task_id")

        # ── Delete from Supabase ─────────────────────────────────────────────
        supabase.table("event_consolidations").delete().eq(
            "id", consolidation_id
        ).execute()

        # Update session decision counters if recurring
        if is_recurring and instance_date:
            session_resp = (
                supabase.table("event_sessions")
                .select("session_id, decisions_first_time, decisions_recommitment, decisions_total")
                .eq("event_id", sb_event_id)
                .eq("session_date", instance_date)
                .limit(1)
                .execute()
            )
            if session_resp.data:
                sess = session_resp.data[0]
                dt_lower = cons_row.get("decision_type", "").lower()
                first_delta = -1 if "first" in dt_lower else 0
                recommit_delta = -1 if "recommit" in dt_lower else 0
                supabase.table("event_sessions").update(
                    {
                        "decisions_first_time": max(0, sess.get("decisions_first_time", 0) + first_delta),
                        "decisions_recommitment": max(0, sess.get("decisions_recommitment", 0) + recommit_delta),
                        "decisions_total": max(0, sess.get("decisions_total", 0) - 1),
                    }
                ).eq("session_id", sess["session_id"]).execute()

        # ── Delete linked MongoDB task ────────────────────────────────────────
        task_deleted = False
        deleted_task_ids: list[str] = []

        if mongo_task_id and ObjectId.is_valid(mongo_task_id):
            del_result = await tasks_collection.delete_one(
                {"_id": ObjectId(mongo_task_id)}
            )
            if del_result.deleted_count:
                task_deleted = True
                deleted_task_ids.append(mongo_task_id)
        else:
            # Fallback: find by consolidation_id field
            task = await tasks_collection.find_one(
                {"consolidation_id": consolidation_id}
            )
            if task:
                await tasks_collection.delete_one({"_id": task["_id"]})
                task_deleted = True
                deleted_task_ids.append(str(task["_id"]))

        # ── Updated counts ────────────────────────────────────────────────────
        cons_count_q = (
            supabase.table("event_consolidations")
            .select("id", count="exact")
            .eq("event_id", sb_event_id)
        )
        if is_recurring and instance_date:
            cons_count_q = cons_count_q.eq("session_date", instance_date)

        np_count_q = (
            supabase.table("event_new_people")
            .select("id", count="exact")
            .eq("event_id", sb_event_id)
        )
        if is_recurring and instance_date:
            np_count_q = np_count_q.eq("session_date", instance_date)

        if is_recurring and instance_date:
            session_resp = (
                supabase.table("event_sessions")
                .select("session_id, checked_in_count")
                .eq("event_id", sb_event_id)
                .eq("session_date", instance_date)
                .limit(1)
                .execute()
            )
            present_count = (
                session_resp.data[0].get("checked_in_count", 0)
                if session_resp.data
                else 0
            )
        else:
            present_count = (
                supabase.table("event_attendees")
                .select("attendee_id", count="exact")
                .eq("event_id", sb_event_id)
                .execute()
                .count or 0
            )

        await _log_activity(
            user_id=current_user.get("email", ""),
            action="CONSOLIDATION_REMOVED",
            details=f"Removed consolidation for {person_name} {person_surname}",
        )

        return {
            "success": True,
            "message": "Consolidation removed successfully",
            "task_deletion": {
                "deleted": task_deleted,
                "count": len(deleted_task_ids),
            },
            "updated_statistics": {
                "present_count": present_count,
                "new_people_count": np_count_q.execute().count or 0,
                "consolidation_count": cons_count_q.execute().count or 0,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Remove consolidation error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ─────────────────────────────────────────────────────────────────────────────
# POST /service-checkin/migrate-consolidations  (admin-only utility)
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/service-checkin/migrate-consolidations")
async def migrate_consolidations(
    current_user: dict = Depends(get_current_user),
):
    """
    One-time admin utility: back-fill mongo_task_id onto Supabase
    event_consolidations rows that were created before this field existed.
    Matches by person email, then by person name.
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        cons_resp = (
            supabase.table("event_consolidations")
            .select("id, person_email, person_name, person_surname, assigned_to, mongo_task_id")
            .is_("mongo_task_id", "null")
            .execute()
        )
        rows = cons_resp.data or []
        updates_made = 0

        for row in rows:
            person_email = row.get("person_email", "")
            person_name = row.get("person_name", "")
            person_surname = row.get("person_surname", "")
            assigned_to = row.get("assigned_to", "")

            task_query: dict = {"is_consolidation_task": True, "type": "consolidation"}
            if person_email:
                task_query["contacted_person.email"] = person_email
            elif person_name:
                task_query["person_name"] = {"$regex": person_name, "$options": "i"}
            if assigned_to:
                task_query["$or"] = [
                    {"leader_name": assigned_to},
                    {"leader_assigned": assigned_to},
                ]

            task = await tasks_collection.find_one(task_query)
            if task:
                task_id = str(task["_id"])
                supabase.table("event_consolidations").update(
                    {"mongo_task_id": task_id}
                ).eq("id", row["id"]).execute()
                updates_made += 1
                logger.info(f"Back-filled mongo_task_id={task_id} for consolidation {row['id']}")

        return {
            "success": True,
            "message": f"Migration complete. Updated {updates_made} consolidations.",
            "updates_made": updates_made,
            "total_checked": len(rows),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Migration error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Migration failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# GET /service-checkin/debug-consolidations  (admin-only utility)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/service-checkin/debug-consolidations")
async def debug_consolidations(
    event_id: Optional[str] = Query(None, description="Event UUID or mongo_id (optional)"),
    current_user: dict = Depends(get_current_user),
):
    """Inspect consolidation rows in Supabase and their linked MongoDB tasks."""
    try:
        query = supabase.table("event_consolidations").select("*")
        if event_id:
            # Accept either a Supabase UUID or a mongo_id via the events table
            if ObjectId.is_valid(event_id):
                sb_id = _supabase_event_id(event_id)
                if sb_id:
                    query = query.eq("event_id", sb_id)
            else:
                query = query.eq("event_id", event_id)

        cons_rows = query.execute().data or []
        debug_results: list[dict] = []

        for row in cons_rows:
            task_id = row.get("mongo_task_id")
            task_exists = False
            task_status = None
            task_name = None

            if task_id and ObjectId.is_valid(task_id):
                task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
                if task:
                    task_exists = True
                    task_status = task.get("status")
                    task_name = task.get("name")

            debug_results.append(
                {
                    "id": row.get("id"),
                    "event_id": row.get("event_id"),
                    "person_name": row.get("person_name", "Unknown"),
                    "person_email": row.get("person_email", ""),
                    "decision_type": row.get("decision_type", ""),
                    "assigned_to": row.get("assigned_to", ""),
                    "has_mongo_task_id": bool(task_id),
                    "mongo_task_id": task_id,
                    "task_exists": task_exists,
                    "task_status": task_status,
                    "task_name": task_name,
                    "created_at": row.get("created_at"),
                }
            )

        return {
            "success": True,
            "total": len(debug_results),
            "debug_results": debug_results,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Debug error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Debug failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /events/{event_id}/toggle-status
# ─────────────────────────────────────────────────────────────────────────────

@router.patch("/events/{event_id}/toggle-status")
async def toggle_event_status(
    event_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Toggle an event (or a specific recurring instance) between
    'complete' / 'closed'  ↔  'incomplete'.

    Writes the new status to Supabase event_sessions (recurring) or
    events (non-recurring) and mirrors it back to MongoDB for backward
    compatibility with any remaining Mongo-backed reads.
    """
    try:
        base_mongo_id, instance_date = _parse_event_id(event_id)
        event = await _get_event_or_404(base_mongo_id)
        is_recurring = bool(event.get("recurring_day"))

        sb_event_id = _supabase_event_id(base_mongo_id)
        now_iso = datetime.utcnow().isoformat()

        # ── Read current status ───────────────────────────────────────────────
        if is_recurring and instance_date:
            session_resp = (
                supabase.table("event_sessions")
                .select("session_id, status")
                .eq("event_id", sb_event_id)
                .eq("session_date", instance_date)
                .limit(1)
                .execute()
            )
            if session_resp.data:
                current_status = session_resp.data[0].get("status", "").lower()
                session_id = session_resp.data[0]["session_id"]
            else:
                current_status = "incomplete"
                session_id = None
        else:
            events_resp = (
                supabase.table("events")
                .select("status")
                .eq("event_id", sb_event_id)
                .limit(1)
                .execute()
            )
            current_status = (
                events_resp.data[0].get("status", "").lower()
                if events_resp.data
                else "incomplete"
            )

        # ── Determine new status ─────────────────────────────────────────────
        if current_status in ("complete", "closed"):
            new_status = "incomplete"
            action_msg = "reopened"
            log_action = "EVENT_REOPENED"
            extra_fields = {"reopened_by": current_user.get("email", ""), "reopened_at": now_iso}
        else:
            new_status = "complete"
            action_msg = "closed"
            log_action = "EVENT_CLOSED"
            extra_fields = {"closed_by": current_user.get("email", ""), "closed_at": now_iso}

        # ── Write to Supabase ─────────────────────────────────────────────────
        if is_recurring and instance_date and session_id:
            supabase.table("event_sessions").update(
                {"status": new_status, **extra_fields}
            ).eq("session_id", session_id).execute()
        elif sb_event_id:
            supabase.table("events").update(
                {"status": new_status, **extra_fields}
            ).eq("event_id", sb_event_id).execute()

        # ── Mirror to MongoDB (backwards-compat) ──────────────────────────────
        mongo_update: dict = {"updated_at": now_iso, "status": new_status, **extra_fields}
        if instance_date:
            mongo_update[f"attendance.{instance_date}.status"] = new_status
        await events_collection.update_one(
            {"_id": ObjectId(base_mongo_id)}, {"$set": mongo_update}
        )

        await _log_activity(
            user_id=current_user.get("email", ""),
            action=log_action,
            details=(
                f"{action_msg.capitalize()} event: "
                f"{event.get('event_name') or event.get('eventName', 'Unknown')} "
                f"(mongo_id: {base_mongo_id}, date: {instance_date})"
            ),
        )

        event_name = event.get("event_name") or event.get("eventName", "Unknown")
        return {
            "success": True,
            "already_closed": False,
            "message": f"Event '{event_name}' {action_msg} successfully",
            "event_id": base_mongo_id,
            "event_name": event_name,
            "previous_status": current_status,
            "new_status": new_status,
            "action": action_msg,
            "actioned_by": current_user.get("email", ""),
            "actioned_at": extra_fields.get("closed_at") or extra_fields.get("reopened_at"),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Toggle status error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error toggling event status: {exc}")

# ─────────────────────────────────────────────────────────────────────────────
# GET /service-checkin/events and /service-checkin/eventsdata
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_event_row(row: dict) -> dict:
    """Return service-checkin-friendly event data from a Supabase event/session row."""
    if not row:
        return {}

    session_date = row.get("session_date") or row.get("date")
    print('ROW',row)
    return {
        "_id": row.get("_id") or row.get("session_id") or row.get("event_id") or row.get("id"),
        "session_id": row.get("session_id"),
        "event_id": row.get("event_id"),
        "UUID": row.get("event_id") or row.get("UUID") or row.get("id"),
        "id": row.get("event_id") or row.get("session_id") or row.get("_id") or row.get("id"),
        "eventName": row.get("event_name") or row.get("eventName") or "Unnamed Event",
        "eventType": row.get("event_type_name") or row.get("eventType") or row.get("event_type") or "Global Events",
        "eventLeaderName": row.get("event_leader") or row.get("eventLeaderName") or "",
        "eventLeaderEmail": row.get("event_leader_email") or row.get("eventLeaderEmail") or "",
        "leader1": row.get("leader_at_1") or row.get("leader1") or "",
        "leader12": row.get("leader_at_12") or row.get("leader12") or "",
        "leader12email": row.get("leader_at_12_email") or row.get("leader12email") or "",
        "day": row.get("recurring_day") or row.get("day") or "",
        "date": session_date,
        "rawDate": session_date or "",
        "display_date": "-".join(reversed(str(session_date).split("-"))) if session_date else "",
        "location": row.get("location") or row.get("Location") or "",
        "isGlobal": row.get("is_global", False) if "is_global" in row else row.get("isGlobal", False),
        "isTicketed": row.get("is_ticketed", False) if "is_ticketed" in row else row.get("isTicketed", False),
        "status": row.get("status") or "incomplete",
        "Status": (row.get("status") or "").capitalize() if row.get("status") else "",
        "did_not_meet": row.get("is_did_not_meet", False),
        "_is_overdue": row.get("_is_overdue", True),
        "is_recurring": row.get("is_recurring"),
        "original_event_id": row.get("mongo_id") or row.get("original_event_id"),
        "is_active": row.get("is_active"),
        "time": row.get("time") or "07:20",
        "description": row.get("description") or "",
    }


async def _fetch_service_checkin_event_rows(
    # current_user: dict,
    status: Optional[str],
    event_type: Optional[str],
    search: Optional[str],
    personal: Optional[bool],
    start_date: Optional[str],
    end_date: Optional[str],
    is_global: Optional[bool],
    page: int,
    limit: int,
):
    """Fetch the same core event list as /events/eventsdata and enrich it with service-checkin data."""
    event_type_filter = event_type
    if event_type_filter == "global_events" or is_global is True:
        event_type_filter = "global_events"

    if not is_global:
        fetched_other_events = supabase.rpc("get_general_events", {"p_event_type": event_type_filter}).execute().data or []
    else:
        fetched_other_events = supabase.rpc("get_global_events").execute().data or []

   

    if not fetched_other_events:
        return {
            "events": [],
            "total_events": 0,
            "total_pages": 1,
            "current_page": page,
            "page_size": limit,
            "has_more": False,
        }

    fetched_event_ids = [event.get("event_id") for event in fetched_other_events if event.get("event_id")]
    if not fetched_event_ids:
        return {
            "events": [],
            "total_events": 0,
            "total_pages": 1,
            "current_page": page,
            "page_size": limit,
            "has_more": False,
        }

    sessions_query = (
        supabase.table("event_sessions")
        .select("*")
        .in_("event_id", [str(i) for i in fetched_event_ids])
    )
    if status:
        sessions_query = sessions_query.eq("status", status)
    if start_date:
        sessions_query = sessions_query.gte("session_date", start_date)
    if end_date:
        sessions_query = sessions_query.lte("session_date", end_date)

    sessions = sessions_query.execute().data or []

    session_map: dict[str, list[dict]] = {}
    for session in sessions:
        event_id = str(session.get("event_id") or "")
        if event_id:
            session_map.setdefault(event_id, []).append(session)

    all_events = []
    print(fetched_other_events)
    for event in fetched_other_events:
        event_id = event.get("event_id")
        matching_sessions = session_map.get(str(event_id), [])
        if not matching_sessions:
            continue

        for session in matching_sessions:
            combined = {**event, **session}
            combined['event_name'] = event.get('event_name') or event.get('eventName')
            base_event = _normalise_event_row(combined)
            session_date = session.get("session_date")
            session_id = session.get("session_id")

            if session_id:
                attendee_resp = (
                    supabase.table("event_session_attendees")
                    .select("*")
                    .eq("session_id", session_id)
                    .execute()
                )
                attendees = attendee_resp.data or []
            else:
                attendee_resp = (
                    supabase.table("event_attendees")
                    .select("*")
                    .eq("event_id", event_id)
                    .execute()
                )
                attendees = attendee_resp.data or []

            new_people_resp = (
                supabase.table("event_new_people")
                .select("*")
                .eq("event_id", event_id)
                .gte("added_at", datetime.today())
            )
            # if session_date:
            #     new_people_resp = new_people_resp.eq("session_date", session_date)
            new_people = new_people_resp.execute().data or []

            cons_resp = (
                supabase.table("event_consolidations")
                .select("*")
                .eq("event_id", event_id)
                .gte("created_at", datetime.today())
            )
            # if session_date:
            #     cons_resp = cons_resp.eq("session_date", session_date)
            consolidations = cons_resp.execute().data or []

            event_payload = {
                **base_event,
                "attendees": attendees,
                "new_people": new_people,
                "consolidations": consolidations,
                "attendanceData": attendees,
                "newPeopleData": new_people,
                "consolidatedData": consolidations,
                "attendance": len(attendees),
                "newPeople": len(new_people),
                "consolidated": len(consolidations),
                "total_attendance": len(attendees),
                "present_attendees": attendees,
                "present_count": len(attendees),
                "new_people_count": len(new_people),
                "consolidation_count": len(consolidations),
            }

            if search:
                search_text = search.lower()
                haystack = " ".join([
                    str(event_payload.get("eventName") or ""),
                    str(event_payload.get("eventType") or ""),
                    str(event_payload.get("location") or ""),
                    str(event_payload.get("status") or ""),
                    str(event_payload.get("leader1") or ""),
                    str(event_payload.get("leader12") or ""),
                ]).lower()
                if search_text not in haystack:
                    continue

            if personal is not None and bool(event_payload.get("isGlobal")) != (not personal):
                continue

            all_events.append(event_payload)

    all_events.sort(key=lambda item: (str(item.get("date") or ""), str(item.get("eventName") or "")), reverse=True)

    total_events = len(all_events)
    total_pages = max(1, (total_events + limit - 1) // limit) if total_events else 1
    start_index = (page - 1) * limit
    end_index = start_index + limit
    paginated_events = all_events[start_index:end_index]

    return {
        "events": paginated_events,
        "total_events": total_events,
        "total_pages": total_pages,
        "current_page": page,
        "page_size": limit,
        "has_more": page < total_pages,
        }


@router.get("/service-checkin/events-history")
# @router.get("/service-checkin/eventsdata")
async def get_service_checkin_events(
    session_id:str,
    start_date: Optional[str] = Query("2024-10-10"),
    end_date: Optional[str] = Query(None),
):
    """Fetch paginated service-checkin events with attendance and session metadata attached."""
    try:
        sessions_query = (
                supabase.table("event_sessions")
                .select("*")
                .eq("session_id", session_id)
            )

        if start_date:
            sessions_query = sessions_query.gte("session_date", start_date)
        if end_date:
            sessions_query = sessions_query.lte("session_date", end_date)
    
        session = sessions_query.execute().data[0] or []
        if session:
            event = supabase.table('events').select('*').eq('event_id', session.get("event_id")).single().execute().data or {}
        else:
            raise HTTPException(status_code=404, detail="Session not found")
        event_id = event.get('event_id')
    
     
        combined = {**event, **session}
        combined['event_name'] = event.get('event_name') or event.get('eventName')
        base_event = _normalise_event_row(combined)
        session_date = session.get("session_date")
        session_id = session.get("session_id")
        att_table = 'event_service_attendees' if not event.get('is_ticketed') else 'event_ticketed_attendees'

        if session_id:
            attendee_resp = (
                supabase.table(att_table)
                .select("*")
                .eq("session_id", session_id)
                .execute()
            )
            attendees = attendee_resp.data or []

        new_people_resp = (
            supabase.table("event_new_people")
            .select("*")
            .eq("event_id", event_id)
            .gte("added_at", datetime.today())
        )
        # if session_date:
        #     new_people_resp = new_people_resp.eq("session_date", session_date)
        new_people = new_people_resp.execute().data or []

        cons_resp = (
            supabase.table("event_consolidations")
            .select("*")
            .eq("event_id", event_id)
            .gte("created_at", datetime.today())
        )
        # if session_date:
        #     cons_resp = cons_resp.eq("session_date", session_date)
        consolidations = cons_resp.execute().data or []

        event_payload = {
            **base_event,
            "attendees": attendees,
            "new_people": new_people,
            "consolidations": consolidations,
            "attendanceData": attendees,
            "newPeopleData": new_people,
            "consolidatedData": consolidations,
            "attendance": len(attendees),
            "newPeople": len(new_people),
            "consolidated": len(consolidations),
            "total_attendance": len(attendees),
            "present_attendees": attendees,
            "present_count": len(attendees),
            "new_people_count": len(new_people),
            "consolidation_count": len(consolidations),
        }
    
    
        return event_payload
        
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error in /service-checkin/events: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(exc)}")