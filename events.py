import json

from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends
from datetime import datetime, timedelta, date, timezone
from typing import Optional
from bson import ObjectId
import uuid
from auth.models import EventCreate, EventTypeCreate, LeaderStatusResponse,AttendanceSubmission
import pytz
import re
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from time import sleep
from urllib.parse import unquote
from database import db, events_collection, people_collection, users_collection, tasks_collection ,tasktypes_collection,consolidations_collection, organizations_collection, org_config_collection
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token , task_type_serializer, get_current_user 
from supabase_helpers.supabase_connection import supabase
import os


router = APIRouter()
DAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

ORG_ID_MAP = {
    "active-church": "69c63afc4c3e2fdfc5a4840d",
    "active church": "69c63afc4c3e2fdfc5a4840d",
}

# Events Section  ----------------------------------------------
SAST_TZ = pytz.timezone('Africa/Johannesburg')
# South African timezone
def normalize_time(time_value: str) -> str:
    """
    Normalize time to HH:MM.
    NO timezone conversion.
    """
    if not time_value or not isinstance(time_value, str):
        return time_value

    try:
        # Defensive: ISO string sent accidentally
        if "T" in time_value:
            time_value = time_value.split("T")[1][:5]

        parts = time_value.split(":")
        if len(parts) >= 2:
            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    except Exception:
        pass

    return time_value



@router.post("/events")
async def create_event(event: EventCreate, current_user: dict = Depends(get_current_user)):
    """
    create an event of any event type
    """
    try:
        print(f"Received event creation request: {event.dict()} from user: {current_user.get('email')}")
        event_data = event.dict()
        event_data["_id"] = ObjectId()


        event_type_name = event_data.get("eventTypeName")
        if not event_type_name:
            raise HTTPException(status_code=400, detail="eventTypeName is required")
        org_id = current_user.get("org_id", "active-teams")
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")
        
        # Make sure organization is properly set (only uppercase)
        if not organization:
            organization = "Active Church"  # Default organization
        
        # Set ONLY the uppercase Organization field
        event_data["org_id"] = org_id
        event_data["Organization"] = organization  # Uppercase O only

        event_type = supabase.table("event_types").select("*").eq("name", event_type_name).eq("org_id", org_id).execute().data
        print("event_type query result:", event_type,org_id,event_type_name)
        # Check if it's a CELLS type
        if event_type_name.upper() in ["CELLS", "ALL CELLS"]:
            print("Creating CELLS event with built-in type")
            event_data["eventTypeId"] = event_type[0]["event_type_id"] 
            event_data["eventTypeName"] = "Cells"
            event_data["hasPersonSteps"] = True
            event_data["isGlobal"] = False
            event_data["status"] = "incomplete"
        else:
            # Try to find the event type 
            
                        
            event_type = event_type[0] if event_type and len(event_type) > 0 else None
            # If found, check if it's global
            if event_type:
                is_global = event_type.get("isGlobal", False)
                print(f"Found event type: {event_type_name} (global={is_global})")
                event_data["eventTypeId"] = event_type.get("event_type_id")
                event_data["eventTypeName"] = event_type.get("name")
                event_data["isGlobal"] = event_type.get("isGlobal", False)
                event_data["hasPersonSteps"] = event_type.get("hasPersonSteps", False)
                event_data["isTicketed"] = event_type.get("isTicketed", False)
                event_data["status"] = "open"
            else:
                # Event type not found, use default
                print(f"Event type '{event_type_name}' not found, using default")
                event_data["eventTypeId"] = None
                event_data["eventTypeName"] = event_type_name
                event_data["isGlobal"] = False
                event_data["hasPersonSteps"] = False
                event_data["isTicketed"] = False
                event_data["status"] = "open"

        
        print(f"Using day value from frontend: {event_data.get('day')}")

        eventTime = event_data.get("time") or event_data.get("Time")
        if eventTime:
            raw_time = eventTime
            print(f"Raw time received from frontend: {raw_time}")
            clean_time = normalize_time(raw_time)
            event_data["time"] = clean_time
            event_data["Time"] = clean_time
            print(f"Time stored as: {clean_time}")

       
        event_data.pop("eventType", None)

        if not event_data.get("eventLeaderEmail"):
            raise HTTPException(status_code=400, detail="eventLeaderEmail is required")

        for key in ["userEmail", "email"]:
            event_data.pop(key, None)

        recurring_days = event_data.get("recurring_day", [])
        if isinstance(recurring_days, str):
            recurring_days = [recurring_days]
        recurring_days = [d.strip() for d in recurring_days if d and d.strip()]
        event_data["recurring_day"] = recurring_days

        if not recurring_days:
            event_data["day"] = event_data.get("day", "One-time")
        else:
            event_data["day"] = recurring_days[0]

        event_data.setdefault("eventLeaderName", event_data.get("eventLeader", ""))
        if event_data.get("hasPersonSteps"):
            event_data.setdefault("leader1", "")
            event_data.setdefault("leader12", "")
            event_data.setdefault("persistent_attendees", [])

        if event_data.get("isTicketed") and event_data.get("priceTiers"):
            event_data["priceTiers"] = [
                {k: (float(v) if k == "price" else v) for k, v in tier.items()}
                for tier in event_data["priceTiers"]
            ]
        else:
            event_data["priceTiers"] = []

        if event_data.get("isGlobal"):
            for field in ["leader1", "leader12"]:
                if field in event_data and not event_data[field]:
                    del event_data[field]

        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data["attendees"])

        reference_date = event_data.get("date")
        if isinstance(reference_date, str):
            try:
                reference_dt = datetime.strptime(reference_date, "%Y-%m-%d")
                reference_date = reference_dt.date()
            except Exception:
                try:
                    reference_date = datetime.fromisoformat(reference_date.replace("Z", "00:00")).date()
                except Exception:
                    reference_date = datetime.now().date()
        elif isinstance(reference_date, datetime):
            reference_date = reference_date.date()
        else:
            reference_date = datetime.now().date()

        if recurring_days:
            first_day_lower = recurring_days[0].lower().strip()
            if first_day_lower in DAY_INDEX:
                target_weekday = DAY_INDEX[first_day_lower]
                days_until = (target_weekday - reference_date.weekday()) % 7
                first_event_date = reference_date + timedelta(days=days_until)
            else:
                first_event_date = reference_date

            # event_data["date"] = first_event_date.isoformat()
            event_data["day"] = recurring_days[0].capitalize()
            event_data["recurring_day"] = recurring_days
            event_data["attendance"] = {}

            try:
                event_data["Date Of Event"] = datetime.combine(first_event_date, datetime.min.time()).isoformat() + "Z"
            except Exception:
                event_data["Date Of Event"] = first_event_date.isoformat()

            print(f"[RECURRING CREATE] Single doc -> day: {event_data['day']}, date: {event_data['date']}, eventName: {event_data.get('eventName') or event_data.get('Event Name')}, Organization: {event_data['Organization']}")
            print("Final event object", event_data)

            final_event_date = event_data.get("date")
            final_created_date = event_data.get("created_at")
            final_data = {
                "event_name": event_data.get("eventName","No Event Name"),
                "event_type_name": event_data.get("eventTypeName","No Event Type"),
                "event_type_id":event_data.get("eventTypeId"),
                "location": event_data.get("location","No Location"),
                "description": event_data.get("description","No Description"),
                "event_leader": event_data.get("eventLeader","No Leader"),
                "event_leader_email": event_data.get("eventLeaderEmail","No Leader Email"),
                "is_ticketed": event_data.get("isTicketed", False),
                "is_global": event_data.get("isGlobal", False),
                "is_active": event_data.get("is_active", True),
                "has_person_steps": event_data.get("hasPersonSteps", False),
                "status": event_data.get("status", "open"),
                "recurring_day": "".join(event_data.get("recurring_day",[""])),
                "is_recurring": bool(event_data.get("recurring_day")),
                "organization": event_data.get("Organization"),
                "event_date": final_event_date.isoformat(),
                "org_id": event_data.get("org_id")

            }  
            
            post_event = supabase.table("events").insert(final_data).execute()
            result = post_event.data[0]["event_id"]
            
            print(f"[RECURRING CREATE] Inserted _id: {result}")

            return {
                "success": True,
                "message": "Recurring event created successfully",
                "created_event_ids": result,
                "id": result,
                "count": 1
            }

        post_event = supabase.table("events").insert(final_data).execute()
        inserted_id = post_event.data[0]["event_id"]
        created_event = post_event.data[0]

        return {
            "success": True,
            "message": "Event created successfully",
            "id": inserted_id,
            "event": {**created_event, "_id": inserted_id}
        }

    except Exception as e:
        print(f"Error creating event: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/event-types")
async def get_event_types(current_user: dict = Depends(get_current_user)):
    """
    Fetches event types for the current user's organization from Supabase.
    """
    try:
        # test = supabase.table("events").select("*").limit(1).execute()
        # print(f"Supabase test query result: {test}")

        org_id = current_user.get("org_id") 
      
        print(f"GET EVENT TYPES — user: {current_user.get('email')} | org_id: {org_id}")

        event_types = supabase.table("event_types").select("*").eq("org_id", org_id).execute().data

        return event_types

    except Exception as e:
        print(f"Error fetching event types: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    

async def get_top_leader_dynamic(gender: str, org_id: str = "active-teams") -> str:
    try:
        config = await org_config_collection.find_one({"_id": org_id})
        gender_lower = gender.lower().strip()
        is_female = gender_lower in ["female", "f", "woman", "lady", "girl"]
        is_male   = gender_lower in ["male", "m", "man", "gentleman", "boy"]

        if config and config.get("top_leaders"):
            top = config["top_leaders"]
            if is_female: return top.get("female", "")
            if is_male:   return top.get("male", "")
            return ""

        # Fallback
        if is_female: return "Vicky Enslin"
        if is_male:   return "Gavin Enslin"
        return ""

    except Exception as e:
        print(f"Error in get_top_leader_dynamic: {e}")
        if "female" in gender.lower(): return "Vicky Enslin"
        if "male" in gender.lower():   return "Gavin Enslin"
        return ""
    
@router.get("/events/cells")
async def get_cell_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None),
    leader_at_12_view: Optional[bool] = Query(None),
    show_personal_cells: Optional[bool] = Query(None),
    show_all_authorized: Optional[bool] = Query(None),
    include_subordinate_cells: Optional[bool] = Query(None),
    leader_at_1_identifier: Optional[str] = Query(None),
    isLeaderAt12: Optional[bool] = Query(None),
    firstName: Optional[str] = Query(None),
    userSurname: Optional[str] = Query(None),
    must_paginate: Optional[bool] = Query(True)
):
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        org_config = await org_config_collection.find_one({"_id": org_id})
        recurring_type = org_config.get("recurring_event_type", "Cells") if org_config else "Cells"

        user_email = current_user.get("email", "")
        role = current_user.get("role", "").lower().strip()
        is_actual_leader_at_12 = (
            role == "leaderat12" or
            "leaderat12" in role or
            "leader at 12" in role or
            "leader@12" in role
        )

        if recurring_type.lower() != "cells":
            return {
                "events": [],
                "total_events": 0,
                "total_pages": 1,
                "current_page": 1,
                "page_size": 25,
            }

        user_name_from_frontend = f"{firstName or ''} {userSurname or ''}".strip()

        person = await people_collection.find_one(
            {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
            {"_id": 1, "Name": 1, "Surname": 1}
        )

        user_person_id = None

        if person:
            user_person_id = person.get("_id")
            db_first = person.get("Name", "").strip()
            db_surname = person.get("Surname", "").strip()
            user_name_from_db = f"{db_first} {db_surname}".strip()
        else:
            user_name_from_db = ""

        user_name_from_token = current_user.get("name", "")

        if user_name_from_frontend:
            user_name = user_name_from_frontend
        elif user_name_from_db:
            user_name = user_name_from_db
        else:
            user_name = user_name_from_token

        print(f"User name resolved as: {user_name}")

        query = {
            "$and": [
                {
                    "$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}},
                        {"EventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeId": "CELLS_BUILT_IN"},
                        {"hasPersonSteps": True},
                    ]
                },
                {"isEventType": {"$ne": True}},
                {
                    "$or": [
                        {"org_id": org_id},
                        {"Organization": {"$regex": re.escape(organization), "$options": "i"}}
                    ]
                },
                {
                    "$or": [
                        {"is_active": True},
                        {"is_active": {"$exists": False}}
                    ]
                },
            ]
        }

        if search and search.strip():
            search_term = search.strip()
            query["$and"].append({
                "$or": [
                    {"Event Name": {"$regex": search_term, "$options": "i"}},
                    {"eventName": {"$regex": search_term, "$options": "i"}},
                    {"Leader": {"$regex": search_term, "$options": "i"}},
                    {"Email": {"$regex": search_term, "$options": "i"}},
                    {"Leader at 12": {"$regex": search_term, "$options": "i"}},
                    {"Leader @12": {"$regex": search_term, "$options": "i"}},
                ]
            })

        def create_name_conditions(target_name, fields):
            conditions = []
            if not target_name:
                return conditions
            clean_name = target_name.strip()
            for field in fields:
                conditions.append({field: {"$regex": f"^{re.escape(clean_name)}$", "$options": "i"}})
                conditions.append({field: {"$regex": re.escape(clean_name), "$options": "i"}})
                title_name = clean_name.title()
                conditions.append({field: {"$regex": f"^{re.escape(title_name)}$", "$options": "i"}})
                name_parts = clean_name.split()
                if len(name_parts) > 0:
                    first_name = name_parts[0].strip()
                    conditions.append({field: {"$regex": re.escape(first_name), "$options": "i"}})
            return conditions

        if role == "admin":
            if personal or show_personal_cells:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

        elif is_actual_leader_at_12 and leader_at_12_view:
            want_personal_view = (show_personal_cells or personal)
            want_disciples_view = (show_all_authorized or include_subordinate_cells)

            if want_personal_view and not want_disciples_view:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

            elif want_disciples_view and not want_personal_view:
                conditions = []
                if user_person_id:
                    conditions.append({"LeaderPath": user_person_id})
                leader_at_12_fields = [
                    "Leader at 12", "Leader @12", "leader12",
                    "Leader12", "LeaderAt12", "leader at 12", "leader @12"
                ]
                for field in leader_at_12_fields:
                    conditions.append({field: {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}})
                    conditions.append({field: {"$regex": re.escape(user_name), "$options": "i"}})
                print(f"Disciples query conditions count: {len(conditions)}")
                if conditions:
                    query["$and"].append({"$or": conditions})
                else:
                    query["$and"].append({"_id": "nonexistent_id"})

            else:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

        elif role == "leader144":
            name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName",
                           "leader144", "Leader at 144", "Leader @144"]
            name_conditions = create_name_conditions(user_name, name_fields)
            email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
            email_conditions = create_name_conditions(user_email, email_fields)
            leader_path_condition = []
            if user_person_id:
                leader_path_condition = [{"leaderLeaderPath": user_person_id}]
            query["$and"].append({"$or": name_conditions + email_conditions + leader_path_condition})

        elif role in ["user", "registrant", "leader"]:
            conditions = []
            if user_name:
                clean_name = user_name.strip()
                for field in ["Leader", "eventLeaderName", "EventLeaderName"]:
                    conditions.append({field: {"$regex": f"^{re.escape(clean_name)}$", "$options": "i"}})
            if user_email:
                clean_email = user_email.strip().lower()
                for field in ["eventLeaderEmail", "EventLeaderEmail", "Email"]:
                    conditions.append({field: {"$regex": f"^{re.escape(clean_email)}$", "$options": "i"}})
            if user_person_id:
                conditions.append({"leaderLeaderPath": user_person_id})
            if conditions:
                query["$and"].append({"$or": conditions})
            else:
                query["$and"].append({"_id": "nonexistent_id"})

        print(f"Final query for cells: {query}")

        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": {
                        "event_name": {"$ifNull": ["$Event Name", "$eventName", "$EventName"]},
                        "leader_email": {"$ifNull": ["$eventLeaderEmail", "$EventLeaderEmail", "$Email"]},
                        "day": {"$ifNull": ["$Day", "$day"]}
                    },
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"Day": 1, "Leader": 1}}
        ]

        events = await events_collection.aggregate(pipeline).to_list(length=None)

        sa_timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(sa_timezone).date()

        try:
            start_date_obj = datetime.strptime(start_date if start_date else "2025-11-30", "%Y-%m-%d").date()
        except:
            start_date_obj = datetime.strptime("2025-11-30", "%Y-%m-%d").date()

        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }

        cell_instances = []

        for event in events:
            try:
                day_name = str(event.get("Day") or event.get("day") or "").strip().lower()
                if not day_name or day_name not in day_mapping:
                    continue

                target_weekday = day_mapping.get(day_name)
                if target_weekday is None:
                    continue

                days_since_monday = today.weekday()
                week_start = today - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)

                if status == "incomplete":
                    max_weeks = 1
                elif start_date:
                    max_weeks = max(
                        1,
                        ((current_week_instance - start_date_obj).days // 7) + 1,
                    )
                else:
                    max_weeks = 4

                for week_back in range(0, max_weeks):
                    instance_date = current_week_instance - timedelta(weeks=week_back)

                    if instance_date > today:
                        continue
                    if instance_date < start_date_obj:
                        continue

                    exact_date = instance_date.isoformat()
                    attendance_data = event.get("attendance", {})
                    attendance = attendance_data.get(exact_date, {})

                    if not attendance:
                        for key, value in attendance_data.items():
                            if isinstance(value, dict):
                                if value.get("event_date_exact") == exact_date:
                                    attendance = value
                                    break
                                event_date_iso = value.get("event_date_iso")
                                if event_date_iso and exact_date in event_date_iso:
                                    attendance = value
                                    break
                        if not attendance:
                            legacy_week_key = instance_date.strftime("%G-W%V")
                            legacy_attendance = attendance_data.get(legacy_week_key, {})
                            if legacy_attendance:
                                attendance = legacy_attendance
                                try:
                                    await events_collection.update_one(
                                        {"_id": event["_id"]},
                                        {"$set": {f"attendance.{exact_date}": legacy_attendance}}
                                    )
                                except Exception as migrate_error:
                                    print(f"Legacy attendance migration skipped: {migrate_error}")

                    if not attendance:
                        event_status = "incomplete"
                        attendees = []
                        did_not_meet = False
                    else:
                        att_status = attendance.get("status", "").lower()
                        attendees = attendance.get("attendees", [])
                        if att_status == "did_not_meet":
                            event_status = "did_not_meet"
                            did_not_meet = True
                        elif att_status == "complete" or len(attendees) > 0:
                            event_status = "complete"
                            did_not_meet = False
                        else:
                            event_status = "incomplete"
                            did_not_meet = False

                    if status and status != 'all' and event_status != status:
                        continue

                    is_overdue = instance_date < today and event_status == "incomplete"

                    leaderAt1 = event.get("leader1") or event.get("Leader @1") or event.get("Leader at 1", "")

                    if not leaderAt1:
                        leaderPipeline = [
                            {"$project": {"Gender": 1, "fullName": {"$concat": ["$Name", " ", "$Surname"]}}},
                            {"$match": {"fullName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", "")}},
                            {"$limit": 1}
                        ]
                        peopleFullnames = await people_collection.aggregate(leaderPipeline).to_list(length=None)
                        if peopleFullnames and len(peopleFullnames) > 0:
                            eventLeader = peopleFullnames[0]
                            if eventLeader:
                                gender = eventLeader.get("Gender", "")
                                leaderAt1 = await get_top_leader_dynamic(gender, org_id)

                    leaderAt12 = (
                        event.get("Leader at 12") or
                        event.get("Leader @12") or
                        event.get("leader12") or
                        event.get("Leader12") or
                        event.get("LeaderAt12") or
                        event.get("leader at 12") or
                        event.get("leader @12") or
                        ""
                    )

                    instance = {
                        "_id": f"{event.get('_id')}_{exact_date}",
                        "UUID": event.get("UUID", ""),
                        "eventName": event.get("Event Name") or event.get("eventName") or event.get("EventName", ""),
                        "eventType": "Cells",
                        "eventLeaderName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", ""),
                        "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("EventLeaderEmail") or event.get("Email", ""),
                        "leader1": leaderAt1,
                        "leader12": leaderAt12,
                        "day": day_name.capitalize(),
                        "date": exact_date,
                        "display_date": instance_date.strftime("%d - %m - %Y"),
                        "location": event.get("Location") or event.get("location", ""),
                        "attendees": attendees,
                        "persistent_attendees": event.get("persistent_attendees", []),
                        "hasPersonSteps": True,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "did_not_meet": did_not_meet,
                        "_is_overdue": is_overdue,
                        "is_recurring": True,
                        "original_event_id": str(event.get("_id")),
                        "attendance": attendance,
                        "is_active": event.get("is_active", ""),
                    }
                    if event.get("time"):
                        instance["time"] = event.get("time")
                    if event.get("Time"):
                        instance["Time"] = event.get("Time")

                    cell_instances.append(instance)

            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {e}")
                continue

        if must_paginate:
            total_count = len(cell_instances)
            total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
            skip = (page - 1) * limit
            paginated = cell_instances[skip:skip + limit]
            return {
                "events": paginated,
                "total_events": total_count,
                "total_pages": total_pages,
                "current_page": page,
                "page_size": limit,
                "user_info": {
                    "name": user_name,
                    "email": user_email,
                    "role": role,
                    "is_leader_at_12": is_actual_leader_at_12,
                    "view_mode": "personal" if (personal or show_personal_cells) else "all"
                }
            }
        else:
            print("SENDING ALL EVENTS")
            return {"events": cell_instances}

    except Exception as e:
        print(f"Error in /events/cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events/eventsdata")
async def get_other_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=500),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query("2025-10-10"),
    end_date: Optional[str] = Query(None),
    show_all_dates: Optional[bool] = Query(False)
):
    try:
        print(f"GET /eventsdata - User: {current_user.get('email')}, Event Type: {event_type}")
        print(f"Query params - status: {status}, personal: {personal}, search: {search}")

        user_role = current_user.get("role", "user").lower()
        user_email = current_user.get("email", "").lower().strip()
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()

        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(timezone)
        today = now.date()

        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else datetime.strptime("2000-01-01", "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else today + timedelta(days=365)
        except Exception as e:
            print(f"Error parsing dates: {e}")
            start_date_obj = datetime.strptime("2000-01-01", "%Y-%m-%d").date()
            end_date_obj = today + timedelta(days=365)

        print(f"OTHER EVENTS - Date range: {start_date_obj} to {end_date_obj}")

        query = {
            "$and": [
                {
                    "$or": [
                        {"org_id": org_id},
                        {"Organization": {"$regex": re.escape(organization), "$options": "i"}}
                    ]
                },
                {
                    "$nor": [
                        {"Event Type": {"$regex": "Cells", "$options": "i"}},
                        {"eventType": {"$regex": "Cells", "$options": "i"}},
                        {"eventTypeName": {"$regex": "Cells", "$options": "i"}}
                    ]
                }
            ]
        }

        if user_role not in ["admin", "leaderat12", "registrant"]:
            visibility_filter = {
                "$or": [
                    {"isGlobal": True},
                    {"isGlobal": "true"},
                    {"eventLeaderEmail": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"userEmail": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"leader1": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"eventLeaderName": {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}},
                    {"Leader": {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}},
                ]
            }
            query["$and"].append(visibility_filter)

        if personal:
            print(f"Applying PERSONAL filter for user: {user_email}")
            query["$and"].append({
                "$or": [
                    {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                    {"leader1": {"$regex": user_email, "$options": "i"}}
                ]
            })
        elif user_role == "user":
            print(f"Regular user - showing personal events: {user_email}")
            query["$and"].append({
                "$or": [
                    {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                    {"leader1": {"$regex": user_email, "$options": "i"}}
                ]
            })

        if event_type and event_type.lower() != 'all':
            print(f"Filtering by event type: '{event_type}'")
            if event_type.lower() not in ["all", "cells"]:
                query["$and"].append({
                    "$or": [
                        {"Event Type": {"$regex": f"^{event_type}$", "$options": "i"}},
                        {"eventType": {"$regex": f"^{event_type}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{event_type}$", "$options": "i"}}
                    ]
                })

        if search and search.strip():
            search_term = search.strip()
            print(f"Applying search filter: '{search_term}'")
            safe_search_term = re.escape(search_term)
            query["$and"].append({
                "$or": [
                    {"Event Name": {"$regex": safe_search_term, "$options": "i"}},
                    {"eventName": {"$regex": safe_search_term, "$options": "i"}},
                    {"Leader": {"$regex": safe_search_term, "$options": "i"}},
                    {"eventLeaderName": {"$regex": safe_search_term, "$options": "i"}},
                    {"eventLeaderEmail": {"$regex": safe_search_term, "$options": "i"}},
                    {"leader1": {"$regex": safe_search_term, "$options": "i"}},
                    {"Location": {"$regex": safe_search_term, "$options": "i"}},
                    {"location": {"$regex": safe_search_term, "$options": "i"}}
                ]
            })

        print(f"Final query: {query}")

        cursor = events_collection.find(query)
        events = await cursor.to_list(length=3000)
        print(f"Found {len(events)} other events")

        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }

        other_events = []

        for event in events:
            try:
                event_name = event.get("Event Name") or event.get("eventName", "")
                event_type_value = event.get("Event Type") or event.get("eventType", "Event")
                recurring_days = event.get("recurring_day", [])
                if not isinstance(recurring_days, list):
                    recurring_days = []
                is_recurring = len(recurring_days) > 0

                # Helper function to enrich attendees with financial data
                def enrich_attendees_with_financials(attendees_list):
                    enriched = []
                    for att in attendees_list:
                        if not isinstance(att, dict):
                            continue
                        # Calculate financials if missing
                        price = att.get("price", 0)
                        paid = att.get("paid", att.get("paidAmount", 0))
                        
                        if paid >= price:
                            owing = 0
                            change = paid - price
                        elif paid > 0 and paid < price:
                            owing = price - paid
                            change = 0
                        else:
                            owing = price
                            change = 0
                        
                        enriched_att = {
                            "id": att.get("id", ""),
                            "name": att.get("name", ""),
                            "fullName": att.get("fullName", att.get("name", "")),
                            "email": att.get("email", ""),
                            "phone": att.get("phone", ""),
                            "leader12": att.get("leader12", ""),
                            "leader144": att.get("leader144", ""),
                            "checked_in": att.get("checked_in", False),
                            "decision": att.get("decision", ""),
                            "priceName": att.get("priceName", ""),
                            "price": price,
                            "ageGroup": att.get("ageGroup", ""),
                            "paymentMethod": att.get("paymentMethod", ""),
                            "paid": paid,
                            "owing": owing,
                            "change": change,
                        }
                        enriched.append(enriched_att)
                    return enriched

                if is_recurring:
                    days_since_monday = today.weekday()
                    week_start = today - timedelta(days=days_since_monday)

                    for day_name_raw in recurring_days:
                        day_key = str(day_name_raw).strip().lower()
                        target_weekday = day_mapping.get(day_key)
                        if target_weekday is None:
                            continue

                        for week_back in range(0, 1):
                            instance_date = (week_start + timedelta(days=target_weekday)) - timedelta(weeks=week_back)
                            if instance_date > today:
                                continue
                            if instance_date < start_date_obj or instance_date > end_date_obj:
                                continue

                            exact_date_str = instance_date.isoformat()
                            days_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            actual_day_value = days_list[instance_date.weekday()]

                            attendance_data = event.get("attendance", {})
                            if not isinstance(attendance_data, dict):
                                attendance_data = {}
                            date_attendance = attendance_data.get(exact_date_str, {})
                            if not isinstance(date_attendance, dict):
                                date_attendance = {}

                            original_date_str = None
                            event_date_field = event.get("date") or event.get("Date Of Event") or event.get("eventDate")
                            if isinstance(event_date_field, datetime):
                                original_date_str = event_date_field.date().isoformat()
                            elif isinstance(event_date_field, str):
                                try:
                                    if 'T' in event_date_field:
                                        original_date_str = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date().isoformat()
                                    else:
                                        original_date_str = event_date_field[:10]
                                except:
                                    pass

                            root_attendees = event.get("attendees", [])
                            if not isinstance(root_attendees, list):
                                root_attendees = []

                            if not date_attendance and exact_date_str == original_date_str and root_attendees:
                                date_attendance = {
                                    "attendees": root_attendees,
                                    "status": str(event.get("status", "")).lower(),
                                    "new_people": event.get("new_people", []),
                                    "consolidations": event.get("consolidations", []),
                                }

                            weekly_attendees = date_attendance.get("attendees", [])
                            if not isinstance(weekly_attendees, list):
                                weekly_attendees = []
                            
                            # Enrich attendees with financial data
                            weekly_attendees = enrich_attendees_with_financials(weekly_attendees)
                            has_weekly_attendees = len(weekly_attendees) > 0

                            new_people = date_attendance.get("new_people", [])
                            if not isinstance(new_people, list):
                                new_people = []
                            consolidations = date_attendance.get("consolidations", [])
                            if not isinstance(consolidations, list):
                                consolidations = []

                            att_status = str(date_attendance.get("status", "")).lower()
                            is_did_not_meet = date_attendance.get("is_did_not_meet", False)

                            if is_did_not_meet or att_status == "did_not_meet":
                                event_status = "did_not_meet"
                            elif att_status in ["open", "incomplete", "reopened", "active"]:
                                event_status = "incomplete"
                            elif has_weekly_attendees or att_status in ["complete", "closed"]:
                                event_status = "complete"
                            else:
                                event_status = "incomplete"

                            if status and status != event_status:
                                continue

                            total_attendance = len(weekly_attendees)

                            instance = {
                                "_id": f"{str(event.get('_id'))}_{exact_date_str}",
                                "UUID": event.get("UUID", ""),
                                "eventName": event_name,
                                "eventType": event_type_value,
                                "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                                "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                                "leader1": event.get("leader1", ""),
                                "leader12": event.get("Leader @12") or event.get("Leader at 12", ""),
                                "day": actual_day_value,
                                "date": exact_date_str,
                                "location": event.get("Location") or event.get("location", ""),
                                "hasPersonSteps": False,
                                "status": event_status,
                                "Status": event_status.replace("_", " ").title(),
                                "_is_overdue": instance_date < today and event_status == "incomplete",
                                "is_recurring": True,
                                "recurring_days": recurring_days,
                                "original_event_id": str(event.get("_id")),
                                "isGlobal": event.get("isGlobal", False),
                                "isTicketed": event.get("isTicketed", False),
                                "priceTiers": event.get("priceTiers", []),
                                "closed_by": date_attendance.get("closed_by") or event.get("closed_by", ""),
                                "closed_at": str(date_attendance.get("closed_at") or event.get("closed_at", "")),
                                "created_at": str(event.get("created_at", "")),
                                "updated_at": str(event.get("updated_at", "") or event.get("updatedAt", "")),
                                "attendees": weekly_attendees,
                                "persistent_attendees": enrich_attendees_with_financials(event.get("persistent_attendees", [])),
                                "new_people": new_people,
                                "consolidations": consolidations,
                                "total_attendance": total_attendance,
                                "new_people_count": len(new_people),
                                "consolidation_count": len(consolidations),
                            }
                            other_events.append(instance)

                else:
                    day_name_raw = event.get("Day") or event.get("day") or event.get("eventDay") or ""
                    day_name = str(day_name_raw).strip()

                    event_date_field = event.get("date") or event.get("Date Of Event") or event.get("eventDate")
                    if isinstance(event_date_field, datetime):
                        event_date = event_date_field.date()
                    elif isinstance(event_date_field, str):
                        try:
                            if 'T' in event_date_field:
                                event_date = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
                            else:
                                event_date = datetime.strptime(event_date_field, "%Y-%m-%d").date()
                        except Exception as e:
                            print(f"Error parsing date '{event_date_field}': {e}")
                            continue
                    else:
                        continue

                    if not day_name:
                        try:
                            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            day_name = days[event_date.weekday()]
                        except Exception as e:
                            print(f"Error calculating day from date: {e}")
                            day_name = "One-time"

                    actual_day_value = day_name.capitalize() if day_name else "One-time"

                    if event_date < start_date_obj or event_date > end_date_obj:
                        continue
                    if event_date > today:
                        continue

                    weekly_attendees = event.get("attendees", [])
                    if not isinstance(weekly_attendees, list):
                        weekly_attendees = []

                    if not weekly_attendees:
                        attendance_data = event.get("attendance", {})
                        if isinstance(attendance_data, dict):
                            event_date_iso = event_date.isoformat()
                            event_attendance = attendance_data.get(event_date_iso, {})
                            weekly_attendees = event_attendance.get("attendees", [])
                            if not isinstance(weekly_attendees, list):
                                weekly_attendees = []

                    # Helper function to enrich attendees with financial data
                    def enrich_attendees_with_financials(attendees_list):
                        enriched = []
                        for att in attendees_list:
                            if not isinstance(att, dict):
                                continue
                            price = att.get("price", 0)
                            paid = att.get("paid", att.get("paidAmount", 0))
                            
                            if paid >= price:
                                owing = 0
                                change = paid - price
                            elif paid > 0 and paid < price:
                                owing = price - paid
                                change = 0
                            else:
                                owing = price
                                change = 0
                            
                            enriched_att = {
                                "id": att.get("id", ""),
                                "name": att.get("name", ""),
                                "fullName": att.get("fullName", att.get("name", "")),
                                "email": att.get("email", ""),
                                "phone": att.get("phone", ""),
                                "leader12": att.get("leader12", ""),
                                "leader144": att.get("leader144", ""),
                                "checked_in": att.get("checked_in", False),
                                "decision": att.get("decision", ""),
                                "priceName": att.get("priceName", ""),
                                "price": price,
                                "ageGroup": att.get("ageGroup", ""),
                                "paymentMethod": att.get("paymentMethod", ""),
                                "paid": paid,
                                "owing": owing,
                                "change": change,
                            }
                            enriched.append(enriched_att)
                        return enriched

                    weekly_attendees = enrich_attendees_with_financials(weekly_attendees)
                    has_weekly_attendees = len(weekly_attendees) > 0

                    new_people = event.get("new_people", [])
                    if not isinstance(new_people, list):
                        new_people = []

                    consolidations = event.get("consolidations", [])
                    if not isinstance(consolidations, list):
                        consolidations = []

                    main_event_status = event.get("status", "").lower()
                    main_event_did_not_meet = event.get("did_not_meet", False)
                    main_event_complete = event.get("Status", "").lower() == "complete"

                    if main_event_did_not_meet or main_event_status == "did_not_meet":
                        event_status = "did_not_meet"
                    elif main_event_status in ["open", "incomplete", "reopened", "active"]:
                        event_status = "incomplete"
                    elif has_weekly_attendees or main_event_complete or main_event_status in ["complete", "closed"]:
                        event_status = "complete"
                    else:
                        event_status = "incomplete"

                    print(f"Event '{event_name}' - attendees: {len(weekly_attendees)}, status: {event_status}")

                    if status and status != event_status:
                        continue

                    total_attendance = event.get("total_attendance")
                    if not isinstance(total_attendance, int) or total_attendance == 0:
                        total_attendance = len(weekly_attendees)

                    instance = {
                        "_id": str(event.get("_id")),
                        "UUID": event.get("UUID", ""),
                        "eventName": event_name,
                        "eventType": event_type_value,
                        "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                        "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                        "leader1": event.get("leader1", ""),
                        "leader12": event.get("Leader @12") or event.get("Leader at 12", ""),
                        "day": actual_day_value,
                        "date": event_date.isoformat(),
                        "location": event.get("Location") or event.get("location", ""),
                        "hasPersonSteps": False,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "_is_overdue": event_date < today and event_status == "incomplete",
                        "is_recurring": False,
                        "recurring_days": [],
                        "original_event_id": str(event.get("_id")),
                        "isGlobal": event.get("isGlobal", False),
                        "isTicketed": event.get("isTicketed", False),
                        "priceTiers": event.get("priceTiers", []),
                        "closed_by": event.get("closed_by", ""),
                        "closed_at": str(event.get("closed_at", "")),
                        "created_at": str(event.get("created_at", "")),
                        "updated_at": str(event.get("updated_at", "") or event.get("updatedAt", "")),
                        "attendees": weekly_attendees,
                        "persistent_attendees": enrich_attendees_with_financials(event.get("persistent_attendees", [])),
                        "new_people": new_people,
                        "consolidations": consolidations,
                        "total_attendance": total_attendance,
                        "new_people_count": len(new_people),
                        "consolidation_count": len(consolidations),
                    }
                    other_events.append(instance)

            except Exception as e:
                print(f"Error processing other event: {str(e)}")
                import traceback
                traceback.print_exc()
                continue

        other_events.sort(key=lambda x: x['date'], reverse=True)

        total_count = len(other_events)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated_events = other_events[skip:skip + limit]

        print(f"Returning {len(paginated_events)} other events (page {page}/{total_pages})")

        return {
            "events": paginated_events,
            "total_events": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }

    except Exception as e:
        print(f"ERROR in /eventsdata: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
  

#not used
@router.get("/events/{event_id}/attendance/{week}")
async def get_weekly_attendance(
    event_id: str = Path(...),
    week: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
       
        exact_date_str = week 
        attendance_data = event.get("attendance", {}).get(exact_date_str)

        if not attendance_data:
            try:
                parsed_date = datetime.strptime(exact_date_str, "%Y-%m-%d").date()
                legacy_week_key = parsed_date.strftime("%G-W%V") 
                legacy_attendance = event.get("attendance", {}).get(legacy_week_key)
                if legacy_attendance:
                    attendance_data = legacy_attendance
                
                    await events_collection.update_one(
                        {"_id": ObjectId(event_id)},
                        {"$set": {f"attendance.{exact_date_str}": legacy_attendance}}
                    )
            except Exception as migrate_error:
                print(f"Legacy attendance migration skipped: {migrate_error}")
        
        if not attendance_data:
            return {
                "week": exact_date_str, 
                "exists": False,
                "message": "No attendance data for this week"
            }
        
        return {
            "week": exact_date_str,
            "exists": True,
            "data": attendance_data,
            "persistent_attendees": event.get("persistent_attendees", []),
            "event_statistics": {
                "total_associated_count": event.get("total_associated_count", 0),
                "last_attendance_count": event.get("last_attendance_count", 0),
                "last_decisions_count": event.get("last_decisions_count", 0)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/events/cells/{identifier}")
async def update_cell_event_working(identifier: str, event_data: dict):
    """
    SINGLE EVENT UPDATE: Update ONLY the existing event, NEVER create new ones
    """
    try:
        from datetime import datetime as dt
        
        # Find the SINGLE event by ID
        event = None
        if ObjectId.is_valid(identifier):
            event = await events_collection.find_one({"_id": ObjectId(identifier)})
        
        if not event:
            raise HTTPException(
                status_code=404,
                detail=f"Event not found with identifier: {identifier}"
            )
        
        # Prepare update fields
        update_fields = {}
        
        # Event Name mapping
        if 'eventName' in event_data or 'Event Name' in event_data:
            event_name_value = event_data.get('eventName') or event_data.get('Event Name')
            update_fields['eventName'] = event_name_value
            update_fields['Event Name'] = event_name_value
        
        # Day mapping
        if 'Day' in event_data or 'day' in event_data:
            day_value = event_data.get('Day') or event_data.get('day')
            update_fields['Day'] = day_value
            update_fields['day'] = day_value
        
        # Address/location mapping
        if 'Address' in event_data or 'location' in event_data:
            location_value = event_data.get('Address') or event_data.get('location')
            update_fields['Address'] = location_value
            update_fields['location'] = location_value
        
        # Time mapping
        if 'Time' in event_data or 'time' in event_data:
            time_value = event_data.get('Time') or event_data.get('time')
            update_fields['Time'] = time_value
            update_fields['time'] = time_value
        
        # Date mapping - Handle both formats AND display_date
        if 'date' in event_data or 'Date Of Event' in event_data:
            date_value = event_data.get('date')
            date_of_event_value = event_data.get('Date Of Event')
            
            if date_of_event_value:
                update_fields['Date Of Event'] = date_of_event_value
                if date_value:
                    update_fields['date'] = date_value
                else:
                    try:
                        dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                        update_fields['date'] = dt_obj.strftime('%Y-%m-%dT%H:%M')
                    except:
                        update_fields['date'] = date_of_event_value
                
                # Update display_date for table
                try:
                    dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    pass
            
            elif date_value:
                update_fields['date'] = date_value
                try:
                    dt_obj = dt.fromisoformat(date_value)
                    update_fields['Date Of Event'] = dt_obj.isoformat() + 'Z'
                    # Update display_date for table
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    update_fields['Date Of Event'] = date_value
        
        # Email mapping
        if 'Email' in event_data or 'eventLeaderEmail' in event_data:
            email_value = event_data.get('Email') or event_data.get('eventLeaderEmail')
            update_fields['Email'] = email_value
            update_fields['eventLeaderEmail'] = email_value
        
        # Leader mapping
        if 'Leader' in event_data or 'eventLeader' in event_data or 'eventLeaderName' in event_data:
            leader_value = event_data.get('Leader') or event_data.get('eventLeader') or event_data.get('eventLeaderName')
            update_fields['Leader'] = leader_value
            update_fields['eventLeader'] = leader_value
            update_fields['eventLeaderName'] = leader_value
        
        # Status mapping
        if 'status' in event_data or 'Status' in event_data:
            status_value = event_data.get('status') or event_data.get('Status')
            update_fields['status'] = status_value
        
        protected_fields = [
            'eventName', 'Event Name', 'Day', 'day', 'Address', 'location', 
            'Time', 'time', 'date', 'Date Of Event', 'Email', 
            'eventLeaderEmail', 'Leader', 'eventLeader', 'eventLeaderName',
            'status', 'Status',
            'persistent_attendees', 
            'attendees',             
            'attendance',           
            '_id', 'id', 'UUID',     
            'created_at',            
            'total_attendance'   
        ]
        
        # Other fields - but skip protected ones
        for key, value in event_data.items():
            if key not in protected_fields:
                update_fields[key] = value
         
        if update_fields.get("deactivation_end"):
            print("yay events!")
            update_fields["deactivation_end"] = datetime.strptime(update_fields["deactivation_end"], "%Y-%m-%dT%H:%M:%S.%f")
        
        update_fields["updated_at"] = datetime.utcnow()
        
        print(f"Updating event {identifier} with fields: {update_fields}")
        print(f"Protected fields excluded: persistent_attendees, attendees, attendance")
        
        # PERFORM THE UPDATE
        result = await events_collection.update_one(
            {"_id": event["_id"]},
            {"$set": update_fields}
        )
        
        return {
            "success": True,
            "message": "Event updated successfully",
            "modified": result.modified_count > 0,
            "event_id": str(event.get("_id"))
        }
        
    except Exception as e:
        print(f"Error updating event: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/events/person/{person_name}/event/{event_name}/day/{day_name}")
async def update_events_by_person_event_and_day(person_name: str, event_name: str, day_name: str, update_data: dict):
    """
    Update ONLY events for a specific person with a SPECIFIC event name AND SPECIFIC day
    """
    try:
        from datetime import datetime as dt
        
        decoded_person = unquote(person_name)
        decoded_event = unquote(event_name)
        decoded_day = unquote(day_name)
        
        print(f"=== UPDATE PERSON+EVENT+DAY (PRECISE) ===")
        print(f"Person: {decoded_person}")
        print(f"Event name: {decoded_event}")
        print(f"Day: {decoded_day}")
        print(f"Update data: {update_data}")
        
        # STRICT query
        strict_query = {
            "$and": [
                {
                    "$or": [
                        {"Leader": decoded_person},
                        {"eventLeader": decoded_person},
                        {"eventLeaderName": decoded_person}
                    ]
                },
                {
                    "$or": [
                        {"Event Name": decoded_event},
                        {"eventName": decoded_event}
                    ]
                },
                {
                    "$or": [
                        {"Day": decoded_day},
                        {"day": decoded_day}
                    ]
                }
            ]
        }
        
        cursor = events_collection.find(strict_query)
        matching_events = await cursor.to_list(length=None)
        
        if not matching_events:
            return {
                "success": False,
                "message": f"No {decoded_day} events found for {decoded_person} with name: {decoded_event}",
                "matched_count": 0,
                "modified_count": 0
            }
        
        print(f"Found {len(matching_events)} matching events")
        
        # Prepare update with proper field mapping
        update_fields = {}
        
        # Event Name mapping
        if 'eventName' in update_data or 'Event Name' in update_data:
            event_name_value = update_data.get('eventName') or update_data.get('Event Name')
            update_fields['eventName'] = event_name_value
            update_fields['Event Name'] = event_name_value
        
        # Day mapping
        if 'Day' in update_data or 'day' in update_data:
            day_value = update_data.get('Day') or update_data.get('day')
            update_fields['Day'] = day_value
            update_fields['day'] = day_value
        
        # Date mapping - Handle both formats AND display_date
        if 'date' in update_data or 'Date Of Event' in update_data:
            date_value = update_data.get('date')
            date_of_event_value = update_data.get('Date Of Event')
            
            if date_of_event_value:
                update_fields['Date Of Event'] = date_of_event_value
                if date_value:
                    update_fields['date'] = date_value
                else:
                    try:
                        dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                        update_fields['date'] = dt_obj.strftime('%Y-%m-%dT%H:%M')
                    except:
                        update_fields['date'] = date_of_event_value
                
                # Update display_date for table
                try:
                    dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    pass
            
            elif date_value:
                update_fields['date'] = date_value
                try:
                    # Handle YYYY-MM-DD format (what frontend sends)
                    if len(date_value) == 10 and '-' in date_value:
                        dt_obj = dt.strptime(date_value, '%Y-%m-%d')
                    else:
                        dt_obj = dt.fromisoformat(date_value)
                    
                    update_fields['Date Of Event'] = dt_obj.isoformat() + 'Z'
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y') 
                except:
                    update_fields['Date Of Event'] = date_value

        
        # Time mapping
        if 'Time' in update_data or 'time' in update_data:
            time_value = update_data.get('Time') or update_data.get('time')
            
            if time_value:
                print(f"DEBUG - Time received from frontend: {time_value}")
                
                # Store exactly as received
                update_fields['Time'] = time_value
                update_fields['time'] = time_value  
                      
        # Address/Location mapping
        if 'Address' in update_data or 'location' in update_data:
            location_value = update_data.get('Address') or update_data.get('location')
            update_fields['Address'] = location_value
            update_fields['location'] = location_value
        
        # Email mapping
        if 'Email' in update_data or 'eventLeaderEmail' in update_data:
            email_value = update_data.get('Email') or update_data.get('eventLeaderEmail')
            update_fields['Email'] = email_value
            update_fields['eventLeaderEmail'] = email_value
        
        # Status mapping
        if 'status' in update_data or 'Status' in update_data:
            status_value = update_data.get('status') or update_data.get('Status')
            update_fields['status'] = status_value
            update_fields['Status'] = status_value
        
        protected_fields = [
            'eventName', 'Event Name', 'Day', 'day', 'date', 'Date Of Event', 
            'Time', 'time', 'Address', 'location', 'Email', 'eventLeaderEmail', 
            'status', 'Status',
            'persistent_attendees', 
            'attendees',            
            'attendance',           
            '_id', 'id', 'UUID',     
            'created_at',            
            'total_attendance'      
        ]
        
        for key, value in update_data.items():
            if key not in protected_fields and key not in update_fields:
                update_fields[key] = value
        
        update_fields["updated_at"] = datetime.utcnow()
        
        for key, value in update_fields.items():
            if 'time' in key.lower() or 'Time' in key:
                print(f"  {key}: {value} (type: {type(value)})")
                
        if update_fields.get("deactivation_end",""):
            print("yay!")
            update_fields["deactivation_end"] = datetime.strptime( update_fields["deactivation_end"], "%Y-%m-%dT%H:%M:%S.%f")
        print(f"Updating with: {update_fields}")
        print(f"Protected fields excluded: persistent_attendees, attendees, attendance")
        
        # Update all matching events
        result = await events_collection.update_many(
            strict_query,
            {"$set": update_fields}
        )
        
        print(f"Updated: matched {result.matched_count}, modified {result.modified_count}")
        
        # Fetch and return one updated event to verify
        updated_event = await events_collection.find_one(strict_query)

        return {
            "success": True,
            "message": f"Updated {result.modified_count} {decoded_day} events named '{decoded_event}'",
            "matched_count": len(matching_events),
            "modified_count": result.modified_count,
            "person": decoded_person,
            "original_event_name": decoded_event,
            "original_day": decoded_day,
            "new_event_name": update_fields.get('Event Name'),
            "new_day": update_fields.get('Day'),
            "sample_time_stored": updated_event.get('time') if updated_event else None
        }
        
    except Exception as e:
        print(f"Error updating events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

#----------------Deactivate cells Endpoints------------
@router.put("/events/deactivate")
async def deactivate_event(
    cell_identifier: str = Query(..., description="Cell name or Person name"),
    weeks: int = Query(..., description="Number of weeks to deactivate (1-12)"),
    reason: Optional[str] = Query(None, description="Reason for deactivation"),
    person_name: Optional[str] = Query(None, description="Person name (if cell_identifier is a cell name)"),
    day_of_week: Optional[str] = Query(None, description="Specific day to deactivate (e.g., 'Wednesday')"),
    is_permanent_deact: bool = Query(None,description="Determines whether it is a permanent or a temporary deactivation"),
):
    try:
        current_time = datetime.utcnow()
        deactivation_end = current_time + timedelta(weeks=weeks)
        print("BOOL",is_permanent_deact)
        updates = {
            "is_active": False,
            "deactivation_start": current_time,
            "deactivation_end": datetime.strptime(str(deactivation_end),"%Y-%m-%d %H:%M:%S.%f"),
            "deactivation_reason": reason,
            "last_status_change": current_time,
            "is_permanent_deact":is_permanent_deact
        }
         
        query = {"$or": []}
        print(cell_identifier, person_name)
        
        if person_name:
            query["$or"].append({
                "$and": [
                    {"$or": [
                        {"eventName": cell_identifier},
                        {"Event Name": cell_identifier}
                    ]},
                    {"$or": [
                        {"eventLeader": person_name},
                        {"Leader": person_name},
                        {"eventLeaderName": person_name}
                    ]}
                ]
            })
        else:
            query["$or"].append({
                "$and": [
                     {"$or": [
                        {"eventName": cell_identifier},
                        {"Event Name": cell_identifier}
                    ]},
                    {"$or": [
                        {"eventLeader": cell_identifier},
                        {"Leader": cell_identifier},
                        {"eventLeaderName": cell_identifier}
                    ]}
                ]
            })
        print("QUERY", query)
        # Add day filter if specified
        if day_of_week:
            if "$or" in query and len(query["$or"]) > 0:
                for i in range(len(query["$or"])):
                    if "$and" in query["$or"][i]:
                        query["$or"][i]["$and"].append(
                            {"$or": [
                                {"Day": day_of_week},
                                {"recurring_day": day_of_week}
                            ]}
                        )
        
        print(f"DEBUG: Query length: {len(str(query))}")  
        
        result = await events_collection.update_many(query, {"$set": updates})
        
        if result.modified_count == 0:
            simple_query = {
                "$or": [
                    {"eventLeader": cell_identifier},
                    {"Leader": cell_identifier},
                    {"eventLeaderName": cell_identifier}
                ]
            }
            
            if day_of_week:
                simple_query["$or"].append({"Day": day_of_week})
                simple_query["$or"].append({"recurring_day": day_of_week})
            
            result = await events_collection.update_many(simple_query, {"$set": updates})
            
            if result.modified_count == 0:
                raise HTTPException(status_code=404, detail="No cells found")
        
        return {
            "success": True,
            "message": f"{result.modified_count} cell(s) deactivated for {weeks} week(s)",
            "weeks": weeks,
            "deactivation_end": deactivation_end.isoformat(),
            "cell_count": result.modified_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/cells/reactivate")
async def reactivate_cell(
    cell_identifier: str = Query(..., description="Cell name or Person name"),
    person_name: Optional[str] = Query(None, description="Person name (if cell_identifier is a cell name)"),
    day_of_week: Optional[str] = Query(None, description="Specific day to reactivate")
):
    try:
        current_time = datetime.utcnow()
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }
        
        query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": "cells"},
                        {"Event Type": "cells"}
                    ]
                },
                {"is_active": False}
            ]
        }
        
        if person_name:
            query["$and"].append({
                "$or": [
                    {"eventName": cell_identifier},
                    {"Event Name": cell_identifier}
                ]
            })
            query["$and"].append({
                "$or": [
                    {"eventLeader": person_name},
                    {"Leader": person_name},
                    {"eventLeaderName": person_name}
                ]
            })
        else:
            query["$and"].append({
                "$or": [
                    {"eventLeader": cell_identifier},
                    {"Leader": cell_identifier},
                    {"eventLeaderName": cell_identifier}
                ]
            })
        
        if day_of_week:
            query["$and"].append({
                "$or": [
                    {"Day": day_of_week},
                    {"recurring_day": day_of_week}
                ]
            })
        
        result = await events_collection.update_many(query, {"$set": updates})
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="No deactivated cells found")
        
        return {
            "success": True,
            "message": f"{result.modified_count} cell(s) reactivated",
            "cell_count": result.modified_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def auto_reactivate_expired_events():
    try:
        current_time = datetime.utcnow()
        
        
        query = {
            "$and": [
                {"is_active": False},
                {"deactivation_end": {"$lte": current_time, "$ne": None}},
                {"$or":[{"is_permanent_deact":{"$ne":True}}]}
            ]
        }
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }
        
        result = await events_collection.update_many(query, {"$set": updates})
        print(result)
        if result.modified_count > 0:
            print(f"Auto-reactivated {result.modified_count} cells")
            
    except Exception as e:
        print(f"Auto-reactivation error: {e}")


scheduler = AsyncIOScheduler()    
scheduler.add_job(auto_reactivate_expired_events,'cron',hour=0,minute=0) 
scheduler.start()
sleep(10)


@router.post("/event-types")
async def create_event_type(event_type: EventTypeCreate, current_user: dict = Depends(get_current_user)):
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        # Convert to title case (first letter of each word uppercase)
        name = event_type.name.strip().title()
        name_lower = name.lower()  # Keep lowercase version for regex checks

        # Check for reserved keywords (case insensitive)
        if re.search(r'\bcell[s]?\b', name_lower) or 'cell' in name_lower:
            raise HTTPException(
                status_code=400,
                detail="Event types containing 'cell' or 'cells' are reserved and cannot be created."
            )

        org_id = current_user.get("org_id") or (current_user.get("organization", "").lower().replace(" ", "-")) or "active-teams"
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        # Check for existing event type (case insensitive)
        existing = await events_collection.find_one({
            "$or": [
                {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
            ],
            "isEventType": True,
            "org_id": org_id
        })

        if existing:
            raise HTTPException(status_code=400, detail=f"Event type '{name}' already exists")

        event_type_data = {
            "name": name,  # Now stored in title case
            "eventType": name,  # Now stored in title case
            "eventTypeName": name,  # Now stored in title case
            "description": event_type.description.strip(),
            "isEventType": True,
            "isTicketed": event_type.isTicketed if hasattr(event_type, 'isTicketed') else False,
            "isGlobal": event_type.isGlobal if hasattr(event_type, 'isGlobal') else False,
            "hasPersonSteps": event_type.hasPersonSteps if hasattr(event_type, 'hasPersonSteps') else False,
            "org_id": org_id,
            "Organization": organization,
            "UUID": str(uuid.uuid4()),
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }

        if event_type_data.get("isGlobal") is None:
            event_type_data["isGlobal"] = "global" in name_lower

        if event_type_data.get("hasPersonSteps") is None:
            event_type_data["hasPersonSteps"] = any(
                keyword in name_lower for keyword in ["person", "individual"]
            )

        result = await events_collection.insert_one(event_type_data)
        inserted = await events_collection.find_one({"_id": result.inserted_id})
        inserted["_id"] = str(inserted["_id"])

        print(f"Created event type: {name} for org: {org_id}")

        return inserted

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating event type: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event type: {str(e)}")


@router.get("/org-config")
async def get_org_config(current_user: dict = Depends(get_current_user)):
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        print(f"ORG CONFIG REQUEST - email: {current_user.get('email')} | org_id in token: {current_user.get('org_id')} | derived org_id: {org_id}")

        config = await org_config_collection.find_one({"_id": org_id})
        print(f"Config found: {config is not None}")  

        if config is None:
            raise HTTPException(status_code=404, detail=f"No org config found for org_id: {org_id}")
        
        config["org_id"] = str(config["_id"])
        config.pop("_id", None)
        return config

    except Exception as e:
        print(f"ORG CONFIG ERROR: {str(e)}") 
        import traceback
        traceback.print_exc()  
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/event-types/{event_type_name}")
async def update_event_type(
    event_type_name: str,
    updated_data: EventTypeCreate = Body(...)
):
    try:
        decoded_event_type_name = unquote(event_type_name)
        
        # Check if event type exists
        existing_event_type = await events_collection.find_one({
            "name": {"$regex": f"^{decoded_event_type_name}$", "$options": "i"},
            "isEventType": True
        })
        
        if not existing_event_type:
            try:
                existing_event_type = await events_collection.find_one({
                    "_id": ObjectId(decoded_event_type_name),
                    "isEventType": True
                })
            except:
                pass
            
            if not existing_event_type:
                raise HTTPException(status_code=404, detail=f"Event type '{decoded_event_type_name}' not found")

        # Convert name to title case (first letter of each word uppercase)
        new_name = updated_data.name.strip().title()
        current_name = existing_event_type["name"]
        name_changed = new_name.lower() != current_name.lower()
        
        # Check if isGlobal is being changed
        current_is_global = existing_event_type.get("isGlobal", False)
        new_is_global = updated_data.isGlobal if updated_data.isGlobal is not None else False
        is_global_changed = current_is_global != new_is_global
        
        # Check for duplicate names (case insensitive)
        if name_changed:
            duplicate = await events_collection.find_one({
                "name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"},
                "isEventType": True,
                "_id": {"$ne": existing_event_type["_id"]}
            })
            if duplicate:
                raise HTTPException(status_code=400, detail="Event type with this name already exists")
        
        events_updated_count = 0
        if name_changed or is_global_changed:
            # Build base query
            update_query = {
                "$or": [
                    {"eventType": current_name},
                    {"eventTypeName": current_name}
                ],
                "isEventType": {"$ne": True}
            }
            
            # Build update fields
            update_fields = {
                "updatedAt": datetime.utcnow()
            }
            
            if name_changed:
                update_fields["eventType"] = new_name
                update_fields["eventTypeName"] = new_name
            
            if is_global_changed:
                # Find events that don't have explicit isGlobal set
                events_without_explicit_isglobal = await events_collection.find({
                    **update_query,
                    "$or": [
                        {"isGlobal": {"$exists": False}},
                        {"isGlobal": None},
                        {"isGlobal": ""},
                        {"isGlobal": current_is_global}
                    ]
                }).to_list(length=None)
                
                events_updated_count = len(events_without_explicit_isglobal)
                
                if events_updated_count > 0:
                    update_fields["isGlobal"] = new_is_global
            
            # Apply the update
            if name_changed or (is_global_changed and events_updated_count > 0):
                await events_collection.update_many(
                    update_query,
                    {"$set": update_fields}
                )

        # Prepare update data
        update_data_dict = updated_data.dict()
        update_data_dict["name"] = new_name
        update_data_dict["eventType"] = new_name 
        update_data_dict["eventTypeName"] = new_name  
        update_data_dict["updatedAt"] = datetime.utcnow()
        
        update_data_dict = {k: v for k, v in update_data_dict.items() if v is not None}
        
        immutable_fields = ["_id", "UUID", "createdAt", "isEventType"]
        for field in immutable_fields:
            update_data_dict.pop(field, None)

        # Update the event type document
        result = await events_collection.update_one(
            {"_id": existing_event_type["_id"]},
            {"$set": update_data_dict}
        )

        if result.modified_count == 0:
            existing_event_type["_id"] = str(existing_event_type["_id"])
            return existing_event_type

        updated_event_type = await events_collection.find_one({"_id": existing_event_type["_id"]})
        updated_event_type["_id"] = str(updated_event_type["_id"])
        
        return updated_event_type

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating event type: {str(e)}")

@router.delete("/event-types/{event_type_name}")
async def delete_event_type(
    event_type_name: str,
    force: bool = Query(False, description="Force delete even if events exist")
):
    try:
        decoded_event_type_name = unquote(event_type_name)
       
        print(f" DELETE EVENT TYPE: {decoded_event_type_name}, force={force}")
       
        existing_event_type = await events_collection.find_one({
            "$or": [
                {"name": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}}
            ],
            "isEventType": True
        })
       
        if not existing_event_type:
            print(f" Event type '{decoded_event_type_name}' not found")
            raise HTTPException(
                status_code=404,
                detail=f"Event type '{decoded_event_type_name}' not found"
            )
       
        actual_identifier = (
            existing_event_type.get("name") or
            existing_event_type.get("eventType") or
            existing_event_type.get("eventTypeName")
        )
        
        # PREVENT DELETION OF "CELLS" EVENT TYPE (BUILT-IN)
        actual_identifier_lower = actual_identifier.lower()
        if any(keyword in actual_identifier_lower for keyword in ["cell", "cells"]):
            raise HTTPException(
                status_code=400,
                detail=f"'{actual_identifier}' is a reserved built-in event type and cannot be modified or deleted."
            )
       
        print(f" Found event type: {actual_identifier}")
       
        events_query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"eventType": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}}
                    ]
                },
                {"isEventType": {"$ne": True}},
                {"$or": [
                    {"eventName": {"$exists": True}},
                    {"Event Name": {"$exists": True}},
                    {"date": {"$exists": True}},
                    {"Date Of Event": {"$exists": True}}
                ]}
            ]
        }
       
        print(f" Searching for events with query: {events_query}")
       
        events_using_type = await events_collection.find(events_query).to_list(length=None)
        events_count = len(events_using_type)
       
        print(f" Found {events_count} events using '{actual_identifier}'")
       
        if events_count > 0:
            event_details = []
            for event in events_using_type[:20]: 
                detail = {
                    "id": str(event["_id"]),
                    "name": event.get("eventName") or event.get("Event Name", "Unnamed"),
                    "type": event.get("eventType") or event.get("Event Type"),
                    "typeName": event.get("eventTypeName"),
                    "date": str(event.get("date") or event.get("Date Of Event", "")),
                    "leader": event.get("eventLeaderName") or event.get("Leader", ""),
                    "status": event.get("status", "unknown")
                }
                event_details.append(detail)
                print(f"  Event: {detail['name']} (ID: {detail['id']}, Status: {detail['status']})")
           
            if not force:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": f"Cannot delete event type '{actual_identifier}': {events_count} event(s) are using it.",
                        "events_count": events_count,
                        "event_samples": event_details,
                        "suggestion": "Please delete these events first, or use force=true to delete everything"
                    }
                )
            else:
                print(f" FORCE DELETE: Deleting {events_count} events...")
               
                delete_result = await events_collection.delete_many(events_query)
                print(f" Deleted {delete_result.deleted_count} events")
       
        result = await events_collection.delete_one({"_id": existing_event_type["_id"]})
       
        if result.deleted_count == 1:
            print(f" Event type '{actual_identifier}' deleted successfully")
            return {
                "success": True,
                "message": f"Event type '{actual_identifier}' deleted successfully",
                "events_deleted": events_count if force else 0
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to delete event type from database"
            )
           
    except HTTPException:
        raise
    except Exception as e:
        print(f" Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting event type: {str(e)}"
        )
   

@router.get("/diagnostic/event-type-usage/{event_type_name}")
async def check_event_type_usage(
    event_type_name: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Diagnostic endpoint to see all events using a specific event type
    """
    try:
        # Only allow admins to use this
        user_role = current_user.get("role", "").lower()
        if user_role != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")
       
        decoded_name = unquote(event_type_name)
       
        print(f" DIAGNOSTIC: Checking usage of event type: {decoded_name}")
       
        # Search for the event type definition
        event_type_doc = await events_collection.find_one({
            "$or": [
                {"name": {"$regex": f"^{re.escape(decoded_name)}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{re.escape(decoded_name)}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{re.escape(decoded_name)}$", "$options": "i"}}
            ],
            "isEventType": True
        })
       
        if not event_type_doc:
            return {
                "event_type_exists": False,
                "message": f"Event type '{decoded_name}' not found",
                "events_using_it": []
            }
       
        actual_name = (
            event_type_doc.get("name") or
            event_type_doc.get("eventType") or
            event_type_doc.get("eventTypeName")
        )
       
        print(f" Found event type definition: {actual_name}")
       
        events_query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": {"$regex": f"^{re.escape(actual_name)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(actual_name)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(actual_name)}$", "$options": "i"}},
                    ]
                },
                {"isEventType": {"$ne": True}},
                {"$or": [
                    {"eventName": {"$exists": True}},
                    {"Event Name": {"$exists": True}}
                ]}
            ]
        }
       
        events = await events_collection.find(events_query).to_list(length=None)
       
        print(f" Found {len(events)} events using '{actual_name}'")
       
        # Get detailed info about each event
        event_details = []
        for event in events:
            detail = {
                "_id": str(event["_id"]),
                "eventName": event.get("eventName") or event.get("Event Name"),
                "eventType": event.get("eventType") or event.get("Event Type"),
                "eventTypeName": event.get("eventTypeName"),
                "date": str(event.get("date") or event.get("Date Of Event", "")),
                "eventLeaderName": event.get("eventLeaderName") or event.get("Leader"),
                "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email"),
                "status": event.get("status"),
                "Status": event.get("Status"),
                "did_not_meet": event.get("did_not_meet"),
                "attendees_count": len(event.get("attendees", [])),
                "isEventType": event.get("isEventType", False),
                "all_type_fields": {
                    "Event Type": event.get("Event Type"),
                    "eventType": event.get("eventType"),
                    "eventTypeName": event.get("eventTypeName")
                }
            }
            event_details.append(detail)
            print(f"   {detail['eventName']} - {detail['date']} - Status: {detail['status']}")
       
        return {
            "event_type_exists": True,
            "event_type_name": actual_name,
            "event_type_id": str(event_type_doc["_id"]),
            "events_count": len(events),
            "events": event_details,
            "query_used": str(events_query)
        }
       
    except HTTPException:
        raise
    except Exception as e:
        print(f" Error in diagnostic: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Diagnostic error: {str(e)}")
 

@router.get("/events/global")
async def get_global_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    last_updated: Optional[str] = Query(None)  
):
    """
    Get Global Events (like Sunday Service) with real-time updates
    Shows events where isGlobal = True
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
       
        
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
       
        print(f"Fetching Global Events from {start_date_obj}")
       
        
        query = {
            "isGlobal": True,
            "eventTypeName": "Global Events"
        }
       
        
        if last_updated:
            try:
                last_updated_dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                query["$or"] = [
                    {"created_at": {"$gte": last_updated_dt}},
                    {"updated_at": {"$gte": last_updated_dt}}
                ]
                print(f"Real-time update: fetching events since {last_updated}")
            except Exception as e:
                print(f"Error parsing last_updated: {e}")
       
        
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
       
        print(f"Query for Global Events: {query}")
       
        
        cursor = events_collection.find(query).sort([("created_at", -1), ("date", -1)])
        all_events = await cursor.to_list(length=None)
       
        print(f"Found {len(all_events)} raw global events")
       
        
        latest_timestamp = None
        if all_events:
            
            timestamps = []
            for event in all_events:
                created = event.get("created_at")
                updated = event.get("updated_at")
                if created:
                    timestamps.append(created if isinstance(created, datetime) else datetime.fromisoformat(created.replace("Z", "+00:00")))
                if updated:
                    timestamps.append(updated if isinstance(updated, datetime) else datetime.fromisoformat(updated.replace("Z", "+00:00")))
           
            if timestamps:
                latest_timestamp = max(timestamps)
                print(f" Latest event timestamp: {latest_timestamp}")
       
        
        processed_events = []
        new_events_count = 0
       
        for event in all_events:
            try:
                is_new_event = False
                if last_updated:
                    event_created = event.get("created_at")
                    event_updated = event.get("updated_at")
                   
                    if event_created:
                        if isinstance(event_created, datetime):
                            created_dt = event_created
                        else:
                            created_dt = datetime.fromisoformat(event_created.replace("Z", "+00:00"))
                       
                        if created_dt > last_updated_dt:
                            is_new_event = True
                            new_events_count += 1
               
                
                event_date_field = event.get("date")
                if isinstance(event_date_field, datetime):
                    event_date = event_date_field.date()
                elif isinstance(event_date_field, str):
                    try:
                        event_date = datetime.fromisoformat(
                            event_date_field.replace("Z", "+00:00")
                        ).date()
                    except Exception:
                        event_date = today_date
                else:
                    event_date = today_date
               
                print(f"  Event date: {event_date}, Start date filter: {start_date_obj}")
               
                
                if event_date < start_date_obj:
                    print(f"   Skipped - before date range")
                    continue
               
                
                event_name = event.get("Event Name") or event.get("eventName", "")
                leader_name = event.get("Leader") or event.get("eventLeader", "")
                location = event.get("Location") or event.get("location", "")
               
                
                
                did_not_meet = event.get("did_not_meet", False)
               
                
                stored_status = event.get("status") or event.get("Status")
               
                print(f"  Status determination: did_not_meet={did_not_meet}, stored_status={stored_status}")
               
                if did_not_meet:
                    event_status = "did_not_meet"
                    status_display = "Did Not Meet"
                elif stored_status:
                    
                    event_status = str(stored_status).lower()
                    status_display = str(stored_status).replace("_", " ").title()
                else:
                    
                    
                    event_status = "open"
                    status_display = "Open"
               
                print(f"  ✓ Final status: {event_status}")
               
                
                if status and status != 'all' and status != event_status:
                    print(f"   Skipped - status filter: requested={status}, actual={event_status}")
                    continue
                
                
                attendees_data = event.get("attendees", []) if isinstance(event.get("attendees", []), list) else []
                new_people_data = event.get("new_people", []) if isinstance(event.get("new_people", []), list) else []
                consolidations_data = event.get("consolidations", []) if isinstance(event.get("consolidations", []), list) else []
                
                print(f"  Data arrays - attendees: {len(attendees_data)}, new_people: {len(new_people_data)}, consolidations: {len(consolidations_data)}")
               
                
                final_event = {
                    "_id": str(event.get("_id", "")),
                    "eventName": event_name,
                    "eventType": "Global Events",
                    "eventLeaderName": leader_name,
                    "eventLeaderEmail": event.get("Email") or event.get("userEmail", ""),
                    "day": event.get("Day", ""),
                    "date": event_date.isoformat(),
                    "time": event.get("time", ""),
                    "location": location,
                    "description": event.get("description", ""),
                    
                    "attendees": attendees_data,
                    "new_people": new_people_data,
                    "consolidations": consolidations_data,
                    
                    "did_not_meet": did_not_meet,
                    "status": event_status,
                    "Status": status_display,
                    "_is_overdue": event_date < today_date and event_status == "incomplete",
                    "isGlobal": True,
                    "isTicketed": event.get("isTicketed", False),
                    "priceTiers": event.get("priceTiers", []),
                    "total_attendance": event.get("total_attendance", 0),
                    "UUID": event.get("UUID", ""),
                    "created_at": event.get("created_at"),
                    "updated_at": event.get("updated_at"),
                    "_is_new": is_new_event,  
                    
                    "closed_by": event.get("closed_by"),
                    "closed_at": event.get("closed_at")
                }
                
                if event.get('time'):
                    final_event['time'] = event.get('time')
                if event.get('Time'):
                    final_event['Time'] = event.get('Time')
               
                processed_events.append(final_event)
                print(f"  Event added to processed list")
               
            except Exception as e:
                print(f"Error processing global event {event.get('_id')}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
       
        print(f"Processed {len(processed_events)} global events after filtering")
        print(f"🆕 New events since last update: {new_events_count}")
       
        
        processed_events.sort(key=lambda x: x['date'], reverse=True)
       
        
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet"),
            "open": sum(1 for e in processed_events if e["status"] == "open"),
            "closed": sum(1 for e in processed_events if e["status"] == "closed")  
        }
       
        print(f"Global Events Status - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}, Open: {status_counts['open']}, Closed: {status_counts['closed']}")
       
        
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
       
        print(f"Returning page {page}/{total_pages}: {len(paginated_events)} global events")
       
        return {
            "events": paginated_events,
            "total_events": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit,
            "status_counts": status_counts,
            "date_range": {
                "start_date": start_date_filter,
                "end_date": today_date.isoformat()
            },
            
            "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None,
            "has_new_events": new_events_count > 0,
            "new_events_count": new_events_count,
            "polling_suggestion": "Use 'last_updated' parameter for real-time updates"
        }
       
    except Exception as e:
        print(f"ERROR in get_global_events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching global events: {str(e)}")

@router.get("/events/global/status-counts")
async def get_global_events_status_counts(
    current_user: dict = Depends(get_current_user),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None)
):
    """Get status counts for Global Events"""
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
       
        
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
       
        
        query = {
            "isGlobal": True,
            "eventType": "Global Events"
        }
       
        
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
       
        
        cursor = events_collection.find(query)
        all_events = await cursor.to_list(length=None)
       
        
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
       
        for event in all_events:
            try:
                
                event_date_field = event.get("date")
                if isinstance(event_date_field, datetime):
                    event_date = event_date_field.date()
                elif isinstance(event_date_field, str):
                    try:
                        event_date = datetime.fromisoformat(
                            event_date_field.replace("Z", "+00:00")
                        ).date()
                    except Exception:
                        event_date = today_date
                else:
                    event_date = today_date
               
                
                if event_date < start_date_obj:
                    continue
               
                
                did_not_meet = event.get("did_not_meet", False)
                attendees = event.get("attendees", [])
                has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
               
                if did_not_meet:
                    did_not_meet_count += 1
                elif has_attendees:
                    complete_count += 1
                else:
                    incomplete_count += 1
                   
            except Exception:
                continue
       
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": incomplete_count + complete_count + did_not_meet_count,
            "date_range": {
                "start_date": start_date_filter,
                "end_date": today_date.isoformat()
            }
        }
       
    except Exception as e:
        print(f"ERROR in global events status counts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
   
@router.put("/events/{event_id}")
async def update_event(event_id: str, event_data: dict, current_user: dict = Depends(get_current_user)):
    """
    FIXED: Update event by _id or UUID
    Now properly updates status for ALL users (bidirectional fix)
    """
    try:
        print(f"Attempting to update event with ID: {event_id}")
        print(f" Received data: {event_data}")
        print(f" Updated by user: {current_user.get('email')} with role: {current_user.get('role')}")
       
        event = None
       
        # Try as MongoDB ObjectId
        if ObjectId.is_valid(event_id):
            try:
                event = await events_collection.find_one({"_id": ObjectId(event_id)})
                if event:
                    print(f"Found event by _id: {event_id}")
            except Exception as e:
                print(f"Could not find by ObjectId: {e}")
       
        # If not found, try by UUID
        if not event:
            event = await events_collection.find_one({"UUID": event_id})
            if event:
                print(f"Found event by UUID: {event_id}")
       
        # If still not found, return 404
        if not event:
            print(f"Event not found with identifier: {event_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Event not found with identifier: {event_id}"
            )
       
        # =========== FIX: Check if status is being updated ===========
        is_status_update = False
        new_status = None
        old_status = event.get('status') or event.get('Status')
        
        # Check both 'status' and 'Status' fields
        if 'status' in event_data and event_data['status'] is not None:
            new_status = event_data['status']
            is_status_update = True
            print(f"Status update detected: {old_status} -> {new_status}")
        elif 'Status' in event_data and event_data['Status'] is not None:
            new_status = event_data['Status']
            is_status_update = True
            print(f"Status update detected: {old_status} -> {new_status}")
       
        # Prepare update data
        update_data = {}
       
        # Fields that can be updated
        updatable_fields = [
            'eventName', 'day', 'location', 'date',
            'status', 'renocaming', 'eventLeader',
            'eventType', 'isTicketed', 'isGlobal',
            'priceTiers'
        ]
       
        for field in updatable_fields:
            if field in event_data and event_data[field] is not None:
                update_data[field] = event_data[field]
       
        if is_status_update and new_status:
            update_data['status'] = new_status
            update_data['Status'] = new_status
            
            update_data['last_updated_by'] = {
                "email": current_user.get('email'),
                "name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                "role": current_user.get('role'),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            print(f"Updated status fields for ALL users: {new_status}")
            print(f"Updated by: {current_user.get('email')} ({current_user.get('role')})")
            
            if new_status in ['complete', 'did_not_meet']:
                try:
                    event_date_field = (
                        event_data.get("date")
                        or event_data.get("Date Of Event")
                        or event.get("date")
                        or event.get("Date Of Event")
                    )
                    event_date = None
                    
                    if isinstance(event_date_field, datetime):
                        event_date = event_date_field.date()
                    elif isinstance(event_date_field, date):
                        event_date = event_date_field
                    elif isinstance(event_date_field, str):
                        try:
                            event_date = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
                        except Exception:
                            try:
                                event_date = datetime.strptime(event_date_field, "%Y-%m-%d").date()
                            except Exception:
                                event_date = None
                    
                    if event_date is None:
                        print("Skipping attendance update: event date is missing or unparseable")
                    else:
                        exact_date_str = event_date.strftime("%Y-%m-%d")  
                        
                        attendance_field = f"attendance.{exact_date_str}.status"
                        update_data[attendance_field] = new_status
                        update_data[f"attendance.{exact_date_str}.updated_by_external"] = {
                            "email": current_user.get('email'),
                            "role": current_user.get('role'),
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        
                        print(f"Also updated date-based attendance ({exact_date_str}) to: {new_status}")
                except Exception as e:
                    print(f"Note: Could not update date-based attendance: {e}")
        
        # Add update timestamp
        update_data['updated_at'] = datetime.utcnow()
       
        print(f"Updating with data: {update_data}")
       
        # Perform the update
        result = await events_collection.update_one(
            {"_id": event["_id"]},  # Always use the found event's _id
            {"$set": update_data}
        )
       
        if result.modified_count == 0:
            print(f"No changes made to event {event_id}")
        else:
            print(f"Event {event_id} updated successfully")
            
            # =========== FIX: Log the synchronization ===========
            if is_status_update:
                print(f"STATUS SYNCHRONIZED: Event {event_id} status changed to {new_status}")
                print(f"  - Changed by: {current_user.get('email')} ({current_user.get('role')})")
                print(f"  - Old status: {old_status}")
                print(f"  - New status: {new_status}")
                print(f"  - Will be visible to ALL users immediately")
       
        # Fetch and return the updated event
        updated_event = await events_collection.find_one({"_id": event["_id"]})
        updated_event["_id"] = str(updated_event["_id"])
        
        # =========== FIX: Return synchronization info ===========
        response_data = {
            **updated_event,
            "sync_info": {
                "status_updated": is_status_update,
                "new_status": new_status,
                "updated_by": current_user.get('email'),
                "updated_by_role": current_user.get('role'),
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Status synchronized for ALL users" if is_status_update else "Event updated"
            }
        }
       
        return response_data
       
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating event: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error updating event: {str(e)}"
        )     


@router.get("/events/cells-user-fixed")
async def get_user_cell_events_fixed_future(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query(None)
):
    """FIXED: Shows cells with proper deduplication"""
    try:
        email = current_user.get("email")
        role = current_user.get("role", "user").lower()
       
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        start_date_obj = datetime.strptime(start_date or "2025-10-20", "%Y-%m-%d").date()
       
        print(f"Fetching cells for user: {email} (role: {role})")
        print(f"Date range: {start_date_obj} onwards")
        print(f"Personal filter: {personal}")

        # Build query based on role and personal filter
        query = {"Event Type": "Cells"}
       
        # Apply role-based filtering
        if role == "admin" and not personal:
            # Admin with "View All" - no email filter
            print("ADMIN VIEW ALL - Showing all cells")
            pass  # No additional filters
        else:
            # Everyone else OR admin with personal filter
            user_cell = await events_collection.find_one({
                "Event Type": "Cells",
                "$or": [
                    {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                    {"email": {"$regex": f"^{email}$", "$options": "i"}},
                ]
            })

            user_name = user_cell.get("Leader", "").strip() if user_cell else ""
           
            query_conditions = [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
           
            if user_name:
                query_conditions.extend([
                    {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                    {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                    {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
                ])
           
            query["$or"] = query_conditions

        # Add event type filter
        if event_type and event_type != 'all':
            query["eventType"] = event_type

        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"Leader": search_regex},
                {"Email": search_regex}
            ]

        # USE AGGREGATION WITH $GROUP TO REMOVE DUPLICATES
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": "$_id",  # Group by unique MongoDB _id
                    "doc": {"$first": "$$ROOT"}  # Take first occurrence
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"Day": 1, "Leader": 1}}
        ]

        cursor = events_collection.aggregate(pipeline)
        all_cells_raw = await cursor.to_list(length=None)
       
        print(f"Found {len(all_cells_raw)} unique cells after deduplication")

        # Process events
        processed_events = []
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
       
        for event in all_cells_raw:
            try:
                event_name = str(event.get("Event Name", "")).strip()
                day = str(event.get("Day", "")).strip().lower()
               
                if day not in day_mapping:
                    continue
               
                # Calculate next occurrence
       
               # Compute current-week instance (Monday..Sunday week)
                # Resolve target weekday from the stored 'day' field (not a missing var)
                target_weekday = day_mapping.get(day)
                if target_weekday is None:
                    # invalid or missing day -> skip this event
                    continue
                # Use today_date (date) consistently in this function
                days_since_monday = today_date.weekday()
                week_start = today_date - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)
                 
                # Choose the most relevant occurrence not in the future
                if current_week_instance > today_date:
                    next_occurrence = current_week_instance - timedelta(weeks=1)
                else:
                    next_occurrence = current_week_instance
                # Ensure within requested start_date (don't return occurrences older than start_date_obj)
                if next_occurrence < start_date_obj:
                    # find first occurrence on/after start_date_obj (but not in the future)
                    days_since_start = start_date_obj.weekday()
                    start_week_start = start_date_obj - timedelta(days=days_since_start)
                    candidate = start_week_start + timedelta(days=target_weekday)
                    if candidate > today_date:
                        next_occurrence = candidate - timedelta(weeks=1)
                    else:
                        next_occurrence = candidate

                # Get leader info
                leader_name = event.get("Leader", "").strip()
                leader_at_12 = event.get("Leader @12", event.get("Leader at 12", "")).strip()
               
                # FIX: Get persistent_attendees from the event
                persistent_attendees = event.get("persistent_attendees", [])
               
                # Determine status
                did_not_meet = event.get("did_not_meet", False)
                attendees = event.get("attendees", [])
               
                if did_not_meet:
                    status_val = "did_not_meet"
                elif attendees:
                    status_val = "complete"
                else:
                    status_val = "incomplete"
               
                # Apply status filter
                if status and status != 'all' and status != status_val:
                    continue

                # Build event object
                final_event = {
                    "_id": str(event.get("_id", "")),
                    "eventName": event_name,
                    "eventType": event.get("eventType", "Cells"),
                    "eventLeaderName": leader_name,
                    "eventLeaderEmail": str(event.get("Email", "")).strip(),
                    "leader1": event.get("leader1", ""),
                    "leader12": leader_at_12,
                    "leader144": event.get("Leader @144", event.get("Leader at 144", "")),
                    "day": day.capitalize(),
                    "date": next_occurrence.isoformat(),
                    "location": event.get("Location", ""),
                    "attendees": attendees,
                    "persistent_attendees": persistent_attendees,  # ADD THIS
                    "did_not_meet": did_not_meet,
                    "status": status_val,
                    "Status": status_val.replace("_", " ").title(),
                    "_is_overdue": next_occurrence < today_date
                }
               
                processed_events.append(final_event)
                print(f"Processed {event_name}: {len(persistent_attendees)} persistent attendees")
               
            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {str(e)}")
                continue

        if event.get('time'):
            final_event['time'] = event.get('time')
        if event.get('Time'):
            final_event['Time'] = event.get('Time')     
            
        # Sort by date
        processed_events.sort(key=lambda x: x['date'])

        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]

        print(f"Returning {len(paginated_events)} events (page {page} of {total_pages})")

        return {
            "events": paginated_events,
            "total_events": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit,
            "today": today_date.isoformat(),
            "start_date": start_date_obj.isoformat()
        }
       
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")

@router.get("/events/cells/optimized")
async def get_cell_events_optimized(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query('2025-11-30'),
    leader_at_12_view: Optional[bool] = Query(None),
    show_personal_cells: Optional[bool] = Query(None),
    show_all_authorized: Optional[bool] = Query(None),
):
    try:
        user_email = current_user.get("email", "")
        role = current_user.get("role", "user").lower()
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        
        is_leader_at_12 = (
            "leaderat12" in role or 
            "leader at 12" in role or
            "leader@12" in role or
            role == "leaderat12" or
            leader_at_12_view
        )
        
        query = {"Event Type": "Cells"}
        
        if search and search.strip():
            search_term = search.strip()
            query["$or"] = [
                {"Event Name": {"$regex": search_term, "$options": "i"}},
                {"Leader": {"$regex": search_term, "$options": "i"}},
                {"Email": {"$regex": search_term, "$options": "i"}},
            ]
        
        if role == "admin":
            if personal or show_personal_cells:
                query["Email"] = user_email
        elif is_leader_at_12:
            want_personal = (show_personal_cells or personal)
            want_disciples = (show_all_authorized)
            
            if want_personal and not want_disciples:
                query["Email"] = user_email
            elif want_disciples and not want_personal:
                query["Leader @12"] = user_name
                query["Email"] = {"$ne": user_email}
            else:
                query["$or"] = [
                    {"Email": user_email},
                    {"Leader @12": user_name}
                ]
        else:
            query["Email"] = user_email

        cursor = events_collection.find(query)
        all_cells = await cursor.to_list(length=None)
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone).date()
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        cell_instances = []
        for cell in all_cells:
            try:
                day_name = str(cell.get("Day", "")).strip().lower()
                if day_name not in day_mapping:
                    continue
                
                target_weekday = day_mapping.get(day_name)
                if target_weekday is None:
                    continue
                attendance_data = cell.get("attendance", {})
                weeks_to_check = 1 if status == "incomplete" else 4
                # Use 'today' (a date) defined at top of this function
                days_since_monday = today.weekday()
                week_start = today - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)
# ...existing code...

                for week_back in range(0, weeks_to_check):
                    instance_date = current_week_instance - timedelta(weeks=week_back)
                    # Strict: skip future dates
                    if instance_date > today:
                        continue
                    if instance_date < start_date_obj:
                        continue
                     
                    exact_date_str = instance_date.isoformat()
                    
                    exact_date_str = instance_date.isoformat()
                    week_attendance = attendance_data.get(exact_date_str, {})
                    
                    if not week_attendance:
                        for key, value in attendance_data.items():
                            if isinstance(value, dict):
                                if value.get("event_date_exact") == exact_date_str:
                                    week_attendance = value
                                    break
                                event_date_iso = value.get("event_date_iso")
                                if event_date_iso and exact_date_str in event_date_iso:
                                    week_attendance = value
                                    break
                    
                    if not week_attendance or not isinstance(week_attendance, dict):
                        cell_status = "incomplete"
                        attendees = []
                        did_not_meet = False
                    else:
                        att_status = week_attendance.get("status", "").lower()
                        attendees = week_attendance.get("attendees", [])
                        
                        if att_status == "did_not_meet":
                            cell_status = "did_not_meet"
                            did_not_meet = True
                        elif att_status == "complete" or len(attendees) > 0:
                            cell_status = "complete"
                            did_not_meet = False
                        else:
                            cell_status = "incomplete"
                            did_not_meet = False
                    
                    if status and status != 'all' and status != cell_status:
                        continue
                    
                    captured_by_leader = week_attendance.get("captured_by_leader_at_12", False) if week_attendance else False
                    
                    if role == "admin" and not (personal or show_personal_cells) and captured_by_leader:
                        continue
                    
                    is_overdue = instance_date < today and cell_status == "incomplete"
                    
                    instance = {
                        "_id": f"{cell['_id']}_{exact_date_str}",
                        "UUID": cell.get("UUID", ""),
                        "eventName": cell.get("Event Name", ""),
                        "eventType": "Cells",
                        "eventLeaderName": cell.get("Leader", ""),
                        "eventLeaderEmail": cell.get("Email", ""),
                        "leader1": cell.get("leader1", ""),
                        "leader12": cell.get("Leader @12", ""),
                        "day": day_name.capitalize(),
                        "date": exact_date_str,
                        "display_date": instance_date.strftime("%d - %m - %Y"),
                        "location": cell.get("Location", ""),
                        "status": cell_status,
                        "attendees": attendees,
                        "persistent_attendees": cell.get("persistent_attendees", []),
                        "_is_overdue": is_overdue,
                        "original_event_id": str(cell["_id"]),
                        "is_recurring": True,
                        "attendance": week_attendance,
                        "did_not_meet": did_not_meet,
                    }
                     
                    if cell.get('time'):
                        instance['time'] = cell.get('time')
                    if cell.get('Time'):
                        instance['Time'] = cell.get('Time')
                    
                    cell_instances.append(instance)
                    
            except Exception as e:
                print(f"Error processing cell {cell.get('_id')}: {str(e)}")
                continue
        
        cell_instances.sort(key=lambda x: x['date'], reverse=True)
        
        unique_instances = {}
        for instance in cell_instances:
            key = f"{instance['original_event_id']}_{instance['date']}"
            if key not in unique_instances:
                unique_instances[key] = instance
        
        cell_instances = list(unique_instances.values())
        
        total = len(cell_instances)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        skip = (page - 1) * limit
        paginated = cell_instances[skip:skip + limit]
        
        return {
            "events": paginated,
            "total_events": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/events/{event_id}/persistent-attendees")
async def update_persistent_attendees(
    event_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update persistent attendees list for an event.
    Saves all attendee information including ticket and financial data.
    """
    try:
        print(f"PUT /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")
        
        # Parse event ID
        actual_event_id = event_id
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
        
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        # Fetch the event
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Get the updated persistent attendees from request
        persistent_attendees = update_data.get("persistent_attendees", [])
        
        # Enrich each attendee with proper fields and calculate financials
        enriched_attendees = []
        for attendee in persistent_attendees:
            if not isinstance(attendee, dict):
                continue
            
            # Get price and paid amount
            event_price = attendee.get("price", 0)
            paid_amount = attendee.get("paidAmount", attendee.get("paid", 0))
            
            # Calculate financials
            if paid_amount >= event_price:
                owing = 0
                change = paid_amount - event_price
            elif paid_amount > 0 and paid_amount < event_price:
                owing = event_price - paid_amount
                change = 0
            else:
                owing = event_price
                change = 0
            
            # Create enriched attendee object
            enriched_attendee = {
                "id": attendee.get("id", ""),
                "name": attendee.get("name", attendee.get("fullName", "")),
                "fullName": attendee.get("fullName", attendee.get("name", "")),
                "email": attendee.get("email", ""),
                "phone": attendee.get("phone", ""),
                "leader12": attendee.get("leader12", ""),
                "leader144": attendee.get("leader144", ""),
                "invitedBy": attendee.get("invitedBy", ""),
                "isPersistent": True,
                # Ticket information
                "priceName": attendee.get("priceName", ""),
                "price": event_price,
                "ageGroup": attendee.get("ageGroup", ""),
                "paymentMethod": attendee.get("paymentMethod", ""),
                # Financial information
                "paid": paid_amount,
                "paidAmount": paid_amount,
                "owing": owing,
                "change": change,
            }
            enriched_attendees.append(enriched_attendee)
        
        # Prepare update fields
        update_fields = {
            "persistent_attendees": enriched_attendees,
            "total_associated_count": len(enriched_attendees),
            "updated_at": datetime.utcnow(),
            "last_updated_by": {
                "email": current_user.get("email"),
                "name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                "role": current_user.get("role", "user"),
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
        # If event has attendance data for specific dates, also update there
        target_date = None
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 2:
                try:
                    target_date = datetime.strptime(parts[1], "%Y-%m-%d").date().isoformat()
                except Exception:
                    pass
        
        if target_date and event.get("attendance", {}).get(target_date):
            # Also update the persistent attendees in the date-specific attendance record
            update_fields[f"attendance.{target_date}.persistent_attendees"] = enriched_attendees
            update_fields[f"attendance.{target_date}.statistics.total_associated"] = len(enriched_attendees)
            update_fields[f"attendance.{target_date}.updated_at"] = datetime.utcnow()
        
        # Execute the update
        result = await events_collection.update_one(
            {"_id": ObjectId(actual_event_id)},
            {"$set": update_fields}
        )
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update persistent attendees")
        
        # Return the updated attendees list
        return {
            "success": True,
            "message": f"Updated {len(enriched_attendees)} persistent attendees",
            "persistent_attendees": enriched_attendees,
            "total_associated": len(enriched_attendees),
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/events/{event_id}/persistent-attendees")
async def get_persistent_attendees(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get persistent attendees for an event with their ticket and financial information.
    Returns attendance_status ONLY for the specific date being requested.
    """
    try:
        print(f"GET /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")

        actual_event_id = event_id
        target_date = None

        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                if len(parts) >= 2:
                    try:
                        target_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
                    except Exception:
                        pass

        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")

        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        if not target_date:
            event_date = None
            for date_field in ["date", "Date Of Event", "eventDate", "startDate"]:
                if date_field in event:
                    date_val = event[date_field]
                    if isinstance(date_val, datetime):
                        event_date = date_val.date()
                        break
                    elif isinstance(date_val, str):
                        try:
                            if "T" in date_val:
                                event_date = datetime.fromisoformat(
                                    date_val.replace("Z", "+00:00")
                                ).date()
                            else:
                                event_date = datetime.strptime(date_val, "%Y-%m-%d").date()
                            break
                        except Exception:
                            continue
            target_date = event_date or datetime.now().date()

        exact_date_str = target_date.isoformat()

        persistent_attendees = event.get("persistent_attendees", [])

        attendance_data   = event.get("attendance", {})
        date_attendance   = attendance_data.get(exact_date_str, {})

        if date_attendance:
            # This date has a submitted record — use it directly.
            attendance_status   = date_attendance.get("status", "incomplete")
            checked_in_from_db  = date_attendance.get("attendees", [])
            total_headcounts    = date_attendance.get("total_headcounts", 0)
        else:
            # ── No record for this date → it is a NEW / uncaptured week ─────
            # Return "incomplete" regardless of what root-level fields say.
            attendance_status   = "incomplete"
            checked_in_from_db  = []
            total_headcounts    = 0

        # Only expose checked-in attendees when the week is actually complete.
        is_complete = attendance_status == "complete"

        # ── Build enriched persistent list ───────────────────────────────────
        # Index the checked-in rows by id for O(1) lookup.
        checked_in_index = {
            att.get("id"): att
            for att in checked_in_from_db
            if isinstance(att, dict) and att.get("id")
        }

        enriched_attendees = []
        for attendee in persistent_attendees:
            if not isinstance(attendee, dict):
                continue

            checked_in_data = checked_in_index.get(attendee.get("id"))

            enriched = {
                "id":           attendee.get("id", ""),
                "name":         attendee.get("name", ""),
                "fullName":     attendee.get("fullName", attendee.get("name", "")),
                "email":        attendee.get("email", ""),
                "phone":        attendee.get("phone", ""),
                "leader12":     attendee.get("leader12", ""),
                "leader144":    attendee.get("leader144", ""),
                "invitedBy":    attendee.get("invitedBy", ""),
                "isPersistent": True,
                # Base ticket / financial data from the persistent record
                "priceName":    attendee.get("priceName", ""),
                "price":        attendee.get("price", 0),
                "ageGroup":     attendee.get("ageGroup", ""),
                "paymentMethod": attendee.get("paymentMethod", ""),
                "paidAmount":   attendee.get("paid", attendee.get("paidAmount", 0)),
                "paid":         attendee.get("paid", attendee.get("paidAmount", 0)),
                "owing":        attendee.get("owing", 0),
                "change":       attendee.get("change", 0),
                # Default check-in state: always FALSE for new weeks
                "checked_in":   False,
                "decision":     "",
                "check_in_date": "",
            }

            # Only override with per-week data when this week is complete
            if is_complete and checked_in_data:
                enriched["checked_in"]    = checked_in_data.get("checked_in", True)
                enriched["decision"]      = checked_in_data.get("decision", "")
                enriched["check_in_date"] = checked_in_data.get("check_in_date", "")

                # Per-week ticket overrides (only when week is captured)
                for field in ("priceName", "ageGroup", "paymentMethod"):
                    val = checked_in_data.get(field)
                    if val:
                        enriched[field] = val
                if checked_in_data.get("price") is not None:
                    enriched["price"] = checked_in_data["price"]
                if checked_in_data.get("paid") is not None:
                    enriched["paidAmount"] = checked_in_data["paid"]
                    enriched["paid"]       = checked_in_data["paid"]
                if checked_in_data.get("owing") is not None:
                    enriched["owing"]  = checked_in_data["owing"]
                if checked_in_data.get("change") is not None:
                    enriched["change"] = checked_in_data["change"]

            enriched_attendees.append(enriched)

        # ── Build checked-in list (only meaningful when complete) ────────────
        checked_in_list = []
        if is_complete:
            for att in checked_in_from_db:
                if not isinstance(att, dict):
                    continue
                checked_in_list.append({
                    "id":            att.get("id", ""),
                    "name":          att.get("name", ""),
                    "fullName":      att.get("fullName", att.get("name", "")),
                    "email":         att.get("email", ""),
                    "phone":         att.get("phone", ""),
                    "leader12":      att.get("leader12", ""),
                    "leader144":     att.get("leader144", ""),
                    "checked_in":    att.get("checked_in", True),
                    "decision":      att.get("decision", ""),
                    "check_in_date": att.get("check_in_date", ""),
                    "priceName":     att.get("priceName", ""),
                    "price":         att.get("price", 0),
                    "ageGroup":      att.get("ageGroup", ""),
                    "paymentMethod": att.get("paymentMethod", ""),
                    "paid":          att.get("paid", 0),
                    "owing":         att.get("owing", 0),
                    "change":        att.get("change", 0),
                })

        return {
            "persistent_attendees":  enriched_attendees,
            "checked_in_attendees":  checked_in_list,
            "attendance_status":     attendance_status,   # "incomplete" | "complete" | "did_not_meet"
            "total_headcounts":      total_headcounts,
            "event_date":            exact_date_str,
            "is_ticketed":           event.get("isTicketed", False),
            "total_associated":      len(persistent_attendees),
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/events/{event_id}/last-attendance")
async def get_last_attendance(
    event_id: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        persistent = event.get("persistent_attendees", [])
        if persistent:
            return {
                "has_previous_attendance": True,
                "attendees": persistent,
                "statistics": {
                    "total_associated": len(persistent),
                    "last_attendance_count": event.get("last_attendance_count", 0),
                    "last_decisions_count": event.get("last_decisions_count", 0)
                }
            }

        attendance = event.get("attendance", {})
        if not attendance:
            return {
                "has_previous_attendance": False,
                "attendees": [],
                "statistics": {
                    "total_associated": 0,
                    "last_attendance_count": 0,
                    "last_decisions_count": 0
                }
            }

        weeks = sorted(attendance.keys(), reverse=True)
        if weeks:
            last_week_data = attendance[weeks[0]]
            return {
                "has_previous_attendance": True,
                "attendees": last_week_data.get("attendees", []),
                "statistics": {
                    "total_associated": event.get("total_associated_count", 0),
                    "last_attendance_count": event.get("last_attendance_count", 0),
                    "last_decisions_count": event.get("last_decisions_count", 0)
                }
            }

        return {
            "has_previous_attendance": False,
            "attendees": [],
            "statistics": {
                "total_associated": 0,
                "last_attendance_count": 0,
                "last_decisions_count": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))     

@router.delete("/events/{event_id}")
async def delete_event(event_id: str = Path(...)):
    try:
        print(f" DELETE REQUEST - Event ID: {event_id}")
        print(f" ID length: {len(event_id)}")
        print(f" ID is valid ObjectId: {ObjectId.is_valid(event_id)}")
        
        if not ObjectId.is_valid(event_id):
            print(f" Invalid ObjectId format: {event_id}")
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        
        if not existing_event:
            print(f" Event not found with ID: {event_id}")
            print(f" Checking if event exists with different casing or format...")
            
            similar_events = await events_collection.find({
                "eventName": {"$regex": ".*", "$options": "i"}
            }).limit(3).to_list(None)
            
            print(f" Sample events in DB:")
            for evt in similar_events:
                print(f"   - ID: {evt.get('_id')}, Name: {evt.get('eventName', 'N/A')}")
            
            raise HTTPException(status_code=404, detail=f"Event not found. ID: {event_id}")
        
        print(f"Found event to delete:")
        print(f"   - ID: {existing_event.get('_id')}")
        print(f"   - Name: {existing_event.get('eventName', 'N/A')}")
        print(f"   - Date: {existing_event.get('dateOfEvent', 'N/A')}")
        
        # Delete the event
        result = await events_collection.delete_one({"_id": ObjectId(event_id)})
        
        if result.deleted_count == 1:
            print(f" Successfully deleted event: {event_id}")
            return {"message": "Event deleted successfully"}
        else:
            print(f" Delete operation failed for: {event_id}")
            raise HTTPException(status_code=500, detail="Failed to delete event")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error deleting event {event_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting event: {str(e)}")


@router.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": {"id": member_id}}})
    if update_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Member not found on event")
    return {"message": "Member removed"}

@router.get("/leaders/cells-for/{email}")
async def get_leader_cells(email: str):
    """
    Return cells visible to a leader:
    - Leader @12 sees their own cells + Leader @1 assigned based on gender
    - Leader @144 sees their own cells + their Leader @12 + Leader @1
    """
    try:
        # STEP 1: Find the user in the people database
        person = await people_collection.find_one({"Email": {"$regex": f"^{email}$", "$options": "i"}})
        if not person:
            return {"error": "Person not found", "email": email}

        user_name = f"{person.get('Name','')} {person.get('Surname','')}".strip()
        user_gender = (person.get("Gender") or "").lower().strip()

        # Helper function to get Leader @1 based on gender
        async def leader_at_1_for(name: str) -> str:
            if not name:
                return ""
            leader_person = await people_collection.find_one({
                "$or": [
                    {"Name": {"$regex": f"^{name}$", "$options": "i"}},
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, name]}}
                ]
            })
            if not leader_person:
                return ""
            gender = (leader_person.get("Gender") or "").lower().strip()
            return "Vicky Enslin" if gender in ["female","f","woman","lady","girl"] else "Gavin Enslin"

        # STEP 2: Find all cells related to this leader
        cells = await events_collection.find({
            "Event Type": "Cells",
            "$or": [
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 144": {"$regex": f"^{user_name}$", "$options": "i"}}
            ]
        }).to_list(None)

        result = []
        for cell in cells:
            leader12_name = cell.get("Leader at 12", "")
            leader1_name = cell.get("Leader at 1", "")

            # Assign Leader @1 dynamically if missing
            if leader12_name and not leader1_name:
                leader1_name = await leader_at_1_for(leader12_name)

            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "leader_email": cell.get("Email"),
                "leader_at_12": leader12_name,
                "leader_at_144": cell.get("Leader at 144", ""),
                "leader_at_1": leader1_name,
                "day": cell.get("Day"),
                "time": cell.get("Time"),
            }
            result.append(cell_info)

        return {
            "leader_email": email,
            "leader_name": user_name,
            "total_cells": len(result),
            "cells": result
        }

    except Exception as e:
        return {"error": str(e)}

@router.get("/leaders/cells-for/{email}")
async def get_leader_cells(email: str):
    """
    Return cells visible to a leader:
    - Leader @12 sees their own cells + Leader @1 assigned based on gender
    - Leader @144 sees their own cells + their Leader @12 + Leader @1
    """
    try:
        # STEP 1: Find the user in the people database
        person = await people_collection.find_one({"Email": {"$regex": f"^{email}$", "$options": "i"}})
        if not person:
            return {"error": "Person not found", "email": email}

        user_name = f"{person.get('Name','')} {person.get('Surname','')}".strip()
        user_gender = (person.get("Gender") or "").lower().strip()

        # Helper function to get Leader @1 based on gender
        async def leader_at_1_for(name: str) -> str:
            if not name:
                return ""
            leader_person = await people_collection.find_one({
                "$or": [
                    {"Name": {"$regex": f"^{name}$", "$options": "i"}},
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, name]}}
                ]
            })
            if not leader_person:
                return ""
            gender = (leader_person.get("Gender") or "").lower().strip()
            return "Vicky Enslin" if gender in ["female","f","woman","lady","girl"] else "Gavin Enslin"

        # STEP 2: Find all cells related to this leader
        cells = await events_collection.find({
            "Event Type": "Cells",
            "$or": [
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 144": {"$regex": f"^{user_name}$", "$options": "i"}}
            ]
        }).to_list(None)

        result = []
        for cell in cells:
            leader12_name = cell.get("Leader at 12", "")
            leader1_name = cell.get("Leader at 1", "")

            # Assign Leader @1 dynamically if missing
            if leader12_name and not leader1_name:
                leader1_name = await leader_at_1_for(leader12_name)

            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "leader_email": cell.get("Email"),
                "leader_at_12": leader12_name,
                "leader_at_144": cell.get("Leader at 144", ""),
                "leader_at_1": leader1_name,
                "day": cell.get("Day"),
                "time": cell.get("Time"),
            }
            result.append(cell_info)

        return {
            "leader_email": email,
            "leader_name": user_name,
            "total_cells": len(result),
            "cells": result
        }

    except Exception as e:
        return {"error": str(e)}

def convert_datetime_to_iso(doc: dict) -> dict:
    """Recursively convert datetime values in a document to ISO strings."""
    if not isinstance(doc, dict):
        return doc
    out = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = convert_datetime_to_iso(v)
        elif isinstance(v, list):
            new_list = []
            for item in v:
                if isinstance(item, dict):
                    new_list.append(convert_datetime_to_iso(item))
                elif isinstance(item, datetime):
                    new_list.append(item.isoformat())
                else:
                    new_list.append(item)
            out[k] = new_list
        else:
            out[k] = v
    return out
import math
def sanitize_document(doc):
    """Recursively sanitize document to replace NaN/Infinity float values with None."""
    for k, v in doc.items():
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                doc[k] = None
        elif isinstance(v, dict):
            sanitize_document(v)
        elif isinstance(v, list):
            for i in range(len(v)):
                if isinstance(v[i], dict):
                    sanitize_document(v[i])
                elif isinstance(v[i], float) and (math.isnan(v[i]) or math.isinf(v[i])):
                    v[i] = None
    return doc

@router.get("/events/{event_id}")
async def get_event_by_id(event_id: str = Path(...)):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
           
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
       
        event["_id"] = str(event["_id"])
        event = convert_datetime_to_iso(event)
        event = sanitize_document(event)
        
        if event.get('time'):
            event['time'] = event['time']
        if event.get('Time'):
            event['Time'] = event['Time']
       
        #  ENSURE NEW FIELDS ARE RETURNED
        event.setdefault("isTicketed", False)
        event.setdefault("isGlobal", False)
        event.setdefault("hasPersonSteps", False)
        event.setdefault("priceTiers", [])
       
        # Ensure leader hierarchy fields
        event.setdefault("leader1", "")
        event.setdefault("leader12", "")
        event.setdefault("leader144", "")
        event.setdefault("leaders", {
            "1": event.get("leader1", ""),
            "12": event.get("leader12", ""),
            "144": event.get("leader144", "")
        })
       
        return event
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving event: {str(e)}")


@router.get("/events/{event_id}/all-people-for-attendance")
async def get_all_people_for_event(
    event_id: str = Path(...),
    perPage: int = Query(200, ge=1, le=500),
    current_user: dict = Depends(get_current_user)
):
    """
    Get ALL people with complete fields for event attendance/modals.
    Returns complete data including all leader fields regardless of organization.
    BEST ENDPOINT FOR: AttendanceModal, event people selection, searching all attendees
    """
    try:
        # Verify event exists and user has access
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Build query to get ALL people (no org filtering for events)
        query = {}
        
        # Get total count
        total_count = await people_collection.count_documents(query)
        
        # Use aggregation pipeline for complete data
        pipeline = [
            {"$match": query},
            {"$limit": perPage},
            {"$project": {
                "_id": 1,
                "Name": 1,
                "Surname": 1,
                "Number": 1,
                "Email": 1,
                "Address": 1,
                "Gender": 1,
                "Birthday": 1,
                "InvitedBy": 1,
                "Stage": 1,
                "org_id": 1,
                "Organization": 1,
                "Organisation": 1,
                "LeaderId": 1,
                "LeaderPath": 1,
                "Leader @1": 1,
                "Leader @12": 1,
                "Leader @144": 1,
                "Leader @1728": 1,
                "leader1": 1,
                "leader12": 1,
                "leader144": 1,
                "leader1728": 1,
                "DateCreated": 1,
                "UpdatedAt": 1,
                "Date Created": 1
            }}
        ]
        
        cursor = people_collection.aggregate(pipeline)
        people_list = []
        async for person in cursor:
            people_list.append(person)
        
        # Resolve LeaderPath to names if it exists
        all_leader_ids = set()
        for person in people_list:
            leader_path = person.get("LeaderPath", [])
            if leader_path:
                for lid in leader_path:
                    if lid:
                        try:
                            if isinstance(lid, ObjectId):
                                all_leader_ids.add(lid)
                            else:
                                all_leader_ids.add(ObjectId(str(lid)))
                        except Exception:
                            pass
        
        name_map = {}
        if all_leader_ids:
            try:
                leader_cursor = people_collection.find(
                    {"_id": {"$in": list(all_leader_ids)}},
                    {"_id": 1, "Name": 1, "Surname": 1}
                )
                async for leader_doc in leader_cursor:
                    name_map[leader_doc["_id"]] = f"{leader_doc.get('Name', '')} {leader_doc.get('Surname', '')}".strip()
            except Exception as e:
                print(f"Error fetching leaders: {e}")
        
        def resolve_leader(lid):
            if not lid:
                return ""
            try:
                if isinstance(lid, ObjectId):
                    return name_map.get(lid, "")
                return name_map.get(ObjectId(str(lid)), "")
            except Exception:
                return ""
        
        # Build final response with all fields
        final_list = []
        for person in people_list:
            leader_path = person.get("LeaderPath", [])
            
            # Resolve from LeaderPath if available, otherwise use existing fields
            leader1 = resolve_leader(leader_path[0]) if len(leader_path) > 0 else (person.get("Leader @1") or person.get("leader1") or "")
            leader12 = resolve_leader(leader_path[1]) if len(leader_path) > 1 else (person.get("Leader @12") or person.get("leader12") or "")
            leader144 = resolve_leader(leader_path[2]) if len(leader_path) > 2 else (person.get("Leader @144") or person.get("leader144") or "")
            leader1728 = resolve_leader(leader_path[3]) if len(leader_path) > 3 else (person.get("Leader @1728") or person.get("leader1728") or "")
            
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            
            mapped = {
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Number": person.get("Number", ""),
                "Email": person.get("Email", ""),
                "Address": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "Birthday": person.get("Birthday", ""),
                "InvitedBy": person.get("InvitedBy", ""),
                "Stage": person.get("Stage", "Win"),
                "org_id": person.get("org_id") or person.get("Org_id", ""),
                "Organization": person.get("Organization") or person.get("Organisation", ""),
                "LeaderId": str(person["LeaderId"]) if person.get("LeaderId") else "",
                "LeaderPath": [str(lid) for lid in leader_path],
                "Date Created": person.get("DateCreated") or person.get("Date Created") or datetime.utcnow().isoformat(),
                "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
                "Leader @1": leader1,
                "Leader @12": leader12,
                "Leader @144": leader144,
                "Leader @1728": leader1728,
                "FullName": full_name
            }
            final_list.append(mapped)
        
        return {
            "event_id": event_id,
            "event_name": event.get("Event Name") or event.get("name", "Unknown Event"),
            "perPage": perPage,
            "total": total_count,
            "results": final_list
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_all_people_for_event: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching people: {str(e)}")

@router.get("/events/{event_id}/consolidations")
async def get_event_consolidations(event_id: str = Path(...)):
    """Get all consolidations for a specific event"""
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
       
        consolidations_collection = db["consolidations"]
        consolidations = await consolidations_collection.find({
            "event_id": event_id
        }).sort("created_at", -1).to_list(length=None)
       
        # Enhance with person details
        enhanced_consolidations = []
        for consolidation in consolidations:
            consolidation["_id"] = str(consolidation["_id"])
           
            # Get person details
            person = await people_collection.find_one({
                "_id": ObjectId(consolidation["person_id"])
            })
            if person:
                consolidation["person_details"] = {
                    "name": person.get("Name", ""),
                    "surname": person.get("Surname", ""),
                    "email": person.get("Email", ""),
                    "phone": person.get("Number", ""),
                    "stage": person.get("Stage", ""),
                    "first_decision_date": person.get("FirstDecisionDate"),
                    "total_recommitments": person.get("TotalRecommitments", 0)
                }
           
            # Get task status
            task = await tasks_collection.find_one({
                "_id": ObjectId(consolidation["task_id"])
            })
            if task:
                consolidation["task_status"] = task.get("status", "Unknown")
                consolidation["task_priority"] = task.get("priority", "medium")
           
            enhanced_consolidations.append(consolidation)
       
        return {
            "event_id": event_id,
            "consolidations": enhanced_consolidations,
            "total": len(enhanced_consolidations)
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/events/{event_id}/new-people")
async def get_event_new_people(event_id: str = Path(...)):
   
   
    """Get attendees who are not yet in the people collection"""
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
       
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
       
        new_people = []
        for attendee in event.get("attendees", []):
            email = attendee.get("email") or attendee.get("person_email")
            if email:
                # Check if person exists in people collection
                existing_person = await people_collection.find_one({
                    "Email": {"$regex": f"^{email}$", "$options": "i"}
                })
               
                if not existing_person:
                    new_people.append({
                        "name": attendee.get("name"),
                        "fullName": attendee.get("fullName"),
                        "email": email,
                        "phone": attendee.get("phone"),
                        "decision": attendee.get("decision"),
                        "attendance_time": attendee.get("time")
                    })
       
        return {
            "event_id": event_id,
            "event_name": event.get("Event Name", "Unknown Event"),
            "new_people": new_people,
            "total_new_people": len(new_people)
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@router.post("/events/{event_id}/initialize-structure")
async def initialize_event_structure(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Initialize a new event with the three-type structure
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        
        if "new_people" in event and "consolidations" in event:
            return {
                "success": True,
                "message": "Event already has the new structure",
                "already_initialized": True
            }

        
        update_data = {
            "attendees": event.get("attendees", []),
            "new_people": event.get("new_people", []),
            "consolidations": event.get("consolidations", []),
            "updated_at": datetime.utcnow().isoformat()
        }

        
        if "total_attendance" not in event:
            update_data["total_attendance"] = len(update_data["attendees"])

        await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )

        print(f"Event structure initialized: {event_id}")

        return {
            "success": True,
            "message": "Event structure initialized successfully",
            "already_initialized": False,
            "attendees_count": len(update_data["attendees"]),
            "new_people_count": len(update_data["new_people"]),
            "consolidations_count": len(update_data["consolidations"])
        }

    except Exception as e:
        print(f"Error initializing event structure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error initializing event: {str(e)}")
  
@router.patch("/events/{event_id}/toggle-status")
async def toggle_event_status(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    try:
        parts = event_id.split("_")
        base_event_id = parts[0]
        instance_date = parts[1] if len(parts) > 1 else None

        print(f"Toggling event status: {base_event_id} (instance date: {instance_date})")

        if not ObjectId.is_valid(base_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        if instance_date:
            attendance_data = event.get("attendance", {})
            date_attendance = attendance_data.get(instance_date, {}) if isinstance(attendance_data, dict) else {}
            current_status = str(date_attendance.get("status", "")).lower() or event.get("status", "").lower()
        else:
            current_status = event.get("status", "").lower()

        # Reopening
        if current_status in ["complete", "closed"]:
            new_status = "incomplete"
            action_msg = "reopened"
            log_action = "EVENT_REOPENED"
            status_fields = {
                "reopened_by": current_user.get("email", ""),
                "reopened_at": datetime.utcnow().isoformat()
            }

        # Closing
        else:
            new_status = "complete"
            action_msg = "closed"
            log_action = "EVENT_CLOSED"
            status_fields = {
                "closed_by": current_user.get("email", ""),
                "closed_at": datetime.utcnow().isoformat()
            }

        update_data = {
            "updated_at": datetime.utcnow().isoformat(),
            **status_fields
        }

        if instance_date:
            update_data[f"attendance.{instance_date}.status"] = new_status
            update_data[f"attendance.{instance_date}.closed_by"] = current_user.get("email", "")
            update_data[f"attendance.{instance_date}.closed_at"] = datetime.utcnow().isoformat()
            update_data["status"] = new_status
            update_data["closed_by"] = current_user.get("email", "")
            update_data["closed_at"] = datetime.utcnow().isoformat()
        else:
            update_data["status"] = new_status

        result = await events_collection.update_one(
            {"_id": ObjectId(base_event_id)},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update event status")

        await log_activity(
            user_id=current_user.get("_id"),
            action=log_action,
            details=f"{action_msg.capitalize()} event: {event.get('eventName', 'Unknown')} (ID: {base_event_id}, date: {instance_date})"
        )

        print(f"Event {event.get('eventName')} {action_msg} successfully")

        return {
            "success": True,
            "already_closed": False,
            "message": f"Event '{event.get('eventName', 'Unknown')}' {action_msg} successfully",
            "event_id": base_event_id,
            "event_name": event.get("eventName", "Unknown"),
            "previous_status": current_status,
            "new_status": new_status,
            "action": action_msg,
            "actioned_by": current_user.get("email", ""),
            "actioned_at": status_fields.get("closed_at") or status_fields.get("reopened_at")
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error toggling event status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error toggling event status: {str(e)}")
   
async def log_activity(user_id: str, action: str, details: str):
    """Log admin activities to database"""
    try:
        activity_doc = {
            "user_id": user_id,
            "action": action,
            "details": details,
            "timestamp": datetime.utcnow()
        }
       
        # Insert into activity_logs collection
        await db.activity_logs.insert_one(activity_doc)
    except Exception as e:
        print(f"Error logging activity: {str(e)}")
        # Don't raise exception, just log the error

async def user_has_cell(user_email: str) -> bool:
    """Return True if the user (email) has at least one cell event."""
    if not user_email:
        return False
    try:
        sample = await events_collection.find_one({
            "$or": [
                {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                {"email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}}
            ],
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
            ]
        })
        return bool(sample)
    except Exception:
        return False

@router.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status(current_user: dict = Depends(get_current_user)):
    """Check if user is a leader OR has a cell"""
    try:
        user_email = current_user.get("email")
        user_role = current_user.get("role", "").lower()
       
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
       
        print(f"Checking access for: {user_email}, role: {user_role}")
       
        # Check if user has a cell (for regular users)  roles determination 
        if user_role == "user":
            has_cell = await user_has_cell(user_email)
            print(f"   User has cell: {has_cell}")
           
            if not has_cell:
                print(f"   User {user_email} has no cell - denying Events page access")
                return {"isLeader": False, "hasCell": False, "canAccessEvents": False}
            else:
                print(f"   User {user_email} has cell - granting Events page access")
                return {"isLeader": False, "hasCell": True, "canAccessEvents": True}
       
        # For admin, registrant, and leaders - check leadership status
        person = await people_collection.find_one({
            "$or": [
                {"email": user_email},
                {"Email": user_email},
            ]
        })

        if person:
            # Check if they're a leader at any level
            is_leader = bool(
                person.get("Leader @12") or
                person.get("Leader @144") or
                person.get("Leader @1728")
            )
           
            if is_leader:
                print(f"   {user_email} is a leader")
                return {"isLeader": True, "hasCell": True, "canAccessEvents": True}
       
        # Fallback for admin/registrant
        if user_role in ["admin", "registrant"]:
            print(f"   {user_email} is {user_role} - granting access")
            return {"isLeader": True, "hasCell": True, "canAccessEvents": True}

        print(f"   {user_email} is not a leader and has no special role")
        return {"isLeader": False, "hasCell": False, "canAccessEvents": False}

    except Exception as e:
        print(f"Error checking leader status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 

@router.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: AttendanceSubmission = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        # Parse event ID and extract date
        actual_event_id = event_id
        extracted_date = None
        
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                if len(parts) >= 2:
                    try:
                        extracted_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
                    except Exception:
                        pass
        
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Get user info
        user_email = current_user.get("email", "")
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        role = current_user.get("role", "user").lower()
        
        # Set timezone
        timezone = pytz.timezone("Africa/Johannesburg")
        
        # Determine event date
        if extracted_date:
            event_date_local = timezone.localize(datetime.combine(extracted_date, datetime.min.time()))
        else:
            event_date = None
            for date_field in ["date", "Date Of Event", "eventDate"]:
                if date_field in event:
                    date_val = event[date_field]
                    if isinstance(date_val, datetime):
                        event_date = date_val.date()
                        break
                    elif isinstance(date_val, str):
                        try:
                            if "T" in date_val:
                                event_date = datetime.fromisoformat(date_val.replace("Z", "+00:00")).date()
                            else:
                                event_date = datetime.strptime(date_val, "%Y-%m-%d").date()
                            break
                        except:
                            continue
            
            if event_date:
                event_date_local = timezone.localize(datetime.combine(event_date, datetime.min.time()))
            else:
                event_date_local = datetime.now(timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        
        exact_date_str = event_date_local.date().isoformat()
        
        # Extract submission data
        attendees_data = submission.attendees or []
        persistent_attendees = getattr(submission, 'persistent_attendees', []) or []
        did_not_meet = submission.did_not_meet
        manual_headcount = getattr(submission, 'headcount', 0)
        is_ticketed = submission.isTicketed

        # ── FIX: Convert Pydantic models to plain dicts ──────────────────────
        def to_dict(obj):
            if isinstance(obj, dict):
                return obj
            if hasattr(obj, 'model_dump'):
                return obj.model_dump()
            if hasattr(obj, 'dict'):
                return obj.dict()
            return dict(obj)

        attendees_data = [to_dict(att) for att in attendees_data]
        persistent_attendees = [to_dict(att) for att in persistent_attendees]
        # ─────────────────────────────────────────────────────────────────────

        try:
            manual_headcount = int(manual_headcount) if manual_headcount else 0
        except:
            manual_headcount = 0
        
        # Debug: Print incoming data
        print(f"Received {len(attendees_data)} attendees")
        for att in attendees_data:
            print(f"Attendee: {att.get('fullName')} - price: {att.get('price')}, paid: {att.get('paid')}, paidAmount: {att.get('paidAmount')}")
        
        # Helper function to enrich attendee with financials
        def enrich_with_financials(attendee_dict):
            """Add paid, owing, change fields based on price and paid amount"""
            # Get price (default to 0 if not present)
            price = attendee_dict.get("price", 0)
            
            # Check multiple possible field names for paid amount
            paid = attendee_dict.get("paid", None)
            if paid is None:
                paid = attendee_dict.get("paidAmount", None)
            if paid is None:
                paid = attendee_dict.get("paid_amount", None)
            if paid is None:
                paid = 0
            
            # Ensure numeric values
            try:
                price = float(price) if price else 0
                paid = float(paid) if paid else 0
            except (ValueError, TypeError):
                price = 0
                paid = 0
            
            # Calculate financials
            if paid >= price:
                owing = 0
                change = paid - price
            elif paid > 0 and paid < price:
                owing = price - paid
                change = 0
            else:
                owing = price
                change = 0
            
            print(f"Financials - price: {price}, paid: {paid}, owing: {owing}, change: {change}")
            
            # Create enriched attendee with all fields
            enriched = {
                "id": attendee_dict.get("id", ""),
                "name": attendee_dict.get("name", attendee_dict.get("fullName", "")),
                "fullName": attendee_dict.get("fullName", attendee_dict.get("name", "")),
                "email": attendee_dict.get("email", ""),
                "phone": attendee_dict.get("phone", ""),
                "leader12": attendee_dict.get("leader12", ""),
                "leader144": attendee_dict.get("leader144", ""),
                "invitedBy": attendee_dict.get("invitedBy", ""),
                "decision": attendee_dict.get("decision", ""),
                "checked_in": attendee_dict.get("checked_in", True),
                "isPersistent": attendee_dict.get("isPersistent", True),
                "priceName": attendee_dict.get("priceName", ""),
                "price": price,
                "ageGroup": attendee_dict.get("ageGroup", ""),
                "paymentMethod": attendee_dict.get("paymentMethod", ""),
                "paid": paid,
                "owing": owing,
                "change": change,
                "check_in_date": datetime.now(timezone).isoformat() if not attendee_dict.get("check_in_date") else attendee_dict.get("check_in_date")
            }
            return enriched
        
        # Process persistent attendees
        persistent_attendees_dict = []
        for attendee in persistent_attendees:
            persistent_attendees_dict.append(enrich_with_financials(attendee))
        
        # Process checked-in attendees
        checked_in_attendees = []
        first_time_count = 0
        recommitment_count = 0
        
        for att in attendees_data:
            attendee_data = enrich_with_financials(att)
            
            # Handle decision tracking
            decision = att.get("decision", "")
            if decision:
                attendee_data["decision"] = decision
                decision_lower = decision.lower()
                if "first" in decision_lower:
                    first_time_count += 1
                elif "re-commitment" in decision_lower or "recommitment" in decision_lower:
                    recommitment_count += 1
            
            checked_in_attendees.append(attendee_data)
        
        # Calculate statistics
        total_associated = len(persistent_attendees_dict) or event.get("total_associated_count", 0)
        weekly_attendance = len(checked_in_attendees)
        total_decisions = first_time_count + recommitment_count
        
        # Determine status
        should_mark_as_did_not_meet = (did_not_meet and weekly_attendance == 0 and manual_headcount == 0)
        
        if should_mark_as_did_not_meet:
            date_status = "did_not_meet"
            has_attendance = False
        elif weekly_attendance == 0 and manual_headcount == 0:
            date_status = "incomplete"
            has_attendance = False
        else:
            date_status = "complete"
            has_attendance = True
        
        now = datetime.now(timezone)
        
        # Create weekly attendance entry
        weekly_attendance_entry = {
            "status": date_status,
            "attendees": checked_in_attendees if has_attendance else [],
            "submitted_at": now,
            "submitted_by": user_email,
            "submitted_by_name": user_name,
            "submitted_date": now.isoformat(),
            "event_date": event_date_local.isoformat(),
            "event_date_iso": exact_date_str,
            "event_date_exact": exact_date_str,
            "persistent_attendees": persistent_attendees_dict if has_attendance else [],
            "is_did_not_meet": (date_status == "did_not_meet"),
            "checked_in_count": weekly_attendance,
            "total_headcounts": manual_headcount,
            "is_ticketed": is_ticketed,
            "statistics": {
                "total_associated": total_associated,
                "weekly_attendance": weekly_attendance,
                "total_headcounts": manual_headcount,
                "decisions": {
                    "first_time": first_time_count,
                    "recommitment": recommitment_count,
                    "total": total_decisions
                }
            }
        }
        
        # Prepare update fields
        update_data = {
            "updated_at": now,
            "last_attendance_count": weekly_attendance,
            "last_headcount": manual_headcount,
            "last_attendance_date": exact_date_str,
            "last_status": date_status,
            "status": date_status,
            f"attendance.{exact_date_str}": weekly_attendance_entry
        }
        
        # Update persistent attendees if provided
        if persistent_attendees_dict:
            update_data["persistent_attendees"] = persistent_attendees_dict
            update_data["total_associated_count"] = len(persistent_attendees_dict)
        
        # For non-recurring events, update root-level attendees
        recurring_days = event.get("recurring_day", [])
        is_recurring = isinstance(recurring_days, list) and len(recurring_days) > 0
        
        if not is_recurring and date_status == "complete":
            update_data["attendees"] = checked_in_attendees
            update_data["total_attendance"] = weekly_attendance
        
        # Execute update
        result = await events_collection.update_one(
            {"_id": ObjectId(actual_event_id)},
            {"$set": update_data}
        )
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
        
        return {
            "message": "Attendance submitted successfully",
            "event_id": actual_event_id,
            "status": date_status,
            "exact_date": exact_date_str,
            "checked_in_count": weekly_attendance,
            "total_headcounts": manual_headcount,
            "statistics": weekly_attendance_entry["statistics"],
            "success": True,
            "timestamp": now.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error submitting attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
