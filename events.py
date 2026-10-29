import json
from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends, params
from datetime import datetime, timedelta, date, timezone
from typing import Optional, List, Dict, Any
from fastapi import Request
from bson import ObjectId
import uuid
from auth.models import EventCreate, EventTypeCreate, LeaderStatusResponse,AttendanceSubmission,ConsolidationCreate
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
from supabase import Client
from uuid import UUID
from people import get_person_leader_fields, get_person_leaders

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

        #check for org id
        event_type = supabase.table("event_types").select("*").eq("name", event_type_name).execute().data
        print("event_type query result:", event_type,org_id,event_type_name)
        if len(event_type) == 0:
            raise HTTPException(status_code=404, detail="Event type not found")
        event_type = event_type[0] if event_type and len(event_type) > 0 else None
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
            
                        
        
            # If found, check if it's global
            if event_type:
                is_global = event_type.get("is_global", False)
                print(f"Found event type: {event_type_name} (global={is_global})")
                event_data["eventTypeId"] = event_type.get("event_type_id")
                event_data["eventTypeName"] = event_type.get("name")
                event_data["isGlobal"] = event_type.get("is_global", False)
                event_data["hasPersonSteps"] = event_type.get("has_person_steps", False)
                event_data["isTicketed"] = event_type.get("is_ticketed", False)
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
        event_data['eventLeaderEmail'] = "divinentambwe28@gmail.com"
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

            current_person = supabase.table("People").select("*").eq("Email", event_data.get("eventLeaderEmail")).execute().data
            #leader of created cell

            leader = current_person[0] if current_person else None

            if not current_person:
                raise HTTPException(400,"Person not found") 

            #temp solution
            if leader.get("LeaderPath[0]") == "692d97e550555e9dfdccea4e":
                leader1 = "Gavin Enslin"
            elif leader.get("LeaderPath[0]") == "692d97ec50555e9dfdccecee":
                leader1 = "Vicky Enslin"

            leader12Query = supabase.table("People").select("*").eq("_id", leader.get("LeaderPath[1]")).limit(1).execute().data
            leader12 = ""
            leader12email= ''
            if len(leader12Query) > 0:
                leader12 = f"{leader12Query[0].get("Name")} {leader12Query[0].get("Surname")}"
                leader12email = leader12Query[0].get("Email")

            leaders = {
                "leader_at_1": leader1,
                "leader_at_12": leader12,
                "leader_at_12_email": leader12email,
            }

            
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
                "recurring_day": ",".join(event_data.get("recurring_day",[""])),
                "is_recurring": bool(event_data.get("recurring_day")),
                "Organization": event_data.get("Organization"),
                "event_date": final_event_date.isoformat(),
                "org_id": event_data.get("org_id"),
                # **leaders
            }  

         
            
            post_event = supabase.table("events").insert(final_data).execute()
            result = post_event.data[0]["event_id"]
            
            print(f"[RECURRING CREATE] Inserted _id: {result}")
            
            #creating price tiers if 
            if event_type.get('is_ticketed') and event.priceTiers:
                price_tiers = event.priceTiers
                for pr in price_tiers:
                    tier = {
                        'event_id': result,
                        'org_id': event_data.get("org_id"),
                        'tier_name': pr.get('name'),
                        'price': pr.get('price'),
                        'member_type': pr.get('memberType','normal'),
                        'age_group': pr.get('ageGroup'),
                        'currency': pr.get('currency','ZAR'),
                        'is_active': True
                    }
                    print('tier',tier)
                    supabase.table('event_price_tiers').insert(tier).execute()
                print('created price tiers')

            #creating first cell instance/s
            start_date =event.date
            
            week_indetifier = start_date.strftime("%Y-%m-%d") 
    
            #week number helps with getting date for a specific day tha week
            week_no_int = int(start_date.strftime("%U")) + 1
            year_int = int(start_date.strftime("%Y"))
    
            #iso calendar week format
            days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            cell_instances = []
            num_of_errors = []
            #getting cell occurance day number
            for day in final_data.get("recurring_day").split(","):
                weekday = days.index(day) + 1

                
                session_date = datetime.fromisocalendar(year_int, week_no_int, weekday).date().strftime("%Y-%m-%d")
                
                cell_event = {
                    "event_id": result,
                    "event_name":final_data.get("event_name"),
                    "session_date": session_date,
                    "week_identifier": week_indetifier,
                    "status":"incomplete",
                    "is_did_not_meet":False
                }
                cell_instances.append(cell_event)
                
                new_instance = supabase.table("event_sessions").insert(cell_event).execute()
                if not new_instance.data:
                    num_of_errors.append(cell_event)
    
            print(cell_instances)
            

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

def get_all_unique_cells(
    supabase: Client,
    event_type: str,
    event_ids: list[UUID],
    event_leader_email: Optional[str] = None,
) -> list[dict]:
    response = (
        supabase.table("events")
        .select("*")
        .eq("event_type_name", event_type)
        .eq("is_active", True)
        .not_.is_("leader_at_1", "null")
        .in_("event_id", [str(i) for i in event_ids])
        .order("event_name")
        .execute()
    )

    email = event_leader_email.lower() if event_leader_email else None

    seen = set()
    result = []
    for row in response.data:
        # IS DISTINCT FROM: NULL emails are kept, matching emails are dropped
        row_email = (row.get("event_leader_email") or "").lower() or None
        if email is not None and row_email == email:
            continue

        # DISTINCT ON (event_name): keep the first row per name
        if row["event_name"] in seen:
            continue
        seen.add(row["event_name"])
        result.append(row)

    return result
  
def _fetch_all_event_sessions(start_date, end_date, event_ids, status=None):
    sessions = []
    page_size = 1000
    offset = 0

    while True:
        query = (
            supabase.table("event_sessions")
            .select("*")
            .gte("session_date", start_date)
            .lte("session_date", end_date)
            .in_("event_id", event_ids)
        )
        if status:
            query = query.eq("status", status)

        page = query.range(offset, offset + page_size - 1).execute().data or []
        sessions.extend(page)
        if len(page) < page_size:
            return sessions
        offset += page_size


@router.get("/events/cells")
async def get_cell_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query('incomplete'),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    all_events: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
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

        # 1 - getting currenr user details
        user_email = current_user.get("email", "")
        role = current_user.get("role", "").lower().strip()
        is_actual_leader_at_12 = (
            role == "leaderat12" or
            "leaderat12" in role or
            "leader at 12" in role or
            "leader@12" in role
        )

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



        # 2 - getting day we fetching for
        today_date = datetime.now()

        today_day = today_date.strftime('%A')
     
        if event_type == 'cells':
            event_type ='Cells'

        #parameters used when fetching the cells
        all_cells_params = {
            "p_event_type": event_type,
            "p_event_leader_email": user_email,
            "p_org_id": current_user.get("org_id")
        }
        print('PARAMS',all_cells_params)

        personal_cells_params = {
            "p_event_type": event_type,
            # "p_status": status,
            # "p_day": today_day,
            "p_leader_email": user_email
        }

        disciples_cells_params = {
            "p_event_type": event_type,
            "p_leader12_email": current_user.get('email')
        }


        cell_events=[]

        

        ref = datetime.strptime(today_date.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
        
        three_weeks_ago = ref - timedelta(weeks=2)

        start_str = start_date or three_weeks_ago.strftime("%Y-%m-%d")
        end_str = end_date or ref.strftime("%Y-%m-%d")

        events=[]
        event_sessions=[]
        
        fetch_all_cells = is_actual_leader_at_12 or role =="admin"
        personal_cells = personal == True
        disciples_cells = leader_at_12_view == True and isLeaderAt12 == True and include_subordinate_cells == True and show_all_authorized ==True


        
        # 3- getting cells according to roles
        if all_events:
            #getting all running small gathering events
            events = supabase.rpc("get_all_unique_small_gatherings", all_cells_params).execute().data
            fetched_event_ids = [
                event.get("event_id") for event in events
            ]
            print("getting every event small gatherings sessions",len(events))

            event_sessions = _fetch_all_event_sessions(
                start_str, end_str, fetched_event_ids
            )
            print(len(event_sessions))
        elif fetch_all_cells and not personal_cells and not disciples_cells:
            #getting all running small gathering events
            events = supabase.rpc("get_all_unique_small_gatherings", all_cells_params).execute().data
            fetched_event_ids = [
                event.get("event_id") for event in events
            ]
            print("getting all event small gatherings sessions",len(events))

            event_sessions = _fetch_all_event_sessions(
                start_str, end_str, fetched_event_ids, status
            )
            print(len(event_sessions))
        elif personal_cells and not disciples_cells:
            print("getting personal cells")
            #getting events of leader:
            events = supabase.rpc("get_personal_unique_events", personal_cells_params).execute().data
            for i in events:
                #getting relevent sessions for each event
                session = supabase.table("event_sessions").select("*").gte("session_date", start_str).lte("session_date", end_str).eq("event_name", i.get("event_name")).eq("status", status).execute().data
                if session:
                    event_sessions.extend(session)
        elif disciples_cells:
            print("getting disciples cells")
            events = supabase.rpc("get_disciples_cells",disciples_cells_params).execute().data
            for i in events:
                session = supabase.table("event_sessions").select("*").gte("session_date", start_str).lte("session_date", end_str).eq("event_name", i.get("event_name")).eq("status", status).execute().data
                if session:
                    for s in session:
                        event_sessions.append(s)
        
        #getting event details & event attendees for each session
        for session in event_sessions:
            for event in events:
                if session.get("event_name") == event.get("event_name"): #checking for event associated with session
                    cell_events.append({**event, **session})
        
        cells = []
        for i in cell_events: 
            cells.append({
                "_id": f"{i.get('mongo_id')}_{i.get('session_date')}",
                "session_id": i.get("session_id"),
                "event_id": i.get("event_id"),
                "UUID": i.get("event_id"),
                "eventName": i.get("event_name"),
                "eventType": i.get("event_type_name"),
                "eventLeaderName": i.get("event_leader"),
                "eventLeaderEmail": i.get("event_leader_email"),
                "leader1": i.get("leader_at_1", ""),  # Fallback to empty string if missing
                "leader12": i.get("leader_at_12", ""),
                "leader12email": i.get("leader_at_12_email", ""),
                "day": i.get("recurring_day"),
                "date": i.get("session_date"),
                "display_date": "-".join(reversed(i.get("session_date", "").split("-"))) if i.get("session_date") else "", # Converts YYYY-MM-DD to DD-MM-YYYY
                "location": i.get("location"),
                "hasPersonSteps": i.get("has_person_steps"),
                "status": i.get("status"),
                "Status": i.get("status", "").capitalize() if i.get("status") else "",
                "did_not_meet": i.get("is_did_not_meet", False),
                "_is_overdue": i.get("_is_overdue", True), # Inferred from the schema target
                "is_recurring": i.get("is_recurring"),
                "original_event_id": i.get("mongo_id"),
                "attendance": {},
                "is_active": i.get("is_active"),
                "time": i.get("time", "07:20") 
            })

        if must_paginate:
            total_count = len(cell_events)
            total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
            skip = (page - 1) * limit
            # paginated = cell_instances[skip:skip + limit]
            # print("SENDING ALL EVENTS", cell_instances)
            return {
                "events": cells,
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


    except Exception as e:
        print(f"Error in /events/cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/create_instances")
async def create_instances():
    """ 
    creates instances of events every week
    """
    try:
        #getting all active cells
        event_types = ['Cells']
        for event_type in event_types:
                params = {
                    "p_event_type": event_type
                }
                events = supabase.rpc("get_all_unique_cells_auto", params).execute().data  

                #formulating current week's date
                today_date = datetime.now() 

                week_indetifier = today_date.strftime("%Y-%m-%d") 

                #week number helps with getting date for a specific day tha week
                week_no_int = int(today_date.strftime("%U")) +1
                year_int = int(today_date.strftime("%Y"))

                #iso calendar week format
                days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
                instances = []
                num_of_errors = []
                for event in events:
                    for day in event.get("recurring_day", "").split(","):
                        weekday = days.index(day) + 1
                        
                        session_date = datetime.fromisocalendar(year_int, week_no_int, weekday).date().strftime("%Y-%m-%d")
                    
                        event = {
                            "event_id": event.get("event_id"),
                            "event_name":event.get("event_name"),
                            "session_date": session_date,
                            "week_identifier": week_indetifier,
                            "status":"incomplete",
                            "is_did_not_meet":False
                        }
                        instances.append(event)
                        
                        new_instance = supabase.table("event_sessions").insert(event).execute()
                        if not new_instance.data:
                            num_of_errors.append(event)
                print(f"Successfully created {len(instances) - len(num_of_errors)} / {len(instances)} cell instances for {event_type}")
                # return {
                #     "message": f"Successfully created {len(instances) - len(num_of_errors)} / {len(instances)} cell instances",
                # }

         
    except Exception as e:
        print(f"Error creating cell instances: {e}")
        raise HTTPException(status_code=500, detail="Error creating cell instances")


scheduler = AsyncIOScheduler()    
# scheduler.add_job(create_instances,'cron',hour=12,minute=48,args=[['global_events','cells' ]]) 
# scheduler.add_job(create_instances, 'cron', day_of_week='sun', hour=1, minute=0,args=['Global Events'])
scheduler.start()
sleep(10)


#POPULATING DATA SCRIPTS
@router.get("/update_event_table")
async def update_event_table():
    people= []
    events = supabase.rpc("get_all_unique_events",{"p_event_type":"Cells"}).execute().data
    for event in events:
        person = supabase.rpc("get_cell_leader",{"p_email":event.get("event_leader_email"),"p_fullname":event.get("event_leader")}).execute().data
        if len(person) > 0:
            leader = person[0]
            leader_fullname = f"{leader.get("Name")} {leader.get("Surname")}" 
            if leader.get("LeaderPath[0]") == "692d97e550555e9dfdccea4e":
                leader1 = "Gavin Enslin"
            elif leader.get("LeaderPath[0]") == "692d97ec50555e9dfdccecee":
                leader1 = "Vicky Enslin"

            leader12Query = supabase.table("People").select("*").eq("_id", leader.get("LeaderPath[1]")).limit(1).execute().data
            leader12 = ""
            leader12email= ''
            if len(leader12Query) > 0:
                leader12 = f"{leader12Query[0].get("Name")} {leader12Query[0].get("Surname")}"
                leader12email = leader12Query[0].get("Email")
            leaders = {
                "leader_at_1": leader1,
                "leader_at_12": leader12,
                "leader_at_12_email": leader12email,
            }
            people.append({
                "leader": leader_fullname,
                "leader1": leader1,
                "leader12": leader12,
                "leader12email": leader12email,
                "event": event
            })   
            updated = supabase.table("events").update(leaders).eq("event_id", event.get("event_id")).execute().data
        else:
            people.append(event.get("event_leader") + " " + event.get("event_name"))
    return people        
    
    
@router.get("/update_some_cells")
async def update_event_table():
    people= []
    events = [
    {"event_name": "Ben Mpasi - Springfield - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Kenny Bebel","leader_at_12_email":"kennybebel@gmail.com"},
    {"event_name": "Blessed Manana - La Rochelle - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Kenny Bebel","leader_at_12_email":"kennybebel@gmail.com"},
    {"event_name": "Cynthia Bebel - Die Fakkel High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Cynthia Bebel - Die Fakkel High School - School cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Cynthia Bebel - Rosettenville - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Cynthia Bebel - Rosettenville Primary - School cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Danielle Holtzhausen - Monument High School - School cell - Friday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
    {"event_name": "Denise van de Sandt - Robertsham - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Elvine Kapila - Forest High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Elvine Kapila - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Elvine Kapila - Forest High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Elvine Kapila - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Louange Ngoyi - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Nhlakanipho Madlanga - Die Fakkel High School - Tuesday","leader_at_1":"Gavin Enslin","leader_at_12":"Nash Bobo Mbankumuna","leader_at_12_email":"mbankumunabobo@gmail.com"},
        {"event_name": "Nhlakanipho Madlanga - Turffontein - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Nash Bobo Mbankumuna","leader_at_12_email":"mbankumunabobo@gmail.com"},
    {"event_name": "Nicholas Shamshum - Tedderfield - Open cell - Saturday","leader_at_1":"Gavin Enslin","leader_at_12":"Shane van der Walt","leader_at_12_email":"shane@theactivechurch.org"},  
 {"event_name": "Tracy Bebel - Die Fakkel High School - Open cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
    {"event_name": "Tracy Bebel - Die Fakkel High School - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
    {"event_name": "Vicky Enslin - Glenanda - Closed cell - Monday","leader_at_1":"","leader_at_12":"","leader_at_12_email":""},
    ]
    for event in events[2:]:
        leaders = {
            "leader_at_1": event.get("leader_at_1"),
            "leader_at_12": event.get("leader_at_12"),
            "leader_at_12_email": event.get("leader_at_12_email")
        }
        # 1. Fetch only the first occurrence matching the event name
        record = (
            supabase.table("events")
            .select("event_id")  # Replace "event_id" with your actual primary key column name
            .eq("event_name", event.get("event_name"))
            .limit(1)
            .execute()
        )

        # 2. Check if a row was found, then update it using its specific ID
        if record.data:
            row_id = record.data[0]["event_id"]

            response = (
                supabase.table("events")
                .update(leaders)
                .eq("event_id", row_id)  # Updates only this specific row
                .execute()
            )
            updated_data = response.data
        else:
            print("No matching event found.")
    return updated_data      


@router.get("/populate_event_attendees")
async def add_event_name():
    try:
        event_attendees = supabase.table("event_attendees").select("*").execute().data
        for att in event_attendees:
            event = supabase.table("events").select("event_name").eq("event_id",att["event_id"]).execute().data
            print(event)
            if event:
                supabase.table("event_attendees").update({"event_name": event[0]["event_name"]}).eq("attendee_id", att["attendee_id"]).execute()


        pass
    except:
        pass
@router.get("/update_session_attendees")
async def update_session_attendees():
    #adding event name to every attendee
    i = 0;
    update = {}
    attendees = supabase.table("event_attendees").select("*").is_("event_name", "null").execute().data
    for attendee in attendees:
        event = supabase.table("events").select("event_name").eq("event_id", attendee.get("event_id")).limit(1).execute().data
        if event:
            update = supabase.table("event_session_attendees").update({"event_name": event[0]["event_name"]}).eq("id", attendee.get("id")).execute()
            if update.data:
                i+=1
    return i
    #adding event name to every session
    # count = 0
    # updated = {}
    # sessions = supabase.table("event_sessions").select("*").is_("event_name", "null").execute().data
    # for session in sessions:
    #     event = supabase.table("events").select("event_name").eq("event_id", session.get("event_id")).limit(1).execute().data
    #     if event:
            
    #         updated = supabase.table("event_sessions").update({"event_name": event[0]["event_name"]}).eq("session_id", session.get("session_id")).execute()
    #         if updated.data:
    #             count+=1
    # print(updated)
    return count



@router.get("/events/eventsdata")
async def get_other_events(
    current_user: dict = Depends(get_current_user),
    # supabase: AsyncClient = Depends(get_supabase_client),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=500),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query("2025-10-10"),
    end_date: Optional[str] = Query(None),
    show_all_dates: Optional[bool] = Query(False),  # still unused
    is_global: Optional[bool] = Query(None)
):  
    try:
        print(f"GET /eventsdata - User: {current_user.get('email')}, Event Type: {event_type}")
        print(f"Query params - status: {status}, personal: {personal}, search: {search}")

        user_role = current_user.get("role", "user").lower()
        user_email = (current_user.get("email") or "").lower().strip()
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()

        org_id = (
            current_user.get("org_id")
            or (current_user.get("organization") or "").lower().replace(" ", "-")
            or "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization") or ""

        timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(timezone)
        today = now.date()
        if event_type == "global_events" or is_global == True:
            event_type = "global_events"

        if not is_global:
            fetched_other_events = supabase.rpc("get_general_events",{"p_event_type": event_type}).execute().data
        else:
            fetched_other_events = supabase.rpc("get_global_events").execute().data

        # print("fetched events",fetched_other_events)
        fetched_event_ids = [ 
            event.get("event_id") for event in fetched_other_events
        ]
        params ={
            "p_event_ids": fetched_event_ids
        }
        isTicketed = False
        if (event_type == "Ticketed Events" or event_type == "ticketed_events"):
            price_tiers = supabase.rpc("get_price_tiers",params).execute().data
            isTicketed = True

        ##getting sessions
        today_date = datetime.now()
        ref = datetime.strptime(today_date.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
        three_weeks_ago = ref - timedelta(weeks=1)
        start_str = three_weeks_ago.strftime("%Y-%m-%d")
        end_str = ref.strftime("%Y-%m-%d")

        sessions =[]
        if status: 
            sessions=(supabase.table("event_sessions")
            .select("*")
            # .gte("session_date", start_str).lte("session_date", end_str)
            .in_("event_id", [str(i) for i in fetched_event_ids])
            .eq("status",status).execute().data

            )
        else:
            sessions = (supabase.table("event_sessions")
            .select("*")
            # .gte("session_date", start_str).lte("session_date", end_str)
            .in_("event_id", [str(i) for i in fetched_event_ids])
            .execute().data
            )
        if not isTicketed:
            general_events = []
            for session in sessions:
                for event in fetched_other_events:
                    if session.get("event_name") == event.get("event_name"): #checking for event associated with session
                        general_events.append({**event, **session})
        
        

            all_other_events = [
                {
                    "_id": f"{i.get('mongo_id')}_{i.get('session_date')}",
                    "session_id": i.get("session_id"),
                    "event_id": i.get("event_id"),
                    "UUID": i.get("event_id"),
                    "eventName": i.get("event_name"),
                    "eventType": i.get("event_type_name"),
                    "eventLeaderName": i.get("event_leader"),
                    "eventLeaderEmail": i.get("event_leader_email"),
                    "leader1": i.get("leader_at_1", ""),  # Fallback to empty string if missing
                    "leader12": i.get("leader_at_12", ""),
                    "leader12email": i.get("leader_at_12_email", ""),
                    "day": i.get("recurring_day"),
                    "date": i.get("session_date"),
                    "display_date": "-".join(reversed(i.get("session_date", "").split("-"))) if i.get("session_date") else "", # Converts YYYY-MM-DD to DD-MM-YYYY
                    "location": i.get("location"),
                    # "hasPersonSteps": i.get("has_person_steps"),
                    "isGlobal": i.get("is_global", False),
                    "isTicketed": i.get("is_ticketed", False),
                    "status": i.get("status"),
                    "Status": i.get("status", "").capitalize() if i.get("status") else "",
                    "did_not_meet": i.get("is_did_not_meet", False),
                    "_is_overdue": i.get("_is_overdue", True), # Inferred from the schema target
                    "is_recurring": i.get("is_recurring"),
                    "original_event_id": i.get("mongo_id"),
                    # "attendance": {},
                    "is_active": i.get("is_active"),
                    "time": i.get("time", "07:20") ,
                    "attendance":  i.get("checked_in_count", 0),
                    "new_people":i.get("new_people_total",0),
                    "total_consolidation":i.get("decisions_total",0)    
                }
                for i in general_events
            ]
        else:
            general_events = []
            for session in sessions:
                for event in fetched_other_events:
                    if session.get("event_name") == event.get("event_name"): #checking for event associated with session
                        event_tiers = []
                        for pr in price_tiers:
                            if pr.get("event_id") == event.get("event_id"):
                                pr ={
                                    'name': pr.get('tier_name', ''),
                                    'price': pr.get('price', 0),
                                    'ageGroup': pr.get('ageGroup', ''),
                                    'paymentMethod': pr.get('paymentMethod', 'cash')

                                }
                                event_tiers.append(pr)
                        general_events.append({**event, **session,"price_tiers": event_tiers})
        
        

            all_other_events = [
                {
                    "_id": f"{i.get('mongo_id')}_{i.get('session_date')}",
                    "session_id": i.get("session_id"),
                    "event_id": i.get("event_id"),
                    "UUID": i.get("event_id"),
                    "eventName": i.get("event_name"),
                    "eventType": i.get("event_type_name"),
                    "eventLeaderName": i.get("event_leader"),
                    "eventLeaderEmail": i.get("event_leader_email"),
                    "leader1": i.get("leader_at_1", ""),  # Fallback to empty string if missing
                    "leader12": i.get("leader_at_12", ""),
                    "leader12email": i.get("leader_at_12_email", ""),
                    "day": i.get("recurring_day"),
                    "date": i.get("session_date"),
                    "display_date": "-".join(reversed(i.get("session_date", "").split("-"))) if i.get("session_date") else "", # Converts YYYY-MM-DD to DD-MM-YYYY
                    "location": i.get("location"),
                    "isGlobal": i.get("is_global", False),
                    "isTicketed": i.get("is_ticketed", False),
                    "status": i.get("status"),
                    "Status": i.get("status", "").capitalize() if i.get("status") else "",
                    "did_not_meet": i.get("is_did_not_meet", False),
                    "_is_overdue": i.get("_is_overdue", True), # Inferred from the schema target
                    "is_recurring": i.get("is_recurring"),
                    "original_event_id": i.get("mongo_id"),
                    # "attendance": {},
                    "is_active": i.get("is_active"),
                    "time": i.get("time", "07:20") ,
                    "price_tiers": i.get("price_tiers", "")

                }
                for i in general_events
            ]

        return {
            "events": all_other_events,
            # "total_events": total_count,
            # "total_pages": total_pages,
            # "current_page": page, 
            # "page_size": limit 
        }
        

    except Exception as e:
        print(f"ERROR in /eventsdata: {e}")
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


@router.put("/events/person/{person_name}/event/{event_name}/day/{day_name}/event_id/{event_id}/session_id/{session_id}")
async def update_events_by_person_event_and_day(person_name: str, event_name: str, day_name: str, event_id: str, session_id: str, update_data: dict):
    """
    Update ONLY events for a specific person with a SPECIFIC event name AND SPECIFIC day
    """
    try:
        
        
        event_updates = {}
        
        normalized = {
            'eventName': 'event_name',
            'Event Name':'event_name',
            'eventLeaderEmail': 'event_leader_email',
            'Email': 'event_leader_email', 
            'Day': 'recurring_day', 
            'day': 'recurring_day', 
            'location': 'location',
            'Address': 'location', 
            'status': 'status', 
            'Status': 'status', 
            'time': 'time', 
            'Time': 'time', 
            'is_permanent_deact':'is_permanent_deact'
        }
        
        for i in update_data:
            if not normalized.get(i):
                continue
            event_updates[normalized.get(i)] = update_data[i]

        session_updates = {
            "session_date": update_data.get("date"),
            "event_name": update_data.get("event_name") or event_name
        }
        print("original data", update_data)
        print("normalized event",event_updates)
        print("session data", session_updates)


        
        #update event
        updated_event = supabase.table("events").update(event_updates).eq('event_id',event_id).execute().data

        #update session 
        if update_data.get("date"):
            supabase.table("event_sessions").update(session_updates).eq('session_id', session_id).execute().data
        elif update_data.get("eventName") and update_data.get("date"):
            supabase.table("event_sessions").update({ "session_date": update_data.get("date") }).eq('session_id', session_id).execute().data
            supabase.table("event_sessions").update({"event_name": update_data.get("eventName")}).eq('event_id', event_id).execute().data
        elif update_data.get("eventName"):  
            supabase.table("event_sessions").update({"event_name": update_data.get("eventName")}).eq('event_id', event_id).execute().data

        return {
            "success": True,
            "message": f"Updated 1 event named '{event_name}'",
            "matched_count": 1,
            "modified_count": 1,
            "person": person_name,
            "original_event_name": event_name,
            "original_day": day_name,
            "new_event_name": updated_event[0].get('Event Name'),
            "new_day": updated_event[0].get('Day'),
            "sample_time_stored": updated_event[0].get('time') if updated_event[0] else None
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
    event_id: str = Query(..., description="ID of the event to deactivate")
):
    try:
        current_time = datetime.now(timezone.utc)
        deactivation_end = current_time + timedelta(weeks=weeks)
        
        updates = {
            "is_active": False,
            "deactivation_start": current_time.isoformat(),
            "deactivation_end": deactivation_end.isoformat(),
            "deactivation_reason": reason,
            "last_status_change": current_time.isoformat(),
            "is_permanent_deact": is_permanent_deact,
        }
      
        event = supabase.table("events").update(updates).eq("event_id", event_id).execute()
        
        return {
            "success": True, 
            "message": f"successfully deactivated event",
            "weeks": weeks,
            "deactivation_end": deactivation_end.isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/cells/reactivate")
async def reactivate_cell(
    cell_identifier: str = Query(..., description="Cell name or Person name"),
    person_name: Optional[str] = Query(None, description="Person name (if cell_identifier is a cell name)"),
    day_of_week: Optional[str] = Query(None, description="Specific day to reactivate"),
    event_id: str = Query(..., description="ID of the event to deactivate")
):
    try:
        current_time = datetime.now(timezone.utc)
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }
        

        event = supabase.table("events").update(updates).eq("event_id", event_id).execute()
        
        return {
            "success": True,
            "message": f"Cell reactivated",
            "cell_count": len(event.data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def auto_reactivate_expired_events():
    try:
        current_time = datetime.now(timezone.utc).isoformat()
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }

        result = (
            supabase.table("events")
            .update(updates)
            .eq("is_active", False)
            .lte("deactivation_end", current_time)
            .not_.is_("deactivation_end", "null")
            .neq("is_permanent_deact", True)
            .execute()
            .data
        )
  
        if len(result) > 0:
            print(f"Auto-reactivated {len(result)} cells")
            
    except Exception as e:
        print(f"Auto-reactivation error: {e}")


scheduler = AsyncIOScheduler()    
scheduler.add_job(auto_reactivate_expired_events,'cron',hour=17,minute=10) 
scheduler.start()
sleep(10)


@router.post("/event-types")
async def create_event_type(event_type: EventTypeCreate, current_user: dict = Depends(get_current_user)):
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        org_id = current_user.get("org_id") or (current_user.get("organization", "").lower().replace(" ", "-")) or "active-teams"
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")
        name = event_type.name  # Convert to title case (first letter of each word uppercase)
        # Check for existing event type (case insensitive)
        existing = supabase.table("event_types").select("*").eq("org_id", org_id).ilike("name", event_type.name.strip()).execute().data

        if existing:
            raise HTTPException(status_code=400, detail=f"Event type '{name}' already exists")

        
        event_type_data = {
            "name": name, 
            "event_type_category": event_type.event_category,
            "description": event_type.description.strip(),
            "is_ticketed": event_type.event_category == "ticketed" if hasattr(event_type, 'event_category') else False,
            "is_global": event_type.isGlobal if hasattr(event_type, 'isGlobal') else False,
            "has_person_steps": event_type.hasPersonSteps if hasattr(event_type, 'hasPersonSteps') else False,
            "org_id": org_id,
            # "created_at": datetime.utcnow(),
            "is_training": event_type.event_category == "training" if hasattr(event_type, 'event_category') else False,
        }

        print("NEW TYPE", event_type_data)
        result = supabase.table("event_types").insert(event_type_data).execute().data
        

        print(f"Created event type: {name} for org: {org_id}",result)

        return result

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
        event_updates = {}
        update_data= event_data
        person_name = current_user.get("name")
        event_name = event_data.get("eventName") or event_data.get("Event Name")
        session_id = event_data.get("session_id")

        if event_data.get("priceTiers",[]) and isinstance(event_data.get("priceTiers"), list):
            price_tiers = event_data.get("priceTiers",[])
            for pt in price_tiers:
                if pt.get("tier_id",""):
                    supabase.table("event_price_tiers").update(pt).eq("tier_id",pt.get("tier_id")).execute().data
                else:
                    price_tier = {
                        "event_id": event_id,
                        "org_id": current_user.get("org_id"),
                        "tier_name": pt.get("name"),
                        "price":pt.get("price"),
                        "age_group":pt.get("ageGroup"),
                        "member_type":pt.get("memberType"),
                        "payment_method": pt.get("paymentMethod"),
                    }
                    supabase.table("event_price_tiers").insert(price_tier).execute().data



        #normalize fields to match data schema  
        normalized = {
            'eventName': 'event_name',
            'Event Name':'event_name',
            'eventLeaderEmail': 'event_leader_email',
            'Email': 'event_leader_email', 
            'Day': 'recurring_day', 
            'day': 'recurring_day', 
            'location': 'location',
            'Address': 'location', 
            'status': 'status', 
            'Status': 'status', 
            'time': 'time', 
            'Time': 'time', 
            'is_permanent_deact':'is_permanent_deact'
        }
        
        for i in update_data:
            if not normalized.get(i):
                continue
            event_updates[normalized.get(i)] = update_data[i]

        
        session_updates = {
            "session_date": update_data.get("date"),
            "event_name": update_data.get("event_name") or event_name
        }
        print("original data", update_data)
        print("normalized event",event_updates)
        print("session data", session_updates)


        
        #update event
        updated_event = supabase.table("events").update(event_updates).eq('event_id',event_id).execute().data

        #update session 
        if update_data.get("date"):
            supabase.table("event_sessions").update(session_updates).eq('session_id', session_id).execute().data
        elif update_data.get("eventName") and update_data.get("date"):
            supabase.table("event_sessions").update({ "session_date": update_data.get("date") }).eq('session_id', session_id).execute().data
            supabase.table("event_sessions").update({"event_name": update_data.get("eventName")}).eq('event_id', event_id).execute().data
        elif update_data.get("eventName"):  
            supabase.table("event_sessions").update({"event_name": update_data.get("eventName")}).eq('event_id', event_id).execute().data

        return {
            "success": True,
            "message": f"Updated 1 event named '{event_name}'",
            "matched_count": 1,
            "modified_count": 1,
            "person": person_name,
            "original_event_name": event_name,
            # "original_day": day_name,
            "new_event_name": updated_event[0].get('Event Name'),
            "new_day": updated_event[0].get('Day'),
            "sample_time_stored": updated_event[0].get('time') if updated_event[0] else None
        }
                
    except Exception as e:
        print(f"Error updating events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
        
        
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

@router.get("/get_person_uuid")
async def get_person_uuid(
    email: str,
    full_name: str
):
    # Implementation for fetching person UUID based on email and full name
    params = {
        "p_email": email,
        "p_fullname": full_name
    } 
    person = supabase.rpc("match_people",params).execute().data
    return person[0]["person_id"]

def _get_event_type_category(event_type: str,org_id:str) -> str:
    """Helper function to categorize event type."""
    supabase.table("event_types").select("event_type_category").eq("name", event_type).execute().data
    result = supabase.table("event_types").select("event_type_category").eq("name", event_type).eq("org_id",org_id).execute().data
    if result and len(result) > 0:
        return result[0].get("event_type_category", "").lower()

    
@router.put("/events/{event_id}/persistent-attendees")
async def update_persistent_attendees(
    event_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update persistent attendees list for an event. 
    Associating a person
    """
    try:
        print(f"PUT /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")
        print("DATA", update_data)

        # Determine event type coming from frontend (support both keys)
        event_type = update_data.get("event_type") or update_data.get("eventType") or "Cells"
        event_type = str(event_type).strip()

        # Choose the correct table based on event type
        event_type_category = _get_event_type_category(event_type, current_user.get("org_id"))
        match event_type_category:
            case "small_gatherings":
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
            case "service":
                query_function = "get_persistent_attendees_service"
                table = "event_service_attendees"
            case 'ticketed':
                query_function = "get_persistent_attendees_ticketed"
                table = "event_ticketed_attendees"
            case 'training':
                query_function = "get_persistent_attendees_training"
                table = "event_training_attendance"
            case _:
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
        table_name = table

        attendee = update_data.get("attendee")
        if not attendee:
            # Nothing to associate/disassociate; return success but no-op
            return {"success": True, "message": "No attendee provided", "updated_at": datetime.utcnow().isoformat()}

        # Work on a shallow copy to avoid mutating caller data
        attendee_data = dict(attendee)
        attendee_data.pop("id", None)
        attendee_data.pop("leader12", None)
        attendee_data.pop("leader144", None)

        print("ATTENDEE (sanitized)", attendee_data)

        if update_data.get("associate") is True:
            new_associate = {
                **attendee_data,
                "is_checked_in": False,
                "event_name": update_data.get("event_name", ""),
                "session_id": update_data.get("session_id", ""),
                "event_id": event_id
            }
            print("Inserting associate into", table_name, new_associate)
            supabase.table(table_name).insert(new_associate).execute()

        elif update_data.get("associate") is False:
            person_id = attendee_data.get("person_id") or attendee_data.get("id")
            print("Removing associate from", table_name, "person_id=", person_id, "event_id=", event_id)
            if person_id:
                supabase.table(table_name).delete().eq("person_id", person_id).eq("event_id", event_id).execute()

        return {
            "success": True,
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.put('/update_attendee_price_tier_info')
async def update_attendee_price_tier_info(
    body: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update an attendee's price tier information in the event_ticketed_attendees table.

    Expected body:
    - event_id: The event ID
    - person_id: The person/attendee ID
    - priceName: New price tier name
    - price: New price
    - ageGroup: New age group
    - paymentMethod: New payment method
    - paid: Amount paid
    """
    try:
        print(f"UPDATE ATTENDEE PRICE TIER - User: {current_user.get('email')}")
        print("BODY", body)

        event_id = body.get("event_id", "")
        person_id = body.get("person_id", "")
        price_name = body.get("priceName", "")
        price = body.get("price", 0)
        age_group = body.get("ageGroup", "")
        payment_method = body.get("paymentMethod", "")
        paid = body.get("paid", 0)
        if price > paid:
            owing = price - paid
            change = 0
        else:
            change = paid - price
            owing = 0

        if not event_id or not person_id:
            raise HTTPException(status_code=400, detail="event_id and person_id are required")

        # Update the event_ticketed_attendees table for ticketed events
        # This table stores persistent attendee price tier information
        table = "event_ticketed_attendees"

        # Check if a record exists for this person and event
        existing = (
            supabase.table(table)
            .select("*")
            .eq("event_id", event_id)
            .eq("person_id", person_id)
            .execute()
        )

        if existing.data:
            # Update existing record
            supabase.table(table).update({
                "priceName": price_name,
                "price": price,
                "ageGroup": age_group,
                "paymentMethod": payment_method,
                "paid": paid,
                "owing": owing,
                "change": change,
                "updated_at": datetime.utcnow().isoformat()
            }).eq("event_id", event_id).eq("person_id", person_id).execute()
        else:
            # Create new attendee record with price tier info
            attendee_data = {
                "event_id": event_id,
                "person_id": person_id,
                "is_persistent": True,
                "priceName": price_name,
                "price": price,
                "ageGroup": age_group,
                "paymentMethod": payment_method,
                "paid": paid,
                "owing": owing,
                "change": change,
                "is_checked_in": False,
                "event_name": body.get("event_name", ""),
                "session_id": body.get("session_id", ""),
            }
            print("inserting new attendee price tier", attendee_data)
            supabase.table(table).insert(attendee_data).execute()

        return {
            "success": True,
            "message": "Attendee price tier info updated successfully",
            "updated_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating attendee price tier info: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.put("/update_check_in_status")
async def check_person_in(
    body: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """Check in or check out a person for an event.
    
    Expects JSON body with:
    - event_id: UUID of the event
    - person_id: UUID of the person
    - person_name: Name of the person
    - person_surname: Surname of the person  
    - person_email: Email of the person
    - checked_in: Boolean - True for check-in, False for check-out
    - event_type: Type of event - "Cells" or "Global Events"
    """
    try:
        print("checking in...", body.get("full_name"))
        event_id = body.get("event_id", "")
        person_id = body.get("person_id", "") or body.get("id", "")
        checked_in = body.get("checked_in", False)
        event_type = body.get("event_type", "Cells")

        event_type_category = _get_event_type_category(event_type, current_user.get("org_id"))
        # Route to the correct table based on event type
        match event_type_category:
            case "small_gatherings":
                table = "event_small_gatherings"
            case "service":
                table = "event_service_attendees"
            case 'ticketed':
                table = "event_ticketed_attendees"
            case 'training':
                table = "event_training_attendance"
            case _:
                table = "event_small_gatherings"
        
        if not event_id or not person_id:
            raise HTTPException(status_code=400, detail="event_id and person_id are required")
        
        # First, check if an attendee record exists for this person and event
        existing = (
            supabase.table(table)
            .select("is_checked_in")
            .eq("event_id", event_id)
            .eq("person_id", person_id)
            .execute()
        )
        
        if existing.data:
            # Update existing record
            supabase.table(table).update({
                "is_checked_in": checked_in
            }).eq("event_id", event_id).eq("person_id", person_id).execute()
        else:
            # Create new attendee record
            attendee_data = {
                "session_id": body.get("session_id", ""),
                "event_id": event_id,
                "person_id": person_id,
                "full_name": body.get("fullName", ""),
                "email": body.get("email", ""),
                "is_persistent": True,
                "is_checked_in": checked_in,
                "phone": body.get("phone", ""),
                "check_in_date": datetime.utcnow().strftime("%Y-%m-%d")
            }

            print("inserting new attendee", attendee_data)
            supabase.table(table).insert(attendee_data).execute()
        
        status_str = "checked in" if checked_in else "checked out"
        return {"message": f"Person {status_str} successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/consolidations")
async def create_consolidation(
    consolidation: ConsolidationCreate,
    current_user: dict = Depends(get_current_user)
):
    try:
        print(f"POST /consolidations - User: {current_user.get('email')}")
        print(consolidation)
        #check if sessions exists
        session_id = consolidation.session_id
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
        person_id = consolidation.person_id
        # ── Prevent duplicate consolidation for this session ─────────────────
        if person_id:
            dup = (
                supabase.table("event_consolidations")
                .select("id")
                .eq("session_id", session_id)
                .eq("person_id", person_id)
                .limit(1)
                .execute()
            )
            if dup.data:
                raise HTTPException(
                    status_code=400,
                )


        #logic to get assigned person's details:
        params = {
            "p_email": consolidation.person_email,
            "p_fullname": f"{consolidation.person_name} {consolidation.person_surname}"
        }
        person_data = supabase.rpc("match_people",params).execute()
        if not person_data.data:
            raise HTTPException(status_code=404, detail=f"Error creating consolidation: Person not found")
        person = person_data.data[0]

        def normalize_leader_path(person: dict) -> str | None:
            # 1. Safely retrieve the leader path using your fallback chain
            raw_path = (
                person.get("LeaderPath[3]")
                or person.get("LeaderPath[2]")
                or person.get("LeaderPath[1]")
                or person.get("LeaderPath[0]")
            )

            if raw_path is None:
                return None

            # 2. Handle MongoDB $oid dictionary structure
            if isinstance(raw_path, dict) and "$oid" in raw_path:
                normalized = str(raw_path["$oid"])
            else:
                # Handle regular strings, numbers, or other types
                normalized = str(raw_path).strip()

            # 3. Optional: Add any extra validation (e.g., check if it's a valid 24-char hex string)
            if len(normalized) == 24:
                return normalized

            return normalized if normalized else None

        direct_leader_path = normalize_leader_path(person)

        leader_details = {}
        print("has leader path", direct_leader_path)
        if not direct_leader_path:
            leader_details["email"] = consolidation.event_leader_email,
            leader_details["full_name"] = consolidation.event_leader_name
        else:
            direct_leader = None
            def is_valid_uuid(val: str, version: int = 4) -> bool:
                try:
                    # Attempt to create a UUID object from the string
                    parsed_uuid = uuid.UUID(str(val))
                    
                    # Optional: If you want to strictly enforce a specific UUID version (e.g., v4)
                    return parsed_uuid.version == version
                except (ValueError, TypeError, AttributeError):
                    return False

            if direct_leader_path:
                try:
                    if not is_valid_uuid(direct_leader_path):
                        response = (
                            supabase.table("People")
                            .select("*")
                            .eq("_id", direct_leader_path)
                            .execute()
                        )
                    else:
                        response = (
                            supabase.table("People")
                            .select("*")
                            .eq("person_id", direct_leader_path)
                            .execute()
                        )
                        
                    # Safely grab the first item if data exists
                    if response.data:
                        direct_leader = response.data[0]
                    else:
                        leader_details["email"] = consolidation.event_leader_email,
                        leader_details["full_name"] = consolidation.event_leader_name
                except Exception as e:
                    print(f"Error querying direct leader: {e}")

            direct_leader_fullname = f'{direct_leader.get("Name") if direct_leader else None} {direct_leader.get("Surname") if direct_leader else None}'
            leader_details["email"] = direct_leader.get("Email") if direct_leader else None
            leader_details["full_name"] = direct_leader_fullname


        decision_type = "First Time" if consolidation.decision_type == "first_time" else "Recommitment"
        #creating consolidation task to add to event consolidation table


        notes= ""
        source_display = ""

        match consolidation.source:
            case "cell_consolidation":
                notes = "Gave life during cell"
                source_display = "Cell"
                description = "Consolidation task gave life during cell"
            case"service_consolidation":
                notes = "Gave life during service"
                source_display = "Service"
                description = "Consolidation task gave life during service"
        if consolidation.task_type in ("service follow up", "new_person", "New person"):
            description = "New person consolidation task"
            notes = "New person added during consolidation"

        event_consolidation = {
        "event_id": consolidation.event_id or  session.get("event_id"),
        "person_name": consolidation.person_name,
        "person_surname": consolidation.person_surname,
        "person_email": consolidation.person_email ,
        "person_phone": consolidation.person_phone,
        "decision_type": consolidation.decision_type,
        "decision_display_name": decision_type,
        "assigned_to":leader_details["full_name"] ,
        "assigned_to_email": leader_details["email"] ,
        "status": "Open",
        "notes": notes,
        "session_id": consolidation.session_id,
        "person_id": consolidation.person_id 
        };
        print("consolidation", event_consolidation)
        consolidation_task = supabase.table("event_consolidations").insert(event_consolidation).execute()
        consolidation_id = consolidation_task.data[0]['id']
      
        my_uuid = uuid.uuid4()
        #creating new task:
        
        task_consolidation = {
            "_id": str(my_uuid), 
            "memberID": person.get("_id"),
            "name":f'Consolidation: {person.get("Name")} {person.get("Surname")} ({decision_type})' ,
            "taskType": consolidation.task_type,
            "description": description,
            "status": "open",
            "assignedfor": direct_leader_fullname,
            "assigned_to_email": direct_leader.get("Email") if direct_leader else None,
            "assigned_to_user_id": direct_leader.get("_id") if direct_leader else None,
            "leader_assigned": direct_leader_fullname,
            "leader_name": direct_leader_fullname,
            "type":consolidation.task_type,
            "priority":"high",
            "consolidation_id": consolidation_id,
            "person_id": consolidation.person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "decision_type": consolidation.decision_type,
            "decision_display_name": decision_type,
            "consolidation_source":consolidation.source,
            "source_display":source_display,
            "contacted_person_name":f'{consolidation.person_name} {consolidation.person_surname}',
            "contacted_person_email":consolidation.person_email,
            "contacted_person_phone":consolidation.person_phone,
            "created_at": datetime.now().isoformat(),
            "created_by": current_user.get("email"),
            "is_consolidation_task":"true",
            "Organization": current_user.get("Organization"),
            "org_id": current_user.get("org_id"),
        }
        print("task_consolidation", task_consolidation)
        new_task = supabase.table("Tasks").insert(task_consolidation).execute()
        print("new_task", new_task)

        #update session
        dt_lower = decision_type.lower()
        first_delta = 1 if "first" in dt_lower else 0
        recommit_delta = 1 if "recommit" in dt_lower else 0

        supabase.table("event_sessions").update(
                    {
                        "decisions_first_time": (session.get("decisions_first_time") or 0) + first_delta,
                        "decisions_recommitment": (session.get("decisions_recommitment") or 0) + recommit_delta,
                        "decisions_total": (session.get("decisions_total") or 0) + 1,
                     
                    }
                ).eq("session_id", session_id).execute()

        return {
            "message": f"{consolidation.person_name} {consolidation.person_surname} recorded successfully and assigned to {direct_leader_fullname}",
            "consolidation_id": consolidation_id,
            "person_id": person.get("_id"),
            "task_id": new_task.data[0]['_id'],
            "decision_type": consolidation.decision_type.value,
            "assigned_to": direct_leader_fullname,
            "assigned_to_email": direct_leader.get("Email") if direct_leader else None,
            "leader_user_id": direct_leader.get("_id") if direct_leader else None,
            # "people_count_updated": total_people_count,
            "success": True
        }

    except Exception as e:
        print(f"Error creating consolidation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creating consolidation: {str(e)}")
 
@router.get("/events/{event_id}/persistent-attendees")
async def get_persistent_attendees(
    event_id: str,
    event_name:str,
    session_id:str,
    event_leader:str,
    leader_at_12:str,
    event_type:str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get persistent attendees for an event with their ticket and financial information.
    Returns attendance_status ONLY for the specific date being requested.
    BEST ENDPOINT FOR: AttendanceModal, event people selection, searching all attendees
    """
    try:
        print(f"GET /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")
        per_params = {"p_event_id":event_id, "p_event_name": event_name}
        query_function = "get_persistent_attendees"
        table = "event_small_gatherings"

        event_type_category = _get_event_type_category(event_type, current_user.get("org_id"))
        match event_type_category:
            case "small_gatherings":
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
            case "service":
                query_function = "get_persistent_attendees_service"
                table = "event_service_attendees"
            case 'training':
                query_function = "get_persistent_attendees_training"
                table = "event_training_attendance"
            case 'ticketed':
                query_function = "get_persistent_attendees_ticketed"
                table = "event_ticketed_attendees"
            case _:
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
    
        persistent_attendees = supabase.rpc(query_function, per_params).execute().data
        attendees = supabase.table(table).select("*").eq("session_id",session_id).eq("is_checked_in",True).execute().data
        # price_tiers = supabase.table("event_price_tiers").select("*").eq("event_id", event.get("event_id")).execute().data
        # for pa in persistent_attendees:
        #     leaders = get_person_leader_fields(pa.get("person_id"))
        #     if leaders:
        #         pa["leader1"] = leaders.get("Leader @1")
        #         pa["leader12"] = leaders.get("Leader @12")
        #         pa["leader144"] = leaders.get("Leader @144")


        return {
            "attendees":  [
                    {  
                    "attendee_id": att.get("id"),
                    "id": att.get("person_id"),
                    "person_id":            att.get("person_id", ""),
                    "name":          att.get("full_name", ""),
                    "fullName":      att.get("full_name", ""),
                    "email":         att.get("email", ""),
                    "phone":         att.get("phone", ""),
                    "leader12":      leader_at_12,
                    "leader144":     event_leader if event_leader != leader_at_12 or event_leader != att.get("full_name", "")  else "",
                    "checked_in":    att.get("is_checked_in", True),
                    "decision":      att.get("decision", ""),
                    "check_in_date": att.get("check_in_date", ""),
                    "priceName":     att.get("priceName", ""),
                    "price":         att.get("price", 0),
                    "ageGroup":      att.get("ageGroup", ""),
                    "paymentMethod": att.get("paymentMethod", ""),
                    "paid":          att.get("paid", 0),
                    "owing":         att.get("owing", 0),
                    "change":        att.get("change", 0),
                }
                for att in attendees
                ],
                "persistent_attendees": [
                    {   
                        "attendee_id": p.get("id"),
                        # "id": p.get("mongo_person_id"),
                        "id": p.get("person_id", ""),
                        "name": p.get("full_name"),
                        "fullName": p.get("full_name"),
                        "email": p.get("email"),
                        "phone": p.get("phone",""),
                        "leader1":       p.get("leader1", ""),
                        "leader12":      p.get("leader12", ""),
                        "leader144":     p.get("leader144", ""),
                        "invitedBy": p.get("invited_by", ""),
                        "isPersistent": p.get("is_persistent", True),
                        "priceName":     p.get("priceName", ""),
                        "price":         p.get("price", 0),
                        "ageGroup":      p.get("ageGroup", ""),
                        "paymentMethod": p.get("paymentMethod", ""),
                        "paid":          p.get("paid", 0),
                        "owing":         p.get("owing", 0),
                        "change":        p.get("change", 0),
                    }
                    for p in persistent_attendees
                ]
        
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
            raise HTTPException(status_code=400, detail="Invalid event ID format")
           
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

@router.delete("/events/{event_id}/{event_name}/{event_type}")
async def delete_event(event_id: str = Path(...), event_name: str = Path(...), event_type: str = Path(...)):
    try:
        print("PAYLOAD",event_id,event_name,event_type)
        table = "event_small_gatherings"
        match event_type:
            case  "Cells":
                table = "event_small_gatherings"
            case "Global Event":
                table = "event_service_attendees"
            case _:
                table = "event_small_gatherings"

        supabase.table("event_consolidations").delete().eq("event_id", event_id).execute()
        supabase.table(table).delete().eq("event_id", event_id).execute()
        deleted_event_sessions = supabase.table("event_sessions").delete().eq("event_id", event_id).execute().data
        deleted_event = supabase.table("events").delete().eq("event_id", event_id).execute().data
        
        
        if len(deleted_event) >= 1:
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
        if not event_id:
            raise HTTPException(status_code=400, detail="Event ID is required")

        event = supabase.table("events").select("*").eq("event_id", event_id).execute().data[0]

        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
       
        # event["_id"] = str(event["_id"])
        event = convert_datetime_to_iso(event)
        event = sanitize_document(event)
        
        if event.get('time'):
            event['time'] = event['time']
        if event.get('Time'):
            event['Time'] = event.get('Time')
       
        if event.get('is_ticketed') :
            price_tiers = supabase.table("event_price_tiers").select("*").eq("event_id", event_id).execute().data
            event['price_tiers'] = price_tiers

        event_normalized = {
            "eventName": event.get("event_name", ""),
            "email": event.get("email", ""),
            "date": event.get("event_date", ""),
            "time": event.get("time", ""),
            "timePeriod": "AM",
            "recurringDays": event.get("recurring_day", []).join(", ") if isinstance(event.get("recurring_day"), list) else event.get("recurring_day", ""),
            "location":event.get("location", ""),
            "eventLeader": event.get("event_leader", ""),
            "eventLeaderEmail": event.get("event_leader_email", ""),
            "description": event.get("description", ""),
            "leader1": event.get("leader1", ""),
            "leader12": event.get("leader12", ""),
            "isSchoolCell": event.get("is_school_cell", False),
            "priceTiers": event.get("price_tiers", []),
            "is_ticketed": event.get("is_ticketed", False),
            "eventType": event.get("event_type_name", ""),
        }
        return event_normalized 
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
        today_str = date.today().strftime("%Y-%m-%d")
        # Route to the correct table based on event type if submission.event_type else "cells"
        event_type = submission.event_type
        match event_type:
            case "cells": 
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
            case "global_events":
                query_function = "get_persistent_attendees_service"
                table = "event_service_attendees"
            case 'ticketed_events':
                query_function = "get_persistent_attendees_ticketed"
                table = "event_ticketed_attendees"
            case _:
                query_function = "get_persistent_attendees"
                table = "event_small_gatherings"
        print(f"submit_attendance: event_type={submission.event_type!r} → table={table}")

        # #inserting attendees:
        # for attendee in submission.attendees:
        #     session_attendee = {
        #         "session_id": submission.session_id,
        #         "event_id": submission.event_id,
        #         "event_name": submission.event_name,
        #         "full_name": attendee.fullName,
        #         "email": attendee.email,
        #         "phone": attendee.phone,
        #         "is_checked_in": attendee.checked_in,
        #         "check_in_date": today_str,
        #         "decision": attendee.decision,
        #         "person_id": attendee.id
        #     }
        #     print(f"INSERTING ATTENDEE: {session_attendee}")
        #     new = supabase.table(table).insert(session_attendee).execute().data
        #     print("data", new)



        #update to increment counts           
        saveSession={
            "status": "complete" if not submission.did_not_meet else "did_not_meet",
            "checked_in_count": len(submission.attendees),
            "total_headcounts": len(submission.attendees),
            "submitted_by": current_user.get("email", ""),
            "submitted_by_name": current_user.get("name", ""),
            "submitted_at": datetime.now().isoformat(),
            "decisions_first_time": len([a for a in submission.attendees if a.decision == "first-time"]),
            "decisions_recommitment": len([a for a in submission.attendees if a.decision == "re-commitment"]),
            "decisions_total": len(submission.attendees),
            "total_associated": len(submission.attendees),
            "captured_by_leader_at_12": True if current_user.get("name") == submission.leader12 else False
        }
       
        saved = supabase.table("event_sessions").update(saveSession).eq("session_id", submission.session_id).execute()
        
            
        return {
            "message": "Attendance submitted successfully",
            # "event_id": actual_event_id,
            # "status": date_status,
            # "exact_date": exact_date_str,
            # "checked_in_count": weekly_attendance,
            # "total_headcounts": manual_headcount,
            # "statistics": weekly_attendance_entry["statistics"],
            "success": True,
            # "timestamp": now.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error submitting attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/school_of_leaders")
async def generate_school_cell_report():
    try:
        today = datetime.utcnow().isoformat()

        # STEP 1: Get all running School of Leaders event types from events table
        events_query = (
            supabase.table("events")
            .select("event_id, event_name, event_type_name, is_training")
            .eq("is_training", True)
            .eq("is_active", True)
            .like("event_name", "%School of Leaders%")
            .execute()
        )
        running_events = events_query.data
        print("FETCHED RUNNING EVENTS", running_events)

        response_payload = []

        #for every event find associated attendees & sessions
        for event in running_events:
            event_id = event["event_id"]

            # STEP 2: Fetch associated people from event_training_attendees with leader_at_12
            associated_attendees_query = (
                supabase.table("event_training_attendance")
                .select('person_id, People(Name,Surname, "LeaderPath[1]")')
                .eq("event_id", event_id)
                .execute()
            )
            print("ASSOCIATED ATTENDEES QUERY:", associated_attendees_query)

          
            formatted_associated_attendees = [
                {
                    "full_name": f"{att.get('People', {}).get('Name', 'Unknown')} {att.get('People', {}).get('Surname', 'Unknown')}",
                    "leader12": supabase.table("People").select("Name, Surname, Email").eq("_id", att.get('People', {}).get('LeaderPath[1]', 'Unknown')).execute().data[0]
                }
                for att in associated_attendees_query.data
            ]
            print("FORMATTED ASSOCIATED ATTENDEES:", formatted_associated_attendees)

            # STEP 3: Determine sessions that already happened by querying by date
            formatted_date = datetime.now().strftime("%Y-%m-%d")
            past_sessions_query = (
                supabase.table("event_sessions")
                .select("session_id, session_date")
                .eq("event_id", event_id)
                .lte("session_date", formatted_date)
                .execute()
            )
            print(past_sessions_query)

            sessions_data = {}

            # STEP 4: Retrieve attendees for each passed session & calculate attendance counts
            j = 0
            for session in past_sessions_query.data:
                session_id = session["session_id"]
                j += 1
                session_num = j 

                session_attendees_query = (
                    supabase.table("event_training_attendance")
                    .select("person_id,full_name")
                    .eq("session_id", session_id)
                    .execute()
                )

                session_attendees_list = []
                for att in session_attendees_query.data:
                    person_id = att["person_id"]

                    # Count total sessions this person has attended across all sessions
                    total_attended_query = (
                        supabase.table("event_session_attendees")
                        .select("session_id", count="exact")
                        .eq("person_id", person_id)
                        .eq("session_id", session_id)
                        .execute()
                    )

                    session_attendees_list.append(
                        {
                            "person_id": person_id,
                            "fullname": att.get("full_name",{}),
                            # "leader12": att.get("people", {}).get("leader_at_12", "N/A"),   
                        } 
                    )

                sessions_data[f"session_{session_num}"] = {
                    "attendees": session_attendees_list
                }

            # STEP 5: Construct event response object matching your schema
            response_payload.append(
                {
                    "training_class": event["event_name"],
                    "associated_attendees": formatted_associated_attendees,
                    "sessions": sessions_data,
                }
            )
        print(response_payload)
        return response_payload

    except Exception as e:
        print("ERROR GENERATING SCHOOL REPORT: ", e)
        raise HTTPException(status_code=500, detail=str(e))