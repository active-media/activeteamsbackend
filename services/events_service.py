"""Events service - handles all event-related business logic"""
# This is a placeholder - extract event logic from main.py
# Events service will contain functions for:
# - Creating/updating/deleting events
# - Getting cell events, other events, global events
# - Event types management
# - Attendance submission
# - Event status calculations
# - Leader assignments
# etc.

from datetime import datetime, date, timedelta
from urllib.parse import unquote
from click import Path
from activeteamsbackend.auth.models import EventTypeCreate
from activeteamsbackend.auth.utils import convert_datetime_to_iso, sanitize_document
from bson import ObjectId
from typing import Optional, List, Dict, Any
from fastapi import HTTPException, Body, FastAPI, Depends, Query
import re
from database import events_collection, people_collection
import pytz
import uuid
from auth.models import EventCreate
from auth.utils import get_current_user  



app = FastAPI()

@app.post("/events")
async def create_event(event: EventCreate):
    """Create a new event"""
    try:
        event_data = event.dict()
        
        event_data["_id"] = ObjectId()
        
        if not event_data.get("UUID"):
            event_data["UUID"] = str(uuid.uuid4())
        
        event_type_name = event_data.get("eventTypeName")
        if not event_type_name:
            raise HTTPException(status_code=400, detail="eventTypeName is required")
        
        print(f"Looking for event type: '{event_type_name}'")
        
        if event_type_name.upper() in ["CELLS", "ALL CELLS"]:
            event_data["eventTypeId"] = "CELLS_BUILT_IN"
            event_data["eventTypeName"] = "CELLS"
            event_data["hasPersonSteps"] = True
            event_data["isGlobal"] = False
            print(f"Using built-in CELLS event type with leader fields enabled")
        else:
            event_type = await events_collection.find_one({
                "$or": [
                    {"name": {"$regex": f"^{event_type_name}$", "$options": "i"}},
                    {"Event Type": {"$regex": f"^{event_type_name}$", "$options": "i"}},
                    {"eventType": {"$regex": f"^{event_type_name}$", "$options": "i"}}
                ],
                "isEventType": True
            })
            
            if not event_type:
                print(f"Event type '{event_type_name}' not found in database")
                available_types = await events_collection.find({"isEventType": True}).to_list(length=50)
                available_type_names = [et.get("name") for et in available_types if et.get("name")]
                print(f"Available event types: {available_type_names}")
                raise HTTPException(status_code=400, detail=f"Event type '{event_type_name}' not found")
            
            print(f"Found event type: {event_type.get('name')}")
            
            exact_event_type_name = event_type.get("name")
            event_data["eventTypeId"] = event_type["UUID"]
            event_data["eventTypeName"] = exact_event_type_name
            
            # Set flags based on event type
            event_type_lower = exact_event_type_name.lower()
            
            if "global" in event_type_lower:
                event_data["isGlobal"] = True
            else:
                event_data["isGlobal"] = event_data.get("isGlobal", False)
            
            if "cell" in event_type_lower:
                event_data["hasPersonSteps"] = True
            else:
                event_data["hasPersonSteps"] = event_data.get("hasPersonSteps", False)
        
        # Remove duplicate email field
        event_data.pop("eventType", None)
        if "userEmail" in event_data:
            del event_data["userEmail"]
        if "email" in event_data:
            del event_data["email"]
        
        # Ensure date is properly saved
        if event_data.get("date"):
            if isinstance(event_data["date"], str):
                try:
                    event_data["date"] = datetime.fromisoformat(event_data["date"].replace("Z", "+00:00"))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format")
        else:
            event_data["date"] = datetime.utcnow()

        print(f"Using day value from frontend: {event_data.get('day')}")

        # Ensure leader fields are properly saved
        event_data.setdefault("eventLeaderName", event_data.get("eventLeader", ""))
        event_data.setdefault("eventLeaderEmail", event_data.get("eventLeaderEmail", ""))
        
        # Keep leader fields for CELLS events
        if event_data.get("hasPersonSteps"):
            event_data.setdefault("leader1", event_data.get("leader1", ""))
            event_data.setdefault("leader12", event_data.get("leader12", ""))
            event_data["persistent_attendees"] = event_data.get("persistent_attendees", [])
            print(f"Saved leader fields - Leader@1: {event_data.get('leader1')}, Leader@12: {event_data.get('leader12')}")

        # Defaults
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data.get("attendees", []))
        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data["status"] = "open"
        
        event_data["isTicketed"] = event_data.get("isTicketed", False)
        
        if event_data.get("isTicketed") and event_data.get("priceTiers"):
            event_data["priceTiers"] = [
                {
                    "name": tier.get("name", ""),
                    "price": float(tier.get("price", 0)),
                    "ageGroup": tier.get("ageGroup", ""),
                    "memberType": tier.get("memberType", ""),
                    "paymentMethod": tier.get("paymentMethod", "")
                }
                for tier in event_data.get("priceTiers", [])
            ]
        else:
            event_data["priceTiers"] = []

        # Clean up fields for Global Events only
        if event_data.get("isGlobal", False):
            fields_to_remove = ["leader1", "leader12"]
            for field in fields_to_remove:
                if field in event_data and not event_data[field]:
                    del event_data[field]

        print(f"DEBUG - Final event data being saved:")
        print(f"  - Event Type: {event_data.get('eventTypeName')}")
        print(f"  - Day: {event_data.get('day')}")
        print(f"  - isGlobal: {event_data.get('isGlobal')}")
        print(f"  - hasPersonSteps: {event_data.get('hasPersonSteps')}")
        print(f"  - leader1: {event_data.get('leader1')}")
        print(f"  - leader12: {event_data.get('leader12')}")

        result = await events_collection.insert_one(event_data)
        
        created_event = await events_collection.find_one({"_id": result.inserted_id})
        
        print(f"Event created successfully: {result.inserted_id}")

        return {
            "success": True,
            "message": "Event created successfully", 
            "id": str(result.inserted_id),
            "event": {
                "_id": str(created_event["_id"]),
                "UUID": created_event.get("UUID"),
                "eventName": created_event.get("eventName"),
                "eventLeaderName": created_event.get("eventLeaderName"),
                "eventLeaderEmail": created_event.get("eventLeaderEmail"),
                "day": created_event.get("day"),
                "date": created_event.get("date"),
                "location": created_event.get("location"),
                "eventTypeName": created_event.get("eventTypeName"),
                "isGlobal": created_event.get("isGlobal"),
                "hasPersonSteps": created_event.get("hasPersonSteps"),
                "leader1": created_event.get("leader1"),
                "leader12": created_event.get("leader12"),
                "status": created_event.get("status", "open")
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f" Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")


@app.get("/events/cells")
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
    userSurname: Optional[str] = Query(None)
):
    try:
        print("=" * 100)
        print("GET /events/cells REQUEST")
        print(f'THEIR NAME {firstName} {userSurname}')
        
        role = current_user.get("role", "user").lower()
        user_email = current_user.get("email", "")

        person = await people_collection.find_one({"Email": user_email})
        if person:
            first_name = person.get('Name', '').strip() or firstName
            surname = person.get('Surname', '').strip() or userSurname
        else:
            first_name = firstName
            surname = userSurname

        user_name = f"{first_name} {surname}".strip()
        if not user_name:
            user_name = first_name or current_user.get("username", "")
        
        print(f"User: {user_name} ({user_email})")
        print(f"Role: {role}")
        print(f"Parameters:")
        print(f"   personal: {personal}")
        print(f"   show_personal_cells: {show_personal_cells}")
        print(f"   show_all_authorized: {show_all_authorized}")
        print(f"   leader_at_12_view: {leader_at_12_view}")
        print(f"   include_subordinate_cells: {include_subordinate_cells}")

        is_actual_leader_at_12 = isLeaderAt12
        print(f"Is Leader at 12: {is_actual_leader_at_12}")

        query = {
            "$and": [
                {
                    "$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}},
                        {"EventType": {"$regex": "^Cells$", "$options": "i"}}
                    ]
                },
                {"isEventType": {"$ne": True}}
            ]
        }

        safe_user_email = re.escape(user_email)
        safe_user_name = re.escape(user_name)

        if search and search.strip():
            search_term = search.strip()
            print(f"SEARCH: '{search_term}'")
            
            query["$and"].append({
                "$or": [
                    {"Event Name": {"$regex": search_term, "$options": "i"}},
                    {"eventName": {"$regex": search_term, "$options": "i"}},
                    {"EventName": {"$regex": search_term, "$options": "i"}},
                    {"Leader": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderName": {"$regex": search_term, "$options": "i"}},
                    {"EventLeaderName": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderEmail": {"$regex": search_term, "$options": "i"}},
                    {"EventLeaderEmail": {"$regex": search_term, "$options": "i"}},
                    {"Email": {"$regex": search_term, "$options": "i"}},
                    {"Leader at 12": {"$regex": search_term, "$options": "i"}},
                    {"Leader @12": {"$regex": search_term, "$options": "i"}},
                    {"leader12": {"$regex": search_term, "$options": "i"}},
                    {"LeaderAt12": {"$regex": search_term, "$options": "i"}},
                ]
            })

        if role == "admin":
            print(f"ADMIN MODE")
            
            if personal or show_personal_cells:
                print("   PERSONAL: Admin's own cells")
                query["$and"].append({
                    "$or": [
                        {"eventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"EventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Email": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Leader": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"eventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"EventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                    ]
                })
            else:
                print("   VIEW ALL: ALL cells")

        elif is_actual_leader_at_12 and leader_at_12_view:
            print(f"LEADER AT 12 MODE")
            
            want_personal_view = (show_personal_cells or personal)
            want_disciples_view = show_all_authorized
            
            print(f"   View preferences - Personal: {want_personal_view}, Disciples: {want_disciples_view}")
            
            if want_personal_view:
                print("   PERSONAL MODE: Leader's own cells only")
                query["$and"].append({
                    "$or": [
                        {"eventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"EventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Email": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Leader": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"eventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"EventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                    ]
                })
            elif want_disciples_view:
                print("   VIEW ALL MODE: Disciples' cells only (where user is Leader at 12)")
                
                name_variations = []
                name_variations.append(user_name)
                
                name_variations.extend([
                    f"{first_name} {surname}", 
                    first_name, 
                    user_name.replace(" ", ""),  
                    user_name.replace(" ", "-"),  
                ])
                
                if "-" in first_name:
                    hyphen_parts = first_name.split("-")
                    name_variations.extend([
                        f"{hyphen_parts[0]} {surname}", 
                        f"{hyphen_parts[0]}-{surname}",  
                        f"{first_name} {surname}",  
                    ])
                    for part in hyphen_parts:
                        if part and part.strip():
                            name_variations.append(f"{part} {surname}")
                
                name_parts = user_name.split()
                if len(name_parts) > 2:
                    name_variations.append(f"{name_parts[0]} {name_parts[-1]}")
                    name_variations.append(name_parts[0])
                    for i in range(1, len(name_parts)):
                        combined_name = " ".join(name_parts[:i+1])
                        name_variations.append(combined_name)
                
                name_variations = list(set([name for name in name_variations if name and name.strip()]))
                
                print(f"   Leader '{user_name}' - Trying to match Leader at 12 with variations: {name_variations}")
                
                or_conditions = []
                
                for name_variant in name_variations:
                    safe_name = re.escape(name_variant)
                    
                    or_conditions.extend([
                        {"leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"Leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"Leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"Leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"leader at 12": {"$regex": safe_name, "$options": "i"}},
                        {"leader @12": {"$regex": safe_name, "$options": "i"}},
                        {"leader12": {"$regex": safe_name, "$options": "i"}},
                        {"Leader at 12": {"$regex": safe_name, "$options": "i"}},
                        {"Leader @12": {"$regex": safe_name, "$options": "i"}},
                        {"Leader12": {"$regex": safe_name, "$options": "i"}},
                        {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
                    ])
                
                if or_conditions:
                    query["$and"].append({"$or": or_conditions})
                    print(f"   Searching for cells where Leader at 12 matches any of: {name_variations}")
                    print(f"   Total OR conditions: {len(or_conditions)}")
                    
                    debug_query = {
                        "$and": [
                            {"$or": [
                                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}},
                                {"EventType": {"$regex": "^Cells$", "$options": "i"}}
                            ]},
                            {"$or": or_conditions}
                        ]
                    }
                    
                    debug_cells = await events_collection.find(debug_query).to_list(length=50)
                    print(f"   DEBUG: Found {len(debug_cells)} potential disciple cells for {user_name}")
                    for i, cell in enumerate(debug_cells):
                        leader_at_12 = (
                            cell.get('Leader at 12') or
                            cell.get('Leader @12') or
                            cell.get('leader12') or
                            cell.get('Leader12') or
                            cell.get('LeaderAt12') or
                            cell.get('leader at 12') or
                            cell.get('leader @12') or
                            'N/A'
                        )
                        cell_leader = (
                            cell.get('Leader') or
                            cell.get('eventLeaderName') or
                            cell.get('EventLeaderName') or
                            'N/A'
                        )
                        print(f"      {i+1}. {cell.get('Event Name', 'N/A')}")
                        print(f"         Cell Leader: {cell_leader}")
                        print(f"         Leader@12: {leader_at_12}")
                else:
                    print("   WARNING: No matching conditions created")
            else:
                print("   FALLBACK: Defaulting to personal cells")
                query["$and"].append({
                    "$or": [
                        {"eventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"EventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Email": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                        {"Leader": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"eventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                        {"EventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                    ]
                })

        elif role in ["user", "registrant", "leader"]:
            print(f"REGULAR USER MODE")
            query["$and"].append({
                "$or": [
                    {"eventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                    {"EventLeaderEmail": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                    {"Email": {"$regex": f"^{safe_user_email}$", "$options": "i"}},
                    {"Leader": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                    {"eventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                    {"EventLeaderName": {"$regex": f"^{safe_user_name}$", "$options": "i"}},
                ]
            })

        else:
            print(f"NO ACCESS")
            query["$and"].append({"_id": "nonexistent_id"})

        print(f"Executing query...")
        print(f"Query structure: {json.dumps(query, indent=2, default=str)}")
        
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
        
        print(f"Found {len(events)} unique cells")
        
        if len(events) > 0:
            print("Sample cells:")
            for i, event in enumerate(events[:5]):
                cell_leader = (
                    event.get("Leader") or
                    event.get("eventLeaderName") or
                    event.get("EventLeaderName") or
                    "N/A"
                )
                leader_at_12 = (
                    event.get("Leader at 12") or
                    event.get("Leader @12") or
                    event.get("leader12") or
                    event.get("Leader12") or
                    event.get("LeaderAt12") or
                    event.get("leader at 12") or
                    event.get("leader @12") or
                    "N/A"
                )
                print(f"   {i+1}. {event.get('Event Name', 'N/A')}")
                print(f"      Leader: {cell_leader}")
                print(f"      Leader@12: {leader_at_12}")
                print(f"      Status from DB: {event.get('Status')}")
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone).date()
        
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
                    print(f"Skipping event: Invalid day '{day_name}'")
                    continue

                target_weekday = day_mapping[day_name]
                
                print(f"\nProcessing cell: {event.get('Event Name', 'Unknown')}")
                print(f"  Day: {day_name.capitalize()} (weekday number: {target_weekday})")
                print(f"  Today is: {today} (weekday: {today.weekday()})")
                print(f"  MongoDB Status: {event.get('Status')}")
                print(f"  MongoDB ID: {event.get('_id')}")
                
                for week_offset in range(4):
                    days_since_target = (today.weekday() - target_weekday) % 7
                    instance_date = today - timedelta(days=(days_since_target + (week_offset * 7)))
                    
                    if instance_date < start_date_obj:
                        continue
                    
                    year, week, _ = instance_date.isocalendar()
                    week_id = f"{year}-W{week:02d}"
                    
                    attendance = event.get("attendance", {}).get(week_id, {})
                    
                    did_not_meet = attendance.get("status") == "did_not_meet"
                    attendees = attendance.get("attendees", [])
                    has_checked_in = any(a.get("checked_in", False) for a in attendees)
                    
                    db_status = str(event.get("Status", "")).strip().lower()
                    
                    if did_not_meet:
                        event_status = "did_not_meet"
                    elif db_status == "complete":
                        event_status = "complete"
                    elif has_checked_in:
                        event_status = "complete"
                    else:
                        event_status = "incomplete"
                    
                    if status and status != 'all' and event_status != status:
                        print(f"  Skipping {instance_date}: status '{event_status}' doesn't match filter '{status}'")
                        continue
                    
                    is_overdue = instance_date < today and event_status == "incomplete"
                    
                    print(f"  Instance: {instance_date} (Week {week_id})")
                    print(f"    Status: {event_status}")
                    print(f"    DB Status: {db_status}")
                    print(f"    Did not meet: {did_not_meet}")
                    print(f"    Has checked in: {has_checked_in}")
                    print(f"    Attendees count: {len(attendees)}")
                    print(f"    Is overdue: {is_overdue}")
                    
                    leaderAt1 = event.get("leader1") or event.get("Leader @1") or event.get("Leader at 1", "")
                    
                    if not leaderAt1:
                        leaderPipeline = [
                            {'$project': {'Gender': 1, 'fullName': { '$concat': ["$Name", " ", "$Surname"] }}},
                            {'$match': { 'fullName': event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", "") }},
                            { '$limit': 1 }
                        ]
                        
                        peopleFullnames = await people_collection.aggregate(leaderPipeline).to_list(length=None)
                        
                        if peopleFullnames and len(peopleFullnames) > 0:
                            eventLeader = peopleFullnames[0]
                            if eventLeader:
                                gender = eventLeader.get("Gender", "")
                                if gender.upper() == "MALE":
                                    leaderAt1 = "Gavin Enslin"
                                elif gender.upper() == "FEMALE":
                                    leaderAt1 = "Vicky Enslin"
                    
                    unique_id = f"{event.get('_id')}_{instance_date.isoformat()}"
                    
                    instance = {
                        "_id": unique_id,
                        "UUID": event.get("UUID", ""),
                        "eventName": event.get("Event Name") or event.get("eventName") or event.get("EventName", ""),
                        "eventType": "Cells",
                        "eventLeaderName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", ""),
                        "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("EventLeaderEmail") or event.get("Email", ""),
                        "leader1": leaderAt1,
                        "leader12": (
                            event.get("Leader at 12") or
                            event.get("Leader @12") or
                            event.get("leader12") or
                            event.get("Leader12") or
                            event.get("LeaderAt12") or
                            event.get("leader at 12") or
                            event.get("leader @12") or
                            ""
                        ),
                        "day": day_name.capitalize(),
                        "date": instance_date.isoformat(),
                        "display_date": instance_date.strftime("%d - %m - %Y"),
                        "location": event.get("Location") or event.get("location", ""),
                        "attendees": attendees,
                        "persistent_attendees": event.get("persistent_attendees", []),
                        "hasPersonSteps": True,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "_is_overdue": is_overdue,
                        "is_recurring": True,
                        "week_identifier": week_id,
                        "original_event_id": str(event.get("_id"))
                    }
                    
                    cell_instances.append(instance)
                    
            except Exception as e:
                print(f"Error processing event {event.get('Event Name', 'Unknown')}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        cell_instances.sort(key=lambda x: x['date'], reverse=True)
        
        total_count = len(cell_instances)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated = cell_instances[skip:skip + limit]
        
        print(f"\nRESULTS SUMMARY:")
        print(f"  Total instances: {total_count}")
        print(f"  Page: {page}/{total_pages}")
        print(f"  Page size: {limit}")
        print(f"  Showing: {len(paginated)} instances")
        
        status_counts = {}
        for inst in cell_instances:
            status_counts[inst['status']] = status_counts.get(inst['status'], 0) + 1
        
        print(f"  Status breakdown: {status_counts}")
        
        print("\nSample instances in response:")
        for i, inst in enumerate(paginated[:5]):
            print(f"  {i+1}. {inst['eventName']}")
            print(f"     Date: {inst['date']} ({inst['display_date']})")
            print(f"     Status: {inst['status']}")
            print(f"     Leader: {inst['eventLeaderName']}")
            print(f"     _id: {inst['_id']}")
        
        print("=" * 100)

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
                "is_leader_at_12": is_actual_leader_at_12
            }
        }

    except Exception as e:
        print(f" ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
   

@app.get("/events/other")
async def get_other_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query('2025-10-10'),
    end_date: Optional[str] = Query(None)
):
    """
    Get Global Events and other non-cell events with their actual dates
    """
    try:
        print(f"GET /events/other - User: {current_user.get('email')}, Event Type: {event_type}")
        print(f"Query params - status: {status}, personal: {personal}, search: {search}")

        user_role = current_user.get("role", "user").lower()
        email = current_user.get("email", "")
       
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
            "$nor": [
                {"Event Type": {"$regex": "Cells", "$options": "i"}},
                {"eventType": {"$regex": "Cells", "$options": "i"}},
                {"eventTypeName": {"$regex": "Cells", "$options": "i"}}
            ]
        }

        user_email = current_user.get("email", "").lower()
       
        if personal:
            print(f"Applying PERSONAL filter for user: {user_email}")
            query["$or"] = [
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]
        elif user_role == "user":
            print(f"Regular user - showing personal events: {user_email}")
            query["$or"] = [
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]

        if event_type and event_type.lower() != 'all':
            print(f"Filtering by event type: '{event_type}'")
           
            event_type_query = {
                "$or": [
                    {"Event Type": {"$regex": f"^{event_type}$", "$options": "i"}},
                    {"eventType": {"$regex": f"^{event_type}$", "$options": "i"}},
                    {"eventTypeName": {"$regex": f"^{event_type}$", "$options": "i"}}
                ]
            }
           
            if "$or" in query:
                query = {"$and": [query, event_type_query]}
            else:
                query["$or"] = event_type_query["$or"]
           
            print(f"Event type filter applied: {event_type_query}")

        if search and search.strip():
            search_term = search.strip()
            print(f"Applying search filter: '{search_term}'")
            safe_search_term = re.escape(search_term)
            search_query = {
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
            }
            query = {"$and": [query, search_query]}
            print(f"Search query applied: {search_query}")

        print(f"Final query: {query}")

        cursor = events_collection.find(query)
        events = await cursor.to_list(length=1000)
       
        print(f"Found {len(events)} other events")

        if events and event_type and event_type.lower() != 'all':
            found_event_types = set()
            for event in events:
                found_event_types.add(event.get("Event Type"))
                found_event_types.add(event.get("eventType"))
                found_event_types.add(event.get("eventTypeName"))
            print(f"Event types found in results: {found_event_types}")

        other_events = []

        for event in events:
            try:
                event_name = event.get("Event Name") or event.get("eventName", "")
                event_type_value = event.get("Event Type") or event.get("eventType", "Event")
               
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

                # If no day is stored, calculate it from the date
                if not day_name:
                    try:
                        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                        day_name = days[event_date.weekday()]
                        print(f"Calculated day '{day_name}' from date {event_date}")
                    except Exception as e:
                        print(f"Error calculating day from date: {e}")
                        day_name = "One-time"

                actual_day_value = day_name.capitalize() if day_name else "One-time"

                if event_date < start_date_obj or event_date > end_date_obj:
                    continue

                attendance_data = event.get("attendance", {})
                event_date_iso = event_date.isoformat()
                event_attendance = attendance_data.get(event_date_iso, {})
               
                did_not_meet = event_attendance.get("status") == "did_not_meet"
                weekly_attendees = event_attendance.get("attendees", [])
                has_weekly_attendees = len(weekly_attendees) > 0
               
                main_event_status = event.get("status", "").lower()
                main_event_did_not_meet = event.get("did_not_meet", False)
                main_event_complete = event.get("Status", "").lower() == "complete"
               
                if did_not_meet or main_event_did_not_meet or main_event_status == "did_not_meet":
                    event_status = "did_not_meet"
                elif has_weekly_attendees or main_event_complete or main_event_status == "complete":
                    event_status = "complete"
                else:
                    event_status = "incomplete"
               
                print(f"Event '{event_name}' status - weekly: {event_attendance.get('status')}, main: {main_event_status}, final: {event_status}")

                if status and status != event_status:
                    continue

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
                    "attendees": weekly_attendees,
                    "hasPersonSteps": False,
                    "status": event_status,
                    "Status": event_status.replace("_", " ").title(),
                    "_is_overdue": event_date < today and event_status == "incomplete",
                    "is_recurring": False,
                    "original_event_id": str(event.get("_id"))
                }
               
                if "persistent_attendees" in event:
                    print(f"Removing persistent_attendees from non-cell event: {event_name}")
               
                other_events.append(instance)
                print(f"Other event: {event_name} on {event_date} (Day: {actual_day_value}, Status: {event_status})")

            except Exception as e:
                print(f"Error processing other event: {str(e)}")
                continue

        other_events.sort(key=lambda x: x['date'], reverse=True)
       
        total_count = len(other_events)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated_events = other_events[skip:skip + limit]

        print(f"Returning {len(paginated_events)} other events (page {page}/{total_pages})")
        print(f"Status breakdown for other events:")
        status_counts = {}
        for event in other_events:
            status_counts[event['status']] = status_counts.get(event['status'], 0) + 1
        for stat, count in status_counts.items():
            print(f"   - {stat}: {count}")

        return {
            "events": paginated_events,
            "total_events": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }

    except Exception as e:
        print(f"ERROR in /events/other: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
  

@app.get("/events/global")
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
                print(f"Processing event {event.get('_id')}: {event.get('eventName', event.get('Event Name', 'Unknown'))}")
               
                
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


@app.put("/events/{event_id}")
async def update_event(event_id: str, event_data: dict):
    """
    FIXED: Update event by _id or UUID
    """
    try:
        print(f"Attempting to update event with ID: {event_id}")
        print(f" Received data: {event_data}")
       
        # Try to find event by _id first (MongoDB ObjectId)
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
       
        # Prepare update data
        update_data = {}
       
        # Fields that can be updated
        updatable_fields = [
            'eventName', 'day', 'location', 'date',
            'status', 'renocaming', 'eventLeader',
            'eventType', 'isTicketed', 'isGlobal'
        ]
       
        for field in updatable_fields:
            if field in event_data and event_data[field] is not None:
                update_data[field] = event_data[field]
       
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
       
        # Fetch and return the updated event
        updated_event = await events_collection.find_one({"_id": event["_id"]})
        updated_event["_id"] = str(updated_event["_id"])
       
        return updated_event
       
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
    

@app.delete("/events/{event_id}")
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
            print(f"🔍 Checking if event exists with different casing or format...")
            
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


@app.get("/events/{event_id}")
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

@app.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Fixed: Uses event date to determine week, not current date
    """
    try:
        print(f"=" * 80)
        print(f"📤 SUBMIT ATTENDANCE STARTED")
        print(f"Event ID: {event_id}")
        print(f"Submission keys: {list(submission.keys())}")

        # EXTRACT OBJECTID FROM COMPOSITE ID
        actual_event_id = event_id
        extracted_date = None
        
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                print(f"Extracted ObjectId: {actual_event_id}")
                
                # Try to extract date from the composite ID
                if len(parts) >= 2:
                    try:
                        date_str = parts[1]
                        # Handle different date formats
                        if "T" in date_str:
                            extracted_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        else:
                            # Just date part
                            extracted_date = datetime.strptime(date_str, "%Y-%m-%d")
                        print(f"Extracted date from event_id: {extracted_date}")
                    except Exception as e:
                        print(f"Could not parse date from event_id: {e}")
            else:
                raise HTTPException(status_code=400, detail="Invalid event ID format")
       
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
       
        # GET EVENT FROM DATABASE
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            print(f"Event not found: {actual_event_id}")
            raise HTTPException(status_code=404, detail="Event not found")
       
        event_name = event.get("Event Name", "Unknown")
        print(f"Found event: {event_name}")
        print(f"Event day: {event.get('Day')}")
        print(f"Event date field: {event.get('date')}")
        print(f"Event Date Of Event: {event.get('Date Of Event')}")

        # **CRITICAL FIX: Determine which date/week this attendance is for**
        event_date = None
        
        # Priority 1: Use extracted date from event_id (most accurate)
        if extracted_date:
            event_date = extracted_date
            print(f"Using extracted date from event_id: {event_date}")
        
        # Priority 2: Check if submission has a specific date
        if not event_date and submission.get('event_date'):
            try:
                event_date = datetime.fromisoformat(submission['event_date'].replace("Z", "+00:00"))
                print(f"Using date from submission: {event_date}")
            except:
                pass
        
        # Priority 3: Use event's date fields
        if not event_date:
            event_date = event.get("date") or event.get("Date Of Event")
            if event_date:
                print(f"Using date from event data: {event_date}")
                if isinstance(event_date, str):
                    try:
                        event_date = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
                    except:
                        # Try other formats
                        try:
                            event_date = datetime.strptime(event_date, "%Y-%m-%d")
                        except:
                            event_date = None
        
        # Priority 4: If still no date, use current date but calculate based on event's day
        if not event_date:
            # Calculate based on event's day of week
            day_name = str(event.get("Day") or event.get("day") or "").strip().lower()
            day_mapping = {
                'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                'friday': 4, 'saturday': 5, 'sunday': 6
            }
            
            if day_name in day_mapping:
                target_weekday = day_mapping[day_name]
                today = datetime.utcnow()
                current_weekday = today.weekday()
                
                # Find the most recent occurrence of this day
                days_since = (current_weekday - target_weekday) % 7
                event_date = today - timedelta(days=days_since)
                event_date = event_date.replace(hour=0, minute=0, second=0, microsecond=0)
                print(f"Calculated date based on day '{day_name}': {event_date}")
            else:
                # Fallback to today
                event_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                print(f"Using today as fallback: {event_date}")
        
        # **Calculate week identifier based on event date**
        sa_timezone = pytz.timezone("Africa/Johannesburg")
        
        # Ensure event_date is timezone aware
        if event_date.tzinfo is None:
            event_date = pytz.utc.localize(event_date)
        
        # Convert to South Africa timezone
        event_date_sa = event_date.astimezone(sa_timezone)
        year, week, _ = event_date_sa.isocalendar()
        current_week = f"{year}-W{week:02d}"
        
        print(f"FINAL DATE CALCULATION:")
        print(f"  Event date: {event_date}")
        print(f"  SA timezone date: {event_date_sa}")
        print(f"  Week identifier: {current_week}")
        print(f"  Day of week: {event_date_sa.strftime('%A')}")

        # EXTRACT SUBMISSION DATA
        attendees_data = submission.get('attendees', [])
        if not attendees_data and 'payload' in submission:
            attendees_data = submission['payload'].get('attendees', [])
       
        print(f"👥 Attendees data length: {len(attendees_data)}")

        # EXTRACT PERSISTENT ATTENDEES
        persistent_attendees = submission.get('persistent_attendees', [])
        if not persistent_attendees and 'payload' in submission:
            persistent_attendees = submission['payload'].get('persistent_attendees', [])
       
        if not persistent_attendees:
            persistent_attendees = submission.get('all_attendees', [])
            if not persistent_attendees and 'payload' in submission:
                persistent_attendees = submission['payload'].get('all_attendees', [])

        print(f"Persistent attendees: {len(persistent_attendees)}")

        # PROCESS PERSISTENT ATTENDEES
        persistent_attendees_dict = []
        if persistent_attendees and isinstance(persistent_attendees, list):
            for attendee in persistent_attendees:
                if isinstance(attendee, dict):
                    clean_attendee = {
                        "id": attendee.get("id", ""),
                        "name": attendee.get("name", ""),
                        "fullName": attendee.get("fullName", attendee.get("name", "")),
                        "email": attendee.get("email", ""),
                        "phone": attendee.get("phone", ""),
                        "leader12": attendee.get("leader12", ""),
                        "leader144": attendee.get("leader144", ""),
                        "isPersistent": True
                    }
                    persistent_attendees_dict.append(clean_attendee)
        else:
            print("No persistent attendees found or invalid format")

        # SAFELY PROCESS DID_NOT_MEET
        did_not_meet = submission.get('did_not_meet', False)
        if not did_not_meet and 'payload' in submission:
            did_not_meet = submission['payload'].get('did_not_meet', False)

        print(f"Did not meet: {did_not_meet}")

        # INITIALIZE ALL VARIABLES AT THE START
        checked_in_attendees = []
        weekly_attendance_entry = {}
        main_update_fields = {}

        # PROCESS ATTENDEES FOR CHECK-IN (regardless of did_not_meet status)
        print(f"👥 Processing attendees for check-in")
        if attendees_data and isinstance(attendees_data, list):
            for att in attendees_data:
                if isinstance(att, dict):
                    attendee_data = {
                        "id": att.get("id", ""),
                        "name": att.get("name", ""),
                        "fullName": att.get("fullName", att.get("name", "")),
                        "email": att.get("email", ""),
                        "phone": att.get("phone", ""),
                        "leader12": att.get("leader12", ""),
                        "leader144": att.get("leader144", ""),
                        "checked_in": True,
                        "check_in_date": datetime.utcnow().isoformat(),
                        "isPersistent": att.get("isPersistent", False)
                    }
                    checked_in_attendees.append(attendee_data)

        # HANDLE DID_NOT_MEET LOGIC
        if did_not_meet:
            print(f"Marking as 'Did Not Meet' for week {current_week} (date: {event_date_sa.date()})")
            weekly_attendance_entry = {
                "status": "did_not_meet",
                "attendees": [],
                "submitted_at": datetime.utcnow(),
                "submitted_by": current_user.get('email', ''),
                "submitted_by_name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                "submitted_date": datetime.utcnow().isoformat(),
                "event_date": event_date_sa.isoformat(),  # Store the specific event date
                "event_date_iso": event_date_sa.date().isoformat(),  # Also store as simple date
                "persistent_attendees_count": len(persistent_attendees_dict),
                "week_identifier": current_week,
                "is_did_not_meet": True
            }
           
            main_update_fields = {
                "Status": "Did Not Meet",
                "status": "did_not_meet",
                "did_not_meet": True,
                "Date Captured": datetime.utcnow().strftime("%d %B %Y"),
                "updated_at": datetime.utcnow(),
            }
        else:
            # HANDLE REGULAR ATTENDANCE
            if len(checked_in_attendees) == 0:
                print(f"No attendees checked in - marking as incomplete for week {current_week}")
                weekly_attendance_entry = {
                    "status": "incomplete",
                    "attendees": [],
                    "submitted_at": datetime.utcnow(),
                    "submitted_by": current_user.get('email', ''),
                    "submitted_by_name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                    "submitted_date": datetime.utcnow().isoformat(),
                    "event_date": event_date_sa.isoformat(),
                    "event_date_iso": event_date_sa.date().isoformat(),
                    "persistent_attendees_count": len(persistent_attendees_dict),
                    "week_identifier": current_week,
                    "is_did_not_meet": False
                }
               
                main_update_fields = {
                    "Status": "Incomplete",
                    "status": "incomplete",
                    "did_not_meet": False,
                    "Date Captured": datetime.utcnow().strftime("%d %B %Y"),
                    "updated_at": datetime.utcnow(),
                }
            else:
                weekly_attendance_entry = {
                    "status": "complete",
                    "attendees": checked_in_attendees,
                    "submitted_at": datetime.utcnow(),
                    "submitted_by": current_user.get('email', ''),
                    "submitted_by_name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                    "submitted_date": datetime.utcnow().isoformat(),
                    "event_date": event_date_sa.isoformat(),
                    "event_date_iso": event_date_sa.date().isoformat(),
                    "persistent_attendees": persistent_attendees_dict,
                    "week_identifier": current_week,
                    "is_did_not_meet": False,
                    "checked_in_count": len(checked_in_attendees)
                }
                print(f"MARKING AS COMPLETE for date {event_date_sa.date()} with {len(checked_in_attendees)} attendees")
               
                main_update_fields = {
                    "Status": "Complete",
                    "status": "complete",
                    "did_not_meet": False,
                    "Date Captured": datetime.utcnow().strftime("%d %B %Y"),
                    "updated_at": datetime.utcnow(),
                }

        # Also update persistent attendees if provided
        if persistent_attendees_dict:
            main_update_fields["persistent_attendees"] = persistent_attendees_dict

        # PREPARE UPDATE DATA
        update_data = {
            **main_update_fields,
            f"attendance.{current_week}": weekly_attendance_entry
        }

        print(f" Saving to database:")
        print(f"   - Event: {event_name}")
        print(f"   - Event date: {event_date_sa.date()}")
        print(f"   - Week: {current_week}")
        print(f"   - Status: {weekly_attendance_entry.get('status', 'unknown')}")
        print(f"   - Persistent attendees: {len(persistent_attendees_dict)}")
        print(f"   - Checked-in this week: {len(checked_in_attendees)}")
        print(f"   - Update fields: {list(update_data.keys())}")

        # UPDATE DATABASE
        result = await events_collection.update_one(
            {"_id": ObjectId(actual_event_id)},
            {"$set": update_data}
        )
       
        print(f" Database result - matched: {result.matched_count}, modified: {result.modified_count}")
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
       
        # PREPARE RESPONSE
        response_data = {
            "message": "Attendance submitted successfully",
            "event_id": actual_event_id,
            "event_name": event_name,
            "status": weekly_attendance_entry.get("status", "unknown"),
            "did_not_meet": did_not_meet,
            "checked_in_count": len(checked_in_attendees),
            "persistent_attendees_count": len(persistent_attendees_dict),
            "week": current_week,
            "event_date": event_date_sa.date().isoformat(),
            "event_day": event_date_sa.strftime("%A"),
            "success": True,
            "timestamp": datetime.utcnow().isoformat()
        }

        print(f"ATTENDANCE SUBMISSION SUCCESSFUL")
        print(f"=" * 80)
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        print(f" ERROR in submit_attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



# EVENTS TYPES SECTION---------------------------------------------------
@app.post("/event-types")
async def create_event_type(event_type: EventTypeCreate):
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        name = event_type.name.strip().title()

        exists = await events_collection.find_one({"isEventType": True, "name": name})
        if exists:
            raise HTTPException(status_code=400, detail="Event type already exists.")

        event_type_data = event_type.dict()
        event_type_data["name"] = name
        event_type_data["isEventType"] = True
        event_type_data["createdAt"] = event_type_data.get("createdAt") or datetime.utcnow()
       
        name_lower = name.lower()
       
        if event_type_data.get("isGlobal") is None:
            event_type_data["isGlobal"] = "global" in name_lower
           
        if event_type_data.get("hasPersonSteps") is None:
            event_type_data["hasPersonSteps"] = any(keyword in name_lower for keyword in ["cell", "person", "individual"])
       
        if not event_type_data.get("UUID"):
            event_type_data["UUID"] = str(uuid.uuid4())

        result = await events_collection.insert_one(event_type_data)
        inserted = await events_collection.find_one({"_id": result.inserted_id})
        inserted["_id"] = str(inserted["_id"])
       
        return inserted

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating event type: {str(e)}")
   
@app.get("/event-types")
async def get_event_types():
    try:
        cursor = events_collection.find({
            "isEventType": True
        }).sort("createdAt", 1)
       
        event_types = []
        async for et in cursor:
           
            et["_id"] = str(et["_id"])
            event_types.append(et)
       
        print(f" Found {len(event_types)} event types (isEventType=True)")
       
        for et in event_types:
            print(f"   - {et.get('name')} (ID: {et.get('_id')})")
       
        return event_types
       
    except Exception as e:
        print(f"Error fetching event types: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching event types: {str(e)}")

@app.put("/event-types/{event_type_name}")
async def update_event_type(
    event_type_name: str,
    updated_data: EventTypeCreate = Body(...)
):
    try:
        # Decode the URL-encoded event type name
        decoded_event_type_name = unquote(event_type_name)
       
        print(f"[EVENT-TYPE UPDATE] Looking for: '{decoded_event_type_name}'")
        print(f"[EVENT-TYPE UPDATE] Update data: {updated_data.dict()}")
       
        # Check if event type exists - FIXED: Use case-insensitive search
        existing_event_type = await events_collection.find_one({
            "name": {"$regex": f"^{decoded_event_type_name}$", "$options": "i"},
            "isEventType": True
        })
       
        if not existing_event_type:
            print(f"[EVENT-TYPE UPDATE] Event type '{decoded_event_type_name}' not found")
            # Try to find by ID as well
            try:
                existing_event_type = await events_collection.find_one({
                    "_id": ObjectId(decoded_event_type_name),
                    "isEventType": True
                })
            except:
                pass
           
            if not existing_event_type:
                raise HTTPException(status_code=404, detail=f"Event type '{decoded_event_type_name}' not found")

        new_name = updated_data.name.strip().title()
        current_name = existing_event_type["name"]
        name_changed = new_name.lower() != current_name.lower()
       
        print(f"[EVENT-TYPE UPDATE] Name change: '{current_name}' -> '{new_name}' (changed: {name_changed})")
       
        if name_changed:
            duplicate = await events_collection.find_one({
                "name": {"$regex": f"^{new_name}$", "$options": "i"},
                "isEventType": True,
                "_id": {"$ne": existing_event_type["_id"]}
            })
            if duplicate:
                print(f"[EVENT-TYPE UPDATE] Duplicate: '{new_name}' already exists")
                raise HTTPException(status_code=400, detail="Event type with this name already exists")

        # Update events that reference this event type
        events_updated_count = 0
        if name_changed:
            print(f"[EVENT-TYPE UPDATE] Updating events from '{current_name}' to '{new_name}'")
           
            # Count and update events
            events_count = await events_collection.count_documents({
                "$or": [
                    {"eventType": current_name},
                    {"eventTypeName": current_name}
                ],
                "isEventType": {"$ne": True}
            })
           
            print(f"[EVENT-TYPE UPDATE] Found {events_count} events to update")
           
            if events_count > 0:
                events_update_result = await events_collection.update_many(
                    {
                        "$or": [
                            {"eventType": current_name},
                            {"eventTypeName": current_name}
                        ],
                        "isEventType": {"$ne": True}
                    },
                    {"$set": {
                        "eventType": new_name,
                        "eventTypeName": new_name,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                events_updated_count = events_update_result.modified_count
                print(f"[EVENT-TYPE UPDATE] Updated {events_updated_count} events")

        # Prepare update data for the event type itself
        update_data = updated_data.dict()
        update_data["name"] = new_name
        update_data["updatedAt"] = datetime.utcnow()
       
        # Remove None values and protect immutable fields
        update_data = {k: v for k, v in update_data.items() if v is not None}
       
        # Protect these fields from being overwritten
        immutable_fields = ["_id", "UUID", "createdAt", "isEventType"]
        for field in immutable_fields:
            update_data.pop(field, None)

        print(f"[EVENT-TYPE UPDATE] Final update data: {update_data}")

        # Update the event type document
        result = await events_collection.update_one(
            {"_id": existing_event_type["_id"]},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            print(f"[EVENT-TYPE UPDATE] No changes made to '{current_name}'")
            # Still return the existing event type
            existing_event_type["_id"] = str(existing_event_type["_id"])
            return existing_event_type

        # Fetch and return the updated event type
        updated_event_type = await events_collection.find_one({"_id": existing_event_type["_id"]})
        updated_event_type["_id"] = str(updated_event_type["_id"])
       
        print(f" [EVENT-TYPE UPDATE] Successfully updated to: {updated_event_type['name']}")
        print(f"[EVENT-TYPE UPDATE] Summary - Events updated: {events_updated_count}")
       
        return updated_event_type

    except HTTPException:
        raise
    except Exception as e:
        print(f"[EVENT-TYPE UPDATE] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating event type: {str(e)}")
   

@app.delete("/event-types/{event_type_name}")
async def delete_event_type(
    event_type_name: str,
    force: bool = Query(False, description="Force delete even if events exist")
):
    try:
        decoded_event_type_name = unquote(event_type_name)
       
        print(f" DELETE EVENT TYPE: {decoded_event_type_name}, force={force}")
       
        # Find the event type document
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
       
        print(f" Found event type: {actual_identifier}")
       
        events_query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        # Also check for the decoded name
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
       
        # Now delete the event type itself
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


def get_current_week_identifier():
    """Get current week identifier in format YYYY-WW using South Africa timezone"""
    try:
        sa_timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_timezone)
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"
    except Exception as e:
        print(f"Error getting week identifier: {e}")
        now = datetime.utcnow()
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"
    

def get_actual_event_status(event: dict, today: date) -> str:
    current_week = get_current_week_identifier()
   
    print(f"Checking status for: {event.get('Event Name', 'Unknown')}")
    print(f"   Current week: {current_week}")
   
    # Check if explicitly marked as did not meet
    if event.get("did_not_meet", False):
        print(f"Marked as 'did_not_meet'")
        return "did_not_meet"
   
    # Check weekly attendance data first
    if "attendance" in event and current_week in event["attendance"]:
        week_data = event["attendance"][current_week]
        week_status = week_data.get("status", "incomplete")
       
        print(f"Found week data - Status: {week_status}")
       
        if week_status == "complete":
            checked_in_count = len([a for a in week_data.get("attendees", []) if a.get("checked_in", False)])
            if checked_in_count > 0:
                print(f" Week marked complete with {checked_in_count} checked-in attendees")
                return "complete"
            else:
                print(f" Week marked complete but no checked-in attendees")
                return "incomplete"
        elif week_status == "did_not_meet":
            return "did_not_meet"
   
    attendees = event.get("attendees", [])
    has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
   
    if has_attendees:
        print(f"Found {len(attendees)} attendees in main array")
        return "complete"
   
    print(f"No attendance data found - marking as incomplete")
    return "incomplete"


def parse_event_date(event_date_field, default_date: date) -> date:
    if not event_date_field:
        return default_date
       
    if isinstance(event_date_field, datetime):
        return event_date_field.date()
    elif isinstance(event_date_field, date):
        return event_date_field
    elif isinstance(event_date_field, str):
        try:
            return datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                if " - " in event_date_field:
                    day, month, year = event_date_field.split(" - ")
                    parsed_date = datetime(int(year), int(month), int(day)).date()
                    print(f"Parsed date '{event_date_field}' -> {parsed_date}")
                    return parsed_date
                # Try other common formats
                return datetime.strptime(event_date_field, "%Y-%m-%d").date()
            except Exception as e:
                print(f"Could not parse date '{event_date_field}': {e}")
                return default_date
    else:
        return default_date

def calculate_this_week_event_date(
    event_day_name: str,
    today_date: date) -> date:
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    event_day_num = day_map.get(event_day_name.lower().strip(), -1)
   
    if event_day_num == -1:
        # Invalid day name, return a date far in the past to ensure it's filtered out
        return date.min

    days_since_monday = today_date.weekday()
   
    week_start_date = today_date - timedelta(days=days_since_monday)
   
    # Calculate the event's date within this Monday-Sunday week
    event_date = week_start_date + timedelta(days=event_day_num)
   
    return event_date


# Add more event-related utility functions as needed

