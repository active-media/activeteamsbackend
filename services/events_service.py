
from datetime import datetime, date, timedelta
from bson import ObjectId
from typing import Optional, List, Dict, Any
from fastapi import HTTPException
from database import events_collection, people_collection
import pytz
import uuid

async def create_event(event_data):
    """Create a new event - extract from main.py line ~858"""
    # TODO: Extract logic from main.py
    pass

async def get_cell_events(current_user, page, limit, status, search, event_type, personal, start_date, **kwargs):
    """Get cell events - extract from main.py line ~1019"""
    # TODO: Extract logic from main.py
    pass

async def get_other_events(current_user, page, limit, status, event_type, search, personal, start_date, end_date):
    """Get other events - extract from main.py line ~1761"""
    # TODO: Extract logic from main.py
    pass

async def get_global_events(current_user, page, limit, status, search, start_date, last_updated):
    """Get global events - extract from main.py line ~4055"""
    # TODO: Extract logic from main.py
    pass

async def update_event(event_id: str, event_data: dict):
    """Update event - extract from main.py line ~4658"""
    # TODO: Extract logic from main.py
    pass

async def delete_event(event_id: str):
    """Delete event - extract from main.py line ~6521"""
    # TODO: Extract logic from main.py
    pass

async def get_event_by_id(event_id: str):
    """Get event by ID - extract from main.py line ~6654"""
    # TODO: Extract logic from main.py
    pass

async def submit_attendance(event_id: str, submission: dict, current_user: dict):
    """Submit attendance - extract from main.py line ~6122"""
    # TODO: Extract logic from main.py
    pass

async def create_event_type(event_type_data):
    """Create event type - extract from main.py line ~2088"""
    # TODO: Extract logic from main.py
    pass

async def get_event_types():
    """Get event types - extract from main.py line ~2127"""
    # TODO: Extract logic from main.py
    pass

async def update_event_type(event_type_name: str, updated_data):
    """Update event type - extract from main.py line ~2467"""
    # TODO: Extract logic from main.py
    pass

async def delete_event_type(event_type_name: str, force: bool):
    """Delete event type - extract from main.py line ~2593"""
    # TODO: Extract logic from main.py
    pass

def get_current_week_identifier():
    """Get current week identifier - extract from main.py line ~843"""
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
    """Get actual event status - extract from main.py line ~2949"""
    # TODO: Extract logic from main.py
    pass

def parse_event_date(event_date_field, default_date: date) -> date:
    """Parse event date - extract from main.py line ~2988"""
    # TODO: Extract logic from main.py
    pass

def calculate_this_week_event_date(event_day_name: str, today_date: date) -> date:
    """Calculate this week's event date - extract from main.py line ~3022"""
    # TODO: Extract logic from main.py
    pass

# Add more event-related utility functions as needed

"""Events service - handles all event-related business logic"""
from datetime import datetime, timedelta, time, date
from typing import List, Optional
import re
import uuid
import pytz
from activeteamsbackend.main import parse_time
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from urllib.parse import unquote
import logging

from activeteamsbackend.auth.models import EventCreate, EventTypeCreate
from activeteamsbackend.auth.utils import get_current_user, convert_datetime_to_iso, sanitize_document
from database import events_collection, people_collection

app = APIRouter(prefix="/events", tags=["events"])

# Enhanced cache storage with background loading
people_cache = {
    "data": [],
    "last_updated": None,
    "expires_at": None,
    "is_loading": False,
    "background_task": None,
    "load_progress": 0,
    "total_loaded": 0,
    "last_error": None,
    "total_in_database": 0
}

@app.on_event("startup")
async def startup_event():
    """Start background loading of all people on startup"""
    print(" Starting background load of ALL people...")
    asyncio.create_task(background_load_all_people())


# Simple helper functions
def get_sa_week() -> str:
    """Get current week in SA timezone."""
    sa_tz = pytz.timezone("Africa/Johannesburg")
    now = datetime.now(sa_tz)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def parse_date_str(date_str) -> date:
    """Parse date string to date object."""
    if not date_str:
        return date.today()
    
    if isinstance(date_str, date):
        return date_str
    if isinstance(date_str, datetime):
        return date_str.date()
    
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            return date.today()


async def get_event(event_id: str) -> dict:
    """Find event by ID or UUID."""
    if ObjectId.is_valid(event_id):
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if event:
            return event
    
    return await events_collection.find_one({"UUID": event_id})

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
 
def parse_event_datetime(event: dict, timezone) -> datetime:
    """
    Parse event datetime from various formats
    """
    event_date_field = event.get("Date Of Event")
    event_time = event.get("Time", "19:00")  # Default to 7:00 PM
   
    # Parse date
    if event_date_field:
        if isinstance(event_date_field, datetime):
            event_date = event_date_field.date()
        elif isinstance(event_date_field, str):
            try:
                event_date = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
            except ValueError:
                event_date = datetime.now(timezone).date()
        else:
            event_date = datetime.now(timezone).date()
    else:
        event_date = datetime.now(timezone).date()
   
    # Parse time
    hour, minute = parse_time(event_time)
   
    # Combine date and time
    event_datetime = datetime.combine(event_date, time(hour, minute))
   
    # Localize to timezone
    return timezone.localize(event_datetime)


# Event endpoints--------------------------
# POST ---
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


async def find_person_by_name(name: str):
    """
    Helper function to find a person by name using multiple search strategies
    """
    if not name or not name.strip():
        return None
   
    cleaned_name = name.strip()
   
    search_queries = [
        # Exact name match
        {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
        # Full name match
        {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
        # Partial name match
        {"Name": {"$regex": cleaned_name, "$options": "i"}},
        # First name only
        {"Name": {"$regex": f"^{cleaned_name.split()[0]}$", "$options": "i"}} if " " in cleaned_name else None,
    ]
   
    # Remove None queries
    search_queries = [q for q in search_queries if q is not None]
   
    for query in search_queries:
        person = await people_collection.find_one(query)
        if person:
            return person
   
    return None


@app.get("/leaders")
async def get_all_leaders():
    try:
        people = await people_collection.find({}).to_list(length=None)
        leaders = []

        for person in people:
            # Leader @12
            if person.get("Leader @12"):
                leader_name = person["Leader @12"].strip()
                if leader_name:
                    leaders.append({
                        "name": leader_name.title(),
                        "position": 12
                    })

            # Leader @144
            if person.get("Leader @144"):
                leader_name = person["Leader @144"].strip()
                if leader_name:
                    leaders.append({
                        "name": leader_name.title(),
                        "position": 144
                    })

        # Remove duplicates (same name & position)
        unique_leaders = [dict(t) for t in {tuple(d.items()) for d in leaders}]

        # Sort by position and name for cleaner frontend usage
        unique_leaders.sort(key=lambda x: (x["position"], x["name"]))

        return {"leaders": unique_leaders}

    except Exception as e:
        print(f"Error fetching leaders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
   
#-------- MIGRATION ENDPOINTS -------------
@app.post("/migrate-event-types-uuids")
async def migrate_event_types_uuids():
    """ ONE-TIME: Add UUIDs to event types that don't have them"""
    try:
        import uuid
       
        # Find all event types without UUIDs
        cursor = events_collection.find({
            "isEventType": True,
            "UUID": {"$exists": False}  # Only those without UUIDs
        })
       
        migrated_count = 0
        async for event_type in cursor:
            # Generate UUID for existing event type
            await events_collection.update_one(
                {"_id": event_type["_id"]},
                {"$set": {"UUID": str(uuid.uuid4())}}
            )
            migrated_count += 1
            print(f"Added UUID to event type: {event_type['name']}")
       
        return {
            "message": f"Added UUIDs to {migrated_count} event types",
            "migrated_count": migrated_count
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


@app.get("/cache/people")
async def get_cached_people():
    """
    Get cached people data - returns whatever is available immediately
    """
    try:
        current_time = datetime.utcnow()
       
        # If we have data and it's not expired, return it
        if (people_cache["data"] and
            people_cache["expires_at"] and
            current_time < datetime.fromisoformat(people_cache["expires_at"])):
           
            print(f"CACHE HIT: Returning {len(people_cache['data'])} people")
            return {
                "success": True,
                "cached_data": people_cache["data"],
                "cached_at": people_cache["last_updated"],
                "expires_at": people_cache["expires_at"],
                "source": "cache",
                "total_count": len(people_cache["data"]),
                "is_complete": True,
                "load_progress": 100
            }
       
        # If we're still loading in background, return progress
        if people_cache["is_loading"]:
            return {
                "success": True,
                "cached_data": people_cache["data"],  # Return whatever we have so far
                "cached_at": people_cache["last_updated"],
                "source": "loading",
                "total_count": len(people_cache["data"]),
                "is_complete": False,
                "load_progress": people_cache["load_progress"],
                "loaded_so_far": people_cache["total_loaded"],
                "total_in_database": people_cache["total_in_database"],
                "message": f"Loading in background... {people_cache['load_progress']}% complete"
            }
       
        # If cache is empty/expired and not loading, trigger background load
        if not people_cache["data"] and not people_cache["is_loading"]:
            print("Cache empty, triggering background load...")
            asyncio.create_task(background_load_all_people())
           
            # Return empty but indicate loading will start
            return {
                "success": True,
                "cached_data": [],
                "cached_at": None,
                "source": "triggered_load",
                "total_count": 0,
                "is_complete": False,
                "message": "Background loading started...",
                "load_progress": 0
            }
           
        # If we have some data but it's expired, return it anyway while refreshing
        if people_cache["data"]:
            print("Cache expired, returning stale data while refreshing...")
            # Trigger refresh in background
            if not people_cache["is_loading"]:
                asyncio.create_task(background_load_all_people())
           
            return {
                "success": True,
                "cached_data": people_cache["data"],
                "cached_at": people_cache["last_updated"],
                "expires_at": people_cache["expires_at"],
                "source": "stale_cache",
                "total_count": len(people_cache["data"]),
                "is_complete": True,
                "message": "Using stale data (refresh in progress)"
            }
       
        # Fallback - return empty
        return {
            "success": True,
            "cached_data": [],
            "cached_at": None,
            "source": "empty",
            "total_count": 0,
            "is_complete": False
        }
       
    except Exception as e:
        print(f"Error in cache endpoint: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "cached_data": [],
            "total_count": 0
        }

@app.post("/cache/people/refresh")
async def refresh_people_cache():
    """
    Manually refresh the people cache
    """
    try:
        if not people_cache["is_loading"]:
            print("Manual cache refresh triggered")
            asyncio.create_task(background_load_all_people())
           
        return {
            "success": True,
            "message": "Cache refresh triggered",
            "is_loading": people_cache["is_loading"],
            "current_progress": people_cache["load_progress"]
        }
       
    except Exception as e:
        print(f"Error refreshing cache: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/cache/people/status")
async def get_cache_status():
    """
    Get detailed cache status and loading progress
    """
    total_in_db = await people_collection.count_documents({})
    cache_size = len(people_cache["data"])
   
    status_info = {
        "cache": {
            "size": cache_size,
            "last_updated": people_cache["last_updated"],
            "expires_at": people_cache["expires_at"],
            "is_loading": people_cache["is_loading"],
            "load_progress": people_cache["load_progress"],
            "total_loaded": people_cache["total_loaded"],
            "last_error": people_cache["last_error"]
        },
        "database": {
            "total_people": total_in_db,
            "coverage_percentage": round((cache_size / total_in_db) * 100, 1) if total_in_db > 0 else 0
        },
        "is_complete": cache_size >= total_in_db if total_in_db > 0 else True
    }
   
    return status_info

@app.get("/people/search")
async def search_people(
    query: str = Query("", min_length=2),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Fast search through cached people data
    """
    try:
        if not people_cache["data"]:
            return {
                "success": False,
                "error": "Cache not ready",
                "results": []
            }
       
        search_term = query.lower().strip()
        results = []
       
        # Search through cached data (very fast)
        for person in people_cache["data"]:
            if (search_term in person.get("FullName", "").lower() or
                search_term in person.get("Email", "").lower() or
                search_term in person.get("Number", "")):
                results.append(person)
               
            if len(results) >= limit:
                break
       
        return {
            "success": True,
            "results": results,
            "total_found": len(results),
            "search_term": query,
            "source": "cache"
        }
       
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "results": []
        } 

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
    event_id: str,
    submission: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """Submit attendance for an event."""
    try:
        # Extract ID and date
        parts = event_id.split("_")
        actual_id = parts[0] if ObjectId.is_valid(parts[0]) else event_id
        event_date = None
        
        if len(parts) > 1:
            try:
                date_str = parts[1]
                if "T" in date_str:
                    event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d")
            except:
                pass
        
        if not ObjectId.is_valid(actual_id):
            raise HTTPException(400, "Invalid event ID")
        
        # Get event
        event = await events_collection.find_one({"_id": ObjectId(actual_id)})
        if not event:
            raise HTTPException(404, "Event not found")
        
        # Determine date
        if not event_date and submission.get('event_date'):
            event_date = datetime.fromisoformat(submission['event_date'].replace("Z", "+00:00"))
        if not event_date:
            event_date = parse_date_str(event.get("date"))
            if isinstance(event_date, date):
                event_date = datetime.combine(event_date, datetime.min.time())
        
        # Calculate week
        sa_tz = pytz.timezone("Africa/Johannesburg")
        if event_date.tzinfo is None:
            event_date = pytz.utc.localize(event_date)
        event_date_sa = event_date.astimezone(sa_tz)
        year, week, _ = event_date_sa.isocalendar()
        week_id = f"{year}-W{week:02d}"
        
        # Process data
        attendees = submission.get('attendees', []) or submission.get('payload', {}).get('attendees', [])
        persistent = submission.get('persistent_attendees', []) or submission.get('payload', {}).get('persistent_attendees', [])
        did_not_meet = submission.get('did_not_meet', False) or submission.get('payload', {}).get('did_not_meet', False)
        
        # Create attendance entry
        if did_not_meet:
            status = "did_not_meet"
            checked_in = []
        elif attendees:
            status = "complete"
            checked_in = [{
                "id": a.get("id", ""),
                "name": a.get("name", ""),
                "email": a.get("email", ""),
                "checked_in": True,
                "check_in_date": datetime.utcnow().isoformat()
            } for a in attendees if isinstance(a, dict)]
        else:
            status = "incomplete"
            checked_in = []
        
        weekly_entry = {
            "status": status,
            "attendees": checked_in,
            "submitted_at": datetime.utcnow(),
            "submitted_by": current_user.get('email', ''),
            "event_date": event_date_sa.isoformat(),
            "week_identifier": week_id
        }
        
        # Update database
        update_data = {
            f"attendance.{week_id}": weekly_entry,
            "updated_at": datetime.utcnow()
        }
        
        if persistent:
            update_data["persistent_attendees"] = persistent
        
        await events_collection.update_one(
            {"_id": ObjectId(actual_id)},
            {"$set": update_data}
        )
        
        return {
            "message": "Attendance submitted successfully",
            "status": status,
            "checked_in_count": len(checked_in),
            "week": week_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.put("/events/{event_id}/toggle-active")
async def toggle_event_active_status(
    event_id: str,
    is_active: bool = Query(...),
    reason: Optional[str] = Query(None),
    weeks: Optional[int] = Query(2),
    current_user: dict = Depends(get_current_user)
):
    """
    Toggle active status of an event
    """
    try:
        # Check if event exists
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        update_data = {
            "is_active": is_active,
            "updated_at": datetime.utcnow()
        }
        
        if is_active:
            # Reactivating - clear deactivation fields
            update_data.update({
                "deactivation_start": None,
                "deactivation_end": None,
                "deactivation_reason": None
            })
        else:
            # Deactivating - set deactivation period
            deactivation_start = datetime.utcnow()
            deactivation_end = deactivation_start + timedelta(weeks=weeks)
            
            update_data.update({
                "deactivation_start": deactivation_start,
                "deactivation_end": deactivation_end,
                "deactivation_reason": reason or "Manually deactivated"
            })
        
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=400, detail="Failed to update event status")
        
        status_text = "activated" if is_active else f"deactivated for {weeks} weeks"
        return {
            "success": True,
            "message": f"Event {status_text} successfully",
            "is_active": is_active,
            "deactivation_end": update_data.get("deactivation_end")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/events/person/{leader_name}/toggle-active-all")
async def toggle_all_person_events_active_status(
    leader_name: str,
    is_active: bool = Query(...),
    reason: Optional[str] = Query(None),
    weeks: Optional[int] = Query(2),
    current_user: dict = Depends(get_current_user)
):
    """
    Toggle active status for all events of a person
    """
    try:
        # Find all events for this leader
        events = await events_collection.find({
            "$or": [
                {"eventLeader": leader_name},
                {"eventLeaderName": leader_name},
                {"Leader": leader_name}
            ]
        }).to_list(None)
        
        if not events:
            raise HTTPException(status_code=404, detail=f"No events found for {leader_name}")
        
        update_count = 0
        for event in events:
            update_data = {
                "is_active": is_active,
                "updated_at": datetime.utcnow()
            }
            
            if is_active:
                update_data.update({
                    "deactivation_start": None,
                    "deactivation_end": None,
                    "deactivation_reason": None
                })
            else:
                deactivation_start = datetime.utcnow()
                deactivation_end = deactivation_start + timedelta(weeks=weeks)
                
                update_data.update({
                    "deactivation_start": deactivation_start,
                    "deactivation_end": deactivation_end,
                    "deactivation_reason": reason or f"All events deactivated for {leader_name}"
                })
            
            result = await events_collection.update_one(
                {"_id": event["_id"]},
                {"$set": update_data}
            )
            update_count += result.modified_count
        
        status_text = "activated" if is_active else f"deactivated for {weeks} weeks"
        return {
            "success": True,
            "message": f"{update_count} events {status_text} successfully",
            "count": update_count,
            "is_active": is_active
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/event-types")
async def create_event_type(event_type: EventTypeCreate):
    """Create a new event type."""
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(400, "Name and description are required")
        
        name = event_type.name.strip().title()
        
        # Check duplicate
        existing = await events_collection.find_one({
            "isEventType": True,
            "name": name
        })
        if existing:
            raise HTTPException(400, "Event type already exists")
        
        # Create
        data = event_type.dict()
        data.update({
            "name": name,
            "isEventType": True,
            "createdAt": datetime.utcnow(),
            "UUID": str(uuid.uuid4()),
            "isGlobal": "global" in name.lower(),
            "hasPersonSteps": any(word in name.lower() for word in ["cell", "person"])
        })
        
        result = await events_collection.insert_one(data)
        inserted = await events_collection.find_one({"_id": result.inserted_id})
        inserted["_id"] = str(inserted["_id"])
        
        return inserted
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.put("/events/cells/{identifier}")
async def update_cell_event_working(identifier: str, event_data: dict):
    """
    WORKING VERSION: Update cell event
    """
    try:
        print(f"[WORKING UPDATE] Identifier: {identifier}")
        print(f"[WORKING UPDATE] Collection: {events_collection.name}")
        

        event = None
        if ObjectId.is_valid(identifier):
            print(f"[WORKING UPDATE] Trying as ObjectId...")
            event = await events_collection.find_one({"_id": ObjectId(identifier)})
        
        if not event:
            print(f"[WORKING UPDATE] Trying as Email (uppercase)...")
            event = await events_collection.find_one({"Email": identifier})
        
        # 3. Try lowercase email
        if not event:
            print(f"[WORKING UPDATE] Trying as email (lowercase)...")
            event = await events_collection.find_one({"email": identifier})
        
        # 4. Try Leader (uppercase L as shown in your data)
        if not event:
            print(f"[WORKING UPDATE] Trying as Leader...")
            event = await events_collection.find_one({"Leader": identifier})
        
        if not event:
            print(f"[WORKING UPDATE] Trying Event Name partial match...")
            event = await events_collection.find_one({
                "Event Name": {"$regex": identifier, "$options": "i"}
            })
        
        if not event:
            count = await events_collection.count_documents({})
            print(f"[WORKING UPDATE] Total docs in collection: {count}")
            
            email_count = await events_collection.count_documents({"Email": {"$exists": True}})
            print(f"[WORKING UPDATE] Docs with Email field: {email_count}")
            
            sample = await events_collection.find_one({})
            if sample:
                print(f"[WORKING UPDATE] Sample fields: {list(sample.keys())}")
            
            raise HTTPException(
                status_code=404,
                detail=f"Cell not found with identifier: {identifier}. Collection has {count} documents."
            )
        
        print(f"[WORKING UPDATE]  Found: {event.get('Event Name')}")
        print(f"[WORKING UPDATE] Event ID: {str(event.get('_id'))}")
        
        update_fields = {}
        
        if "Day" in event_data:
            update_fields["Day"] = event_data["Day"]
        if "day" in event_data:
            update_fields["day"] = event_data["day"]
        
        for key, value in event_data.items():
            if key not in ["Day", "day"]:
                update_fields[key] = value
        
        update_fields["updated_at"] = datetime.utcnow()
        
        print(f"[WORKING UPDATE] Updating with: {update_fields}")
        
        # Perform update
        result = await events_collection.update_one(
            {"_id": event["_id"]},
            {"$set": update_fields}
        )
        
        print(f"[WORKING UPDATE] Modified: {result.modified_count}")
        
        # Return simple success response
        return {
            "success": True,
            "message": "Cell updated successfully",
            "cell_name": event.get("Event Name"),
            "old_day": event.get("Day"),
            "new_day": event_data.get("Day") or event_data.get("day"),
            "modified": result.modified_count > 0
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[WORKING UPDATE] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/events/update-person-cells/{person_name}")
async def update_person_cells_improved(person_name: str, update_data: dict):
    """
    Improved: Updates Event Name when Day changes for BOTH old and new formats
    """
    try:
        decoded_person_name = unquote(person_name)
        
        # Find cells by person name - check BOTH old and new field names
        query = {
            "$or": [
                {"Leader": decoded_person_name},  
                {"eventLeader": decoded_person_name},  # New format
                {"eventLeaderName": decoded_person_name}  # Also check this
            ]
        }
        
        cursor = events_collection.find(query)
        matching_cells = await cursor.to_list(length=None)
        
        if not matching_cells:
            return {"message": f"No cells found for: {decoded_person_name}", "updated_count": 0}
        
        set_operations = {}
        unset_operations = {}
        
        # Map field names between old and new formats
        field_mapping = {
            "Day": ["Day", "recurring_day"],  # When updating Day, update both fields
            "recurring_day": ["recurring_day", "Day"],
            "Event Name": ["Event Name", "eventName"],
            "eventName": ["eventName", "Event Name"],
            "Leader": ["Leader", "eventLeader", "eventLeaderName"],
            "eventLeader": ["eventLeader", "Leader", "eventLeaderName"],
            "Email": ["Email", "eventLeaderEmail"],
            "eventLeaderEmail": ["eventLeaderEmail", "Email"],
            "Address": ["Address", "location"],
            "location": ["location", "Address"],
            "Time": ["Time", "time"],
            "time": ["time", "Time"],
            "status": ["status", "Status"],
            "Status": ["Status", "status"]
        }
        
        # Process update data
        for field, value in update_data.items():
            if value is None:
                # Add to unset operations
                unset_operations[field] = ""
                # Also unset mapped fields
                if field in field_mapping:
                    for mapped_field in field_mapping[field]:
                        if mapped_field != field: 
                            unset_operations[mapped_field] = ""
            else:
                set_operations[field] = value
                if field in field_mapping:
                    for mapped_field in field_mapping[field]:
                        if mapped_field != field:  
                            # Handle type conversions
                            if field == "Day" and mapped_field == "recurring_day":
                                # Convert string Day to array recurring_day
                                set_operations[mapped_field] = [value] if value else []
                            elif field == "recurring_day" and mapped_field == "Day":
                                # Convert array recurring_day to string Day
                                if isinstance(value, list) and len(value) > 0:
                                    set_operations[mapped_field] = value[0]  # Take first day from array
                                elif isinstance(value, str):
                                    set_operations[mapped_field] = value
                                else:
                                    set_operations[mapped_field] = ""
                            else:
                                set_operations[mapped_field] = value
        
        update_query = {}
        if set_operations:
            set_operations["updated_at"] = datetime.utcnow()
            update_query["$set"] = set_operations
        if unset_operations:
            update_query["$unset"] = unset_operations
        
        if not update_query:
            return {"message": "No operations to perform", "updated_count": 0}
        
        # First update all matching cells
        result = await events_collection.update_many(
            query,
            update_query
        )
        
        # Now update Event Names for OLD format cells when Day changes
        event_names_updated = 0
        day_updated = False
        new_day = None
        
        # Check if Day was updated
        if "Day" in update_data and update_data["Day"]:
            day_updated = True
            new_day = update_data["Day"]
        elif "recurring_day" in update_data and update_data["recurring_day"]:
            day_updated = True
            # Get day from recurring_day array
            if isinstance(update_data["recurring_day"], list) and len(update_data["recurring_day"]) > 0:
                new_day = update_data["recurring_day"][0]
            elif isinstance(update_data["recurring_day"], str):
                new_day = update_data["recurring_day"]
        
        if day_updated and new_day:
            for cell in matching_cells:
                cell_id = cell["_id"]
                
                # Get current event names
                old_event_name = cell.get("Event Name", "")
                old_event_name2 = cell.get("eventName", "")
                
                # Update OLD FORMAT Event Name (pattern: Name - Location - Type - Day)
                if old_event_name and " - " in old_event_name:
                    parts = old_event_name.split(" - ")
                    if len(parts) >= 4:
                        # Keep first 3 parts, replace last part with new day
                        new_event_name = " - ".join(parts[:-1]) + f" - {new_day}"
                        
                        await events_collection.update_one(
                            {"_id": cell_id},
                            {"$set": {"Event Name": new_event_name, "updated_at": datetime.utcnow()}}
                        )
                        event_names_updated += 1
                        print(f"[UPDATE] Updated old format Event Name: {old_event_name} -> {new_event_name}")
                
                # Update NEW FORMAT eventName (if it follows the same pattern)
                if old_event_name2 and " - " in old_event_name2:
                    parts = old_event_name2.split(" - ")
                    if len(parts) >= 4:
                        new_event_name2 = " - ".join(parts[:-1]) + f" - {new_day}"
                        
                        await events_collection.update_one(
                            {"_id": cell_id},
                            {"$set": {"eventName": new_event_name2, "updated_at": datetime.utcnow()}}
                        )
                        event_names_updated += 1
                        print(f"[UPDATE] Updated new format eventName: {old_event_name2} -> {new_event_name2}")
                # Also update if the eventName is just a name (convert to full format)
                elif old_event_name2 and " - " not in old_event_name2:
                    # Get location from cell
                    location = cell.get("location") or cell.get("Address") or "Unknown Location"
                    # Get event type from cell
                    event_type = cell.get("Event Type") or cell.get("eventTypeName") or "Cell"
                    
                    new_event_name2 = f"{old_event_name2} - {location} - {event_type} - {new_day}"
                    
                    await events_collection.update_one(
                        {"_id": cell_id},
                        {"$set": {"eventName": new_event_name2, "updated_at": datetime.utcnow()}}
                    )
                    event_names_updated += 1
                    print(f"[UPDATE] Converted simple eventName to full format: {old_event_name2} -> {new_event_name2}")
        
        # Also handle special case: if updating location/Address, update Event Name too
        location_updated = False
        new_location = None
        
        if "Address" in update_data and update_data["Address"]:
            location_updated = True
            new_location = update_data["Address"]
        elif "location" in update_data and update_data["location"]:
            location_updated = True
            new_location = update_data["location"]
        
        if location_updated and new_location:
            for cell in matching_cells:
                cell_id = cell["_id"]
                
                # Get current values
                old_event_name = cell.get("Event Name", "")
                old_event_name2 = cell.get("eventName", "")
                current_day = cell.get("Day") or (cell.get("recurring_day")[0] if cell.get("recurring_day") else "Unknown Day")
                
                # Update OLD FORMAT Event Name
                if old_event_name and " - " in old_event_name:
                    parts = old_event_name.split(" - ")
                    if len(parts) >= 4:
                        # Replace location part (typically index 1)
                        parts[1] = new_location
                        new_event_name = " - ".join(parts)
                        
                        await events_collection.update_one(
                            {"_id": cell_id},
                            {"$set": {"Event Name": new_event_name, "updated_at": datetime.utcnow()}}
                        )
                        event_names_updated += 1
                
                # Update NEW FORMAT eventName
                if old_event_name2 and " - " in old_event_name2:
                    parts = old_event_name2.split(" - ")
                    if len(parts) >= 4:
                        # Replace location part (typically index 1)
                        parts[1] = new_location
                        new_event_name2 = " - ".join(parts)
                        
                        await events_collection.update_one(
                            {"_id": cell_id},
                            {"$set": {"eventName": new_event_name2, "updated_at": datetime.utcnow()}}
                        )
                        event_names_updated += 1
        
        return {
            "success": True,
            "message": f"Updated {result.modified_count} cells, {event_names_updated} Event Names",
            "matched_count": len(matching_cells),
            "modified_count": result.modified_count,
            "event_names_updated": event_names_updated,
            "set_operations": list(set_operations.keys()) if set_operations else None,
            "unset_operations": list(unset_operations.keys()) if unset_operations else None
        }
        
    except Exception as e:
        print(f"[UPDATE ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
   

@app.get("/event-types")
async def get_event_types():
    """Get all event types."""
    cursor = events_collection.find({"isEventType": True}).sort("createdAt", 1)
    types = []
    
    async for et in cursor:
        et["_id"] = str(et["_id"])
        types.append(et)
    
    return types


@app.put("/event-types/{event_type_name}")
async def update_event_type(event_type_name: str, updated_data: EventTypeCreate):
    """Update an event type."""
    # Find existing
    existing = await events_collection.find_one({
        "name": {"$regex": f"^{event_type_name}$", "$options": "i"},
        "isEventType": True
    })
    
    if not existing:
        raise HTTPException(404, "Event type not found")
    
    new_name = updated_data.name.strip().title()
    
    # Check for name change
    if new_name.lower() != existing["name"].lower():
        duplicate = await events_collection.find_one({
            "name": {"$regex": f"^{new_name}$", "$options": "i"},
            "isEventType": True,
            "_id": {"$ne": existing["_id"]}
        })
        if duplicate:
            raise HTTPException(400, "Event type with this name already exists")
    
    # Update
    updates = updated_data.dict()
    updates["name"] = new_name
    updates["updatedAt"] = datetime.utcnow()
    
    # Remove None values
    updates = {k: v for k, v in updates.items() if v is not None}
    
    # Don't change these
    for field in ["_id", "UUID", "createdAt", "isEventType"]:
        updates.pop(field, None)
    
    await events_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": updates}
    )
    
    updated = await events_collection.find_one({"_id": existing["_id"]})
    updated["_id"] = str(updated["_id"])
    
    return updated


@app.delete("/event-types/{event_type_name}")
async def delete_event_type(
    event_type_name: str,
    force: bool = Query(False, description="Force delete even if events exist")
):
    """Delete an event type."""
    # Find event type
    existing = await events_collection.find_one({
        "$or": [
            {"name": {"$regex": f"^{event_type_name}$", "$options": "i"}},
            {"eventType": {"$regex": f"^{event_type_name}$", "$options": "i"}}
        ],
        "isEventType": True
    })
    
    if not existing:
        raise HTTPException(404, f"Event type '{event_type_name}' not found")
    
    actual_name = existing.get("name") or existing.get("eventType")
    
    # Check if events use this type
    events = await events_collection.find({
        "$or": [
            {"eventType": {"$regex": f"^{actual_name}$", "$options": "i"}},
            {"eventTypeName": {"$regex": f"^{actual_name}$", "$options": "i"}}
        ],
        "isEventType": {"$ne": True}
    }).to_list(length=None)
    
    if events and not force:
        raise HTTPException(400, f"Cannot delete: {len(events)} events use this type")
    
    # Delete events if forced
    if force and events:
        await events_collection.delete_many({
            "$or": [
                {"eventType": {"$regex": f"^{actual_name}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{actual_name}$", "$options": "i"}}
            ],
            "isEventType": {"$ne": True}
        })
    
    # Delete event type
    await events_collection.delete_one({"_id": existing["_id"]})
    
    return {
        "success": True,
        "message": f"Event type '{actual_name}' deleted",
        "events_deleted": len(events) if force else 0
    }

@app.get("/diagnostic/event-type-usage/{event_type_name}")
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
                # Show ALL type-related fields
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

@app.get("/debug/emails")
async def debug_emails():
    try:
        # Fetch sample documents
        sample_docs = []
        cursor = events_collection.find({}).limit(5)
        async for doc in cursor:
            doc_info = {key: value for key, value in doc.items() if key != "_id"}
            sample_docs.append(doc_info)

        # Check distinct email fields
        email_fields_to_check = ["Email", "email", "EMAIL", "user_email", "userEmail"]
        email_info = {}

        for field in email_fields_to_check:
            try:
                distinct_emails = await events_collection.distinct(field)
                if distinct_emails:
                    email_info[field] = {
                        "distinct_emails": distinct_emails,
                        "count": len(distinct_emails)
                    }
            except Exception:
                continue  

        return {
            "database_name": events_collection.database.name,
            "collection_name": events_collection.name,
            "all_collections": await events_collection.database.list_collection_names(),
            "total_documents": await events_collection.count_documents({}),
            "sample_documents": sample_docs,
            "email_fields_found": email_info,
        }

    except Exception as e:
        return {"error": str(e)}

logging.basicConfig(level=logging.INFO)


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


@app.put("/events/{event_id}/persistent-attendees")
async def update_persistent_attendees(
    event_id: str = Path(...),
    data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update persistent attendees for an event
    This is called immediately when adding/removing people from the associate tab
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        persistent_attendees = data.get("persistent_attendees", [])
        
        print(f"\n{'='*80}")
        print(f" UPDATING PERSISTENT ATTENDEES")
        print(f"{'='*80}")
        print(f"Event ID: {event_id}")
        print(f"User: {current_user.get('email', 'Unknown')}")
        print(f"Attendees count: {len(persistent_attendees)}")
        
        cleaned_attendees = []
        for attendee in persistent_attendees:
            if isinstance(attendee, dict):
                cleaned_attendees.append({
                    "id": attendee.get("id", ""),
                    "name": attendee.get("name", ""),
                    "fullName": attendee.get("fullName", attendee.get("name", "")),
                    "email": attendee.get("email", ""),
                    "phone": attendee.get("phone", ""),
                    "leader12": attendee.get("leader12", ""),
                    "leader144": attendee.get("leader144", "")
                })
        
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {
                "$set": {
                    "persistent_attendees": cleaned_attendees,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        if result.matched_count == 0:
            print(f" Event not found: {event_id}")
            raise HTTPException(status_code=404, detail="Event not found")
        
        print(f" Successfully updated persistent attendees: {len(cleaned_attendees)} people")
        print(f"{'='*80}\n")
        
        return {
            "success": True,
            "message": "Persistent attendees updated successfully",
            "count": len(cleaned_attendees),
            "attendees": cleaned_attendees
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f" Error updating persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/{event_id}/persistent-attendees")
async def get_persistent_attendees(
    event_id: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    """Get persistent attendees for an event"""
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one(
            {"_id": ObjectId(event_id)},
            {"persistent_attendees": 1, "Event Name": 1}
        )
        
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        return {
            "persistent_attendees": event.get("persistent_attendees", []),
            "event_name": event.get("Event Name", "Unknown")
        }
        
    except Exception as e:
        print(f"Error getting persistent attendees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/events/{event_id}/last-attendance")
async def get_last_attendance(
    event_id: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    """Get last week's attendance for pre-filling names"""
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
                "attendees": persistent
            }
        
        # If no persistent, try to find last week's data
        attendance = event.get("attendance", {})
        if not attendance:
            return {"has_previous_attendance": False, "attendees": []}
        
        # Get most recent week
        weeks = sorted(attendance.keys(), reverse=True)
        if weeks:
            last_week_data = attendance[weeks[0]]
            return {
                "has_previous_attendance": True,
                "attendees": last_week_data.get("attendees", [])
            }
        
        return {"has_previous_attendance": False, "attendees": []}
        
    except Exception as e:
        print(f"Error getting last attendance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
   

@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": {"id": member_id}}})
    if update_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Member not found on event")
    return {"message": "Member removed"}

@app.post("/admin/events/bulk-assign-all-leaders")
async def bulk_assign_all_leaders_comprehensive(current_user: dict = Depends(get_current_user)):
    """
     COMPREHENSIVE: Bulk assign Leader @1 for ALL cell events
    This ensures every cell event has the correct Leader @1 from People database
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        print("\n" + "="*80)
        print(" STARTING BULK LEADER @1 ASSIGNMENT FOR ALL CELL EVENTS")
        print("="*80 + "\n")
       
        # Find ALL cell events (no filters)
        cell_events = await events_collection.find({
            "$or": [
                {"Event Type": "Cells"},
                {"eventType": "Cells"},
                {"Event Type": "Cell"},
                {"eventType": "Cell"}
            ]
        }).to_list(length=None)
       
        updated_count = 0
        failed_count = 0
        skipped_count = 0
        results = {
            "updated": [],
            "failed": [],
            "skipped": []
        }
       
        print(f"Found {len(cell_events)} cell events to process\n")
       
        for idx, event in enumerate(cell_events, 1):
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
            event_leader = event.get("Leader", "").strip()
           
            # Get Leader @12 from either field name
            leader_at_12 = (
                event.get("Leader at 12") or
                event.get("Leader @12") or
                event.get("leader12") or
                ""
            ).strip()
           
            print(f"\n[{idx}/{len(cell_events)}] Processing: {event_name}")
            print(f"   Event Leader: {event_leader}")
            print(f"   Current Leader @12: {leader_at_12}")
           
            # Skip if no Leader @12
            if not leader_at_12:
                print(f"    SKIPPED - No Leader @12 found")
                skipped_count += 1
                results["skipped"].append({
                    "event_name": event_name,
                    "event_leader": event_leader,
                    "reason": "No Leader @12"
                })
                continue
           
            print(f"   Looking up Leader @1 for '{leader_at_12}'...")
            leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12)
           
            if leader_at_1:
                update_data = {
                    "leader1": leader_at_1,
                    "Leader @1": leader_at_1,
                    "leader12": leader_at_12,
                    "Leader @12": leader_at_12,
                    "Leader at 12": leader_at_12,
                    "updated_at": datetime.utcnow()
                }
               
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": update_data}
                )
               
                updated_count += 1
                results["updated"].append({
                    "event_name": event_name,
                    "event_leader": event_leader,
                    "leader_at_12": leader_at_12,
                    "assigned_leader_at_1": leader_at_1
                })
                print(f" SUCCESS - Assigned Leader @1: {leader_at_1}")
               
            else:
                failed_count += 1
                results["failed"].append({
                    "event_name": event_name,
                    "event_leader": event_leader,
                    "leader_at_12": leader_at_12,
                    "reason": "Person not found in People database or no gender specified"
                })
                print(f"   FAILED - Could not find Leader @1 for '{leader_at_12}'")
       
        print("\n" + "="*80)
        print("BULK ASSIGNMENT COMPLETE")
        print("="*80)
        print(f"Updated: {updated_count}")
        print(f"Failed: {failed_count}")
        print(f" Skipped: {skipped_count}")
        print(f"Total Processed: {len(cell_events)}")
        print("="*80 + "\n")
       
        return {
            "success": True,
            "message": f"Successfully assigned Leader @1 to {updated_count} events. {failed_count} failed, {skipped_count} skipped.",
            "summary": {
                "total_processed": len(cell_events),
                "updated": updated_count,
                "failed": failed_count,
                "skipped": skipped_count
            },
            "results": results
        }
       
    except Exception as e:
        print(f"\nERROR in bulk assign: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error assigning leaders: {str(e)}")

@app.post("/admin/events/fix-all-leaders-at-1")
async def fix_all_leaders_at_1(current_user: dict = Depends(get_current_user)):
    """
     FIXED: Assign Leader @1 based on EVENT LEADER's gender
    This assigns Gavin/Vicky based on who is leading the cell
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        print("\n" + "="*80)
        print(" FIXING ALL LEADERS @1 BASED ON EVENT LEADER'S GENDER")
        print("="*80 + "\n")
       
        # Get ALL events
        all_events = await events_collection.find({}).to_list(length=None)
       
        updated_count = 0
        failed_count = 0
        skipped_count = 0
        results = []
       
        for idx, event in enumerate(all_events, 1):
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
           
            # Get the LEADER of this event (the person running it)
            leader_name = event.get("Leader", "").strip()
           
            if not leader_name:
                print(f"[{idx}/{len(all_events)}] Skipping {event_name} - No leader")
                skipped_count += 1
                continue
           
            print(f"\n[{idx}/{len(all_events)}] {event_name}")
            print(f"   Event Leader: {leader_name}")
           
            # Find this LEADER in People database
            person = await people_collection.find_one({
                "$or": [
                    # Try full name match
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, leader_name]}},
                    # Try first name only
                    {"Name": {"$regex": f"^{leader_name.split()[0]}$", "$options": "i"}},
                ]
            })
           
            if not person:
                print(f"   Leader '{leader_name}' not found in People database")
                failed_count += 1
                results.append({
                    "event": event_name,
                    "leader": leader_name,
                    "status": "failed - not found in People"
                })
                continue
           
            # Get their gender
            gender = person.get("Gender", "").strip()
            person_full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
           
            print(f"   ✓ Found: {person_full_name}")
            print(f"   Gender: {gender}")
           
            # Assign Leader @1 based on gender
            leader_at_1 = ""
            if gender == "Female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "Male":
                leader_at_1 = "Gavin Enslin"
            else:
                print(f"   Unknown gender: '{gender}'")
                failed_count += 1
                results.append({
                    "event": event_name,
                    "leader": leader_name,
                    "gender": gender,
                    "status": "failed - unknown gender"
                })
                continue
           
            # Update the event
            await events_collection.update_one(
                {"_id": event_id},
                {"$set": {
                    "leader1": leader_at_1,
                    "Leader @1": leader_at_1,
                    "updated_at": datetime.utcnow()
                }}
            )
           
            updated_count += 1
            results.append({
                "event": event_name,
                "leader": leader_name,
                "gender": gender,
                "assigned_leader_at_1": leader_at_1,
                "status": "success"
            })
            print(f"   Assigned Leader @1: {leader_at_1}")
       
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Updated: {updated_count}")
        print(f"Failed: {failed_count}")
        print(f" Skipped: {skipped_count}")
        print(f"Total: {len(all_events)}")
        print("="*80 + "\n")
       
        return {
            "success": True,
            "message": f"Fixed {updated_count} events successfully!",
            "summary": {
                "updated": updated_count,
                "failed": failed_count,
                "skipped": skipped_count,
                "total": len(all_events)
            },
            "results": results[:20]  
        }
       
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))      


@app.get("/admin/events/verify-leaders")
async def verify_leaders_assignment(current_user: dict = Depends(get_current_user)):
    """
    Verify Leader @1 assignments in cell events
    Shows statistics and sample data
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        # Get all cell events
        cell_events = await events_collection.find({
            "$or": [
                {"Event Type": "Cells"},
                {"eventType": "Cells"}
            ]
        }).to_list(length=None)
       
        # Categorize events
        with_leader_1 = []
        without_leader_1 = []
        with_leader_12_no_leader_1 = []
       
        for event in cell_events:
            leader_1 = event.get("leader1") or event.get("Leader @1", "")
            leader_12 = event.get("leader12") or event.get("Leader @12", "")
           
            if leader_1 and leader_1.strip():
                with_leader_1.append({
                    "event_name": event.get("Event Name"),
                    "leader_1": leader_1,
                    "leader_12": leader_12
                })
            else:
                without_leader_1.append({
                    "event_name": event.get("Event Name"),
                    "leader_12": leader_12
                })
               
                if leader_12 and leader_12.strip():
                    with_leader_12_no_leader_1.append({
                        "event_name": event.get("Event Name"),
                        "leader_12": leader_12
                    })
       
        return {
            "total_cell_events": len(cell_events),
            "with_leader_at_1": {
                "count": len(with_leader_1),
                "percentage": round((len(with_leader_1) / len(cell_events)) * 100, 1) if cell_events else 0,
                "sample": with_leader_1[:10]
            },
            "without_leader_at_1": {
                "count": len(without_leader_1),
                "percentage": round((len(without_leader_1) / len(cell_events)) * 100, 1) if cell_events else 0,
                "sample": without_leader_1[:10]
            },
            "needs_assignment": {
                "count": len(with_leader_12_no_leader_1),
                "description": "Events with Leader @12 but missing Leader @1",
                "sample": with_leader_12_no_leader_1[:10]
            }
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/events/cells-debug")
async def get_admin_cell_events_debug(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None)  # ADD START DATE PARAMETER
):
    """Optimized admin cells endpoint with pagination and deduplication"""
    try:
        role = current_user.get("role", "")
        if role.lower() != "admin":
            raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
       
        # USE PROVIDED START DATE OR DEFAULT TO OCT 20, 2025
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
       
        print(f"Admin - Cells from {start_date_obj} to {today_date}, Page {page}")
        print(f"Search: '{search}', Status: '{status}', Personal: {personal}, Event Type: '{event_type}', Start Date: '{start_date_filter}'")

        # Build match filter
        match_filter = {"Event Type": "Cells"}
       
        # Add event type filter if provided
        if event_type and event_type != 'all':
            match_filter["eventType"] = event_type
            print(f"Filtering by event type: {event_type}")
       
        # Add personal filtering logic
        if personal:
            user_email = current_user.get("email", "")
            print(f"PERSONAL FILTER ACTIVATED for user: {user_email}")
           
            # Find user's name from their cell
            user_cell = await events_collection.find_one({
                "Event Type": "Cells",
                "$or": [
                    {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                    {"email": {"$regex": f"^{user_email}$", "$options": "i"}},
                ]
            })
           
            user_name = user_cell.get("Leader", "").strip() if user_cell else ""
            print(f"User name found: '{user_name}'")
           
            # Build personal query conditions
            personal_conditions = [
                {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"email": {"$regex": f"^{user_email}$", "$options": "i"}},
            ]
           
            if user_name:
                personal_conditions.extend([
                    {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                    {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                    {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
                ])
           
            match_filter["$or"] = personal_conditions
            print(f"Personal query conditions: {len(personal_conditions)} conditions")
       
        # Add search filter if provided (only if not in personal mode)
        elif search and search.strip():
            search_term = search.strip()
            print(f"Applying search filter for: '{search_term}'")
           
            match_filter["$or"] = [
                {"Event Name": {"$regex": search_term, "$options": "i"}},
                {"Leader": {"$regex": search_term, "$options": "i"}},
                {"Email": {"$regex": search_term, "$options": "i"}},
                {"Leader at 12": {"$regex": search_term, "$options": "i"}},
                {"Leader @12": {"$regex": search_term, "$options": "i"}},
            ]
       
        #  FETCH ALL CELLS AND DEDUPLICATE IN PYTHON
        cursor = events_collection.find(match_filter)
        all_cells_raw = await cursor.to_list(length=None)
       
        print(f"Found {len(all_cells_raw)} cells before deduplication and date filtering")
       
        # Deduplicate using Python (more reliable than MongoDB aggregation)
        seen_cells = set()
        all_cells = []
       
        for cell in all_cells_raw:
            # Create a unique key from event name, email, and day
            event_name = (cell.get("Event Name") or "").strip().lower()
            email = (cell.get("Email") or "").strip().lower()
            day = (cell.get("Day") or "").strip().lower()
           
            # Skip if no event name (invalid cell)
            if not event_name:
                continue
           
            # Create unique identifier
            cell_key = f"{event_name}|{email}|{day}"
           
            # Only add if we haven't seen this combination before
            if cell_key not in seen_cells:
                seen_cells.add(cell_key)
                all_cells.append(cell)
            else:
                print(f"Skipping duplicate: {event_name} ({email}) on {day}")
       
        print(f"After deduplication: {len(all_cells)} unique cells")
       
        # Batch fetch all leader info at once
        leader_names = []
        for cell in all_cells:
            leader_12 = cell.get("Leader @12", cell.get("Leader at 12", "")).strip()
            if leader_12:
                leader_names.append(leader_12)
                if " " in leader_12:
                    leader_names.append(leader_12.split()[0])
           
            event_leader = cell.get("Leader", "").strip()
            if event_leader:
                leader_names.append(event_leader)
                if " " in event_leader:
                    leader_names.append(event_leader.split()[0])
       
        leader_names = list(set(leader_names))
       
        # Single database query for all leaders
        leader_at_1_map = {}
        if leader_names:
            try:
                people_cursor = people_collection.find({
                    "$or": [
                        {"Name": {"$in": leader_names}},
                        {"$expr": {
                            "$or": [
                                {"$in": ["$Name", leader_names]},
                                {"$in": [{"$concat": ["$Name", " ", "$Surname"]}, leader_names]}
                            ]
                        }}
                    ]
                }, {"Name": 1, "Surname": 1, "Leader @1": 1})
               
                async for person in people_cursor:
                    full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
                    first_name = person.get('Name', '').strip()
                    leader_at_1 = person.get("Leader @1", "").strip()
                   
                    if leader_at_1:
                        leader_at_1_map[full_name.lower()] = leader_at_1
                        leader_at_1_map[first_name.lower()] = leader_at_1
            except Exception as e:
                print(f"Error fetching leaders from People collection: {str(e)}")
       
        print(f"Found {len(leader_at_1_map)} leaders with Leader @1")
       
        # Day mapping
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
       
        # Process events
        processed_events = []
       
        for event in all_cells:
            try:
                event_name = str(event.get("Event Name", "")).strip()
                day = str(event.get("Day", "")).strip().lower()
               
                if day not in day_mapping:
                    continue
               
                # Calculate most recent occurrence
                target_weekday = day_mapping[day]
                current_weekday = today_date.weekday()
                days_diff = (current_weekday - target_weekday) % 7
               
                most_recent_occurrence = today_date - timedelta(days=days_diff) if days_diff > 0 else today_date
               
                # FILTER BY DATE RANGE (Oct 20, 2025 to today)
                if most_recent_occurrence < start_date_obj or most_recent_occurrence > today_date:
                    print(f"Skipping {event_name} - date {most_recent_occurrence} outside range {start_date_obj} to {today_date}")
                    continue
               
                # Get leader info
                leader_name = event.get("Leader", "").strip()
                leader_at_12 = event.get("Leader @12", event.get("Leader at 12", "")).strip()
                leader_at_144 = event.get("Leader @144", event.get("Leader at 144", ""))
               
                # Get Leader at 1
                leader_at_1 = ""
               
                # Priority 1: Use Leader at 12
                if leader_at_12:
                    leader_at_1 = leader_at_1_map.get(leader_at_12.lower(), "")
                    if not leader_at_1 and " " in leader_at_12:
                        first_name = leader_at_12.split()[0].lower()
                        leader_at_1 = leader_at_1_map.get(first_name, "")
               
                # Priority 2: Use event leader
                if not leader_at_1 and leader_name:
                    if leader_name not in ["Gavin Enslin", "Vicky Enslin"]:
                        leader_at_1 = leader_at_1_map.get(leader_name.lower(), "")
                        if not leader_at_1 and " " in leader_name:
                            first_name = leader_name.split()[0].lower()
                            leader_at_1 = leader_at_1_map.get(first_name, "")
               
                # Determine status
                did_not_meet = event.get("did_not_meet", False)
                attendees = event.get("attendees", [])
                has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
               
                if did_not_meet:
                    cell_status = "did_not_meet"
                    status_display = "Did Not Meet"
                elif has_attendees:
                    cell_status = "complete"
                    status_display = "Complete"
                else:
                    cell_status = "incomplete"
                    status_display = "Incomplete"
               
                # Build event object
                final_event = {
                    "_id": str(event.get("_id", "")),
                    "eventName": event_name,
                    "eventType": event.get("eventType", "Cells"),
                    "eventLeaderName": leader_name,
                    "eventLeaderEmail": str(event.get("Email", "")).strip(),
                    "leader1": leader_at_1,
                    "leader12": leader_at_12,
                    "leader144": leader_at_144,
                    "day": day.capitalize(),
                    "date": most_recent_occurrence.isoformat(),
                    "location": event.get("Location", ""),
                    "attendees": attendees if isinstance(attendees, list) else [],
                    "did_not_meet": did_not_meet,
                    "status": cell_status,
                    "Status": status_display,
                    "_is_overdue": most_recent_occurrence < today_date
                }
               
                processed_events.append(final_event)
               
            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {str(e)}")
                continue
       
        print(f"Processed {len(processed_events)} events after date filtering")
       
        # Calculate status counts from ALL processed events
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
        }
       
        print(f"Status counts - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
       
        # Filter by status AFTER counting
        if status and status != 'all':
            processed_events = [e for e in processed_events if e["status"] == status]
            print(f"Filtered to {len(processed_events)} events with status '{status}'")
       
        # Sort by date
        processed_events.sort(key=lambda x: (x['date'], x['eventLeaderName'].lower()))
       
        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
       
        print(f"Returning page {page}/{total_pages}: {len(paginated_events)} events")
       
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
            }
        }
       
    except Exception as e:
        print(f"ERROR in get_admin_cell_events_debug: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")      
  
@app.post("/admin/add-uuids-to-all-events")
async def add_uuids_to_all_events(current_user: dict = Depends(get_current_user)):
    """Add UUIDs to ALL events that don't have them - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        import uuid
       
        events_without_uuid = await events_collection.find({
            "UUID": {"$exists": False}
        }).to_list(length=None)
       
        updated_count = 0
       
        for event in events_without_uuid:
            # Generate new UUID
            new_uuid = str(uuid.uuid4())
           
            # Update the event
            await events_collection.update_one(
                {"_id": event["_id"]},
                {"$set": {"UUID": new_uuid}}
            )
            updated_count += 1
       
        print(f"Added UUIDs to {updated_count} events")
       
        return {
            "message": f"Successfully added UUIDs to {updated_count} events",
            "updated_count": updated_count
        }
       
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))