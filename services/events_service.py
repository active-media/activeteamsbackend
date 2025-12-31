"""Events service - handles all event-related business logic"""

from datetime import datetime, date, timedelta
from typing import List, Optional
import re
import uuid
import pytz
from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from activeteamsbackend.auth.models import EventCreate, EventTypeCreate
from activeteamsbackend.auth.utils import get_current_user, convert_datetime_to_iso, sanitize_document
from database import events_collection, people_collection

app = APIRouter(prefix="/events", tags=["events"])


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


# Event endpoints 

@app.post("/")
async def create_event(event: EventCreate):
    """Create a new event."""
    try:
        event_data = event.dict()
        
        # Set IDs
        event_data["UUID"] = event_data.get("UUID") or str(uuid.uuid4())
        event_data["_id"] = ObjectId()
        
        # Handle event type
        event_type_name = event_data.get("eventTypeName", "")
        if not event_type_name:
            raise HTTPException(400, "eventTypeName is required")
        
        # Check if it's CELLS
        if event_type_name.upper() in ["CELLS", "ALL CELLS"]:
            event_data.update({
                "eventTypeId": "CELLS_BUILT_IN",
                "eventTypeName": "CELLS",
                "hasPersonSteps": True,
                "isGlobal": False
            })
        else:
            # Find event type
            event_type = await events_collection.find_one({
                "$or": [
                    {"name": {"$regex": f"^{event_type_name}$", "$options": "i"}},
                    {"Event Type": {"$regex": f"^{event_type_name}$", "$options": "i"}}
                ],
                "isEventType": True
            })
            
            if not event_type:
                raise HTTPException(400, f"Event type '{event_type_name}' not found")
            
            exact_name = event_type.get("name")
            event_data.update({
                "eventTypeId": event_type["UUID"],
                "eventTypeName": exact_name,
                "isGlobal": "global" in exact_name.lower(),
                "hasPersonSteps": "cell" in exact_name.lower()
            })
        
        # Clean data
        for field in ["eventType", "userEmail", "email"]:
            event_data.pop(field, None)
        
        # Set defaults
        defaults = {
            "date": datetime.utcnow(),
            "eventLeaderName": event_data.get("eventLeader", ""),
            "eventLeaderEmail": event_data.get("eventLeaderEmail", ""),
            "attendees": [],
            "total_attendance": 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "status": "open",
            "isTicketed": False,
            "priceTiers": []
        }
        
        for key, value in defaults.items():
            event_data.setdefault(key, value)
        
        # Handle cell-specific fields
        if event_data.get("hasPersonSteps"):
            event_data.setdefault("leader1", event_data.get("leader1", ""))
            event_data.setdefault("leader12", event_data.get("leader12", ""))
            event_data.setdefault("persistent_attendees", [])
        
        # Insert event
        result = await events_collection.insert_one(event_data)
        created = await events_collection.find_one({"_id": result.inserted_id})
        
        # Prepare response
        response = {
            "success": True,
            "message": "Event created successfully",
            "id": str(result.inserted_id),
            "event": {
                "_id": str(created["_id"]),
                "UUID": created.get("UUID"),
                "eventName": created.get("eventName"),
                "eventLeaderName": created.get("eventLeaderName"),
                "eventLeaderEmail": created.get("eventLeaderEmail"),
                "day": created.get("day"),
                "date": created.get("date"),
                "location": created.get("location"),
                "eventTypeName": created.get("eventTypeName"),
                "isGlobal": created.get("isGlobal"),
                "hasPersonSteps": created.get("hasPersonSteps"),
                "leader1": created.get("leader1"),
                "leader12": created.get("leader12"),
                "status": created.get("status", "open")
            }
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error creating event: {str(e)}")


@app.get("/cells")
async def get_cell_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None),
    leader_at_12_view: Optional[bool] = Query(None),
    show_personal_cells: Optional[bool] = Query(None),
    show_all_authorized: Optional[bool] = Query(None),
    isLeaderAt12: Optional[bool] = Query(None)
):
    """Get cell events."""
    try:
        user_role = current_user.get("role", "user").lower()
        user_email = current_user.get("email", "")
        
        # Get user info
        person = await people_collection.find_one({"Email": user_email})
        user_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip() if person else ""
        
        # Base cell query
        query = {
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
            ],
            "isEventType": {"$ne": True}
        }
        
        # Search filter
        if search and search.strip():
            search_re = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_re},
                {"eventName": search_re},
                {"Leader": search_re},
                {"eventLeaderName": search_re},
                {"eventLeaderEmail": search_re},
                {"Leader at 12": search_re}
            ]
        
        # User-specific filters
        if user_role == "admin" and personal:
            query["$or"] = [
                {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}}
            ]
        elif user_role == "user":
            query["$or"] = [
                {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}}
            ]
        elif isLeaderAt12 and leader_at_12_view:
            if show_personal_cells:
                query["$or"] = [
                    {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
                    {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}}
                ]
            elif show_all_authorized:
                # Find cells where user is Leader at 12
                name_vars = set([user_name])
                if " " in user_name:
                    name_vars.add(user_name.split()[0])
                    name_vars.add(user_name.replace(" ", ""))
                    name_vars.add(user_name.replace(" ", "-"))
                
                or_conds = []
                for name in name_vars:
                    if name:
                        safe = re.escape(name)
                        or_conds.extend([
                            {"Leader at 12": {"$regex": f"^{safe}$", "$options": "i"}},
                            {"Leader @12": {"$regex": f"^{safe}$", "$options": "i"}},
                            {"leader12": {"$regex": f"^{safe}$", "$options": "i"}}
                        ])
                
                if or_conds:
                    query["$or"] = or_conds
        
        # Get unique cells
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": {
                        "event_name": {"$ifNull": ["$Event Name", "$eventName"]},
                        "leader_email": {"$ifNull": ["$eventLeaderEmail", "$Email"]},
                        "day": {"$ifNull": ["$Day", "$day"]}
                    },
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"Day": 1, "Leader": 1}}
        ]
        
        events = await events_collection.aggregate(pipeline).to_list(length=None)
        
        # Generate instances
        today = datetime.now(pytz.timezone("Africa/Johannesburg")).date()
        start_date_obj = parse_date_str(start_date) or date(2025, 11, 30)
        
        day_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        all_instances = []
        for event in events:
            day_name = str(event.get("Day") or event.get("day") or "").lower()
            if day_name not in day_map:
                continue
            
            target_day = day_map[day_name]
            
            # Create instances for next 4 weeks
            for week in range(4):
                days_diff = (today.weekday() - target_day) % 7
                instance_date = today - timedelta(days=(days_diff + (week * 7)))
                
                if instance_date < start_date_obj:
                    continue
                
                # Check status
                year, week_num, _ = instance_date.isocalendar()
                week_id = f"{year}-W{week_num:02d}"
                attendance = event.get("attendance", {}).get(week_id, {})
                
                if attendance.get("status") == "did_not_meet":
                    event_status = "did_not_meet"
                elif attendance.get("attendees") or event.get("Status", "").lower() == "complete":
                    event_status = "complete"
                else:
                    event_status = "incomplete"
                
                if status and status != 'all' and event_status != status:
                    continue
                
                # Get leader at 1
                leader_at_1 = event.get("leader1") or ""
                if not leader_at_1:
                    leader_name = event.get("Leader") or event.get("eventLeaderName", "")
                    if leader_name:
                        person = await people_collection.find_one({
                            "$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, leader_name]}
                        })
                        if person:
                            gender = (person.get("Gender") or "").upper()
                            leader_at_1 = "Gavin Enslin" if gender == "MALE" else "Vicky Enslin" if gender == "FEMALE" else ""
                
                instance = {
                    "_id": f"{event.get('_id')}_{instance_date.isoformat()}",
                    "UUID": event.get("UUID", ""),
                    "eventName": event.get("Event Name") or event.get("eventName", ""),
                    "eventType": "Cells",
                    "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                    "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                    "leader1": leader_at_1,
                    "leader12": event.get("Leader at 12") or event.get("leader12", ""),
                    "day": day_name.capitalize(),
                    "date": instance_date.isoformat(),
                    "display_date": instance_date.strftime("%d - %m - %Y"),
                    "location": event.get("Location") or event.get("location", ""),
                    "attendees": attendance.get("attendees", []),
                    "persistent_attendees": event.get("persistent_attendees", []),
                    "hasPersonSteps": True,
                    "status": event_status,
                    "Status": event_status.replace("_", " ").title(),
                    "_is_overdue": instance_date < today and event_status == "incomplete",
                    "is_recurring": True,
                    "week_identifier": week_id,
                    "original_event_id": str(event.get("_id"))
                }
                
                all_instances.append(instance)
        
        # Paginate
        all_instances.sort(key=lambda x: x['date'], reverse=True)
        total = len(all_instances)
        pages = (total + limit - 1) // limit if total > 0 else 1
        skip = (page - 1) * limit
        
        return {
            "events": all_instances[skip:skip + limit],
            "total_events": total,
            "total_pages": pages,
            "current_page": page,
            "page_size": limit,
            "user_info": {
                "name": user_name,
                "email": user_email,
                "role": user_role,
                "is_leader_at_12": isLeaderAt12
            }
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/other")
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
    """Get non-cell events."""
    try:
        user_email = current_user.get("email", "").lower()
        user_role = current_user.get("role", "user").lower()
        
        # Base query: not cells
        query = {
            "$nor": [
                {"Event Type": {"$regex": "Cells", "$options": "i"}},
                {"eventType": {"$regex": "Cells", "$options": "i"}},
                {"eventTypeName": {"$regex": "Cells", "$options": "i"}}
            ]
        }
        
        # Personal filter
        if personal or user_role == "user":
            query["$or"] = [
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]
        
        # Event type filter
        if event_type and event_type.lower() != 'all':
            query["$or"] = [
                {"Event Type": {"$regex": f"^{event_type}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{event_type}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{event_type}$", "$options": "i"}}
            ]
        
        # Search filter
        if search and search.strip():
            search_re = {"$regex": search.strip(), "$options": "i"}
            search_q = {
                "$or": [
                    {"Event Name": search_re},
                    {"eventName": search_re},
                    {"Leader": search_re},
                    {"eventLeaderName": search_re}
                ]
            }
            query = {"$and": [query, search_q]}
        
        # Get events
        events = await events_collection.find(query).to_list(length=1000)
        
        # Filter by date
        start = parse_date_str(start_date) or date(2025, 10, 10)
        end = parse_date_str(end_date) or date.today() + timedelta(days=365)
        
        result_events = []
        for event in events:
            event_date = parse_date_str(event.get("date"))
            
            if event_date < start or event_date > end:
                continue
            
            # Determine status
            date_str = event_date.isoformat()
            attendance = event.get("attendance", {}).get(date_str, {})
            
            if attendance.get("status") == "did_not_meet":
                event_status = "did_not_meet"
            elif attendance.get("attendees") or event.get("status") == "complete":
                event_status = "complete"
            else:
                event_status = "incomplete"
            
            if status and status != event_status:
                continue
            
            # Create instance
            instance = {
                "_id": str(event.get("_id")),
                "UUID": event.get("UUID", ""),
                "eventName": event.get("Event Name") or event.get("eventName", ""),
                "eventType": event.get("Event Type") or event.get("eventType", "Event"),
                "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                "leader1": event.get("leader1", ""),
                "leader12": event.get("Leader @12", ""),
                "day": event.get("Day") or "One-time",
                "date": event_date.isoformat(),
                "location": event.get("Location") or event.get("location", ""),
                "attendees": attendance.get("attendees", []),
                "hasPersonSteps": False,
                "status": event_status,
                "Status": event_status.replace("_", " ").title(),
                "_is_overdue": event_date < date.today() and event_status == "incomplete",
                "is_recurring": False,
                "original_event_id": str(event.get("_id"))
            }
            
            result_events.append(instance)
        
        # Paginate
        result_events.sort(key=lambda x: x['date'], reverse=True)
        total = len(result_events)
        pages = (total + limit - 1) // limit if total > 0 else 1
        skip = (page - 1) * limit
        
        return {
            "events": result_events[skip:skip + limit],
            "total_events": total,
            "total_pages": pages,
            "current_page": page,
            "page_size": limit
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/global")
async def get_global_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    last_updated: Optional[str] = Query(None)
):
    """Get global events."""
    try:
        # Base query
        query = {
            "isGlobal": True,
            "eventTypeName": "Global Events"
        }
        
        # Real-time updates
        if last_updated:
            try:
                last_dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                query["$or"] = [
                    {"created_at": {"$gte": last_dt}},
                    {"updated_at": {"$gte": last_dt}}
                ]
            except:
                pass
        
        # Search filter
        if search and search.strip():
            search_re = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_re},
                {"eventName": search_re},
                {"Leader": search_re},
                {"Location": search_re}
            ]
        
        # Get events
        events = await events_collection.find(query).sort("created_at", -1).to_list(length=None)
        
        # Filter by date
        start = parse_date_str(start_date) or date(2025, 10, 20)
        
        result_events = []
        for event in events:
            event_date = parse_date_str(event.get("date"))
            
            if event_date < start:
                continue
            
            # Determine status
            if event.get("did_not_meet"):
                event_status = "did_not_meet"
            elif event.get("status"):
                event_status = str(event.get("status")).lower()
            else:
                event_status = "open"
            
            if status and status != 'all' and status != event_status:
                continue
            
            # Create response
            result = {
                "_id": str(event.get("_id", "")),
                "eventName": event.get("Event Name") or event.get("eventName", ""),
                "eventType": "Global Events",
                "eventLeaderName": event.get("Leader") or event.get("eventLeader", ""),
                "eventLeaderEmail": event.get("Email") or event.get("userEmail", ""),
                "day": event.get("Day", ""),
                "date": event_date.isoformat(),
                "time": event.get("time", ""),
                "location": event.get("Location") or event.get("location", ""),
                "description": event.get("description", ""),
                "attendees": event.get("attendees", []),
                "new_people": event.get("new_people", []),
                "consolidations": event.get("consolidations", []),
                "did_not_meet": event.get("did_not_meet", False),
                "status": event_status,
                "Status": event_status.replace("_", " ").title(),
                "_is_overdue": event_date < date.today() and event_status == "incomplete",
                "isGlobal": True,
                "isTicketed": event.get("isTicketed", False),
                "priceTiers": event.get("priceTiers", []),
                "total_attendance": event.get("total_attendance", 0),
                "UUID": event.get("UUID", "")
            }
            
            result_events.append(result)
        
        # Paginate
        result_events.sort(key=lambda x: x['date'], reverse=True)
        total = len(result_events)
        pages = (total + limit - 1) // limit if total > 0 else 1
        skip = (page - 1) * limit
        
        return {
            "events": result_events[skip:skip + limit],
            "total_events": total,
            "total_pages": pages,
            "current_page": page,
            "page_size": limit
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))


@app.put("/{event_id}")
async def update_event(event_id: str, event_data: dict):
    """Update event by ID."""
    event = await get_event(event_id)
    if not event:
        raise HTTPException(404, "Event not found")
    
    # Update allowed fields
    update_fields = [
        'eventName', 'day', 'location', 'date', 'status',
        'eventLeader', 'eventType', 'isTicketed', 'isGlobal'
    ]
    
    updates = {}
    for field in update_fields:
        if field in event_data and event_data[field] is not None:
            updates[field] = event_data[field]
    
    updates['updated_at'] = datetime.utcnow()
    
    await events_collection.update_one(
        {"_id": event["_id"]},
        {"$set": updates}
    )
    
    updated = await events_collection.find_one({"_id": event["_id"]})
    updated["_id"] = str(updated["_id"])
    
    return updated


@app.delete("/{event_id}")
async def delete_event(event_id: str):
    """Delete event by ID."""
    if not ObjectId.is_valid(event_id):
        raise HTTPException(400, "Invalid event ID")
    
    event = await events_collection.find_one({"_id": ObjectId(event_id)})
    if not event:
        raise HTTPException(404, "Event not found")
    
    await events_collection.delete_one({"_id": ObjectId(event_id)})
    
    return {"message": "Event deleted successfully"}


@app.get("/{event_id}")
async def get_event_by_id(event_id: str):
    """Get event by ID."""
    event = await get_event(event_id)
    if not event:
        raise HTTPException(404, "Event not found")
    
    event["_id"] = str(event["_id"])
    event = convert_datetime_to_iso(event)
    event = sanitize_document(event)
    
    # Ensure all fields
    event.setdefault("isTicketed", False)
    event.setdefault("isGlobal", False)
    event.setdefault("hasPersonSteps", False)
    event.setdefault("priceTiers", [])
    event.setdefault("leader1", "")
    event.setdefault("leader12", "")
    event.setdefault("leader144", "")
    
    return event


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


# Event type endpoints - same names
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