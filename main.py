# main.py
import os
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi import Body, FastAPI, HTTPException, Query, Path, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import datetime
from bson import ObjectId


from auth.models import (
    EventCreate, CheckIn, UncaptureRequest, UserCreate, UserLogin,
    CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest,
    RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest,
    TaskModel, TaskUpdate, Person
)


# -------------------------
# Models
# -------------------------
from auth.models import (
    Person, TaskModel, TaskUpdate, UserCreate, UserLogin,
    CheckIn, UncaptureRequest, CellEventCreate,
    AddMemberNamesRequest, RemoveMemberRequest
)

# -------------------------
# Database
# -------------------------
from database import (
    users_collection, events_collection, tasks_collection,
    people_collection
)

# -------------------------
# Utils
# -------------------------
from utils import verify_password, hash_password, get_current_user, require_role

# -------------------------
# App Setup
# -------------------------
app = FastAPI()

origins = [
    "https://activeteams.netlify.app",  # your frontend
    "http://localhost:5173",             # local dev (Vite default)
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        # can be ["*"] for testing, but not recommended in prod
    allow_credentials=True,
    allow_methods=["*"],          # GET, POST, PUT, DELETE, etc
    allow_headers=["*"],          # Accepts all headers
)
# -------------------------
# Validation Exception
# -------------------------
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    formatted = [
        {"field": ".".join(err["loc"][1:]), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"errors": formatted})

app.add_exception_handler(RequestValidationError, validation_exception_handler)

# -------------------------
# Auth Endpoints
# -------------------------
@app.post("/auth/signup")
async def signup(user: UserCreate):
    existing_user = await users_collection.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_dict = user.dict()
    user_dict["password"] = hash_password(user_dict["password"])
    result = await users_collection.insert_one(user_dict)
    return {"message": "User created", "user_id": str(result.inserted_id)}

@app.post("/auth/login")
async def login(user: UserLogin):
    db_user = await users_collection.find_one({"email": user.email})
    if not db_user or not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    return {"message": "Login successful", "user_id": str(db_user["_id"])}

@app.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return current_user

# -------------------------
# People Endpoints
# -------------------------
@app.post("/people", dependencies=[Depends(require_role("admin", "registrant"))])
async def create_person(person: Person):
    person_dict = person.dict()
    result = await people_collection.insert_one(person_dict)
    person_dict["_id"] = str(result.inserted_id)
    return {"message": "Person created successfully", "person": person_dict}

@app.get("/people/{person_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_person(person_id: str):
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    person["_id"] = str(person["_id"])
    return person

@app.patch("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def update_person(person_id: str, update: dict = Body(...)):
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    await people_collection.update_one({"_id": ObjectId(person_id)}, {"$set": update})
    return {"message": "Person updated successfully"}

@app.delete("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def delete_person(person_id: str):
    result = await people_collection.delete_one({"_id": ObjectId(person_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"message": "Person deleted successfully"}

# -------------------------
# Task Endpoints
# -------------------------
@app.get("/tasks", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_all_tasks():
    tasks = []
    cursor = tasks_collection.find()
    async for t in cursor:
        t["_id"] = str(t["_id"])
        tasks.append(t)
    return {"tasks": tasks}

@app.get("/tasks/member/{member_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_tasks_by_member(member_id: str):
    tasks = []
    cursor = tasks_collection.find({"memberID": member_id})
    async for t in cursor:
        t["_id"] = str(t["_id"])
        tasks.append(t)
    return {"tasks": tasks}

@app.post("/tasks", dependencies=[Depends(require_role("admin"))])
async def create_task(task: TaskModel):
    task_dict = task.dict()
    result = await tasks_collection.insert_one(task_dict)
    task_dict["_id"] = str(result.inserted_id)
    return {"message": "Task created successfully", "task": task_dict}

@app.patch("/tasks/{task_id}", dependencies=[Depends(require_role("admin"))])
async def update_task(task_id: str, update: TaskUpdate):
    task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    update_dict = {k: v for k, v in update.dict(exclude_unset=True).items()}
    await tasks_collection.update_one({"_id": ObjectId(task_id)}, {"$set": update_dict})
    return {"message": "Task updated successfully"}

@app.delete("/tasks/{task_id}", dependencies=[Depends(require_role("admin"))])
async def delete_task(task_id: str):
    result = await tasks_collection.delete_one({"_id": ObjectId(task_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task deleted successfully"}

# -------------------------
# Event Endpoints

@app.post("/events")
async def create_event(event: EventCreate):
    try:
        event_data = event.dict()

        # Parse date
        if "date" in event_data and event_data["date"]:
            if isinstance(event_data["date"], str):
                try:
                    event_data["date"] = datetime.fromisoformat(event_data["date"].replace("Z", "+00:00"))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format")
        else:
            event_data["date"] = datetime.utcnow()

        # Defaults
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data["attendees"])
        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data["status"] = "open"
        event_data["isTicketed"] = getattr(event, "isTicketed", False)
        event_data["price"] = getattr(event, "price", None)

        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/events")
async def get_events(status: Optional[str] = Query(None, description="Filter events by status")):
    try:
        query = {}
        if status == "open":
            # Filter for events that are not closed
            query = {"status": {"$ne": "closed"}}
        elif status:
            # Filter by exact status if provided (e.g. status=closed)
            query = {"status": status}

        events = []
        cursor = events_collection.find(query).sort("created_at", -1)

        async for event in cursor:
            event["_id"] = str(event["_id"])
            event = convert_datetime_to_iso(event)
            event = sanitize_document(event)
            events.append(event)

        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving events: {str(e)}")


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
        
        return event
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving event: {str(e)}")

@app.put("/events/{event_id}")
async def update_event(event: EventCreate, event_id: str = Path(...)):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not existing_event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        update_data = event.dict(exclude_unset=True)
        
        if "date" in update_data and isinstance(update_data["date"], str):
            try:
                update_data["date"] = datetime.fromisoformat(update_data["date"].replace("Z", "+00:00"))
            except ValueError:
                update_data["date"] = datetime.fromisoformat(update_data["date"])
        
        # Handle ticket info
        if "isTicketed" in update_data:
            update_data["isTicketed"] = update_data["isTicketed"]
        if "price" in update_data:
            update_data["price"] = update_data["price"]
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            return {"message": "No changes were made to the event"}
        
        return {"message": "Event updated successfully"}
    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating event: {str(e)}")

@app.patch("/allevents/{event_id}")
async def close_event(event_id: str = Path(...), attendees: list = None, did_not_meet: bool = False):
    try:
        update_data = {"status": "closed", "updated_at": datetime.utcnow()}
        if attendees is not None:
            update_data["attendees"] = attendees
            update_data["total_attendance"] = len(attendees)
        await events_collection.update_one({"_id": ObjectId(event_id)}, {"$set": update_data})
        return {"message": "Event closed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error closing event: {str(e)}")


# Pseudo Python example
@app.route('/events')
def get_events():
    status = request.args.get('status')
    if status == 'open':
        query = {"status": {"$ne": "closed"}}
    else:
        query = {}
    events = db.events.find(query)
    return jsonify(events)


@app.delete("/events/{event_id}")
async def delete_event(event_id: str = Path(...)):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not existing_event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        result = await events_collection.delete_one({"_id": ObjectId(event_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        
        return {"message": "Event deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting event: {str(e)}")



@app.get("/cells/upcoming")
async def get_upcoming_cells():
    try:
        now_utc = datetime.utcnow()
        
        # Calculate "show from" datetime for each day
        # 11 PM previous day SAST -> 21:00 UTC
        show_from_utc = now_utc.replace(hour=21, minute=0, second=0, microsecond=0)
        if now_utc.hour < 21:
            show_from_utc -= timedelta(days=1)

        one_week_later = now_utc + timedelta(days=7)

        cells = []
        cursor = events_collection.find({
            "type": "cell",
            "status": "open"
        }).sort("created_at", -1)

        async for event in cursor:
            start_date = event.get("start_date")
            if isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date)

            # Only consider the next occurrence for recurring cells
            next_occurrence = start_date
            if event.get("recurring") and event.get("recurring_day") is not None:
                next_occurrence = get_next_occurrence_single(start_date, event.get("recurring_day"))

            # Only show if next occurrence is within the next week and after "show from"
            if next_occurrence <= one_week_later and next_occurrence >= show_from_utc:
                event["_id"] = str(event["_id"])
                event["next_occurrence"] = next_occurrence.isoformat()
                event = sanitize_document(convert_datetime_to_iso(event))
                cells.append(event)

        return {"cells": cells}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving upcoming cells: {str(e)}")

# -------------------------
# Check-in / Uncapture Endpoints
# -------------------------
@app.post("/checkin", dependencies=[Depends(require_role("admin", "registrant"))])
async def check_in_person(checkin: CheckIn):
    if not ObjectId.is_valid(checkin.event_id):
        raise HTTPException(status_code=400, detail="Invalid event ID format")
    event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    if checkin.name not in [a.get("name") for a in attendees]:
        attendees.append({"name": checkin.name, "checked_in_at": datetime.utcnow()})
        await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {"$set": {"attendees": attendees, "total_attendance": len(attendees)}}
        )
    return {"message": "Check-in successful"}

@app.post("/uncapture", dependencies=[Depends(require_role("admin"))])
async def uncapture_person(uncapture: UncaptureRequest):
    if not ObjectId.is_valid(uncapture.event_id):
        raise HTTPException(status_code=400, detail="Invalid event ID format")
    event = await events_collection.find_one({"_id": ObjectId(uncapture.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    attendees = [a for a in attendees if a.get("name") != uncapture.name]
    await events_collection.update_one(
        {"_id": ObjectId(uncapture.event_id)},
        {"$set": {"attendees": attendees, "total_attendance": len(attendees)}}
    )
    return {"message": "Person uncaptured successfully"}
# -------------------------
# Check-in / Uncapture Endpoints
# -------------------------
@app.post("/checkin", dependencies=[Depends(require_role("admin", "registrant"))])
async def check_in_person(checkin: CheckIn):
    event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    if checkin.name not in [a.get("name") for a in attendees]:
        attendees.append({"name": checkin.name, "checked_in_at": datetime.utcnow()})
        await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {"$set": {"attendees": attendees, "total_attendance": len(attendees)}}
        )
    return {"message": f"{checkin.name} checked in successfully"}

@app.post("/uncapture", dependencies=[Depends(require_role("admin"))])
async def uncapture_person(request: UncaptureRequest):
    event = await events_collection.find_one({"_id": ObjectId(request.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    new_attendees = [a for a in attendees if a.get("name") != request.name]
    await events_collection.update_one(
        {"_id": ObjectId(request.event_id)},
        {"$set": {"attendees": new_attendees, "total_attendance": len(new_attendees)}}
    )
    return {"message": f"{request.name} uncaptured successfully"}

# -------------------------
# Cell Event Management
# -------------------------
@app.post("/cells", dependencies=[Depends(require_role("admin"))])
async def create_cell_event(cell: CellEventCreate):
    cell_dict = cell.dict()
    result = await events_collection.insert_one(cell_dict)
    cell_dict["_id"] = str(result.inserted_id)
    return {"message": "Cell event created", "cell": cell_dict}

@app.post("/cells/{cell_id}/add_member", dependencies=[Depends(require_role("admin"))])
async def add_member(cell_id: str, member: AddMemberNamesRequest):
    cell = await events_collection.find_one({"_id": ObjectId(cell_id)})
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")
    members = cell.get("members", [])
    if member.name not in members:
        members.append(member.name)
        await events_collection.update_one({"_id": ObjectId(cell_id)}, {"$set": {"members": members}})
    return {"message": f"{member.name} added to cell"}

@app.post("/cells/{cell_id}/remove_member", dependencies=[Depends(require_role("admin"))])
async def remove_member(cell_id: str, member: RemoveMemberRequest):
    cell = await events_collection.find_one({"_id": ObjectId(cell_id)})
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")
    members = [m for m in cell.get("members", []) if m != member.name]
    await events_collection.update_one({"_id": ObjectId(cell_id)}, {"$set": {"members": members}})
    return {"message": f"{member.name} removed from cell"}
