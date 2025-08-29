import os
from datetime import datetime, timedelta
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from auth.models import (
    EventCreate, CheckIn, UncaptureRequest, UserCreate, UserLogin,
    CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest,
    RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest,
    TaskModel, TaskUpdate, Person
)
from auth.utils import (
    hash_password, verify_password, require_role, get_current_user,
    get_next_occurrence_single, parse_time_string, get_leader_cell_name_async,
    create_access_token, decode_access_token, convert_datetime_to_iso,
    sanitize_document
)
import math
import secrets
from database import db, events_collection, people_collection, users_collection, tasks_collection
from auth.email_utils import send_reset_password_email
from typing import Optional, Literal, List
from pymongo import ReturnDocument

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
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    person["_id"] = str(person["_id"])
    return person

@app.patch("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def update_person(person_id: str, update: dict = Body(...)):
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    await people_collection.update_one({"_id": ObjectId(person_id)}, {"$set": update})
    return {"message": "Person updated successfully"}

@app.delete("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def delete_person(person_id: str):
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
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
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=400, detail="Invalid task ID format")
    task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    update_dict = {k: v for k, v in update.dict(exclude_unset=True).items()}
    await tasks_collection.update_one({"_id": ObjectId(task_id)}, {"$set": update_dict})
    return {"message": "Task updated successfully"}

@app.delete("/tasks/{task_id}", dependencies=[Depends(require_role("admin"))])
async def delete_task(task_id: str):
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=400, detail="Invalid task ID format")
    result = await tasks_collection.delete_one({"_id": ObjectId(task_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task deleted successfully"}

# -------------------------
# Event Endpoints
# -------------------------
@app.post("/event")
async def create_event(event: EventCreate):
    try:
        event_data = event.dict()
        
        # Date handling
        if "date" in event_data and isinstance(event_data["date"], str):
            try:
                event_data["date"] = datetime.fromisoformat(event_data["date"].replace("Z", "+00:00"))
            except ValueError:
                event_data["date"] = datetime.fromisoformat(event_data["date"])
        elif "date" not in event_data or not event_data["date"]:
            event_data["date"] = datetime.utcnow()
        
        # Ensure attendees and total_attendance
        event_data.setdefault("attendees", [])
        event_data.setdefault("total_attendance", len(event_data["attendees"]))
        
        # Add creation timestamp
        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        
        # Add status
        event_data["status"] = "open"
        
        # Add ticket info
        event_data["isTicketed"] = getattr(event, "isTicketed", False)
        event_data["price"] = getattr(event, "price", None)
        
        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}
    
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/events")
async def get_events():
    try:
        events = []
        cursor = events_collection.find({"status": "open"}).sort("created_at", -1)  # fetch open events, newest first

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
# Password Reset
# -------------------------
@app.post("/forgot-password")
async def forgot_password(req: ForgotPasswordRequest):
    user = await users_collection.find_one({"email": req.email})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    token = secrets.token_urlsafe(32)
    reset_record = {
        "user_id": str(user["_id"]),
        "token": token,
        "expires_at": datetime.utcnow() + timedelta(hours=1)
    }
    await db.reset_tokens.insert_one(reset_record)
    await send_reset_password_email(user["email"], token)
    return {"message": "Reset password email sent"}

@app.post("/reset-password")
async def reset_password(req: ResetPasswordRequest):
    record = await db.reset_tokens.find_one({"token": req.token})
    if not record or record["expires_at"] < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    hashed_password = hash_password(req.new_password)
    await users_collection.update_one({"_id": ObjectId(record["user_id"])}, {"$set": {"password": hashed_password}})
    await db.reset_tokens.delete_one({"token": req.token})
    return {"message": "Password reset successful"}

