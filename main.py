import os
from datetime import datetime, timedelta
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from auth.models import Event, CheckIn, UncaptureRequest, UserCreate, UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token
import math
import secrets
from database import db, events_collection, people_collection, users_collection
from auth.email_utils import send_reset_password_email
from typing import Optional, Literal, List


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def sanitize_document(doc):
    """Recursively sanitize document to replace NaN/Infinity float values with None."""
    if not isinstance(doc, dict):
        return doc
    
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

def convert_datetime_to_iso(obj):
    """Convert datetime objects to ISO strings recursively."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, datetime):
                obj[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                obj[key] = convert_datetime_to_iso(value)
    elif isinstance(obj, list):
        for i in range(len(obj)):
            if isinstance(obj[i], datetime):
                obj[i] = obj[i].isoformat()
            elif isinstance(obj[i], (dict, list)):
                obj[i] = convert_datetime_to_iso(obj[i])
    return obj

# SIGNUP AND LOGIN ENDPOINTS (kept for user management)
# http://localhost:8000/signup
@app.post("/signup")
async def signup(user: UserCreate):
    existing = await db["Users"].find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed = hash_password(user.password)
    user_dict = {
        "name": user.name,
        "surname": user.surname,
        "date_of_birth": user.date_of_birth,
        "home_address": user.home_address,
        "invited_by": user.invited_by,
        "phone_number": user.phone_number,
        "email": user.email,
        "gender": user.gender,
        "password": hashed,
        "confirm_password": hashed,
        "role": "user"  # default role; adjust as needed
    }
    await db["Users"].insert_one(user_dict)
    return {"message": "User created successfully"}

# JWT CONFIG
JWT_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))

# http://localhost:8000/login
@app.post("/login")
async def login(user: UserLogin):
    existing = await users_collection.find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Create access token
    token_expires = timedelta(minutes=JWT_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "user_id": str(existing["_id"]),
            "email": existing["email"],
            "role": existing.get("role", "registrant")
        },
        expires_delta=token_expires,
    )

    # Create refresh token
    refresh_token_id = secrets.token_urlsafe(16)
    refresh_plain = secrets.token_urlsafe(32)
    refresh_hash = hash_password(refresh_plain)
    refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    # Store refresh token data in DB
    await users_collection.update_one(
        {"_id": existing["_id"]},
        {
            "$set": {
                "refresh_token_id": refresh_token_id,
                "refresh_token_hash": refresh_hash,
                "refresh_token_expires": refresh_expires,
            }
        }
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token_id": refresh_token_id,
        "refresh_token": refresh_plain,
    }

# http://localhost:8000/refresh-token
@app.post("/refresh-token")
async def refresh_token(payload: RefreshTokenRequest = Body(...)):
    user = await users_collection.find_one({"refresh_token_id": payload.refresh_token_id})
    if (
        not user
        or not user.get("refresh_token_hash")
        or not verify_password(payload.refresh_token, user["refresh_token_hash"])
        or not user.get("refresh_token_expires")
        or user["refresh_token_expires"] < datetime.utcnow()
    ):
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    token_expires = timedelta(minutes=JWT_EXPIRE_MINUTES)
    token = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "registrant")},
        expires_delta=token_expires,
    )

    # Rotate refresh token on each refresh for extra security
    new_refresh_token_id = secrets.token_urlsafe(16)
    new_refresh_plain = secrets.token_urlsafe(32)
    new_refresh_hash = hash_password(new_refresh_plain)
    new_refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    await users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "refresh_token_id": new_refresh_token_id,
            "refresh_token_hash": new_refresh_hash,
            "refresh_token_expires": new_refresh_expires,
        }},
    )

    return {
        "access_token": token,
        "token_type": "bearer",
        "refresh_token_id": new_refresh_token_id,
        "refresh_token": new_refresh_plain,
    }

# http://localhost:8000/logout
@app.post("/logout")
async def logout(user_id: str = Body(..., embed=True)):
    await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {
            "$set": {
                "refresh_token_id": None,
                "refresh_token_hash": None,
                "refresh_token_expires": None,
            }
        },
    )
    return {"message": "Logged out successfully"}

# --- FORGOT PASSWORD ---
# http://localhost:8000/forgot-password
@app.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest):
    email = payload.email
    user = await users_collection.find_one({"email": email})
    if not user:
        return {"message": "If your email exists, you will receive a password reset email shortly."}

    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1),
    )
    reset_link = f"https://yourfrontend.com/reset-password?token={reset_token}"

    status_code = send_reset_password_email(email, reset_link)
    if not status_code or status_code >= 400:
        raise HTTPException(status_code=500, detail="Failed to send reset email")

    return {
        "message": "If your email exists, you will receive a password reset email shortly.",
        "reset_link": reset_link,
        "token": reset_token
    }

# --- RESET PASSWORD ---
# http://localhost:8000/reset-password
@app.post("/reset-password")
async def reset_password(data: ResetPasswordRequest):
    try:
        # Verify the JWT token and get payload data
        payload = decode_access_token(data.token)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid token payload")

    hashed_pw = hash_password(data.new_password)

    result = await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"password": hashed_pw}}
    )

    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found or password unchanged")

    return {"message": "Password has been reset successfully."}

# EVENT ENDPOINTS

# -------------------------
# EVENT ENDPOINTS
# -------------------------

# http://localhost:8000/event
@app.post("/event")
async def create_event(event: Event):
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

# http://localhost:8000/events
@app.get("/events")
async def get_all_events():

        events = []
        cursor = events_collection.find({"status": "open"}).sort("created_at", -1)  

# http://localhost:8000/events/type/{event_type}
@app.get("/events/type/{event_type}")
async def get_events_by_type(event_type: str = Path(...)):
    try:
        events = []
        # Use "type" field to match your existing cell events structure
        cursor = events_collection.find({"type": event_type})
        async for event in cursor:
            event["_id"] = str(event["_id"])
            event = convert_datetime_to_iso(event)  
            event = sanitize_document(event)        
            events.append(event)

        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving events: {str(e)}")




# http://localhost:8000/events/{event_id}
@app.put("/events/{event_id}")
async def update_event(event: Event, event_id: str = Path(...)):
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


# http://localhost:8000/events/{event_id}
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


# Close event
@app.patch("/allevents/{event_id}")
async def close_event(event_id: str = Path(...), attendees: list = None, did_not_meet: bool = False):
    try:
        update_data = {"status": "closed", "updated_at": datetime.utcnow()}
        if attendees:
            update_data["attendees"] = attendees
        await events_collection.update_one({"_id": ObjectId(event_id)}, {"$set": update_data})
        return {"message": "Event closed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error closing event: {str(e)}")


# http://localhost:8000/events/{event_id}
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
# Check-in (no auth required)
# -------------------------
# http://localhost:8000/checkin
@app.post("/checkin")
async def check_in_person(checkin: CheckIn):
    try:
        # Validate event ID format
        if not ObjectId.is_valid(checkin.event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Validate person name is not empty
        if not checkin.name or checkin.name.strip() == "":
            raise HTTPException(status_code=400, detail="Name cannot be empty")

        person = await people_collection.find_one({"Name": {"$regex": f"^{checkin.name.strip()}$", "$options": "i"}})
        if not person:
            raise HTTPException(status_code=400, detail="Person not found in people database")

        # Check if person already checked in (case-insensitive)
        already_checked = any(
            a.get("name", "").lower() == checkin.name.strip().lower() 
            for a in event.get("attendees", [])
        )
        if already_checked:
            raise HTTPException(status_code=400, detail="Person already checked in")

        attendee_record = {
            "name": checkin.name.strip(),
            "time": datetime.utcnow(),
        }

        # Update event with new attendee
        result = await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {
                "$push": {"attendees": attendee_record}, 
                "$inc": {"total_attendance": 1}
            },
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to check in person")
            
        return {"message": f"{checkin.name.strip()} checked in successfully."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during check-in: {str(e)}")

# -------------------------
# View Check-ins
# -------------------------
@app.get("/checkins/{event_id}")
async def get_checkins(event_id: str):
    try:
        # Validate event ID format
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Convert datetime objects in attendees to ISO strings
        attendees = event.get("attendees", [])
        for attendee in attendees:
            if "time" in attendee and isinstance(attendee["time"], datetime):
                attendee["time"] = attendee["time"].isoformat()

        return {
            "event_id": event_id,
            "service_name": event.get("service_name", ""),
            "attendees": attendees,
            "total_attendance": event.get("total_attendance", 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving check-ins: {str(e)}")

# -------------------------
# Cell event creation and management
# -------------------------
# http://localhost:8000/events/cell
@app.post("/events/cell")
async def create_cell_event(payload: CellEventCreate):
    try:
        if payload.recurring and not payload.recurring_day:
            raise HTTPException(status_code=400, detail="recurring_day is required when recurring=True")

        start_dt = payload.start_date
        
        # Parse time if provided
        if payload.start_time:
            parsed_time = parse_time_string(payload.start_time)
            if parsed_time:
                start_dt = datetime.combine(start_dt.date(), parsed_time)

        # Get cell name
        cell_name = await get_leader_cell_name_async(payload.leader_id)

        event_doc = {
            "type": "cell",
            "service_name": payload.service_name,
            "leader_id": payload.leader_id,
            "cell_name": cell_name,
            "start_date": start_dt,
            "recurring": payload.recurring,
            "recurring_day": payload.recurring_day,
            "members": payload.members or [],
            "created_at": datetime.utcnow(),
            "total_attendance": len(payload.members) if payload.members else 0,
        }

        result = await events_collection.insert_one(event_doc)
        return {"message": "Cell event created", "id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating cell event: {str(e)}")

# Capturing people into cells
# http://localhost:8000/events/{event_id}/checkin
@app.post("/events/{event_id}/checkin")
async def checkin_single_member_to_cell(event_id: str, data: AddMemberNamesRequest):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    person = await people_collection.find_one({"Name": {"$regex": f"^{data.name}$", "$options": "i"}})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

        # Check if member already checked in
        members = event.get("members", [])
        if any(m.get("id") == str(person["_id"]) for m in members):
            raise HTTPException(status_code=400, detail="Person already checked in")

        member_obj = {
            "id": str(person["_id"]),
            "name": person["Name"],
            "email": person.get("Email", ""),
            "leader": person.get("Leader", ""),
            "checkin_time": datetime.utcnow().isoformat(),
        }

        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {
                "$push": {"members": member_obj},
                "$inc": {"total_attendance": 1}
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to check in member")

# http://localhost:8000/events/{event_id}/uncheckin
@app.post("/events/{event_id}/uncheckin")
async def uncheckin_single_member(event_id: str, data: RemoveMemberRequest):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    try:
        if not event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        person = await people_collection.find_one({"Name": {"$regex": f"^{data.name}$", "$options": "i"}})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")

        person = await people_collection.find_one({"Name": {"$regex": f"^{data.name.strip()}$", "$options": "i"}})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")

        update_result = await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {"$pull": {"members": {"id": str(person["_id"])}}},
            )

        if update_result.modified_count == 0:
                raise HTTPException(status_code=404, detail="Person not found in this cell event")

            # Decrement attendance count
        await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {"$inc": {"total_attendance": -1}}
            )

        return {"message": f"{person['Name']} has been removed from the cell event."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing member: {str(e)}")

@app.get("/events/cell")
# http://localhost:8000/events/cell
async def list_cell_events():
    try:
        cursor = events_collection.find({"type": "cell"})

        results = []
        async for e in cursor:
            start_date = e.get("start_date")
            if isinstance(start_date, str):
                try:
                    start_date = datetime.fromisoformat(start_date)
                except Exception:
                    start_date = datetime.utcnow()

            next_occurrence = None
            if e.get("recurring") and e.get("recurring_day"):
                next_occurrence_dt = get_next_occurrence_single(start_date, e.get("recurring_day"))
                next_occurrence = next_occurrence_dt.isoformat()
            else:
                next_occurrence = start_date.isoformat() if start_date else None

            results.append({
                "id": str(e["_id"]),
                "service_name": e.get("service_name"),
                "leader_id": e.get("leader_id"),
                "cell_name": e.get("cell_name"),
                "members": e.get("members", []),
                "recurring": e.get("recurring", False),
                "recurring_day": e.get("recurring_day"),
                "start_date": start_date.isoformat() if start_date else None,
                "next_occurrence": next_occurrence,
                "total_attendance": e.get("total_attendance", 0),
            })
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing cell events: {str(e)}")

# Removing people from cell
# http://localhost:8000/events/cell/{event_id}/members/{member_id}
@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    try:
        # Validate event ID format
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
        if not event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": member_id}})
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Member not found in event")
            
        # Decrement attendance count
        await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$inc": {"total_attendance": -1}}
        )
        
        return {"message": "Member removed"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/uncapture
@app.post("/uncapture")
async def uncapture_person(data: UncaptureRequest):
    try:
        # Validate event ID format
        if not ObjectId.is_valid(data.event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
            
        # Validate name
        if not data.name or data.name.strip() == "":
            raise HTTPException(status_code=400, detail="Name cannot be empty")
            
        update_result = await events_collection.update_one(
            {"_id": ObjectId(data.event_id)},
            {
                "$pull": {"attendees": {"name": {"$regex": f"^{data.name.strip()}$", "$options": "i"}}},
                "$inc": {"total_attendance": -1}
            }
        )
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found or already removed")

        return {"message": f"{data.name.strip()} removed from check-ins."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uncapturing person: {str(e)}")

# PEOPLE ENDPOINTS
# http://localhost:8000/people?page=1&perPage=10
@app.get("/people")
async def get_people(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=1, le=500),
    name: str = None,
    gender: str = None,
    dob: str = None,
    location: str = None,
    leader: str = None,
    stage: str = None
):
    try:
        skip = (page - 1) * perPage
        query = {}

        if name:
            query["Name"] = {"$regex": name, "$options": "i"}
        if gender:
            query["Gender"] = {"$regex": gender, "$options": "i"}
        if dob:
            query["Birthday"] = dob  # map to DB field
        if location:
            query["Address"] = {"$regex": location, "$options": "i"}
        if leader:
            # Match any of the leader fields
            query["$or"] = [
                {"Leader @12": {"$regex": leader, "$options": "i"}},
                {"Leader @144": {"$regex": leader, "$options": "i"}},
                {"Leader @ 1728": {"$regex": leader, "$options": "i"}}
            ]
        if stage:
            query["Stage"] = {"$regex": stage, "$options": "i"}

        people = []
        cursor = people_collection.find(query).skip(skip).limit(perPage)
        async for person in cursor:
            person["_id"] = str(person["_id"])
            # 🔹 normalize fields
            mapped = {
                "_id": person["_id"],
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Phone": person.get("Number", ""),
                "Email": person.get("Email", ""),
                "Location": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "DateOfBirth": person.get("Birthday", ""),
                "Leader": (
                    person.get("Leader @12")
                    or person.get("Leader @144")
                    or person.get("Leader @ 1728")
                    or ""
                ),
                "Stage": person.get("Stage", "Win"),
            }
            people_list.append(mapped)

        total = await people_collection.count_documents(query)
        return {
            "people": people,
            "total": total,
            "page": page,
            "perPage": perPage
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/people/search")
async def search_people(name: str = Query(..., min_length=1)):
    try:
        cursor = people_collection.find({"Name": {"$regex": name, "$options": "i"}})
        people = []
        async for p in cursor:
            p["_id"] = str(p["_id"])
            people.append({"_id": p["_id"], "Name": p.get("Name", "")})
        return {"results": people}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/people/{person_id}")
async def get_person_by_id(person_id: str = Path(...)):
    try:
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        person["_id"] = str(person["_id"])
        mapped = {
            "_id": person["_id"],
            "Name": person.get("Name", ""),
            "Surname": person.get("Surname", ""),
            "Phone": person.get("Number", ""),
            "Email": person.get("Email", ""),
            "Location": person.get("Address", ""),
            "Gender": person.get("Gender", ""),
            "DateOfBirth": person.get("Birthday", ""),
            "Leader": (
                person.get("Leader @12")
                or person.get("Leader @144")
                or person.get("Leader @ 1728")
                or ""
            ),
            "Stage": person.get("Stage", "Win"),
        }
        return mapped
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/people")
async def create_or_update_person(person_data: dict = Body(...)):
    try:
        # Update existing person
        if "_id" in person_data:
            person_id = person_data["_id"]
            del person_data["_id"]
            result = await people_collection.update_one(
                {"_id": ObjectId(person_id)},
                {"$set": person_data}
            )
            if result.modified_count == 0:
                raise HTTPException(status_code=404, detail="Person not found or no changes made")
            return {"message": "Person updated successfully"}

        # Check for duplicates before creating
        query = {}
        if "email" in person_data and person_data["email"]:
            query["email"] = person_data["email"].strip().lower()
        else:
            # fallback: name + surname + dob
            query = {
                "name": person_data.get("name", "").strip(),
                "surname": person_data.get("surname", "").strip(),
                "dob": person_data.get("dob", "")
            }

        existing_person = await people_collection.find_one(query)
        if existing_person:
            raise HTTPException(status_code=400, detail="Person already exists")

        # Insert new person
        result = await people_collection.insert_one(person_data)
        return {"message": "Person created successfully", "id": str(result.inserted_id)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/people/{person_id}")
async def delete_person(person_id: str = Path(...)):
    try:
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# PROFILE ENDPOINTS
# http://localhost:8000/profile/{user_id}
@app.get("/profile/{user_id}")
async def get_profile(user_id: str = Path(...)):
    try:
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Remove sensitive data from response
        user["_id"] = str(user["_id"])
        user.pop("password", None)
        user.pop("confirm_password", None)
        user.pop("refresh_token_hash", None)
        user.pop("refresh_token_id", None)
        user.pop("refresh_token_expires", None)
        
        user = sanitize_document(user)
        return user
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/profile/{user_id}
@app.put("/profile/{user_id}")
async def update_profile(
    profile_data: dict = Body(...),
    user_id: str = Path(...)
):
    try:
        # Sensitive fields no one should update
        sensitive_fields = [
            "password", "confirm_password", "refresh_token_hash",
            "refresh_token_id", "refresh_token_expires", "_id"
        ]
        for field in sensitive_fields:
            profile_data.pop(field, None)

        # Make sure user exists
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # No valid fields to update
        if not profile_data:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        # Add update metadata
        profile_data["updated_at"] = datetime.utcnow()

        # Apply update
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": profile_data}
        )

        if result.modified_count == 0:
            return {"message": "No changes made"}

        return {"message": "Profile updated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
