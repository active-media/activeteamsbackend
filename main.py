import os
from datetime import datetime, timedelta, time
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from auth.models import EventCreate, UserProfile,UserProfileUpdate, CheckIn, UncaptureRequest, UserCreate, UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel, PersonCreate
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token
import math
import secrets
from database import db, events_collection, people_collection, users_collection
from auth.email_utils import send_reset_password_email
from typing import Optional, Literal, List
from collections import Counter
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
# @app.post("/login")
# async def login(user: UserLogin):
#     existing = await users_collection.find_one({"email": user.email})
#     if not existing or not verify_password(user.password, existing["password"]):
#         raise HTTPException(status_code=401, detail="Invalid credentials")

#     # Create access token
#     token_expires = timedelta(minutes=JWT_EXPIRE_MINUTES)
#     access_token = create_access_token(
#         data={
#             "user_id": str(existing["_id"]),
#             "email": existing["email"],
#             "role": existing.get("role", "registrant")
#         },
#         expires_delta=token_expires,
#     )

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

    # Return both token AND user information
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": str(existing["_id"]),  # ← This is what the frontend needs!
            "email": existing["email"],
            "name": existing.get("name", ""),
            "surname": existing.get("surname", ""),
            "role": existing.get("role", "registrant"),
            "date_of_birth": existing.get("date_of_birth", ""),
            "home_address": existing.get("home_address", ""),
            "phone_number": existing.get("phone_number", ""),
            "gender": existing.get("gender", ""),
            "invited_by": existing.get("invited_by", "")
        }
    }

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
    reset_link = f"http://localhost:5173/reset-password?token={reset_token}"

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
def convert_datetime_to_iso(doc):
    """
    Recursively convert any datetime fields in a dict to ISO format strings.
    """
    if isinstance(doc, dict):
        return {k: convert_datetime_to_iso(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [convert_datetime_to_iso(i) for i in doc]
    elif isinstance(doc, datetime):
        return doc.isoformat()
    else:
        return doc


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

               # Auto-assign leader roles if it's a Cell event
        if event_data.get("eventType", "").lower().strip() == "cell":
            leader_name = event_data.get("eventLeader", "").strip()

            if leader_name:
                try:
                    leader_info = await get_leader_info(leader_name)
                    event_data["leaderPosition"] = leader_info["position"]

                    event_data["leaders"] = {
                        "12": leader_name if leader_info["position"] == 12 else None,
                        "144": leader_name if leader_info["position"] == 144 else None,
                        "1728": leader_name if leader_info["position"] == 1728 else None
                    }
                except HTTPException:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Leader '{leader_name}' not found in the system."
                    )

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

            # Auto-fill leaders for legacy events if missing
            if event.get("eventType", "").lower().strip() == "cell" and not event.get("leaders"):
                leader_name = event.get("eventLeader", "").strip()

                if leader_name:
                    try:
                        leader_info = await get_leader_info(leader_name)
                        event["leaderPosition"] = leader_info["position"]
                        event["leaders"] = {
                            "12": leader_name if leader_info["position"] == 12 else None,
                            "144": leader_name if leader_info["position"] == 144 else None,
                            "1728": leader_name if leader_info["position"] == 1728 else None
                        }

                        # Save back to DB
                        await events_collection.update_one(
                            {"_id": ObjectId(event["_id"])},
                            {"$set": {"leaders": event["leaders"], "leaderPosition": event["leaderPosition"]}}
                        )
                    except:
                        event["leaders"] = {"12": None, "144": None, "1728": None}

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
                # Handle leader role assignment if Cell event
        if update_data.get("eventType", "").lower().strip() == "cell":
            leader_name = update_data.get("eventLeader", "").strip()

            if leader_name:
                try:
                    leader_info = await get_leader_info(leader_name)
                    update_data["leaderPosition"] = leader_info["position"]

                    update_data["leaders"] = {
                        "12": leader_name if leader_info["position"] == 12 else None,
                        "144": leader_name if leader_info["position"] == 144 else None,
                        "1728": leader_name if leader_info["position"] == 1728 else None
                    }
                except HTTPException:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Leader '{leader_name}' not found in the system."
                    )

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

@app.put("/allevents/{event_id}")
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
@app.post('/events')
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


# ----------------
# CELLS ENDPOINTS
# -----------------

# ----------- Helper Functions -----------

def get_next_occurrence_single(start_date: datetime, recurring_day: str) -> datetime:
    """
    Calculate the next occurrence datetime for a recurring event on a specified day of the week.
    """
    days_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
    }

    if recurring_day.capitalize() not in days_map:
        raise ValueError("Invalid recurring day")

    target_weekday = days_map[recurring_day.capitalize()]
    current_weekday = start_date.weekday()

    days_ahead = target_weekday - current_weekday
    if days_ahead <= 0:
        days_ahead += 7

    next_date = start_date + timedelta(days=days_ahead)
    return next_date.replace(hour=start_date.hour, minute=start_date.minute, second=0, microsecond=0)

def convert_datetime_to_iso(doc):
    """
    Recursively convert all datetime fields in the dict to ISO strings.
    """
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, dict):
            convert_datetime_to_iso(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    convert_datetime_to_iso(item)
    return doc

def sanitize_document(doc):
    """
    Remove any sensitive/unnecessary fields if needed. Stub here.
    """
    # You can add sanitization logic here if needed
    return doc

def parse_time_string(time_str: str) -> Optional[time]:
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except Exception:
        return None

@app.post("/events/cell")
async def create_cell_event(payload: CellEventCreate):
    try:
        if payload.recurring and not payload.recurring_day:
            raise HTTPException(status_code=400, detail="recurring_day is required when recurring=True")

        start_dt = payload.start_date
        parsed_time = parse_time_string(payload.start_time)
        if parsed_time:
            start_dt = datetime.combine(start_dt.date(), parsed_time)

        # Replace this with your actual async leader cell name fetch function
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
            "total_attendance": 0,
        }

        result = await events_collection.insert_one(event_doc)
        return {"message": "Cell event created", "id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/cell/{cell_id}/common-attendees")
async def get_common_attendees(cell_id: str):
    try:
        cell_event = await events_collection.find_one({"_id": ObjectId(cell_id), "type": "cell"})
        if not cell_event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        leader_id = cell_event.get("leader_id")
        cell_name = cell_event.get("cell_name")

        # Get all past cell events by this leader
        cursor = events_collection.find({
            "type": "cell",
            "leader_id": leader_id,
            "cell_name": cell_name,
            "members": {"$exists": True, "$ne": []}
        })

        member_counter = Counter()
        member_details = {}

        async for event in cursor:
            for member in event.get("members", []):
                member_id = member.get("id")
                if member_id:
                    member_counter[member_id] += 1
                    if member_id not in member_details:
                        member_details[member_id] = {
                            "id": member_id,
                            "name": member.get("name"),
                            "email": member.get("email", ""),
                            "leader": member.get("leader", "")
                        }

        # Return top members sorted by most frequent
        sorted_members = [
            member_details[mid] for mid, _ in member_counter.most_common()
        ]

        return {"common_attendees": sorted_members}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/cells/upcoming")
async def get_upcoming_cells():
    try:
        now_utc = datetime.utcnow()

        # Show from previous day 11 PM SAST = 21:00 UTC previous day
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

            next_occurrence = start_date
            if event.get("recurring") and event.get("recurring_day") is not None:
                next_occurrence = get_next_occurrence_single(start_date, event.get("recurring_day"))

            if show_from_utc <= next_occurrence <= one_week_later:
                event["_id"] = str(event["_id"])
                event["next_occurrence"] = next_occurrence.isoformat()
                event = sanitize_document(convert_datetime_to_iso(event))
                cells.append(event)

        return {"cells": cells}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving upcoming cells: {str(e)}")


@app.get("/events/cell")
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
        raise HTTPException(status_code=500, detail=str(e))
    

# --------Endpoints to add leaders from cell events --------
@app.get("/leaders")
async def get_all_leaders():
    """Get all unique leaders from the people collection"""
    try:
        # Get all people and extract unique leaders
        people = await people_collection.find({}).to_list(length=None)
        
        leaders = {}
        
        for person in people:
            # Check Leader @12
            if person.get("Leader @12"):
                leader_name = person["Leader @12"].strip()
                if leader_name:
                    leaders[leader_name.lower()] = {
                        "name": leader_name,
                        "position": 12
                    }
            
            # Check Leader @144  
            if person.get("Leader @144"):
                leader_name = person["Leader @144"].strip()
                if leader_name:
                    leaders[leader_name.lower()] = {
                        "name": leader_name,
                        "position": 144
                    }
            
            # Check Leader @ 1728
            if person.get("Leader @ 1728"):
                leader_name = person["Leader @ 1728"].strip()
                if leader_name:
                    leaders[leader_name.lower()] = {
                        "name": leader_name,
                        "position": 1728
                    }
        
        return {"leaders": leaders}
    except Exception as e:
        print(f"Error fetching leaders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/leader/{leader_name}")
async def get_leader_info(leader_name: str):
    """Get specific leader information and position"""
    try:
        leader_key = leader_name.lower().strip()
        
        # Find people with this leader
        people_with_leader = await people_collection.find({
            "$or": [
                {"Leader @12": {"$regex": f"^{leader_name}$", "$options": "i"}},
                {"Leader @144": {"$regex": f"^{leader_name}$", "$options": "i"}},
                {"Leader @ 1728": {"$regex": f"^{leader_name}$", "$options": "i"}}
            ]
        }).to_list(length=1)
        
        if not people_with_leader:
            raise HTTPException(status_code=404, detail="Leader not found")
        
        person = people_with_leader[0]
        leader_info = {"name": leader_name, "position": None}
        
        if person.get("Leader @12", "").lower() == leader_key:
            leader_info["position"] = 12
        elif person.get("Leader @144", "").lower() == leader_key:
            leader_info["position"] = 144
        elif person.get("Leader @ 1728", "").lower() == leader_key:
            leader_info["position"] = 1728
            
        return leader_info
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching leader info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/events/{event_id}/checkin")
async def checkin_single_member_to_cell(event_id: str, data: AddMemberNamesRequest):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    person = await people_collection.find_one({"Name": {"$regex": f"^{data.name}$", "$options": "i"}})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

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

    await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {
            "$push": {"members": member_obj},
            "$inc": {"total_attendance": 1}
        }
    )

    return {"message": f"{person['Name']} checked in successfully to the cell event."}


@app.post("/events/{event_id}/uncheckin")
async def uncheckin_single_member(event_id: str, data: RemoveMemberRequest):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    person = await people_collection.find_one({"Name": {"$regex": f"^{data.name}$", "$options": "i"}})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    update_result = await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {"$pull": {"members": {"id": str(person["_id"])}}},
    )

    if update_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Person not found in this cell event")

    await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {"$inc": {"total_attendance": -1}}
    )

    return {"message": f"{person['Name']} has been removed from the cell event."}


@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": {"id": member_id}}})
    if update_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Member not found on event")
    return {"message": "Member removed"}
    

# Check-in (no auth required)
# -------------------------
# http://localhost:8000/checkin
@app.post("/checkin")
async def check_in_person(checkin: CheckIn):
    try:
        event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        person = await people_collection.find_one({"Name": {"$regex": f"^{checkin.name}$", "$options": "i"}})
        if not person:
            raise HTTPException(status_code=400, detail="Person not found in people database")

        already_checked = any(a.get("name", "").lower() == checkin.name.lower() for a in event.get("attendees", []))
        if already_checked:
            raise HTTPException(status_code=400, detail="Person already checked in")

        attendee_record = {
            "name": checkin.name,
            "time": datetime.utcnow(),
        }

        await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {"$push": {"attendees": attendee_record}, "$inc": {"total_attendance": 1}},
        )
        return {"message": f"{checkin.name} checked in successfully."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# View Check-ins
# -------------------------
@app.get("/checkins/{event_id}")
async def get_checkins(event_id: str):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        return {
            "event_id": event_id,
            "service_name": event.get("service_name"),
            "attendees": event.get("attendees", []),
            "total_attendance": event.get("total_attendance", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# http://localhost:8000/uncapture
@app.post("/uncapture")
async def uncapture_person(data: UncaptureRequest):
    try:
        update_result = await events_collection.update_one(
            {"_id": ObjectId(data.event_id)},
            {
                "$pull": {"attendees": {"name": data.name}},
                "$inc": {"total_attendance": -1}
            }
        )
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found or already removed")

        return {"message": f"{data.name} removed from check-ins."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---PROFILE ENDPOINTS---
# GET user profile by ID
@app.get("/profile/{user_id}", response_model=UserProfile)
async def get_profile(user_id: str = Path(...)):
    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user ID")

    user = await users_collection.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": str(user["_id"]),
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "date_of_birth": user.get("date_of_birth", ""),
        "home_address": user.get("home_address", ""),
        "invited_by": user.get("invited_by", ""),
        "phone_number": user.get("phone_number", ""),
        "email": user.get("email", ""),
        "gender": user.get("gender", ""),
        "role": user.get("role", "user"),
    }

# PUT update user profile by ID
@app.put("/profile/{user_id}", response_model=UserProfile)
async def update_profile(user_id: str, profile_update: UserProfileUpdate = Body(...)):
    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user ID")

    existing_user = await users_collection.find_one({"_id": ObjectId(user_id)})
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = profile_update.dict(exclude_unset=True)

    # Update the user document in DB
    await users_collection.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})

    # Fetch updated user to return
    updated_user = await users_collection.find_one({"_id": ObjectId(user_id)})

    return {
        "id": str(updated_user["_id"]),
        "name": updated_user.get("name", ""),
        "surname": updated_user.get("surname", ""),
        "date_of_birth": updated_user.get("date_of_birth", ""),
        "home_address": updated_user.get("home_address", ""),
        "invited_by": updated_user.get("invited_by", ""),
        "phone_number": updated_user.get("phone_number", ""),
        "email": updated_user.get("email", ""),
        "gender": updated_user.get("gender", ""),
        "role": updated_user.get("role", "user"),
    }


# PEOPLE ENDPOINTS
@app.get("/people")
async def get_people(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=0),  # Allow 0 to fetch all
    name: Optional[str] = None,
    gender: Optional[str] = None,
    dob: Optional[str] = None,
    location: Optional[str] = None,
    leader: Optional[str] = None,
    stage: Optional[str] = None
):
    try:
        query = {}

        # Construct the query based on provided parameters
        if name:
            query["Name"] = {"$regex": name, "$options": "i"}
        if gender:
            query["Gender"] = {"$regex": gender, "$options": "i"}
        if dob:
            query["DateOfBirth"] = dob
        if location:
            query["$or"] = [
                {"Location": {"$regex": location, "$options": "i"}},
                {"HomeAddress": {"$regex": location, "$options": "i"}}
            ]
        if leader:
            query["$or"] = [
                {"Leader @12": {"$regex": leader, "$options": "i"}},
                {"Leader @144": {"$regex": leader, "$options": "i"}},
                {"Leader @ 1728": {"$regex": leader, "$options": "i"}}
            ]
        if stage:
            query["Stage"] = {"$regex": stage, "$options": "i"}

        # Handle pagination or fetch all
        if perPage == 0:
            # Fetch all documents
            cursor = people_collection.find(query)
        else:
            # Paginated fetch
            skip = (page - 1) * perPage
            cursor = people_collection.find(query).skip(skip).limit(perPage)

        people_list = []
        async for person in cursor:
            person["_id"] = str(person["_id"])
            
            # Map to consistent field names with all leader fields included
            mapped = {
                "_id": person["_id"],
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Phone": person.get("Number", ""),  # Maps Number -> Phone
                "Email": person.get("Email", ""),
                "Location": person.get("HomeAddress") or person.get("Location", ""),  # Handle both
                "Gender": person.get("Gender", ""),
                "DateOfBirth": person.get("Birthday") or person.get("DateOfBirth", ""),  # Handle both
                "HomeAddress": person.get("HomeAddress") or person.get("Address", ""),
                "InvitedBy": person.get("InvitedBy", ""),
                # Include ALL leader fields separately
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @ 1728": person.get("Leader @ 1728", ""),
                # Primary leader field (for backwards compatibility)
                "Leader": (
                    person.get("Leader @12") or 
                    person.get("Leader @144") or 
                    person.get("Leader @ 1728") or 
                    ""
                ),
                "Stage": person.get("Stage", "Win"),
                "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
                "CreatedAt": person.get("CreatedAt") or datetime.utcnow().isoformat(),
                "Present": person.get("Present", False)
            }
            people_list.append(mapped)

        # Get total count for pagination metadata
        total_count = await people_collection.count_documents(query)

        return {
            "page": page,
            "perPage": perPage,
            "total": total_count,
            "results": people_list
        }
        
    except Exception as e:
        print(f"Error fetching people: {e}")  # Add logging for debugging
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


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
            "Location": person.get("Address") or person.get("Location", ""),
            "Gender": person.get("Gender", ""),
            "DateOfBirth": person.get("Birthday") or person.get("DateOfBirth", ""),
            "HomeAddress": person.get("Address") or person.get("HomeAddress", ""),
            "InvitedBy": person.get("InvitedBy", ""),
            # Include ALL leader fields
            "Leader @12": person.get("Leader @12", ""),
            "Leader @144": person.get("Leader @144", ""),
            "Leader @ 1728": person.get("Leader @ 1728", ""),
            "Leader": (
                person.get("Leader @12") or 
                person.get("Leader @144") or 
                person.get("Leader @ 1728") or 
                ""
            ),
            "Stage": person.get("Stage", "Win"),
            "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
            "CreatedAt": person.get("CreatedAt") or datetime.utcnow().isoformat(),
            "Present": person.get("Present", False)
        }
        return mapped
    except Exception as e:
        print(f"Error fetching person by ID: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def normalize_person_data(data: dict) -> dict:
    """Normalize person data for database operations"""
    return {
        "Name": data.get("Name") or data.get("name", ""),
        "Surname": data.get("Surname") or data.get("surname", ""),
        "Number": data.get("Number") or data.get("number", ""),  # Store as Number
        "Email": data.get("Email") or data.get("email", ""),
        "HomeAddress": data.get("HomeAddress") or data.get("address") or data.get("location", ""),
        "Birthday": data.get("Birthday") or data.get("dob", ""),  # Store as Birthday
        "Gender": data.get("Gender") or data.get("gender", ""),
        "InvitedBy": data.get("InvitedBy") or data.get("invitedBy", ""),
        "Leader @12": data.get("Leader @12") or data.get("leader12", ""),
        "Leader @144": data.get("Leader @144") or data.get("leader144", ""),
        "Leader @ 1728": data.get("Leader @ 1728") or data.get("leader1728", ""),
        "Stage": data.get("Stage") or data.get("stage", "Win"),
        "Present": data.get("Present", False),
        "UpdatedAt": datetime.utcnow().isoformat()
    }


@app.patch("/people/{person_id}")
async def update_person(person_id: str = Path(...), update_data: dict = Body(...)):
    try:
        normalized_data = normalize_person_data(update_data)
        
        result = await people_collection.update_one(
            {"_id": ObjectId(person_id)},
            {"$set": normalized_data}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")

        # Fetch the updated person document
        updated_person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not updated_person:
            raise HTTPException(status_code=404, detail="Person not found after update")

        # Return the updated person in the same format as GET
        updated_person["_id"] = str(updated_person["_id"])
        mapped = {
            "_id": updated_person["_id"],
            "Name": updated_person.get("Name", ""),
            "Surname": updated_person.get("Surname", ""),
            "Phone": updated_person.get("Number", ""),
            "Email": updated_person.get("Email", ""),
            "Location": updated_person.get("HomeAddress", ""),
            "Gender": updated_person.get("Gender", ""),
            "DateOfBirth": updated_person.get("Birthday", ""),
            "HomeAddress": updated_person.get("HomeAddress", ""),
            "InvitedBy": updated_person.get("InvitedBy", ""),
            "Leader @12": updated_person.get("Leader @12", ""),
            "Leader @144": updated_person.get("Leader @144", ""),
            "Leader @ 1728": updated_person.get("Leader @ 1728", ""),
            "Leader": (
                updated_person.get("Leader @12") or 
                updated_person.get("Leader @144") or 
                updated_person.get("Leader @ 1728") or 
                ""
            ),
            "Stage": updated_person.get("Stage", "Win"),
            "UpdatedAt": updated_person.get("UpdatedAt"),
            "Present": updated_person.get("Present", False)
        }
        return mapped

    except Exception as e:
        print(f"Error updating person: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/people")
async def create_person(person_data: PersonCreate):
    try:
        # Normalize email
        email = person_data.email.lower().strip()

        # Check if email already exists
        if email:
            existing_person = await people_collection.find_one({"Email": email})
            if existing_person:
                raise HTTPException(
                    status_code=400,
                    detail=f"A person with email '{email}' already exists"
                )

        # Extract leader fields from the list
        leader12 = person_data.leaders[0] if len(person_data.leaders) > 0 else ""
        leader144 = person_data.leaders[1] if len(person_data.leaders) > 1 else ""
        leader1728 = person_data.leaders[2] if len(person_data.leaders) > 2 else ""

        # Prepare the document
        person_doc = {
            "Name": person_data.name.strip(),
            "Surname": person_data.surname.strip(),
            "Email": email,
            "Number": person_data.number.strip(),
            "HomeAddress": person_data.address.strip(),
            "Gender": person_data.gender.strip(),
            "Birthday": person_data.dob.strip(),
            "InvitedBy": person_data.invitedBy.strip(),
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @ 1728": leader1728,
            "Stage": person_data.stage or "Win",
            "Present": False,
            "CreatedAt": datetime.utcnow().isoformat(),
            "UpdatedAt": datetime.utcnow().isoformat()
        }

        # Insert into MongoDB
        result = await people_collection.insert_one(person_doc)

        # Return the created person object
        created_person = {
            "_id": str(result.inserted_id),
            "Name": person_doc["Name"],
            "Surname": person_doc["Surname"],
            "Email": person_doc["Email"],
            "Phone": person_doc["Number"],
            "Gender": person_doc["Gender"],
            "DateOfBirth": person_doc["Birthday"],
            "HomeAddress": person_doc["HomeAddress"],
            "InvitedBy": person_doc["InvitedBy"],
            "Leader @12": person_doc["Leader @12"],
            "Leader @144": person_doc["Leader @144"],
            "Leader @ 1728": person_doc["Leader @ 1728"],
            "Leader": leader12 or leader144 or leader1728,
            "Stage": person_doc["Stage"],
            "Present": person_doc["Present"],
            "CreatedAt": person_doc["CreatedAt"],
            "UpdatedAt": person_doc["UpdatedAt"]
        }

        return {
            "message": "Person created successfully",
            "id": str(result.inserted_id),
            "_id": str(result.inserted_id),
            "person": created_person
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating person: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete("/people/{person_id}")
async def delete_person(person_id: str = Path(...)):
    try:
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}
    except Exception as e:
        print(f"Error deleting person: {e}")
        raise HTTPException(status_code=500, detail=str(e))