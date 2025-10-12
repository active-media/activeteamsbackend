import os
from datetime import datetime, timedelta, time, date
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks

from fastapi.middleware.cors import CORSMiddleware
from auth.models import EventCreate, UserProfile, UserProfileUpdate, CheckIn, UncaptureRequest, UserCreate,UserCreater,  UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel, PersonCreate, EventTypeCreate, UserListResponse, UserList, MessageResponse, PermissionUpdate, RoleUpdate, AttendanceSubmission, TaskUpdate, EventUpdate
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token
import math
import secrets
from database import db, events_collection, people_collection, users_collection, tasks_collection
from auth.email_utils import send_reset_email
from typing import Optional, Literal, List
from collections import Counter
from auth.utils import get_current_user  
from auth.models import UserProfile, AttendanceSubmission
from datetime import datetime, timezone
import logging
import pytz
import base64
from fastapi import File, UploadFile
from fastapi.security import HTTPBearer
oauth2_scheme = HTTPBearer()
from passlib.context import CryptContext

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")  # 👈 Add this route
def root():
    return {"message": "App is live on Render!"}

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

# --- Password hashing setup ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

# JWT expiration
JWT_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS = 30


# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth")

# FastAPI & middleware
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))

# ---------------- Signup ----------------
@app.post("/signup")
async def signup(user: UserCreate):
    logger.info(f"Signup attempt: {user.email}")
    existing = await db["Users"].find_one({"email": user.email})
    if existing:
        logger.warning(f"Signup failed - email already registered: {user.email}")
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
        "role": "user"
    }
    await db["Users"].insert_one(user_dict)
    logger.info(f"User created successfully: {user.email}")
    return {"message": "User created successfully"}

# ---------------- Login ----------------
@app.post("/login")
async def login(user: UserLogin):
    logger.info(f"Login attempt: {user.email}")
    existing = await users_collection.find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        logger.warning(f"Login failed: {user.email}")
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token(
        {"user_id": str(existing["_id"]), "email": existing["email"], "role": existing.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

    refresh_token_id = secrets.token_urlsafe(16)
    refresh_plain = secrets.token_urlsafe(32)
    refresh_hash = hash_password(refresh_plain)
    refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    await users_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": {
            "refresh_token_id": refresh_token_id,
            "refresh_token_hash": refresh_hash,
            "refresh_token_expires": refresh_expires,
        }}
    )

    logger.info(f"Login successful: {user.email}")
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token_id": refresh_token_id,
        "refresh_token": refresh_plain,
        "user": {
        "id": str(existing["_id"]),
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

# ---------------- Forgot Password ----------------
@app.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, background_tasks: BackgroundTasks):
    logger.info(f"Forgot password requested: {payload.email}")
    user = await users_collection.find_one({"email": payload.email})

    if not user:
        logger.info(f"Forgot password - email not found: {payload.email}")
        return {"message": "If your email exists, a reset link has been sent."}

    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1)
    )
    reset_link = f"https://new-active-teams.netlify.app/reset-password?token={reset_token}"
    logger.info(f"Reset link generated for {payload.email}: {reset_link}")

    background_tasks.add_task(send_reset_email, payload.email, reset_link)
    logger.info(f"Reset email task added for {payload.email}")

    return {"message": "If your email exists, a reset link has been sent."}

# ---------------- Reset Password ----------------
@app.post("/reset-password")
async def reset_password(data: ResetPasswordRequest):
    try:
        payload = decode_access_token(data.token)
    except Exception:
        logger.warning("Invalid or expired reset token")
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    user_id = payload.get("user_id")
    if not user_id:
        logger.warning("Invalid token payload")
        raise HTTPException(status_code=400, detail="Invalid token payload")

    hashed_pw = hash_password(data.new_password)
    result = await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"password": hashed_pw, "confirm_password": hashed_pw}}
    )

    if result.modified_count == 0:
        logger.warning(f"Reset password failed - user not found or unchanged: {user_id}")
        raise HTTPException(status_code=404, detail="User not found or password unchanged")

    user = await users_collection.find_one({"_id": ObjectId(user_id)})
    access_token = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

    logger.info(f"Password reset successful for {user['email']}")
    return {
        "message": "Password has been reset successfully.",
        "access_token": access_token,
        "token_type": "bearer"
    }

# ---------------- Refresh Token ----------------
@app.post("/refresh-token")
async def refresh_token(payload: RefreshTokenRequest = Body(...)):
    logger.info(f"Refresh token requested: {payload.refresh_token_id}")
    user = await users_collection.find_one({"refresh_token_id": payload.refresh_token_id})
    if (
        not user
        or not user.get("refresh_token_hash")
        or not verify_password(payload.refresh_token, user["refresh_token_hash"])
        or not user.get("refresh_token_expires")
        or user["refresh_token_expires"] < datetime.utcnow()
    ):
        logger.warning(f"Refresh token invalid/expired: {payload.refresh_token_id}")
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    token = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

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

    logger.info(f"Refresh token rotated for user: {user['email']}")
    return {
        "access_token": token,
        "token_type": "bearer",
        "refresh_token_id": new_refresh_token_id,
        "refresh_token": new_refresh_plain,
    }

# ---------------- Logout ----------------
@app.post("/logout")
async def logout(user_id: str = Body(..., embed=True)):
    await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {
            "refresh_token_id": None,
            "refresh_token_hash": None,
            "refresh_token_expires": None,
        }},
    )
    logger.info(f"User logged out: {user_id}")
    return {"message": "Logged out successfully"}



# EVENT ENDPOINTS AS EMAILS 
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

# EVENTS ENDPOINTS

@app.post("/events")
async def create_event(event: EventCreate):
    try:
        event_data = event.dict()

        # Parse date
        if event_data.get("date"):
            if isinstance(event_data["date"], str):
                try:
                    event_data["date"] = datetime.fromisoformat(event_data["date"].replace("Z", "+00:00"))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format")
        else:
            event_data["date"] = datetime.utcnow()

        # Defaults
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data.get("attendees", []))
        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data["status"] = "open"
        event_data["isTicketed"] = getattr(event, "isTicketed", False)
        event_data["price"] = getattr(event, "price", None)

        # Auto-assign leader roles if it's a Cell event
        if event_data.get("eventType", "").lower().strip() == "cell":
            leader_name = (event_data.get("eventLeader") or "").strip().lower()

            # Fetch all people from the database
            all_people = await people_collection.find({}).to_list(length=None)

            # Map leader1 (position 12) and leader12 (position 144)
            leader1_match = next(
                (p["Leader @12"].title() for p in all_people if p.get("Leader @12") and p["Leader @12"].strip().lower() == leader_name),
                event_data.get("leader1")
            )
            leader12_match = next(
                (p["Leader @144"].title() for p in all_people if p.get("Leader @144") and p["Leader @144"].strip().lower() == leader_name),
                event_data.get("leader12")
            )

            event_data["leaders"] = {
                "1": leader1_match,
                "12": leader12_match
            }

        # Insert event into database
        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")

@app.get("/events")
async def get_events(status: Optional[str] = Query(None, description="Filter events by status")):
    try:
        query = {"isEventType": {"$ne": True}}  # Exclude event types from events

        if status == "open":
            query["status"] = {"$ne": "closed"}
        elif status:
            query["status"] = status

        events = []
        cursor = events_collection.find(query).sort("createdAt", -1)
        all_people = await people_collection.find({}).to_list(length=None)

        async for event in cursor:
            event_id = event["_id"]  # keep original ObjectId
            event["_id"] = str(event["_id"])

            # Auto-fill leaders for legacy cell events if missing
            if event.get("eventType", "").lower().strip() == "cell" and not event.get("leaders"):
                leader_name = (event.get("eventLeader") or "").strip().lower()

                # Find matching leaders in people database
                leader1_match = next(
                    (p["Leader @12"].title() for p in all_people
                     if p.get("Leader @12") and p["Leader @12"].strip().lower() == leader_name),
                    None
                )
                leader12_match = next(
                    (p["Leader @144"].title() for p in all_people
                     if p.get("Leader @144") and p["Leader @144"].strip().lower() == leader_name),
                    None
                )

                event["leaders"] = {
                    "1": leader1_match,
                    "12": leader12_match
                }

                # Save back to DB with proper ObjectId
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": {"leaders": event["leaders"]}}
                )

            # Convert datetime fields to ISO strings
            for k, v in event.items():
                if isinstance(v, datetime):
                    event[k] = v.isoformat()

            events.append(event)

        return {"events": events}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving events: {str(e)}")


# -----------------
# EVENTS TYPES SECTION

@app.post("/event-types")
async def create_event_type(event_type: EventTypeCreate):
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        # Normalize name (e.g., "Cell" not "cell")
        name = event_type.name.strip().title()

        # Prevent duplicate names
        exists = await events_collection.find_one({"isEventType": True, "name": name})
        if exists:
            raise HTTPException(status_code=400, detail="Event type already exists.")

        event_type_data = event_type.dict()
        event_type_data["name"] = name
        event_type_data["createdAt"] = event_type_data.get("createdAt") or datetime.utcnow()
        event_type_data["isEventType"] = True

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
        cursor = events_collection.find({"isEventType": True}).sort("createdAt", 1)
        event_types = []
        async for et in cursor:
            et["_id"] = str(et["_id"])
            event_types.append(et)
        return event_types
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching event types: {str(e)}")
    


# CELLS ENDPOINTS SECTION 
# ----------------------------
#  Debug email fields & documents
@app.get("/debug/emails")
async def debug_emails():
    """Check what emails and field names exist in the database."""
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
                continue  # Field may not exist

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

    
# Add these test endpoints to your FastAPI application
# Place them near your other endpoints

@app.get("/test/leader12-debug/{email}")
async def test_leader12_debug(email: str):
    """Debug endpoint to see what names are being matched"""
    try:
        # Find user's cell record
        user_cell = await events_collection.find_one({
            "Email": {"$regex": f"^{email}$", "$options": "i"}
        })
        
        if not user_cell:
            return {"error": "User not found", "email": email}
        
        user_name = user_cell.get("Leader", "").strip()
        
        # Find all cells where this user is Leader at 12 (NO STATUS FILTER)
        cells_where_leader12 = await events_collection.find({
            "Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"},
            "Event Type": "Cells"
        }).to_list(length=100)
        
        # Also check without Event Type filter
        cells_where_leader12_any_type = await events_collection.find({
            "Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}
        }).to_list(length=100)
        
        # Get today's info
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_day_name = today.strftime("%A")
        
        return {
            "user_email": email,
            "user_name_from_leader_field": user_name,
            "today": today.strftime("%Y-%m-%d"),
            "today_day_name": today_day_name,
            "cells_found_as_leader12_with_event_type_cells": len(cells_where_leader12),
            "cells_found_as_leader12_any_type": len(cells_where_leader12_any_type),
            "cells_details": [
                {
                    "event_name": c.get("Event Name"),
                    "leader": c.get("Leader"),
                    "leader_at_12": c.get("Leader at 12"),
                    "email": c.get("Email"),
                    "status": c.get("Status"),
                    "day": c.get("Day"),
                    "date_of_event": str(c.get("Date Of Event")),
                    "event_type": c.get("Event Type")
                } 
                for c in cells_where_leader12[:10]  # First 10 only
            ]
        }
        
    except Exception as e:
        logging.error(f"Error in test_leader12_debug: {e}")
        return {"error": str(e)}


# @app.get("/test/leader12-by-name/{name}")
# async def test_leader12_by_name(name: str):
#     """Test finding cells by Leader at 12 name directly"""
#     try:
#         timezone = pytz.timezone("Africa/Johannesburg")
#         today = datetime.now(timezone)
#         today_day_name = today.strftime("%A")
        
#         # Find all cells where this name appears in Leader at 12 (NO FILTERS)
#         cells = await events_collection.find({
#             "Leader at 12": {"$regex": f".*{name}.*", "$options": "i"}
#         }).to_list(length=100)
        
#         # Also check with Event Type = Cells
#         cells_with_type = await events_collection.find({
#             "Leader at 12": {"$regex": f".*{name}.*", "$options": "i"},
#             "Event Type": "Cells"
#         }).to_list(length=100)
        
#         # Check with Status filter
#         cells_not_complete = await events_collection.find({
#             "Leader at 12": {"$regex": f".*{name}.*", "$options": "i"},
#             "Event Type": "Cells",
#             "Status": {"$nin": ["Complete", "Closed"]}
#         }).to_list(length=100)
        
#         return {
#             "search_name": name,
#             "today": today.strftime("%Y-%m-%d"),
#             "today_day_name": today_day_name,
#             "total_cells_found": len(cells),
#             "cells_with_event_type": len(cells_with_type),
#             "cells_not_complete_or_closed": len(cells_not_complete),
#             "cells": [
#                 {
#                     "event_name": c.get("Event Name"),
#                     "leader": c.get("Leader"),
#                     "leader_at_12": c.get("Leader at 12"),
#                     "email": c.get("Email"),
#                     "status": c.get("Status"),
#                     "day": c.get("Day"),
#                     "date_of_event": str(c.get("Date Of Event")),
#                     "event_type": c.get("Event Type")
#                 } 
#                 for c in cells[:20]  # First 20
#             ]
#         }
        
#     except Exception as e:
#         logging.error(f"Error in test_leader12_by_name: {e}")
#         return {"error": str(e)}

# --------Endpoints to add leaders from cell events --------
@app.get("/leaders")
async def get_all_leaders():
    """Get all unique leaders from the people collection"""
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

# GET CELLS BASED ON OCCURING DAYS 
# -----------------------
# Configure logging
logging.basicConfig(level=logging.INFO)


def get_actual_event_status(event: dict, today: date) -> str:
    """
    ✅ FIXED: Check BOTH 'Status' and 'status' fields, return lowercase
    """
    # Check both field names (case-insensitive)
    status_from_lowercase = (event.get("status") or "").strip().lower()
    status_from_uppercase = (event.get("Status") or "").strip().lower()
    
    # Use whichever is set (prefer 'status' field)
    stored_status = status_from_lowercase or status_from_uppercase
    
    did_not_meet = event.get("did_not_meet", False)
    attendees = event.get("attendees", [])
    
    # Priority 1: If attendees exist
    if attendees and len(attendees) > 0:
        return "complete"
    
    # Priority 2: Explicitly marked as did_not_meet
    if did_not_meet or stored_status == "did_not_meet":
        return "did_not_meet"
    
    # Priority 3: Marked as complete/closed
    if stored_status in ["complete", "closed"]:
        return "complete"
    
    # Default: incomplete
    return "incomplete"

# ===== HELPER FUNCTION: Parse Time from Various Formats =====
def parse_time(time_value) -> tuple:
    """Parse time from string or datetime. Returns (hour, minute)"""
    if isinstance(time_value, datetime):
        return time_value.hour, time_value.minute
    elif isinstance(time_value, str):
        try:
            parts = time_value.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            return hour, minute
        except:
            return 19, 0
    else:
        return 19, 0



# ===== HELPER FUNCTION: Parse Event Date =====
def parse_event_date(event_date_field, default_date: date) -> date:
    """Parse event date from various formats"""
    if not event_date_field:
        return default_date
        
    if isinstance(event_date_field, datetime):
        return event_date_field.date()
    elif isinstance(event_date_field, date):
        return event_date_field
    elif isinstance(event_date_field, str):
        try:
            return datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
        except:
            return default_date
    else:
        return default_date


# ===== HELPER: Get Day Order =====
def get_day_order(day: str) -> int:
    """Return sort order for days of week"""
    day_map = {
        'monday': 0,
        'tuesday': 1,
        'wednesday': 2,
        'thursday': 3,
        'friday': 4,
        'saturday': 5,
        'sunday': 6
    }
    return day_map.get(day.lower().strip(), 999)
    
@app.get("/debug/test-leader-at-12-search/{name}")
async def debug_test_leader_at_12_search(name: str):
    """
    Test how many cells have this name in Leader at 12 field.
    """
    try:
        # Exact regex used in main query
        cells = await events_collection.find({
            "Event Type": "Cells",
            "Leader at 12": {"$regex": f".*{name}.*", "$options": "i"}
        }).to_list(length=100)
        
        return {
            "search_name": name,
            "total_found": len(cells),
            "cells": [
                {
                    "event_name": c.get("Event Name"),
                    "leader": c.get("Leader"),
                    "leader_at_12": c.get("Leader at 12"),
                    "email": c.get("Email"),
                    "day": c.get("Day")
                }
                for c in cells[:20]  # First 20
            ]
        }
        
    except Exception as e:
        return {"error": str(e)}

# ===== HELPER FUNCTION: Parse Event Date =====
def parse_event_date(event_date_field, default_date: date) -> date:
    """
    Parse event date from various formats.
    Returns: date object
    """
    if not event_date_field:
        return default_date
        
    if isinstance(event_date_field, datetime):
        return event_date_field.date()
    elif isinstance(event_date_field, date):
        return event_date_field
    elif isinstance(event_date_field, str):
        try:
            return datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
        except:
            return default_date
    else:
        return default_date


# ===== HELPER FUNCTION: Should Include Event =====
def should_include_event(event_date: date, status: str, today_date: date, is_admin: bool = False) -> bool:
    """
    Determine if an event should be included.
    
    Admin: Shows ALL incomplete cells (including overdue) + today/future captured
    User: Shows only today/future cells (hides overdue incomplete)
    """
    if is_admin:
        # Admin sees ALL incomplete cells regardless of date
        if status == 'incomplete':
            return True
        # For captured cells, only show today and future
        if status in ['complete', 'did_not_meet']:
            return event_date >= today_date
        return True
    else:
        # Regular users: only today and future
        return event_date >= today_date


def build_event_object(event: dict, timezone, today_date: date) -> dict:
    """Build event object with proper status"""
    event_name = event.get("Event Name", "")
    recurring_day = event.get("Day", "").strip().lower()
    event_date = parse_event_date(event.get("Date Of Event"), today_date)
    
    status = get_actual_event_status(event, today_date)
    
    time_value = event.get("Time", "19:00")
    hour, minute = parse_time(time_value)
    time_str = f"{hour}:{minute:02d}"
    
    try:
        event_datetime = datetime(
            event_date.year, event_date.month, event_date.day,
            hour, minute, 0
        )
        event_datetime = timezone.localize(event_datetime)
    except Exception:
        today = datetime.now(timezone)
        event_datetime = today.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    return {
        "_id": str(event["_id"]),
        "eventName": event_name,
        "eventType": "Cell",
        "date": event_datetime.isoformat(),
        "location": event.get("Address", ""),
        "status": status,        # lowercase for frontend
        "Status": status,        # uppercase for compatibility
        "eventLeaderName": event.get("Leader", ""),
        "eventLeaderEmail": event.get("Email", ""),
        "leader1": event.get("Leader at 1", ""),
        "leader12": event.get("Leader at 12", ""),
        "leader144": event.get("Leader at 144", ""),
        "time": time_str,
        "recurringDays": [recurring_day],
        "day": recurring_day,
        "isVirtual": bool(recurring_day),
        "isTicketed": False,
        "price": 0,
        "description": event_name,
        "attendees": event.get("attendees", []),
        "did_not_meet": event.get("did_not_meet", False),
        "_event_date": event_date,
        "_day_order": get_day_order(recurring_day),
    }

@app.get("/debug/user-hierarchy/{email}")
async def debug_user_hierarchy(email: str):
    """Debug what cells a user should see"""
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # Find user's name
        user_cell = await events_collection.find_one({
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        })
        
        if not user_cell:
            return {"error": f"No cell found for {email}"}
        
        user_name = user_cell.get("Leader", "").strip()
        
        # Find all cells (own + supervised)
        query = {
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
            ]
        }
        
        cells = await events_collection.find(query).to_list(length=None)
        
        # Categorize by status
        incomplete_cells = []
        complete_cells = []
        did_not_meet_cells = []
        
        for cell in cells:
            status = get_actual_event_status(cell, today_date)
            
            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "email": cell.get("Email"),
                "day": cell.get("Day"),
                "leader_at_12": cell.get("Leader at 12"),
                "status": status
            }
            
            if status == "incomplete":
                incomplete_cells.append(cell_info)
            elif status == "complete":
                complete_cells.append(cell_info)
            elif status == "did_not_meet":
                did_not_meet_cells.append(cell_info)
        
        return {
            "user_email": email,
            "user_name": user_name,
            "total_cells_in_database": len(cells),
            "cells_visible_to_user": len(incomplete_cells),
            "breakdown": {
                "incomplete_cells_visible": {
                    "count": len(incomplete_cells),
                    "cells": incomplete_cells
                },
                "complete_cells_hidden": {
                    "count": len(complete_cells),
                    "cells": complete_cells
                },
                "did_not_meet_cells_hidden": {
                    "count": len(did_not_meet_cells),
                    "cells": did_not_meet_cells
                }
            },
            "note": "User only sees incomplete cells. Complete and did_not_meet cells are hidden.",
            "status": "success"
        }
        
    except Exception as e:
        return {"error": str(e)}

# ===== MAIN: USER CELLS ENDPOINT =====
@app.get("/events/cells-user")
async def get_user_cell_events(current_user: dict = Depends(get_current_user)):
    """
    ✅ FIXED: Shows ONLY cells for TODAY'S day of the week
    """
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found in token")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day_name = today.strftime("%A").lower()  # "sunday"

        logging.info(f"========================================")
        logging.info(f"📅 TODAY: {today_day_name.upper()} ({today_date})")
        logging.info(f"🔍 Fetching cells ONLY for {today_day_name}")
        logging.info(f"========================================")

        # Find user's name
        user_cell = await events_collection.find_one({
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        })

        user_name = ""
        if user_cell:
            user_name = user_cell.get("Leader", "").strip()
            logging.info(f"✓ User name: '{user_name}'")

        # Build query - ONLY for TODAY'S day
        query_conditions = [
            {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            {"email": {"$regex": f"^{email}$", "$options": "i"}},
        ]
        
        if user_name:
            query_conditions.extend([
                {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
            ])
        
        # ✅ CRITICAL: Only get TODAY's day cells
        query = {
            "Event Type": "Cells",
            "Day": {"$regex": f"^{today_day_name}$", "$options": "i"},
            "$or": query_conditions
        }

        logging.info(f"📋 Query: Cells where Day = '{today_day_name}'")

        cursor = events_collection.find(query)
        
        events = []
        seen_keys = set()

        async for event in cursor:
            event_name = event.get("Event Name", "")
            event_email = event.get("Email", "").lower().strip()
            recurring_day = event.get("Day", "").strip().lower()
            
            # Verify it's today's day
            if recurring_day != today_day_name:
                logging.warning(f"⚠️ Skipping {recurring_day} cell: {event_name}")
                continue
            
            # Deduplicate
            dedup_key = f"{event_name}-{event_email}-{recurring_day}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            # Build event object
            event_obj = build_event_object(event, timezone, today_date)
            events.append(event_obj)
            
            logging.info(f"✓ Added {recurring_day} cell: {event_name} (status: {event_obj['status']})")

        # Sort by leader name
        events.sort(key=lambda x: x.get("eventLeaderName", "").lower())

        # Clean up temporary fields
        for event in events:
            del event["_event_date"]
            del event["_day_order"]

        logging.info(f"========================================")
        logging.info(f"✅ Returning {len(events)} cells for {today_day_name}")
        logging.info(f"========================================")

        return {
            "user_email": email,
            "user_name": user_name if user_name else "Unknown",
            "today": today.strftime("%Y-%m-%d"),
            "today_day": today_day_name,
            "total_events": len(events),
            "events": events,
            "status": "success"
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/test/hierarchy-for/{email}")
async def test_hierarchy_for_email(email: str):
    """Test hierarchy logic for any email without authentication"""
    try:
        # STEP 1: Find what name this user appears as in the cells collection
        user_cell = await events_collection.find_one({
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        })
        
        if not user_cell:
            return {
                "user_email": email,
                "message": "No cells found for this user",
                "status": "not_found"
            }
        user_name_in_cells = user_cell.get("Leader", "").strip()
        
        if not user_name_in_cells:
            return {
                "user_email": email,
                "message": "Could not determine user name from cells",
                "status": "error"
            }
        all_related_cells = await events_collection.find({
            "Event Type": "Cells",
            "Status": {"$ne": "closed"},
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"Leader": {"$regex": f"^{user_name_in_cells}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f"^{user_name_in_cells}$", "$options": "i"}},
                {"Leader at 144": {"$regex": f"^{user_name_in_cells}$", "$options": "i"}}
            ]
        }).to_list(None)
        
        # Categorize the cells
        own_cells = []
        supervised_cells = []
        
        for cell in all_related_cells:
            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "leader_email": cell.get("Email"),
                "leader_at_12": cell.get("Leader at 12"),
                "leader_at_144": cell.get("Leader at 144"),
                "day": cell.get("Day"),
                "time": cell.get("Time")
            }
            
            # Check if it's their own cell or supervised cell
            is_own = (cell.get("Email", "").lower() == email.lower() or 
                     cell.get("Leader", "").lower() == user_name_in_cells.lower())
            
            if is_own:
                own_cells.append(cell_info)
            else:
                supervised_cells.append(cell_info)
        
        return {
            "user_email": email,
            "user_name_in_cells": user_name_in_cells,
            "own_cells_count": len(own_cells),
            "supervised_cells_count": len(supervised_cells),
            "own_cells": own_cells,
            "supervised_cells": supervised_cells,
            "status": "success"
        }
        
    except Exception as e:
        return {"error": str(e)}

# ===== ADMIN CELLS ENDPOINT =====


@app.get("/admin/events/cells")
async def get_admin_cell_events(day: str = None, current_user: dict = Depends(get_current_user)):
    try:
        role = current_user.get("role", "")
        if role.lower() != "admin":
            raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone).date()

        # If no day query param, default to today
        query_day = day.lower() if day else today.strftime("%A").lower()

        # Calculate the next date that corresponds to the query_day (to handle tomorrow or any day)
        days_ahead = (["monday","tuesday","wednesday","thursday","friday","saturday","sunday"].index(query_day) - today.weekday()) % 7
        if days_ahead == 0:
            next_date = today  # today is the day
        else:
            next_date = today + timedelta(days=days_ahead)

        # Query must filter by:
        # - Event Type = "Cells"
        # - Recurring days includes query_day
        # - date >= next_date (filter out past dates)
        query = {
            "Event Type": "Cells",
            "recurringDays": {"$in": [query_day]},
            "date": {"$gte": datetime.combine(next_date, datetime.min.time()).astimezone(timezone)}
        }

        cursor = events_collection.find(query)
        events = []
        seen_keys = set()

        async for event in cursor:
            # Deduplicate by name/email/day
            event_name = event.get("Event Name", "")
            event_email = event.get("Email", "").lower().strip()
            recurring_days = [d.lower() for d in event.get("recurringDays", [])]
            dedup_key = f"{event_name}-{event_email}-{'-'.join(recurring_days)}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            event_obj = build_event_object(event, timezone, next_date)
            events.append(event_obj)

        events.sort(key=lambda x: x.get("eventLeaderName", "").lower())

        for event in events:
            event.pop("_event_date", None)
            event.pop("_day_order", None)

        return {
            "today": next_date.strftime("%Y-%m-%d"),
            "today_day": query_day,
            "total_events": len(events),
            "events": events,
            "status": "success"
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/events/{event_id}")
async def update_event(event: EventUpdate, event_id: str = Path(...)):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")

        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not existing_event:
            raise HTTPException(status_code=404, detail="Event not found")

        update_data = event.dict(exclude_unset=True)

        # 🔥 Handle ISO datetime string
        if "date" in update_data and isinstance(update_data["date"], str):
            try:
                update_data["date"] = datetime.fromisoformat(update_data["date"].replace("Z", "+00:00"))
            except ValueError as ve:
                raise HTTPException(status_code=422, detail=f"Invalid date format: {str(ve)}")

        # 🔧 Clean up if not ticketed
        if update_data.get("isTicketed") is False:
            update_data["price"] = None

        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            return {"message": "No changes were made to the event", "success": True}

        return {"message": "Event updated successfully", "success": True}

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating event: {str(e)}")



# ===== COMPLETE FIX: Show only TODAY's cells + proper status handling =====

@app.get("/events/cells-user")
async def get_user_cell_events(current_user: dict = Depends(get_current_user)):
    """
    ✅ FIXED: Shows ONLY cells for TODAY'S day of the week
    """
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found in token")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day_name = today.strftime("%A").lower()  # "sunday"

        logging.info(f"========================================")
        logging.info(f"📅 TODAY: {today_day_name.upper()} ({today_date})")
        logging.info(f"🔍 Fetching cells ONLY for {today_day_name}")
        logging.info(f"========================================")

        # Find user's name
        user_cell = await events_collection.find_one({
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        })

        user_name = ""
        if user_cell:
            user_name = user_cell.get("Leader", "").strip()
            logging.info(f"✓ User name: '{user_name}'")

        # Build query - ONLY for TODAY'S day
        query_conditions = [
            {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            {"email": {"$regex": f"^{email}$", "$options": "i"}},
        ]
        
        if user_name:
            query_conditions.extend([
                {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
            ])
        
        # ✅ CRITICAL: Only get TODAY's day cells
        query = {
            "Event Type": "Cells",
            "Day": {"$regex": f"^{today_day_name}$", "$options": "i"},
            "$or": query_conditions
        }

        logging.info(f"📋 Query: Cells where Day = '{today_day_name}'")

        cursor = events_collection.find(query)
        
        events = []
        seen_keys = set()

        async for event in cursor:
            event_name = event.get("Event Name", "")
            event_email = event.get("Email", "").lower().strip()
            recurring_day = event.get("Day", "").strip().lower()
            
            # Verify it's today's day
            if recurring_day != today_day_name:
                logging.warning(f"⚠️ Skipping {recurring_day} cell: {event_name}")
                continue
            
            # Deduplicate
            dedup_key = f"{event_name}-{event_email}-{recurring_day}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            # Build event object
            event_obj = build_event_object(event, timezone, today_date)
            events.append(event_obj)
            
            logging.info(f"✓ Added {recurring_day} cell: {event_name} (status: {event_obj['status']})")

        # Sort by leader name
        events.sort(key=lambda x: x.get("eventLeaderName", "").lower())

        # Clean up temporary fields
        for event in events:
            del event["_event_date"]
            del event["_day_order"]

        logging.info(f"========================================")
        logging.info(f"✅ Returning {len(events)} cells for {today_day_name}")
        logging.info(f"========================================")

        return {
            "user_email": email,
            "user_name": user_name if user_name else "Unknown",
            "today": today.strftime("%Y-%m-%d"),
            "today_day": today_day_name,
            "total_events": len(events),
            "events": events,
            "status": "success"
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"❌ Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/events/cells")
async def get_admin_cell_events(current_user: dict = Depends(get_current_user)):
    """
    ✅ ADMIN: Shows ONLY cells for TODAY'S day of the week
    """
    try:
        role = current_user.get("role", "")
        if role.lower() != "admin":
            raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day_name = today.strftime("%A").lower()

        logging.info(f"👑 Admin fetching cells for: {today_day_name.upper()}")

        # ✅ Get ONLY today's day cells
        query = {
            "Event Type": "Cells",
            "Day": {"$regex": f"^{today_day_name}$", "$options": "i"}
        }

        cursor = events_collection.find(query)
        events = []
        seen_keys = set()

        async for event in cursor:
            event_name = event.get("Event Name", "")
            event_email = event.get("Email", "").lower().strip()
            recurring_day = event.get("Day", "").strip().lower()
            
            dedup_key = f"{event_name}-{event_email}-{recurring_day}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            event_obj = build_event_object(event, timezone, today_date)
            events.append(event_obj)

        # Sort by leader name
        events.sort(key=lambda x: x.get("eventLeaderName", "").lower())

        for event in events:
            del event["_event_date"]
            del event["_day_order"]

        logging.info(f"👑 Admin: {len(events)} cells for {today_day_name}")

        return {
            "today": today.strftime("%Y-%m-%d"),
            "today_day": today_day_name,
            "total_events": len(events),
            "events": events,
            "status": "success"
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ===== FIX: submit_attendance - ensure both fields updated =====

@app.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: AttendanceSubmission = Body(...)
):
    """
    ✅ FIXED: Saves lowercase status to BOTH 'Status' and 'status' fields
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        event_name = event.get("Event Name", "Unknown")
        logging.info(f"📝 Submitting attendance for: {event_name}")
        
        if submission.did_not_meet:
            status_value = "did_not_meet"  # lowercase
            attendees_list = []
            total_attendance = 0
            did_not_meet_flag = True
            logging.info(f"❌ Marking as DID NOT MEET")
        else:
            status_value = "complete"  # lowercase
            attendees_list = [
                {
                    "id": att.id,
                    "name": att.name or att.fullName,
                    "fullName": att.fullName or att.name,
                    "leader12": att.leader12,
                    "leader144": att.leader144,
                    "time": att.time.isoformat() if att.time else None,
                    "email": att.email,
                    "phone": att.phone,
                    "decision": att.decision
                }
                for att in submission.attendees
            ]
            total_attendance = len(attendees_list)
            did_not_meet_flag = False
            logging.info(f"✅ Capturing {total_attendance} attendees")
        
        # ✅ CRITICAL: Update BOTH 'Status' and 'status' fields with lowercase value
        update_data = {
            "Status": status_value,      # Uppercase field name, lowercase value
            "status": status_value,      # Lowercase field name, lowercase value
            "updated_at": datetime.utcnow(),
            "attendees": attendees_list,
            "total_attendance": total_attendance,
            "did_not_meet": did_not_meet_flag,
            "captured_by": {
                "leaderEmail": submission.leaderEmail,
                "leaderName": submission.leaderName,
                "captured_at": datetime.utcnow().isoformat()
            }
        }
        
        logging.info(f"💾 Saving: Status='{status_value}', status='{status_value}'")
        
        result = await events_collection.update_one(
            {"_id": event["_id"]},
            {"$set": update_data}
        )
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
        
        # Verify the update
        updated_event = await events_collection.find_one({"_id": event["_id"]})
        db_status = updated_event.get("status", "unknown")
        db_Status = updated_event.get("Status", "unknown")
        logging.info(f"✓ Verified in DB: status='{db_status}', Status='{db_Status}'")
        
        return {
            "message": "Attendance captured" if not did_not_meet_flag else "Marked as did not meet",
            "event_id": str(event["_id"]),
            "event_name": event_name,
            "status": status_value,
            "success": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"❌ Error submitting attendance: {e}", exc_info=True)

        raise HTTPException(status_code=500, detail=str(e))

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


@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": {"id": member_id}}})
    if update_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Member not found on event")
    return {"message": "Member removed"}
    

@app.get("/leaders/position/{level}")
async def get_leaders_by_position(level: int):
    """Return a list of people who are leaders at the specified level: 12, 144, or 1728."""
    try:
        if level not in [12, 144, 1728]:
            raise HTTPException(status_code=400, detail="Invalid leadership level")

        field_map = {
            12: "Leader @12",
            144: "Leader @144",
            1728: "Leader @ 1728"
        }

        field_name = field_map[level]

        people = await people_collection.find({field_name: {"$exists": True, "$ne": ""}}).to_list(length=None)

        result = []
        for person in people:
            leader_name = person.get(field_name, "").strip()
            if leader_name:
                result.append({
                    "name": leader_name,
                    "person_id": str(person["_id"])
                })

        return {"leaders": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

@app.get("/test/user-cells/{email}")
async def test_user_cells(email: str):
    """
    Debug endpoint to see what cells a regular user should see.
    Tests the exact logic used in /events/cells-user
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # Step 1: Find user's cell by email
        user_cell = await events_collection.find_one({
            "Email": {"$regex": f"^{email}$", "$options": "i"}
        })
        
        user_name = ""
        if user_cell:
            user_name = user_cell.get("Leader", "").strip()
        
        # Step 2: Build the same query as the endpoint
        query_conditions = [
            {"Email": {"$regex": f"^{email}$", "$options": "i"}},
        ]
        
        if user_name:
            query_conditions.extend([
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f"{user_name}", "$options": "i"}},
            ])
        
        query = {
            "Event Type": "Cells",
            "$or": query_conditions
        }
        
        # Step 3: Find all matching cells
        all_cells = await events_collection.find(query).to_list(length=None)
        
        # Step 4: Process each cell
        cells_by_category = {
            "own_cells_by_email": [],
            "own_cells_by_name": [],
            "supervised_cells": [],
            "filtered_out_overdue": [],
            "filtered_out_complete": []
        }
        
        for cell in all_cells:
            cell_email = cell.get("Email", "").lower().strip()
            cell_leader = cell.get("Leader", "").strip()
            cell_name = cell.get("Event Name", "")
            event_date = parse_event_date(cell.get("Date Of Event"), today_date)
            status = get_actual_event_status(cell, today_date)
            
            cell_info = {
                "event_name": cell_name,
                "leader": cell_leader,
                "email": cell_email,
                "leader_at_12": cell.get("Leader at 12"),
                "day": cell.get("Day"),
                "date": str(event_date),
                "status": status
            }
            
            # Check if should be included
            should_include = should_include_event(event_date, status, today_date, is_admin=False)
            
            if not should_include:
                if status == 'incomplete':
                    cells_by_category["filtered_out_overdue"].append(cell_info)
                else:
                    cells_by_category["filtered_out_complete"].append(cell_info)
                continue
            
            # Categorize included cells
            if cell_email == email.lower():
                cells_by_category["own_cells_by_email"].append(cell_info)
            elif user_name and cell_leader.lower() == user_name.lower():
                cells_by_category["own_cells_by_name"].append(cell_info)
            else:
                cells_by_category["supervised_cells"].append(cell_info)
        
        total_visible = (
            len(cells_by_category["own_cells_by_email"]) +
            len(cells_by_category["own_cells_by_name"]) +
            len(cells_by_category["supervised_cells"])
        )
        
        return {
            "user_email": email,
            "user_name_found": user_name or "NOT FOUND",
            "today": today.strftime("%Y-%m-%d"),
            "query_used": {
                "Event Type": "Cells",
                "OR_conditions": [
                    f"Email matches {email}",
                    f"Leader matches {user_name}" if user_name else "SKIPPED - no name",
                    f"Leader at 12 contains {user_name}" if user_name else "SKIPPED - no name"
                ]
            },
            "total_cells_found": len(all_cells),
            "total_visible_to_user": total_visible,
            "cells_breakdown": cells_by_category,
            "issue_diagnosis": {
                "has_cell_record": bool(user_cell),
                "has_name_in_database": bool(user_name),
                "email_query_working": len(cells_by_category["own_cells_by_email"]) > 0,
                "name_query_working": len(cells_by_category["own_cells_by_name"]) > 0,
            }
        }
        
    except Exception as e:
        logging.error(f"Error in test_user_cells: {e}", exc_info=True)
        return {"error": str(e)}
# @app.get("/cells/under-female-12s")
# async def get_cells_under_female_12s(
#     current_user: dict = Depends(get_current_user),
#     day: str = Query(...)
# ):
#     try:
#         email = current_user.get("email")
#         if not email:
#             raise HTTPException(401, "User email not found")

#         person = await people_collection.find_one({"Email": email})
#         if not person:
#             raise HTTPException(404, "User not found")

#         full_name = f"{person['Name']} {person['Surname']}"

#         now = datetime.utcnow()

#         # Query cells where "Leader at 12" == full_name, and Day & future date
#         cell_events = await events_collection.find({
#             "Day": day,
#             "Leader at 12": full_name,
#             "Status": {"$ne": "closed"},
#             "Date Of Event": {"$gt": now}
#         }).to_list(None)

#         if not cell_events:
#             return {
#                 "requested_by": full_name,
#                 "day": day,
#                 "total_events": 0,
#                 "events": [],
#                 "message": "No upcoming cells found under you as Leader at 12"
#             }

#         # Extract participant names or relevant info from events
#         participants = [cell.get("Participant Name") for cell in cell_events if "Participant Name" in cell]

#         return {
#             "requested_by": full_name,
#             "day": day,
#             "total_events": len(cell_events),
#             "participants": participants,
#             "events": cell_events
#         }

#     except Exception as e:
#         raise HTTPException(500, str(e))
# # Admins can see all cells happening today






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


# --- PROFILE PICTURE ENDPOINTS ---

@app.get("/profile/{user_id}", response_model=UserProfile)
async def get_profile(user_id: str, current_user: dict = Depends(get_current_user)):
    """Get user profile - uses consistent authentication"""
    # Verify user owns this account
    token_user_id = current_user.get("user_id")
    
    if not token_user_id or token_user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to access this profile")

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
        "profile_picture": user.get("profile_picture", ""),
    }

@app.put("/profile/{user_id}", response_model=UserProfile)
async def update_profile(
    user_id: str, 
    profile_update: UserProfileUpdate = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """Update user profile - uses consistent authentication"""
    # Verify user owns this account
    token_user_id = current_user.get("user_id")
    
    if not token_user_id or token_user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to update this profile")

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
        "profile_picture": updated_user.get("profile_picture", ""),
    }

@app.post("/users/{user_id}/avatar")
async def upload_avatar(
    user_id: str,
    avatar: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """Upload profile picture - uses consistent authentication"""
    try:
        # Verify user owns this account
        token_user_id = current_user.get("user_id")
        
        if not token_user_id or token_user_id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")
        
        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Validate file type
        if not avatar.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")

        # Validate file size (e.g., max 5MB)
        contents = await avatar.read()
        if len(contents) > 5 * 1024 * 1024:  # 5MB
            raise HTTPException(status_code=400, detail="File too large. Maximum size is 5MB")
        
        # Convert to base64 for storage
        image_base64 = base64.b64encode(contents).decode('utf-8')
        image_data_url = f"data:{avatar.content_type};base64,{image_base64}"

        # Update user with profile picture
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"profile_picture": image_data_url}}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")

        return {"message": "Avatar uploaded successfully", "avatarUrl": image_data_url}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading avatar: {str(e)}")

@app.put("/users/{user_id}/password")
async def change_password(
    user_id: str,
    password_data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Change user password - uses consistent authentication"""
    try:
        # Verify user owns this account
        token_user_id = current_user.get("user_id")
        
        if not token_user_id or token_user_id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        current_password = password_data.get("currentPassword")
        new_password = password_data.get("newPassword")

        if not current_password or not new_password:
            raise HTTPException(status_code=400, detail="Current password and new password are required")

        # Basic password validation
        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="New password must be at least 8 characters long")

        # Get user and verify current password
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Verify current password
        if not verify_password(current_password, user["password"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # Hash new password and update
        hashed_new_password = hash_password(new_password)
        
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"password": hashed_new_password}}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update password")

        return {"message": "Password updated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error changing password: {str(e)}")



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
    

# -------------------------
# Tasks Management
# -------------------------

# Create a new task

# POST /tasks

from fastapi.encoders import jsonable_encoder

@app.post("/tasks")
async def create_task(task: TaskModel, current_user: dict = Depends(get_current_user)):
    try:
        # Convert Pydantic model to dict
        new_task_dict = task.dict()
        # Attach the creator's email for backward compatibility
        new_task_dict["assignedfor"] = current_user["email"]

        # Insert into MongoDB
        result = await db["tasks"].insert_one(new_task_dict)

        # Add the MongoDB _id as a string for the response
        new_task_dict["_id"] = str(result.inserted_id)

        # Encode safely for JSON response
        return {"status": "success", "task": jsonable_encoder(new_task_dict)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}

# Retrieve all tasks

# GET /tasks 

@app.get("/tasks")
async def get_user_tasks(
    email: str = Query(None),
    userId: str = Query(None),
    view_all: bool = Query(False),  # Add explicit parameter for viewing all tasks
    current_user: dict = Depends(get_current_user)
):
    try:
        # Check if current user is a leader
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        
        # Determine user email based on parameters or current user
        user_email = None
        
        if email:
            user_email = email
        elif userId:
            user = await users_collection.find_one({"_id": ObjectId(userId)})
            if user:
                user_email = user.get("email")
        else:
            # No parameters provided - use current user's email
            user_email = current_user.get("email")
        
        if not user_email:
            return {"error": "User email not found", "status": "failed"}
        
        timezone = pytz.timezone("Africa/Johannesburg")
        
        # Build query based on permissions
        # Only show all tasks if user is a leader AND explicitly requests it with view_all=true
        if is_leader and view_all:
            query = {}
        else:
            # Always filter by specific user email (current user or specified user)
            query = {"assignedfor": user_email}
        
        # Fetch tasks
        cursor = tasks_collection.find(query)
        all_tasks = []
        
        async for task in cursor:
            task_date_str = task.get("followup_date")
            task_datetime = None
            
            # Parse followup_date
            if task_date_str:
                if isinstance(task_date_str, datetime):
                    task_datetime = task_date_str
                else:
                    try:
                        task_datetime = datetime.fromisoformat(task_date_str)
                        task_datetime = task_datetime.astimezone(timezone)
                    except ValueError:
                        logging.warning(f"Invalid date format: {task_date_str}")
                        continue
            
            all_tasks.append({
                "_id": str(task["_id"]),
                "name": task.get("name", "Unnamed Task"),
                "taskType": task.get("taskType", ""),
                "followup_date": task_datetime.isoformat() if task_datetime else None,
                "status": task.get("status", "Open"),
                "assignedfor": task.get("assignedfor", ""),
                "type": task.get("type", "call"),
                "contacted_person": task.get("contacted_person", {}),
                "isRecurring": bool(task.get("recurring_day")),
            })
        
        # Sort by date (newest first)
        all_tasks.sort(key=lambda t: t["followup_date"] or "", reverse=True)
        
        return {
            "user_email": user_email if not view_all else "all_users",
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "status": "success",
            "is_leader_view": is_leader and view_all
        }
        
    except Exception as e:
        logging.error(f"Error in get_user_tasks: {e}")
        return {"error": str(e), "status": "failed"}  
    
# STATS ENDPOINTS
# Add to your FastAPI backend
# Add to your main.py or stats endpoints file

from datetime import datetime, timedelta
from collections import defaultdict

@app.get("/stats/overview")
async def get_stats_overview(period: str = "monthly"):
    """Get overall statistics for the dashboard with time period filtering"""
    try:
        # Calculate date range based on period
        now = datetime.utcnow()
        if period == "daily":
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif period == "weekly":
            start_date = now - timedelta(days=now.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
        else:  # monthly
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 12:
                end_date = now.replace(year=now.year + 1, month=1, day=1)
            else:
                end_date = now.replace(month=now.month + 1, day=1)

        # Count outstanding cells (cells with status != "completed" or "closed")
        # Assuming cells are events with eventType "Cell" and have a status field
        outstanding_cells = await events_collection.count_documents({
            "eventType": "Cell",
            "status": {"$nin": ["completed", "closed", "done"]}
        })
        
        # Count outstanding tasks from tasks collection
        # Assuming tasks have a status field and are not completed/closed
        outstanding_tasks = await tasks_collection.count_documents({
            "status": {"$nin": ["completed", "closed", "done"]}
        })
        
        # Get total people (assuming you have a people collection)
        total_people = await people_collection.count_documents({})
        
        # Get events for the period to calculate attendance and growth
        # Only include non-cell events for attendance calculation
        period_events = await events_collection.find({
            "date": {"$gte": start_date, "$lt": end_date},
            "status": {"$in": ["completed", "closed"]},
            "eventType": {"$ne": "Cell"}  # Exclude cells from attendance calculation
        }).to_list(length=None)
        
        # Calculate total attendance for the period
        total_attendance = sum(event.get("total_attendance", 0) for event in period_events)
        
        # Calculate previous period for growth comparison
        if period == "daily":
            prev_start = start_date - timedelta(days=1)
            prev_end = start_date
        elif period == "weekly":
            prev_start = start_date - timedelta(days=7)
            prev_end = start_date
        else:  # monthly
            if start_date.month == 1:
                prev_start = start_date.replace(year=start_date.year - 1, month=12)
            else:
                prev_start = start_date.replace(month=start_date.month - 1)
            prev_end = start_date
        
        # Get previous period attendance (exclude cells)
        prev_events = await events_collection.find({
            "date": {"$gte": prev_start, "$lt": prev_end},
            "status": {"$in": ["completed", "closed"]},
            "eventType": {"$ne": "Cell"}
        }).to_list(length=None)
        
        prev_attendance = sum(event.get("total_attendance", 0) for event in prev_events)
        
        # Calculate growth rate
        if prev_attendance > 0:
            growth_rate = ((total_attendance - prev_attendance) / prev_attendance) * 100
        else:
            growth_rate = 100 if total_attendance > 0 else 0
        
        # Calculate weekly/daily attendance breakdown (exclude cells)
        attendance_breakdown = {}
        for event in period_events:
            if event.get("date"):
                event_date = event["date"]
                if period == "daily":
                    # Group by hour for daily view
                    hour = event_date.hour
                    key = f"{hour:02d}:00"
                elif period == "weekly":
                    # Group by day name for weekly view
                    key = event_date.strftime("%A")
                else:
                    # Group by week number for monthly view
                    week_num = event_date.isocalendar()[1]
                    key = f"Week {week_num}"
                
                if key not in attendance_breakdown:
                    attendance_breakdown[key] = 0
                attendance_breakdown[key] += event.get("total_attendance", 0)
        
        return {
            "outstanding_cells": outstanding_cells,
            "outstanding_tasks": outstanding_tasks,  # Changed from outstanding_events to outstanding_tasks
            "total_people": total_people,
            "total_attendance": total_attendance,
            "growth_rate": round(growth_rate, 1),
            "attendance_breakdown": attendance_breakdown,
            "period": period
        }
    except Exception as e:
        print(f"Error in stats overview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/outstanding-items")
async def get_outstanding_items():
    """Get detailed outstanding cells and tasks for the dashboard"""
    try:
        # Get outstanding cells with details
        outstanding_cells = await events_collection.find({
            "eventType": "Cell",
            "status": {"$nin": ["completed", "closed", "done"]}
        }).to_list(length=None)
        
        # Get outstanding tasks with details
        outstanding_tasks = await tasks_collection.find({
            "status": {"$nin": ["completed", "closed", "done"]}
        }).to_list(length=None)
        
        # Format cells data
        cells_data = []
        for cell in outstanding_cells:
            cells_data.append({
                "name": cell.get("eventLeader", "Unknown Leader"),
                "location": cell.get("location", "Unknown Location"),
                "title": cell.get("eventName", "Untitled Cell"),
                "date": cell.get("date"),
                "status": cell.get("status", "pending")
            })
        
        # Format tasks data
        tasks_data = []
        for task in outstanding_tasks:
            tasks_data.append({
                "name": task.get("assignedTo", task.get("eventLeader", "Unassigned")),
                "email": task.get("email", ""),
                "title": task.get("taskName", task.get("title", "Untitled Task")),
                "count": task.get("priority", 1),  # Using priority as count or you can count tasks per person
                "dueDate": task.get("dueDate", task.get("date")),
                "status": task.get("status", "pending")
            })
        
        return {
            "outstanding_cells": cells_data,
            "outstanding_tasks": tasks_data
        }
        
    except Exception as e:
        print(f"Error in outstanding items: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/people-with-tasks")
async def get_people_capture_stats():
    """
    Get team members and how many people they have captured/recruited
    """
    try:
        client = get_database_client()
        db = client[DB_NAME]
        
        # Count how many people each team member has captured
        pipeline = [
            {
                "$match": {
                    "captured_by": {"$exists": True, "$ne": None}  # Only people who were captured by someone
                }
            },
            {
                "$group": {
                    "_id": "$captured_by",  # Group by the person who captured them
                    "people_captured_count": {"$sum": 1},
                    "captured_people": {
                        "$push": {
                            "name": "$fullName",
                            "email": "$email", 
                            "capture_date": "$created_date"  # or whatever field tracks when
                        }
                    }
                }
            },
            {
                "$lookup": {
                    "from": "people",
                    "localField": "_id",
                    "foreignField": "_id",  # or "email" depending on your schema
                    "as": "capturer_details"
                }
            },
            {
                "$unwind": {
                    "path": "$capturer_details",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "capturer_id": "$_id",
                    "capturer_name": {
                        "$ifNull": ["$capturer_details.fullName", "$capturer_details.name", "Unknown Capturer"]
                    },
                    "capturer_email": {
                        "$ifNull": ["$capturer_details.email", "No email"]
                    },
                    "people_captured_count": 1,
                    "captured_people": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"people_captured_count": -1}  # Sort by most captures first
            }
        ]
        
        results = list(db.people.aggregate(pipeline))  # Query the PEOPLE collection
        
        if not results:
            return {
                "capture_stats": [],
                "total_capturers": 0,
                "total_people_captured": 0,
                "message": "No capture data found"
            }
        
        total_people_captured = sum(item['people_captured_count'] for item in results)
        
        return {
            "capture_stats": results,
            "total_capturers": len(results),
            "total_people_captured": total_people_captured,
            "message": f"Found {len(results)} team members who captured {total_people_captured} people total"
        }
        
    except Exception as e:
        print(f"Error fetching capture stats: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to fetch capture statistics: {str(e)}"
        )
        
        
# --- ROLE MANAGEMENT ENDPOINTS (Admin only) ---

# Role permissions configuration
ROLE_PERMISSIONS = {
    "admin": {
        "manage_users": True,
        "manage_leaders": True,
        "manage_events": True,
        "view_reports": True,
        "system_settings": True
    },
    "leader": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": True,
        "system_settings": False
    },
    "user": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": False,
        "view_reports": False,
        "system_settings": False
    },
    "registrant": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": False,
        "system_settings": False
    }
}

# --- ADMIN ENDPOINTS ---
@app.post("/admin/users", response_model=MessageResponse)
async def create_user(
    user_data: UserCreater,
    current_user: dict = Depends(get_current_user)
):
    """Create a new user - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Check if user already exists
        existing_user = await users_collection.find_one({"email": user_data.email})
        if existing_user:
            raise HTTPException(status_code=400, detail="User with this email already exists")
        
        # Validate role
        if user_data.role not in ["admin", "leader", "user", "registrant"]:
            raise HTTPException(status_code=400, detail="Invalid role")
        
        # Hash password
        from passlib.context import CryptContext
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_password = pwd_context.hash(user_data.password)
        
        # Create user document
        user_doc = {
            "name": user_data.name,
            "surname": user_data.surname,
            "email": user_data.email,
            "password": hashed_password,
            "phone_number": user_data.phone_number,
            "date_of_birth": user_data.date_of_birth,
            "address": user_data.address,
            "gender": user_data.gender,
            "invitedBy": user_data.invitedBy,
            "leader12": user_data.leader12,
            "leader144": user_data.leader144,
            "leader1728": user_data.leader1728,
            "stage": user_data.stage or "Win",
            "role": user_data.role,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # Insert into database
        result = await users_collection.insert_one(user_doc)
        
        # Log activity
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="USER_CREATED",
            details=f"Created new user: {user_data.name} {user_data.surname} ({user_data.role})"
        )
        
        return MessageResponse(message=f"User {user_data.name} {user_data.surname} created successfully")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")

@app.get("/admin/users", response_model=UserList)
async def get_all_users(current_user: dict = Depends(get_current_user)):
    """Get all users - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        users = []
        cursor = users_collection.find({})
        
        async for user in cursor:
            users.append(UserListResponse(
                id=str(user["_id"]),
                name=user.get("name", ""),
                surname=user.get("surname", ""),
                email=user.get("email", ""),
                role=user.get("role", "user"),
                date_of_birth=user.get("date_of_birth"),
                phone_number=user.get("phone_number"),
                address=user.get("address"),
                gender=user.get("gender"),
                invitedBy=user.get("invitedBy"),
                leader12=user.get("leader12"),
                leader144=user.get("leader144"),
                leader1728=user.get("leader1728"),
                stage=user.get("stage"),
                created_at=user.get("created_at")
            ))
        
        return UserList(users=users)
        
    except Exception as e:
        import traceback
        print(f"ERROR: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")

@app.put("/admin/users/{user_id}/role", response_model=MessageResponse)
async def update_user_role(
    user_id: str,
    role_update: RoleUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update user role - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Validate role
        if role_update.role not in ["admin", "leader", "user", "registrant"]:
            raise HTTPException(status_code=400, detail="Invalid role")
        
        # Check if user exists
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        old_role = user.get("role", "user")
        
        # Update role
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {
                "$set": {
                    "role": role_update.role,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=400, detail="Failed to update user role")
        
        # Log activity
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="ROLE_UPDATED",
            details=f"Updated {user.get('name')} {user.get('surname')}'s role from {old_role} to {role_update.role}"
        )
        
        return MessageResponse(message=f"User role updated to {role_update.role}")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating role: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating role: {str(e)}")

@app.delete("/admin/users/{user_id}", response_model=MessageResponse)
async def delete_user(
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a user - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Check if user exists
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Prevent deleting self
        if str(user["_id"]) == str(current_user.get("_id")):
            raise HTTPException(status_code=400, detail="Cannot delete your own account")
        
        user_name = f"{user.get('name')} {user.get('surname')}"
        
        # Delete user
        result = await users_collection.delete_one({"_id": ObjectId(user_id)})
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=400, detail="Failed to delete user")
        
        # Log activity
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="USER_DELETED",
            details=f"Deleted user: {user_name}"
        )
        
        return MessageResponse(message=f"User {user_name} deleted successfully")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting user: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting user: {str(e)}")

@app.put("/admin/roles/{role_name}/permissions", response_model=MessageResponse)
async def update_role_permissions(
    role_name: str,
    permission_update: PermissionUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update role permissions - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Validate role
        if role_name not in ROLE_PERMISSIONS:
            raise HTTPException(status_code=400, detail="Invalid role")
        
        # Update in-memory permissions (in production, store in database)
        ROLE_PERMISSIONS[role_name][permission_update.permission] = permission_update.enabled
        
        # Log activity
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="PERMISSION_UPDATED",
            details=f"Updated {permission_update.permission} for {role_name} role to {permission_update.enabled}"
        )
        
        return MessageResponse(
            message=f"Permission {permission_update.permission} updated for role {role_name}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating permissions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating permissions: {str(e)}")

@app.get("/admin/roles/{role_name}/permissions")
async def get_role_permissions(
    role_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Get role permissions - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    if role_name not in ROLE_PERMISSIONS:
        raise HTTPException(status_code=400, detail="Invalid role")
    
    return {"role": role_name, "permissions": ROLE_PERMISSIONS[role_name]}

# Helper function to log activities
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

@app.get("/admin/activity-logs")
async def get_activity_logs(
    limit: int = 50,
    current_user: dict = Depends(get_current_user)
):
    """Get activity logs - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        logs = []
        cursor = db.activity_logs.find({}).sort("timestamp", -1).limit(limit)
        
        async for log in cursor:
            logs.append({
                "id": str(log["_id"]),
                "action": log.get("action"),
                "details": log.get("details"),
                "timestamp": log.get("timestamp"),
                "user_id": log.get("user_id")
            })
        
        return {"logs": logs}
        
    except Exception as e:
        print(f"Error fetching activity logs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")
