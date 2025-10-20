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
        
        # 🔥 FIX: Properly save event type flags
        event_data["isTicketed"] = event_data.get("isTicketed", False)
        event_data["isGlobal"] = event_data.get("isGlobal", False)
        event_data["hasPersonSteps"] = event_data.get("hasPersonSteps", False)
        
        # 🔥 FIX: Save price tiers for ticketed events
        if event_data.get("isTicketed") and event_data.get("priceTiers"):
            # Ensure price tiers are properly formatted
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
        
        # 🔥 NEW: SMART LEADER ASSIGNMENT FOR CELL EVENTS
        if event_data.get("eventType", "").lower().strip() == "cell":
            leader_name = (event_data.get("eventLeader") or "").strip()
            leader_at_12 = event_data.get("leader12", "").strip()
            leader_at_144 = event_data.get("leader144", "").strip()
            leader_at_1728 = event_data.get("leader1728", "").strip()
            
            print(f"🧠 SMART LEADER ASSIGNMENT for cell event:")
            print(f"   Leader: {leader_name}")
            print(f"   Leader @12: {leader_at_12}")
            print(f"   Leader @144: {leader_at_144}")
            print(f"   Leader @1728: {leader_at_1728}")
            
            # Determine Leader at 1 based on the hierarchy
            leader_at_1 = ""
            
            if leader_at_1728:
                # If we have Leader at 1728, get Leader at 1 from their Leader at 144 -> Leader at 12
                leader_at_1 = await get_leader_at_1_for_leader_at_1728(leader_at_1728)
                print(f"   → Leader @1 from Leader @1728 '{leader_at_1728}': {leader_at_1}")
            elif leader_at_144:
                # If we have Leader at 144, get Leader at 1 from their Leader at 12
                leader_at_1 = await get_leader_at_1_for_leader_at_144(leader_at_144)
                print(f"   → Leader @1 from Leader @144 '{leader_at_144}': {leader_at_1}")
            elif leader_at_12:
                # If we have Leader at 12, determine Leader at 1 based on gender
                leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12)
                print(f"   → Leader @1 from Leader @12 '{leader_at_12}': {leader_at_1}")
            
            # Set the determined Leader at 1
            if leader_at_1:
                event_data["leader1"] = leader_at_1
                print(f"   ✅ FINAL Leader @1 assigned: {leader_at_1}")
            else:
                print(f"   ⚠️  Could not determine Leader @1")
            
            # Also set the leader hierarchy for backward compatibility
            event_data["leaders"] = {
                "1": event_data.get("leader1", ""),
                "12": leader_at_12,
                "144": leader_at_144,
                "1728": leader_at_1728
            }

        # Insert event into database
        result = await events_collection.insert_one(event_data)
        
        print(f"✅ Event created with ID: {result.inserted_id}")
        print(f"   Event Type Flags: isTicketed={event_data.get('isTicketed')}, isGlobal={event_data.get('isGlobal')}, hasPersonSteps={event_data.get('hasPersonSteps')}")
        print(f"   Leader Hierarchy: @1={event_data.get('leader1')}, @12={event_data.get('leader12')}, @144={event_data.get('leader144')}")
        
        return {"message": "Event created", "id": str(result.inserted_id)}

    except Exception as e:
        print(f"❌ Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")


@app.get("/debug/leader-assignment/{leader_name}")
async def debug_leader_assignment(leader_name: str):
    """
    Debug endpoint to test leader assignment logic
    """
    try:
        # Try to find the person by Name (exact match first)
        person = await people_collection.find_one({
            "$or": [
                {"Name": leader_name},  # Exact match
                {"Name": {"$regex": f"^{leader_name}$", "$options": "i"}},  # Case insensitive
                {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, leader_name]}}  # Full name match
            ]
        })
        
        if not person:
            return {"error": f"Person '{leader_name}' not found in people database"}

        person_data = {
            "name": person.get("Name"),
            "surname": person.get("Surname"),
            "gender": person.get("Gender"),
            "leader_1": person.get("Leader @1"),
            "leader_12": person.get("Leader @12"),
            "leader_144": person.get("Leader @144"),
            "leader_1728": person.get("Leader @ 1728")
        }
        
        # Test all leader assignment scenarios
        leader_at_1_from_12 = await get_leader_at_1_for_leader_at_12(leader_name)
        leader_at_1_from_144 = await get_leader_at_1_for_leader_at_144(leader_name)
        leader_at_1_from_1728 = await get_leader_at_1_for_leader_at_1728(leader_name)
        
        return {
            "person_found": person_data,
            "leader_assignment_tests": {
                "as_leader_12": {
                    "result": leader_at_1_from_12,
                    "logic": "Vicky for female, Gavin for male"
                },
                "as_leader_144": {
                    "result": leader_at_1_from_144,
                    "logic": "Get Leader @1 from their Leader @12"
                },
                "as_leader_1728": {
                    "result": leader_at_1_from_1728,
                    "logic": "Get Leader @1 from their Leader @144 -> Leader @12"
                }
            },
            "recommended_leader_at_1": {
                "if_leader_12": leader_at_1_from_12,
                "if_leader_144": leader_at_1_from_144,
                "if_leader_1728": leader_at_1_from_1728
            }
        }
        
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/events")
async def get_events(status: Optional[str] = Query(None, description="Filter events by status")):
    try:
        query = {"isEventType": {"$ne": True}}  # Exclude event types from events

        if status == "open":
            query["status"] = {"$ne": "closed"}
        elif status:
            query = {
                "$or": [
                    {"status": status},
                    {"Status": status},
                    {"did_not_meet": True if status == "did_not_meet" else False}
                ]
            }

        events = []
        cursor = events_collection.find(query).sort("createdAt", -1)
        all_people = await people_collection.find({}).to_list(length=None)

        async for event in cursor:
            event_id = event["_id"]
            event["_id"] = str(event["_id"])

            # Auto-fill leaders for legacy cell events (EXISTING LOGIC - KEEP IT)
            if event.get("eventType", "").lower().strip() == "cell" and not event.get("leaders"):
                # ... existing code ...
                pass

            # Convert datetime fields to ISO strings
            for k, v in event.items():
                if isinstance(v, datetime):
                    event[k] = v.isoformat()

            # 🔥 ENSURE THESE FIELDS ARE RETURNED (they should be auto-included)
            # If not in database, set defaults:
            event.setdefault("isTicketed", False)
            event.setdefault("isGlobal", False)
            event.setdefault("hasPersonSteps", False)
            event.setdefault("priceTiers", [])
            event.setdefault("leader1", "")
            event.setdefault("leader12", "")

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



@app.get("/test/leader12-debug/{email}")
async def test_leader12_debug(email: str):
    """Find all events where a person's Leader @12 appears as Leader at 12 in events"""
    try:
        # Get person record from People collection
        person = await people_collection.find_one({
            "Email": {"$regex": f"^{email}$", "$options": "i"}
        })

        if not person:
            return {"error": f"No person found with email {email}"}

        # Get Leader @12 from person record
        leader_at_12_name = person.get("Leader @12", "").strip()
        
        if not leader_at_12_name:
            return {"error": f"Leader @12 not found for user with email {email}"}

        # Find events where this person is Leader at 12
        matching_events = await events_collection.find({
            "Leader at 12": {"$regex": f".*{leader_at_12_name}.*", "$options": "i"},
            "Event Type": "Cells"
        }).to_list(length=100)

        return {
            "person_email": email,
            "person_name": f"{person.get('Name')} {person.get('Surname')}",
            "leader_at_12_name": leader_at_12_name,
            "total_matching_events": len(matching_events),
            "matching_events": [
                {
                    "event_name": e.get("Event Name"),
                    "leader": e.get("Leader"),
                    "leader_at_12": e.get("Leader at 12"),
                    "day": e.get("Day"),
                    "date": str(e.get("Date Of Event")),
                    "status": e.get("Status")
                }
                for e in matching_events[:10]  # Return first 10 only
            ]
        }

    except Exception as e:
        return {"error": str(e)}



@app.get("/test/show-leader-chain/{email}")
async def show_leader_chain(email: str):
    try:
        person = await people_collection.find_one({
            "Email": {"$regex": f"^{email}$", "$options": "i"}
        })

        if not person:
            return {"error": f"No person found with email {email}"}

        # Direct leaders from the person's record
        levels = {
            "Leader @1": person.get("Leader @1", "").strip() or None,
            "Leader @12": person.get("Leader @12", "").strip() or None,
            "Leader @144": person.get("Leader @144", "").strip() or None,
            "Leader @1728": person.get("Leader @1728", "").strip() or None,
        }

        # Optionally, you can build a chain of leader names if you want to resolve them all recursively,
        # but for now this just shows what is stored directly in the record

        return {
            "email": email,
            "name": f"{person.get('Name')} {person.get('Surname')}",
            "leaders": levels
        }

    except Exception as e:
        return {"error": str(e)}

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
    ✅ FIXED: Correct status detection
    Priority:
    1. Check did_not_meet flag
    2. Check if has attendees AND status indicates it was captured
    3. Default to incomplete
    """
    # Check both field names (case-insensitive)
    status_from_lowercase = (event.get("status") or "").strip().lower()
    status_from_uppercase = (event.get("Status") or "").strip().lower()
    
    # Use whichever is set (prefer 'status' field)
    stored_status = status_from_lowercase or status_from_uppercase
    
    # Check did_not_meet flag (highest priority)
    did_not_meet = event.get("did_not_meet", False)
    
    # Check attendees
    attendees = event.get("attendees", [])
    has_attendees = len(attendees) > 0
    
    print(f"🔍 STATUS DEBUG for {event.get('Event Name', 'Unknown')}:")
    print(f"   - did_not_meet: {did_not_meet}")
    print(f"   - stored_status: '{stored_status}'")
    print(f"   - attendees count: {len(attendees)}")
    
    # Priority 1: Explicitly marked as did_not_meet
    if did_not_meet:
        print(f"🔴 Status: DID NOT MEET (flag is True)")
        return "did_not_meet"
    
    # Priority 2: Only mark as complete if BOTH conditions are true:
    # - Has attendees (was actually captured)
    # - AND status field indicates it's complete/did_not_meet
    if has_attendees and stored_status in ["complete", "closed", "did_not_meet"]:
        print(f"✅ Status: COMPLETE (has attendees and status indicates complete)")
        return "complete"
    
    # Priority 3: If status says complete but no attendees, it's actually incomplete
    if stored_status in ["complete", "closed"] and not has_attendees:
        print(f"⚠️ Status: INCOMPLETE (status says complete but no attendees)")
        return "incomplete"
    
    if stored_status == "did_not_meet":
        print(f"🔴 Status: DID NOT MEET (from status field)")
        return "did_not_meet"
    
    # Default: incomplete
    print(f"⏳ Status: INCOMPLETE (default - no capture data)")
    return "incomplete"

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

@app.get("/debug/event-status/{event_id}")
async def debug_event_status(event_id: str):
    """
    Debug endpoint to check event status details
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        return {
            "event_id": str(event["_id"]),
            "event_name": event.get("Event Name", ""),
            "status_field_lowercase": event.get("status", "NOT SET"),
            "status_field_uppercase": event.get("Status", "NOT SET"),
            "did_not_meet_flag": event.get("did_not_meet", False),
            "attendees_count": len(event.get("attendees", [])),
            "computed_status": get_actual_event_status(event, datetime.now().date()),
            "raw_event_data": {
                k: v for k, v in event.items() 
                if k in ["status", "Status", "did_not_meet", "attendees", "total_attendance"]
            }
        }
    except Exception as e:
        return {"error": str(e)}

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


def parse_time(time_str):
    """Parse time string and return (hour, minute)"""
    if not time_str:
        return 19, 0  # Default to 7:00 PM
    
    try:
        # Handle various time formats
        if ':' in time_str:
            parts = time_str.split(':')
            hour = int(parts[0])
            minute = int(parts[1])
        elif ' ' in time_str:
            # Handle "7 PM" format
            parts = time_str.split()
            hour = int(parts[0])
            if len(parts) > 1 and parts[1].upper() == 'PM' and hour < 12:
                hour += 12
            minute = 0
        else:
            # Assume it's just an hour
            hour = int(time_str)
            minute = 0
            
        return hour, minute
    except:
        return 19, 0  

@app.get("/debug/cell-dates")
async def debug_cell_dates():
    """Debug endpoint to check cell date calculations"""
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day = today.strftime("%A").lower()
        
        print(f"🔍 DEBUG: Today is {today_day} ({today_date})")
        
        # Get a few sample cells
        sample_cells = await events_collection.find({
            "Event Type": "Cells"
        }).limit(10).to_list(length=10)
        
        results = []
        for cell in sample_cells:
            event_obj = build_event_object(cell, timezone, today_date)
            results.append({
                "event_name": cell.get("Event Name"),
                "recurring_day": cell.get("Day"),
                "calculated_date": event_obj["date"],
                "status": event_obj["status"],
                "is_overdue": event_obj["_is_overdue"],
                "today": today_date.isoformat()
            })
        
        return {
            "today": today_date.isoformat(),
            "today_day": today_day,
            "sample_cells": results
        }
        
    except Exception as e:
        return {"error": str(e)}

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
async def get_user_cell_events(current_user: dict = Depends(get_current_user)):
    """
    ✅ FIXED: Shows cells for TODAY'S day of the week (recurring schedule)
    """
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found in token")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day_name = today.strftime("%A").lower()  # "monday"

        logging.info(f"========================================")
        logging.info(f"📅 TODAY: {today_day_name.upper()} ({today_date})")
        logging.info(f"🔍 Fetching cells for {today_day_name}")
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

        # Build query conditions
        query_conditions = [
            {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            {"email": {"$regex": f"^{email}$", "$options": "i"}},
        ]
        
        if user_name:
            query_conditions.extend([
                {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
                {"Leader at 144": {"$regex": f".*{user_name}.*", "$options": "i"}},
            ])
        
        # ✅ CRITICAL: Query cells that RECUR on today's day
        # Don't filter by date - show all cells that happen on this day of the week
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
            event.pop("_event_date", None)
            event.pop("_day_order", None)

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

@app.get("/admin/events/status-counts")
async def get_admin_events_status_counts(
    current_user: dict = Depends(get_current_user),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    search: Optional[str] = Query(None, description="Search by event name or leader")
):
    """Get status counts for events - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Build query
        query = {"Event Type": "Cells"}
        
        # Add event type filter
        if event_type and event_type != 'all':
            query["Event Type"] = event_type
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"Leader": search_regex},
                {"Leader at 12": search_regex}
            ]
        
        # Get all matching events (no pagination for counts)
        cursor = events_collection.find(query)
        events = []
        
        async for event in cursor:
            events.append(event)
        
        # Calculate counts using the same logic as build_event_object
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        for event in events:
            # Use the same status logic as in build_event_object
            did_not_meet = event.get("did_not_meet", False)
            attendees = event.get("attendees", [])
            has_attendees = len(attendees) > 0
            
            if did_not_meet:
                did_not_meet_count += 1
            elif has_attendees:
                complete_count += 1
            else:
                incomplete_count += 1
        
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": len(events)
        }
        
    except Exception as e:
        logging.error(f"Error in status counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/registrant/events/status-counts")
async def get_registrant_events_status_counts(
    current_user: dict = Depends(get_current_user),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    search: Optional[str] = Query(None, description="Search by event name or leader")
):
    """Get status counts for events - Registrant"""
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")
        
        # Build base query
        query = {"Event Type": "Cells"}
        
        # Add event type filter
        if event_type and event_type != 'all':
            query["Event Type"] = event_type
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"Leader": search_regex},
                {"Leader at 12": search_regex}
            ]
        
        # For registrants, only show their events
        query["$or"] = query.get("$or", [])
        query["$or"].append({"Email": {"$regex": f"^{email}$", "$options": "i"}})
        
        # Get all matching events
        cursor = events_collection.find(query)
        events = []
        
        async for event in cursor:
            events.append(event)
        
        # Calculate counts
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        for event in events:
            did_not_meet = event.get("did_not_meet", False)
            attendees = event.get("attendees", [])
            has_attendees = len(attendees) > 0
            
            if did_not_meet:
                did_not_meet_count += 1
            elif has_attendees:
                complete_count += 1
            else:
                incomplete_count += 1
        
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": len(events)
        }
        
    except Exception as e:
        logging.error(f"Error in registrant status counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/status-counts")
async def get_user_events_status_counts(
    current_user: dict = Depends(get_current_user),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    search: Optional[str] = Query(None, description="Search by event name or leader")
):
    """Get status counts for events - Regular user"""
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        # Find user's cell to get their name
        user_cell = await events_collection.find_one({
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        })

        user_name = user_cell.get("Leader", "").strip() if user_cell else ""

        # Build query conditions
        query_conditions = [
            {"Email": {"$regex": f"^{email}$", "$options": "i"}},
            {"email": {"$regex": f"^{email}$", "$options": "i"}},
        ]
        
        if user_name:
            query_conditions.extend([
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f".*{user_name}.*", "$options": "i"}},
            ])
        
        # Base query
        query = {
            "Event Type": "Cells",
            "$or": query_conditions
        }
        
        # Add event type filter
        if event_type and event_type != 'all':
            query["Event Type"] = event_type
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            if "$or" in query:
                # Add to existing OR conditions
                query["$or"].extend([
                    {"Event Name": search_regex},
                    {"Leader": search_regex},
                    {"Leader at 12": search_regex}
                ])
            else:
                query["$or"] = [
                    {"Event Name": search_regex},
                    {"Leader": search_regex},
                    {"Leader at 12": search_regex}
                ]
        
        # Get all matching events
        cursor = events_collection.find(query)
        events = []
        
        async for event in cursor:
            events.append(event)
        
        # Calculate counts
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        for event in events:
            did_not_meet = event.get("did_not_meet", False)
            attendees = event.get("attendees", [])
            has_attendees = len(attendees) > 0
            
            if did_not_meet:
                did_not_meet_count += 1
            elif has_attendees:
                complete_count += 1
            else:
                incomplete_count += 1
        
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": len(events)
        }
        
    except Exception as e:
        logging.error(f"Error in user status counts: {e}")
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

        # 🔥 NEW: Handle price tiers for ticketed events
        if "isTicketed" in update_data:
            if update_data["isTicketed"] and "priceTiers" in update_data:
                # Format price tiers properly
                update_data["priceTiers"] = [
                    {
                        "name": tier.get("name", ""),
                        "price": float(tier.get("price", 0)),
                        "ageGroup": tier.get("ageGroup", ""),
                        "memberType": tier.get("memberType", ""),
                        "paymentMethod": tier.get("paymentMethod", "")
                    }
                    for tier in update_data.get("priceTiers", [])
                ]
            elif not update_data["isTicketed"]:
                # Clear price tiers if not ticketed
                update_data["priceTiers"] = []
        
        # 🔥 NEW: Handle leader hierarchy
        if "hasPersonSteps" in update_data:
            if update_data["hasPersonSteps"]:
                # Keep leader1 and leader12 if provided
                if "leader1" not in update_data:
                    update_data["leader1"] = ""
                if "leader12" not in update_data:
                    update_data["leader12"] = ""
            else:
                # Clear leaders if not personal steps event
                update_data.pop("leader1", None)
                update_data.pop("leader12", None)

        update_data["updated_at"] = datetime.utcnow()

        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            return {"message": "No changes were made to the event", "success": True}

        print(f"✅ Event {event_id} updated successfully")
        if "priceTiers" in update_data:
            print(f"   Price tiers: {len(update_data.get('priceTiers', []))} tiers")
        if "leader1" in update_data or "leader12" in update_data:
            print(f"   Leaders: @1={update_data.get('leader1')}, @12={update_data.get('leader12')}")

        return {"message": "Event updated successfully", "success": True}

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(ve)}")
    except Exception as e:
        print(f"❌ Error updating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating event: {str(e)}")
    

@app.post("/admin/events/bulk-update-leader-at-1")
async def bulk_update_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Bulk update Leader at 1 for all cell events based on Leader at 12 gender"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Find all cell events
        cell_events = await events_collection.find({
            "Event Type": "Cells"
        }).to_list(length=None)
        
        updated_count = 0
        results = []
        
        for event in cell_events:
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
            leader_at_12 = event.get("Leader at 12", "").strip()
            
            # Skip if no Leader at 12
            if not leader_at_12:
                continue
            
            # Get the correct Leader at 1 based on Leader at 12's gender
            leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12)
            
            if leader_at_1:
                # Update the event
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": {"leader1": leader_at_1}}
                )
                updated_count += 1
                results.append({
                    "event_id": str(event_id),
                    "event_name": event_name,
                    "leader_at_12": leader_at_12,
                    "assigned_leader_at_1": leader_at_1
                })
        
        return {
            "message": f"Updated Leader at 1 for {updated_count} events",
            "updated_count": updated_count,
            "results": results[:10]  # Return first 10 results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in bulk update: {str(e)}")


async def build_event_object(event: dict, timezone, today_date: date) -> dict:
    """Build event object with auto-calculated Leader at 1"""
    # ... (previous code remains the same until leader logic)
    
    # 🔥 CORRECTED LEADER HIERARCHY LOGIC
    event_leader_name = event.get("Leader", "").strip()
    leader_at_12_from_event = event.get("Leader at 12", "").strip()
    leader_at_1 = ""
    
    print(f"\n🔍 Processing event: {event_name}")
    print(f"   Event Leader: {event_leader_name}")
    print(f"   Leader at 12 (from event): {leader_at_12_from_event}")
    
    # PRIORITY 1: Use Leader at 12 to determine Leader at 1
    if leader_at_12_from_event:
        print(f"   ✅ Event has Leader at 12 → Looking up in People database")
        leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12_from_event)
        print(f"   → Leader @1 from Leader @12 '{leader_at_12_from_event}': {leader_at_1}")
    
    # If no Leader at 1 was assigned from Leader at 12, try event leader
    if not leader_at_1 and event_leader_name:
        print(f"   ⚠️ No Leader at 1 from Leader at 12 → Checking event leader")
        
        # Don't assign Leader at 1 if event leader is already Gavin/Vicky
        if event_leader_name not in ["Gavin Enslin", "Vicky Enslin"]:
            print(f"   → Event leader '{event_leader_name}' is not Gavin/Vicky, checking if they are Leader at 12")
            
            # Look up event leader in People database
            event_leader_person = await find_person_by_name(event_leader_name)
            
            if event_leader_person:
                person_name = f"{event_leader_person.get('Name', '')} {event_leader_person.get('Surname', '')}".strip()
                person_leader_12 = event_leader_person.get("Leader @12", "").strip()
                person_leader_144 = event_leader_person.get("Leader @144", "").strip()
                person_leader_1728 = event_leader_person.get("Leader @ 1728", "").strip()
                
                print(f"   📋 Found event leader in database: '{person_name}'")
                print(f"      - Their Leader @12: '{person_leader_12}'")
                print(f"      - Their Leader @144: '{person_leader_144}'")
                print(f"      - Their Leader @1728: '{person_leader_1728}'")
                
                # Check if this person IS a Leader at 12 (has NO leaders above them)
                is_leader_at_12 = (
                    not person_leader_12 and 
                    not person_leader_144 and 
                    not person_leader_1728
                )
                
                if is_leader_at_12:
                    print(f"   ✅ Event leader '{person_name}' IS a Leader at 12 (no leaders above them)")
                    # Determine Leader at 1 based on their gender
                    gender = event_leader_person.get("Gender", "").lower().strip()
                    print(f"   → Person's gender: '{gender}'")
                    
                    if gender in ["female", "f", "woman", "lady", "girl"]:
                        leader_at_1 = "Vicky Enslin"
                        print(f"   ✅ Assigned Vicky Enslin (female Leader at 12)")
                    elif gender in ["male", "m", "man", "gentleman", "boy"]:
                        leader_at_1 = "Gavin Enslin"
                        print(f"   ✅ Assigned Gavin Enslin (male Leader at 12)")
                    else:
                        print(f"   ⚠️ Gender '{gender}' not recognized, cannot assign Leader at 1")
                else:
                    print(f"   ❌ Event leader '{person_name}' is NOT a Leader at 12 (has leaders above them)")
            else:
                print(f"   ❌ Event leader '{event_leader_name}' NOT found in People database")
        else:
            print(f"   ⏭️ Event leader is already Gavin/Vicky - no Leader at 1 needed")
    
    print(f"   🎯 FINAL Leader at 1: '{leader_at_1 or 'EMPTY'}'")
    
    return {
        "_id": str(event["_id"]),
        "eventName": event_name,
        "eventType": "Cell",
        "date": event_datetime.isoformat(),
        "location": event.get("Address", ""),
        "status": status,
        "Status": status,
        "eventLeaderName": event_leader_name,
        "eventLeaderEmail": event.get("Email", ""),
        "leader1": leader_at_1,  # ✅ Auto-calculated based on database lookup
        "leader12": leader_at_12_from_event,
        "leader144": event.get("Leader at 144", ""),
        "time": time_str,
        "recurringDays": [recurring_day],
        "day": recurring_day,
        "isVirtual": bool(recurring_day),
        "isTicketed": False,
        "price": 0,
        "description": event_name,
        "attendees": attendees,
        "did_not_meet": did_not_meet,
        "_event_date": event_date,
        "_day_order": get_day_order(recurring_day),
        "_is_overdue": is_overdue,
    }


@app.post("/admin/events/fix-missing-leader-at-1")
async def fix_missing_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Fix missing Leader at 1 for all events"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Find all cell events that have Leader at 12 but no Leader at 1
        cell_events = await events_collection.find({
            "Event Type": "Cells",
            "Leader at 12": {"$exists": True, "$ne": ""},
            "$or": [
                {"leader1": {"$exists": False}},
                {"leader1": ""},
                {"leader1": None}
            ]
        }).to_list(length=None)
        
        updated_count = 0
        results = []
        
        for event in cell_events:
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
            leader_at_12 = event.get("Leader at 12", "").strip()
            
            # Get the correct Leader at 1 using database lookup only
            leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12)
            
            if leader_at_1:
                # Update the event
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": {"leader1": leader_at_1}}
                )
                updated_count += 1
                results.append({
                    "event_name": event_name,
                    "leader_at_12": leader_at_12,
                    "assigned_leader_at_1": leader_at_1
                })
        
        return {
            "message": f"Assigned Leader at 1 for {updated_count} events using database lookup only",
            "updated_count": updated_count,
            "results": results[:10]  # First 10 only
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fixing leaders: {str(e)}")



@app.get("/debug/leader-check/{leader_name}")
async def debug_leader_check(leader_name: str):
    """Debug endpoint to check why a specific leader isn't getting Leader at 1"""
    try:
        print(f"🔍 DEBUG: Checking leader: {leader_name}")
        
        # Try to find the person in People collection
        person = await people_collection.find_one({
            "$or": [
                {"Name": {"$regex": f"^{leader_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{leader_name}$", "$options": "i"}},
                {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, leader_name]}},
            ]
        })
        
        if person:
            gender = person.get("Gender", "").lower().strip()
            person_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            
            print(f"✅ Found person: {person_name}")
            print(f"   Gender: {gender}")
            
            # Determine Leader at 1
            if gender in ["female", "f", "woman", "lady", "girl"]:
                assigned_leader = "Vicky Enslin"
            elif gender in ["male", "m", "man", "gentleman", "boy"]:
                assigned_leader = "Gavin Enslin"
            else:
                assigned_leader = "UNKNOWN GENDER"
            
            return {
                "leader_name": leader_name,
                "found_in_database": True,
                "person_data": {
                    "name": person.get("Name"),
                    "surname": person.get("Surname"),
                    "gender": gender,
                    "leader_12": person.get("Leader @12"),
                    "leader_1": person.get("Leader @1")
                },
                "assigned_leader_at_1": assigned_leader
            }
        else:
            print(f"❌ Person not found in database")
            return {
                "leader_name": leader_name,
                "found_in_database": False,
                "assigned_leader_at_1": "NOT FOUND - CANNOT ASSIGN"
            }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/events/cells")
async def get_admin_cell_events(current_user: dict = Depends(get_current_user)):
    """
    Admin sees ALL cells with their ACTUAL dates and AUTO-ASSIGNED Leader at 1
    """
    try:
        role = current_user.get("role", "")
        if role.lower() != "admin":
            raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()

        query = {"Event Type": "Cells"}
        
        cursor = events_collection.find(query)
        events = []
        seen_cells = set()
        
        async for event in cursor:
            cell_key = (
                event.get("Event Name", "").strip().lower(),
                event.get("Email", "").strip().lower(),
                event.get("Day", "").strip().lower()
            )
            
            if cell_key in seen_cells:
                continue
                
            seen_cells.add(cell_key)

            event_obj = await build_event_object(event, timezone, today_date)
            
            final_event = {
                "_id": event_obj["_id"],
                "eventName": event_obj["eventName"],
                "eventType": event_obj["eventType"],
                "eventLeaderName": event_obj["eventLeaderName"],
                "eventLeaderEmail": event_obj["eventLeaderEmail"],
                "leader1": event_obj["leader1"],
                "leader12": event_obj["leader12"],
                "leader144": event_obj.get("leader144", ""),
                "day": event_obj["day"].capitalize(),
                "date": event_obj["date"],
                "location": event_obj["location"],
                "attendees": event_obj["attendees"],
                "did_not_meet": event_obj["did_not_meet"],
                "status": event_obj["status"],
                "Status": event_obj["Status"],
                "_is_overdue": event_obj["_is_overdue"]
            }

            events.append(final_event)

        day_order = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
                    'Friday': 4, 'Saturday': 5, 'Sunday': 6}
        events.sort(key=lambda x: (day_order.get(x.get('day', ''), 999), x.get('eventLeaderName', '').lower()))

        status_counts = {
            "incomplete": len([e for e in events if e["status"] == "incomplete"]),
            "complete": len([e for e in events if e["status"] == "complete"]),
            "did_not_meet": len([e for e in events if e["status"] == "did_not_meet"])
        }

        return {
            "status": "success",
            "events": events,
            "total_events": len(events),
            "status_counts": status_counts,
            "today": today_date.isoformat(),
            "message": f"Showing {len(events)} unique cells (all days)"
        }

    except Exception as e:
        logging.error(f"Error in admin events: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/events/cells-user")
async def get_user_cell_events(current_user: dict = Depends(get_current_user)):
    """
    User sees ALL their cells with ACTUAL dates and AUTO-ASSIGNED Leader at 1
    """
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()

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
            ])
        
        query = {
            "Event Type": "Cells",
            "$or": query_conditions
        }

        cursor = events_collection.find(query)
        events = []
        seen_cells = set()

        async for event in cursor:
            cell_key = (
                event.get("Event Name", "").strip().lower(),
                event.get("Email", "").strip().lower(), 
                event.get("Day", "").strip().lower()
            )
            
            if cell_key in seen_cells:
                continue
                
            seen_cells.add(cell_key)

            event_obj = await build_event_object(event, timezone, today_date)
            
            final_event = {
                "_id": event_obj["_id"],
                "eventName": event_obj["eventName"],
                "eventType": event_obj["eventType"],
                "eventLeaderName": event_obj["eventLeaderName"],
                "eventLeaderEmail": event_obj["eventLeaderEmail"],
                "leader1": event_obj["leader1"],
                "leader12": event_obj["leader12"],
                "leader144": event_obj.get("leader144", ""),
                "day": event_obj["day"].capitalize(),
                "date": event_obj["date"],
                "location": event_obj["location"],
                "attendees": event_obj["attendees"],
                "did_not_meet": event_obj["did_not_meet"],
                "status": event_obj["status"],
                "Status": event_obj["Status"],
                "_is_overdue": event_obj["_is_overdue"]
            }

            events.append(final_event)

        day_order = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
                    'Friday': 4, 'Saturday': 5, 'Sunday': 6}
        events.sort(key=lambda x: (day_order.get(x.get('day', ''), 999), x.get('eventLeaderName', '').lower()))

        return {
            "status": "success", 
            "events": events,
            "total_events": len(events),
            "today": today_date.isoformat(),
            "user_email": email,
            "message": f"Showing {len(events)} unique cells (all days)"
        }

    except Exception as e:
        logging.error(f"Error in user events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/leader-check/{leader_name}")
async def debug_leader_check(leader_name: str):
    """Debug endpoint to check why a specific leader isn't getting Leader at 1"""
    try:
        print(f"🔍 DEBUG: Checking leader: {leader_name}")
        
        # Clean the name for searching
        cleaned_name = leader_name.strip()
        
        # We need to find the PERSON who IS the leader, not people who HAVE the leader
        search_queries = [
            # The person's own name matches
            {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            # Full name combination matches
            {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
            # First name only (more flexible)
            {"Name": {"$regex": f"^{cleaned_name.split()[0]}$", "$options": "i"}},
        ]
        
        person = None
        search_method = ""
        
        for i, query in enumerate(search_queries):
            found_person = await people_collection.find_one(query)
            if found_person:
                person = found_person
                search_method = f"Strategy {i+1}"
                break
        
        if person:
            gender = person.get("Gender", "").lower().strip()
            person_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            
            print(f"✅ Found THE PERSON: {person_name} using {search_method}")
            print(f"   Gender: {gender}")
            print(f"   Their Leader @12: {person.get('Leader @12')}")
            print(f"   Their Leader @1: {person.get('Leader @1')}")
            
            # Determine Leader at 1 based only on gender from database
            if gender in ["female", "f", "woman", "lady", "girl"]:
                assigned_leader = "Vicky Enslin"
            elif gender in ["male", "m", "man", "gentleman", "boy"]:
                assigned_leader = "Gavin Enslin"
            else:
                assigned_leader = "UNKNOWN GENDER"
            
            return {
                "leader_name": leader_name,
                "found_in_database": True,
                "search_method_used": search_method,
                "person_data": {
                    "name": person.get("Name"),
                    "surname": person.get("Surname"),
                    "gender": gender,
                    "leader_12": person.get("Leader @12"),  # Who leads THEM
                    "leader_1": person.get("Leader @1")     # Who leads their leader
                },
                "assigned_leader_at_1": assigned_leader
            }
        else:
            print(f"❌ Person '{cleaned_name}' not found in database with any search method")
            return {
                "leader_name": leader_name,
                "found_in_database": False,
                "assigned_leader_at_1": "NOT FOUND - CANNOT ASSIGN"
            }
        
    except Exception as e:
        return {"error": str(e)}

# GIVE LEADER AT !" A LEADER AT "
async def get_leader_at_1_for_leader_at_12(leader_at_12_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 12.
    FIXED VERSION - CORRECT query order
    """
    if not leader_at_12_name:
        return ""
    
    print(f"🔍 Getting Leader at 1 for Leader @12: {leader_at_12_name}")
    
    cleaned_name = leader_at_12_name.strip().title()
    
    # FIXED: Correct query order - find the ACTUAL leader first
    search_queries = [
        # FIRST: Try full name match (Name + Surname)
        {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
        # SECOND: Try exact name match
        {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
        # LAST: Try Leader @12 field (this finds followers, not the leader)
    ]
    
    person = None
    for i, query in enumerate(search_queries):
        found_person = await people_collection.find_one(query)
        if found_person:
            person = found_person
            print(f"✅ Found using query {i+1}")
            break
    
    if person:
        gender = person.get("Gender", "").lower().strip()
        person_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
        
        print(f"✅ Found THE LEADER: {person_name}")
        print(f"   Gender: {gender}")
        
        # Assign Leader at 1 based on gender
        if gender in ["female", "f", "woman", "lady", "girl"]:
            print(f"   → Assigning Vicky Enslin")
            return "Vicky Enslin"
        elif gender in ["male", "m", "man", "gentleman", "boy"]:
            print(f"   → Assigning Gavin Enslin")
            return "Gavin Enslin"
        else:
            print(f"   → Gender unknown, cannot assign")
            return ""
    
    print(f"❌ Leader '{cleaned_name}' not found in People database")
    return ""

async def find_person_by_name(name: str):
    """
    Enhanced person finder that tries multiple search strategies
    """
    cleaned_name = name.strip().title()
    
    search_strategies = [
        # Strategy 1: Exact name match
        {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
        
        # Strategy 2: Leader @12 field match
        {"Leader @12": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
        
        # Strategy 3: Full name match (Name + Surname)
        {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
        
        # Strategy 4: First name only
        {"Name": {"$regex": f"^{cleaned_name.split()[0]}$", "$options": "i"}},
        
        # Strategy 5: Partial match in any name field
        {"$or": [
            {"Name": {"$regex": cleaned_name.split()[0], "$options": "i"}},
            {"Surname": {"$regex": cleaned_name.split()[0], "$options": "i"}},
            {"Leader @12": {"$regex": cleaned_name.split()[0], "$options": "i"}}
        ]}
    ]
    
    for strategy in search_strategies:
        person = await people_collection.find_one(strategy)
        if person:
            return person
    
    return None

async def get_leader_at_1_for_leader_at_144(leader_at_144_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 144.
    This should come from their Leader at 12
    """
    if not leader_at_144_name:
        return ""
    
    print(f"🔍 Getting Leader at 1 for Leader @144: {leader_at_144_name}")
    
    # FIRST: Try to find the person by Name (their own record)
    person = await people_collection.find_one({
        "$or": [
            {"Name": {"$regex": f"^{leader_at_144_name}$", "$options": "i"}},
            {"Name": leader_at_144_name}  # Exact match
        ]
    })
    
    if person and person.get("Leader @12"):
        # Get the Leader at 12's name and determine their Leader at 1
        leader_at_12_name = person.get("Leader @12")
        print(f"🎯 Leader @144 {leader_at_144_name} has Leader @12: {leader_at_12_name}")
        return await get_leader_at_1_for_leader_at_12(leader_at_12_name)
    
    print(f"⚠️ Could not find Leader @12 for Leader @144: {leader_at_144_name}")
    return ""

async def get_leader_at_1_for_leader_at_1728(leader_at_1728_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 1728.
    This should come from their Leader at 144 -> Leader at 12
    """
    if not leader_at_1728_name:
        return ""
    
    print(f"🔍 Getting Leader at 1 for Leader @1728: {leader_at_1728_name}")
    
    # FIRST: Try to find the person by Name (their own record)
    person = await people_collection.find_one({
        "$or": [
            {"Name": {"$regex": f"^{leader_at_1728_name}$", "$options": "i"}},
            {"Name": leader_at_1728_name}  # Exact match
        ]
    })
    
    if person and person.get("Leader @144"):
        # Get the Leader at 144's name and determine their Leader at 1
        leader_at_144_name = person.get("Leader @144")
        print(f"🎯 Leader @1728 {leader_at_1728_name} has Leader @144: {leader_at_144_name}")
        return await get_leader_at_1_for_leader_at_144(leader_at_144_name)
    
    print(f"⚠️ Could not find Leader @144 for Leader @1728: {leader_at_1728_name}")
    return ""

@app.get("/current-user/leader-at-1")
async def get_current_user_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Get the current user's recommended Leader at 1"""
    try:
        user_name = current_user.get("name", "").strip()
        user_email = current_user.get("email", "").strip()
        
        print(f"🔍 Getting Leader at 1 for user: {user_name} ({user_email})")
        
        if not user_name and not user_email:
            print("❌ No user name or email found in token")
            return {"leader_at_1": ""}
        
        # Extract username part from email for fuzzy matching
        email_username = ""
        if user_email and "@" in user_email:
            email_username = user_email.split("@")[0]
            print(f"📧 Email username part: {email_username}")
        
        # Build flexible search query
        query_conditions = []
        
        if user_name:
            query_conditions.append({"Name": {"$regex": f"^{user_name}$", "$options": "i"}})
        
        if user_email:
            # Exact email match
            query_conditions.append({"Email": {"$regex": f"^{user_email}$", "$options": "i"}})
            
            # Fuzzy email matching for common typos
            if email_username:
                # Match same username with any domain
                query_conditions.append({"Email": {"$regex": f"^{email_username}.*@", "$options": "i"}})
                # Match similar usernames (handles character substitutions like 1/l, 0/O)
                query_conditions.append({"Email": {"$regex": f"tkgenia.*@", "$options": "i"}})
        
        # Also search by name if we have it
        if user_name:
            query_conditions.append({"Name": {"$regex": f"^{user_name}$", "$options": "i"}})
        
        if not query_conditions:
            print("❌ No search conditions available")
            return {"leader_at_1": ""}
        
        query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
        
        print(f"🔍 Search query: {query}")
        
        # Try to find the user in people collection
        person = await people_collection.find_one(query)
        
        if not person:
            print(f"❌ User not found in people database with any search criteria")
            # Try one more fallback: search by partial name match
            if user_name:
                fallback_person = await people_collection.find_one({
                    "Name": {"$regex": user_name, "$options": "i"}
                })
                if fallback_person:
                    print(f"✅ Found user with fallback search: {fallback_person.get('Name')}")
                    person = fallback_person
        
        if not person:
            return {"leader_at_1": ""}
        
        print(f"✅ Found user in people database: {person.get('Name')} {person.get('Surname', '')}")
        print(f"📊 User data - Leader @12: {person.get('Leader @12')}, Leader @144: {person.get('Leader @144')}, Leader @1728: {person.get('Leader @ 1728')}")
        
        # Get Leader at 1 based on the user's position in hierarchy
        leader_at_1 = ""
        
        # Check if user is a Leader at 12
        if person.get("Leader @12"):
            print(f"🎯 User {person.get('Name')} is a Leader @12")
            leader_at_1 = await get_leader_at_1_for_leader_at_12(person.get("Name"))
        # Check if user is a Leader at 144  
        elif person.get("Leader @144"):
            print(f"🎯 User {person.get('Name')} is a Leader @144")
            leader_at_1 = await get_leader_at_1_for_leader_at_144(person.get("Name"))
        # Check if user is a Leader at 1728
        elif person.get("Leader @ 1728"):
            print(f"🎯 User {person.get('Name')} is a Leader @1728")
            leader_at_1 = await get_leader_at_1_for_leader_at_1728(person.get("Name"))
        else:
            print(f"ℹ️ User {person.get('Name')} has no leadership position")
        
        print(f"✅ Recommended Leader at 1 for {person.get('Name')}: {leader_at_1}")
        return {"leader_at_1": leader_at_1}
        
    except Exception as e:
        print(f"❌ Error getting current user leader at 1: {e}")
        return {"leader_at_1": ""}
    


@app.get("/debug/leader-gender/{leader_name}")
async def debug_leader_gender(leader_name: str):
    """
    Debug endpoint to check gender detection for a specific leader
    """
    try:
        # Find the person in people collection
        person = await people_collection.find_one({
            "$or": [
                {"Name": {"$regex": f"^{leader_name}$", "$options": "i"}},
                {"Name": leader_name}
            ]
        })
        
        if not person:
            return {
                "leader_name": leader_name,
                "found_in_database": False,
                "error": "Person not found in people database"
            }
        
        gender = person.get("Gender", "").lower().strip()
        leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_name)
        
        return {
            "leader_name": leader_name,
            "found_in_database": True,
            "person_data": {
                "name": person.get("Name"),
                "surname": person.get("Surname"),
                "gender": gender,
                "leader_12": person.get("Leader @12"),
                "leader_144": person.get("Leader @144"),
                "leader_1728": person.get("Leader @ 1728")
            },
            "assigned_leader_at_1": leader_at_1,
            "gender_detection": {
                "raw_gender": gender,
                "is_female": gender in ["female", "f", "woman", "lady", "girl"],
                "is_male": gender in ["male", "m", "man", "gentleman", "boy"]
            }
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/find-exact-leader/{leader_name}")
async def debug_find_exact_leader(leader_name: str):
    """Find the exact leader in People database"""
    try:
        print(f"🔍 DEBUG: Finding EXACT leader: {leader_name}")
        
        cleaned_name = leader_name.strip()
        
        # Try exact matching
        exact_person = await people_collection.find_one({
            "$or": [
                {"Name": cleaned_name},
                {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
            ]
        })
        
        if exact_person:
            return {
                "leader_name": leader_name,
                "found": True,
                "exact_match": True,
                "person_data": {
                    "name": exact_person.get("Name"),
                    "surname": exact_person.get("Surname"),
                    "gender": exact_person.get("Gender"),
                    "leader_12": exact_person.get("Leader @12"),
                    "leader_1": exact_person.get("Leader @1")
                }
            }
        
        # Try partial matching
        first_name = cleaned_name.split()[0]
        partial_person = await people_collection.find_one({
            "Name": first_name
        })
        
        if partial_person:
            return {
                "leader_name": leader_name,
                "found": True,
                "exact_match": False,
                "match_type": "first_name_only",
                "person_data": {
                    "name": partial_person.get("Name"),
                    "surname": partial_person.get("Surname"),
                    "gender": partial_person.get("Gender"),
                    "leader_12": partial_person.get("Leader @12"),
                    "leader_1": partial_person.get("Leader @1")
                }
            }
        
        return {
            "leader_name": leader_name,
            "found": False,
            "exact_match": False
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/test-leader-assignment/{leader_name}")
async def debug_test_leader_assignment(leader_name: str):
    """Test the actual leader assignment logic used in build_event_object"""
    try:
        print(f"🔍 TESTING ACTUAL LOGIC for: {leader_name}")
        
        # This is the EXACT logic from build_event_object
        leader_at_1 = ""
        
        if leader_name:
            print(f"   ✅ Leader at 12 provided: {leader_name}")
            leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_name)
            print(f"   → Leader @1 assigned: {leader_at_1}")
        
        return {
            "leader_at_12_input": leader_name,
            "leader_at_1_output": leader_at_1,
            "success": bool(leader_at_1)
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/debug-search/{leader_name}")
async def debug_debug_search(leader_name: str):
    """Debug the exact search logic in get_leader_at_1_for_leader_at_12"""
    try:
        print(f"🔍 DEBUGGING SEARCH for: {leader_name}")
        
        # This is the EXACT logic from get_leader_at_1_for_leader_at_12
        cleaned_name = leader_name.strip().title()
        
        search_queries = [
            {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            {"Leader @12": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
        ]
        
        results = []
        for i, query in enumerate(search_queries):
            person = await people_collection.find_one(query)
            if person:
                results.append({
                    "query_number": i+1,
                    "query": str(query),
                    "found": True,
                    "person_found": f"{person.get('Name')} {person.get('Surname')}",
                    "gender": person.get('Gender'),
                    "leader_12": person.get('Leader @12')
                })
            else:
                results.append({
                    "query_number": i+1, 
                    "query": str(query),
                    "found": False
                })
        
        return {
            "leader_name": leader_name,
            "cleaned_name": cleaned_name,
            "search_results": results,
            "any_found": any(r["found"] for r in results)
        }
        
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/debug/get-current-function")
async def debug_get_current_function():
    """Check what the current get_leader_at_1_for_leader_at_12 function looks like"""
    # This will help us see if the function was updated correctly
    return {
        "function_exists": True,
        "note": "Check your backend code to see the actual implementation of get_leader_at_1_for_leader_at_12"
    }


@app.get("/debug/find-user")
async def debug_find_user(current_user: dict = Depends(get_current_user)):
    """Debug endpoint to test user finding logic"""
    try:
        user_name = current_user.get("name", "").strip()
        user_email = current_user.get("email", "").strip()
        
        results = {
            "token_data": {"name": user_name, "email": user_email},
            "search_attempts": []
        }
        
        # Try different search strategies
        search_strategies = [
            {"strategy": "exact_email", "query": {"Email": {"$regex": f"^{user_email}$", "$options": "i"}}},
            {"strategy": "exact_name", "query": {"Name": {"$regex": f"^{user_name}$", "$options": "i"}}},
        ]
        
        if user_email and "@" in user_email:
            email_user = user_email.split("@")[0]
            search_strategies.extend([
                {"strategy": "fuzzy_email_username", "query": {"Email": {"$regex": f"^{email_user}.*@", "$options": "i"}}},
                {"strategy": "similar_emails", "query": {"Email": {"$regex": f"tkgenia.*@", "$options": "i"}}}
            ])
        
        for strategy in search_strategies:
            person = await people_collection.find_one(strategy["query"])
            results["search_attempts"].append({
                "strategy": strategy["strategy"],
                "query": strategy["query"],
                "found": bool(person),
                "person": {
                    "name": person.get("Name") if person else None,
                    "email": person.get("Email") if person else None,
                    "leader_12": person.get("Leader @12") if person else None
                } if person else None
            })
        
        return results
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/current-user-info")
async def debug_current_user_info(current_user: dict = Depends(get_current_user)):
    """Debug endpoint to see current user information"""
    try:
        user_name = current_user.get("name", "").strip()
        user_email = current_user.get("email", "").strip()
        user_id = current_user.get("user_id", "")
        
        # Search for user in people collection
        person_by_name = await people_collection.find_one({
            "Name": {"$regex": f"^{user_name}$", "$options": "i"}
        })
        
        person_by_email = await people_collection.find_one({
            "Email": {"$regex": f"^{user_email}$", "$options": "i"}
        })
        
        return {
            "token_data": {
                "user_id": user_id,
                "name": user_name,
                "email": user_email,
                "role": current_user.get("role")
            },
            "search_results": {
                "by_name": {
                    "found": bool(person_by_name),
                    "name": person_by_name.get("Name") if person_by_name else None,
                    "email": person_by_name.get("Email") if person_by_name else None,
                    "leader_12": person_by_name.get("Leader @12") if person_by_name else None
                },
                "by_email": {
                    "found": bool(person_by_email),
                    "name": person_by_email.get("Name") if person_by_email else None,
                    "email": person_by_email.get("Email") if person_by_email else None,
                    "leader_12": person_by_email.get("Leader @12") if person_by_email else None
                }
            }
        }
        
    except Exception as e:
        return {"error": str(e)}

async def get_leader_at_1_for_leader_at_1728(leader_at_1728_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 1728.
    This should come from their Leader at 144 -> Leader at 12
    """
    if not leader_at_1728_name:
        return ""
    
    # FIRST: Try to find the person by Name (their own record)
    person = await people_collection.find_one({
        "$or": [
            {"Name": {"$regex": f"^{leader_at_1728_name}$", "$options": "i"}},
            {"Name": leader_at_1728_name}  # Exact match
        ]
    })
    
    if person and person.get("Leader @144"):
        # Get the Leader at 144's name and determine their Leader at 1
        leader_at_144_name = person.get("Leader @144")
        return await get_leader_at_1_for_leader_at_144(leader_at_144_name)
    
    return ""

def calculate_next_occurrence(recurring_day: str, from_date: date) -> date:
    """
    Calculate the next occurrence of a recurring event based on the day of week
    Returns the date when this cell should next occur
    """
    if not recurring_day:
        return from_date
    
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    target_weekday = day_map.get(recurring_day.lower(), from_date.weekday())
    current_weekday = from_date.weekday()
    
    # Calculate days until next occurrence
    days_ahead = target_weekday - current_weekday
    if days_ahead < 0:  # Target day has passed this week
        days_ahead += 7
    
    return from_date + timedelta(days=days_ahead)

@app.get("/debug/cell-status/{event_id}")
async def debug_cell_status(event_id: str):
    """
    Debug endpoint to check why a cell shows as complete/incomplete
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        attendees = event.get("attendees", [])
        status_lower = event.get("status", "")
        status_upper = event.get("Status", "")
        did_not_meet = event.get("did_not_meet", False)
        
        return {
            "event_name": event.get("Event Name", ""),
            "event_id": str(event["_id"]),
            "status_field_lowercase": status_lower,
            "status_field_uppercase": status_upper,
            "did_not_meet_flag": did_not_meet,
            "attendees_count": len(attendees),
            "attendees": attendees[:5] if attendees else [],  # First 5 only
            "computed_status": get_actual_event_status(event, datetime.now().date()),
            "diagnosis": {
                "has_attendees": len(attendees) > 0,
                "has_status_field": bool(status_lower or status_upper),
                "is_did_not_meet": did_not_meet,
                "why_complete": "Has attendees" if len(attendees) > 0 else "No attendees but status field says complete"
            }
        }
    except Exception as e:
        return {"error": str(e)}

# ===== FIX: submit_attendance - ensure both fields updated =====

@app.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: AttendanceSubmission = Body(...)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        event_name = event.get("Event Name", "Unknown")
        
        print(f"🎯 SUBMIT ATTENDANCE for: {event_name}")
        print(f"📦 Submission data:")
        print(f"   - did_not_meet: {submission.did_not_meet}")
        print(f"   - attendees count: {len(submission.attendees)}")
        print(f"   - isTicketed: {submission.isTicketed}")
        
        if submission.did_not_meet:
            update_data = {
                "Status": "Did Not Meet",           
                "status": "did_not_meet",           
                "did_not_meet": True,              
                "attendees": [],                 
                "total_attendance": 0,             
                "Date Captured": datetime.now().strftime("%d %B %Y"),  
                "updated_at": datetime.utcnow()
            }
            
            print(f" MARKING AS DID NOT MEET: {event_name}")
            
        else:
            attendees_list = []
            for att in submission.attendees:
                attendee_data = {
                    "id": att.id,
                    "name": att.name or att.fullName,
                    "fullName": att.fullName or att.name,
                    "leader12": att.leader12,
                    "leader144": att.leader144,
                    "time": att.time,
                    "email": att.email,
                    "phone": att.phone,
                    "decision": att.decision
                }
                # Add ticketed fields if applicable
                if submission.isTicketed:
                    attendee_data.update({
                        "priceTier": att.priceTier,
                        "price": att.price,
                        "ageGroup": att.ageGroup,
                        "memberType": att.memberType,
                        "paymentMethod": att.paymentMethod,
                        "paid": att.paid,
                        "owing": att.owing
                    })
                attendees_list.append(attendee_data)
            
            update_data = {
                "Status": "Complete",               # ✅ Capital S (like your existing data)
                "status": "complete",               # ✅ Also set lowercase
                "did_not_meet": False,              # ✅ Set the flag
                "attendees": attendees_list,
                "total_attendance": len(attendees_list),
                "Date Captured": datetime.now().strftime("%d %B %Y"),  # ✅ Set capture date
                "updated_at": datetime.utcnow()
            }
            
            print(f"✅ MARKING AS COMPLETE: {event_name} with {len(attendees_list)} attendees")
        
        print(f"📤 Update data to save: {update_data}")
        
        # UPDATE THE DATABASE
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        print(f"📝 Database update result: matched={result.matched_count}, modified={result.modified_count}")
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
        
        # ✅ Verify the update worked
        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        print(f"✅ DATABASE VERIFICATION for {event_name}:")
        print(f"   - Status: '{updated_event.get('Status')}'")
        print(f"   - status: '{updated_event.get('status')}'")
        print(f"   - did_not_meet: {updated_event.get('did_not_meet')}")
        print(f"   - attendees count: {len(updated_event.get('attendees', []))}")
        
        return {
            "message": "Success",
            "event_id": str(event["_id"]),
            "status": "did_not_meet" if submission.did_not_meet else "complete",
            "did_not_meet": submission.did_not_meet,
            "total_attendance": 0 if submission.did_not_meet else len(attendees_list),
            "success": True
        }
        
    except Exception as e:
        print(f"❌ Error in submit_attendance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/debug/event/{event_id}")
async def debug_event_status(event_id: str):
    """Debug endpoint to check event status"""
    try:
        if not ObjectId.is_valid(event_id):
            return {"error": "Invalid event ID"}
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            return {"error": "Event not found"}
        
        return {
            "event_id": str(event["_id"]),
            "event_name": event.get("Event Name"),
            "did_not_meet": event.get("did_not_meet"),
            "status_field": event.get("status"),
            "Status_field": event.get("Status"),
            "attendees_count": len(event.get("attendees", [])),
            "attendees": event.get("attendees", []),
            "all_fields": {k: v for k, v in event.items() if k not in ['_id']}
        }
    except Exception as e:
        return {"error": str(e)}

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
        
        # 🔥 ENSURE NEW FIELDS ARE RETURNED
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

@app.post("/admin/events/assign-leaders")
async def bulk_assign_leaders(current_user: dict = Depends(get_current_user)):
    """
    Bulk assign Leader at 1 for all existing cell events
    Admin only
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Find all cell events without Leader at 1
        cell_events = await events_collection.find({
            "eventType": "cell",
            "$or": [
                {"leader1": {"$exists": False}},
                {"leader1": ""},
                {"leader1": None}
            ]
        }).to_list(length=None)
        
        updated_count = 0
        results = []
        
        for event in cell_events:
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
            leader_at_12 = event.get("Leader at 12", "").strip()
            leader_at_144 = event.get("Leader at 144", "").strip()
            
            leader_at_1 = ""
            
            if leader_at_144:
                leader_at_1 = await get_leader_at_1_for_leader_at_144(leader_at_144)
            elif leader_at_12:
                leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_at_12)
            
            if leader_at_1:
                # Update the event
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": {"leader1": leader_at_1}}
                )
                updated_count += 1
                results.append({
                    "event_id": str(event_id),
                    "event_name": event_name,
                    "leader_at_12": leader_at_12,
                    "leader_at_144": leader_at_144,
                    "assigned_leader_at_1": leader_at_1
                })
        
        return {
            "message": f"Assigned Leader at 1 for {updated_count} events",
            "updated_count": updated_count,
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in bulk assignment: {str(e)}")

def calculate_next_occurrence(recurring_day: str, from_date: date) -> date:
    """
    Calculate the next occurrence of a recurring event based on the day of week
    """
    if not recurring_day:
        return from_date
    
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    target_weekday = day_map.get(recurring_day.lower(), from_date.weekday())
    current_weekday = from_date.weekday()
    
    # Calculate days until next occurrence
    days_ahead = target_weekday - current_weekday
    if days_ahead <= 0:  # Target day is today or has passed this week
        days_ahead += 7
    
    return from_date + timedelta(days=days_ahead)

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
#  END OF EVENTS


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