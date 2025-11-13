import os
from datetime import datetime, timedelta, time, date
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from auth.models import EventCreate,DecisionType, UserProfile, ConsolidationCreate, UserProfileUpdate, CheckIn, UncaptureRequest, UserCreate,UserCreater,  UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel, PersonCreate, EventTypeCreate, UserListResponse, UserList, MessageResponse, PermissionUpdate, RoleUpdate, AttendanceSubmission, TaskUpdate, EventUpdate ,TaskTypeIn ,TaskTypeOut , LeaderStatusResponse
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token , task_type_serializer
import math
import secrets
from database import db, events_collection, people_collection, users_collection, tasks_collection ,tasktypes_collection
from auth.email_utils import send_reset_email
from typing import Optional, Literal, List, Any, Dict, Optional
from collections import Counter
from auth.utils import get_current_user  
from auth.models import UserProfile, AttendanceSubmission
import logging
import pytz
import base64
import uuid
from fastapi.security import HTTPBearer
oauth2_scheme = HTTPBearer()
from passlib.context import CryptContext
import json
from fastapi import Request
from urllib.parse import unquote
from fastapi import Depends, Query, HTTPException, File, UploadFile
import pytz
import time
import traceback

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://localhost:5173",  # Vite dev server
        "https://new-active-teams.netlify.app",
        "https://activeteams.netlify.app",
        "https://activeteamsbackend2.0.onrender.com"  # Your render backend itself
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=[
        "Authorization", 
        "Content-Type", 
        "Accept",
        "Origin", 
        "X-Requested-With",
        "Access-Control-Allow-Origin"
    ],
    expose_headers=["*"],
    max_age=3600,
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

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth")



oauth2_scheme = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))

from typing import Dict, List, Optional
import asyncio
from datetime import datetime, timedelta
import time

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

CACHE_DURATION_MINUTES = 1440  # 24 hours - since we're loading everything
BACKGROUND_LOAD_DELAY = 2  # seconds before starting background load

@app.on_event("startup")
async def startup_event():
    """Start background loading of all people on startup"""
    print("🚀 Starting background load of ALL people...")
    asyncio.create_task(background_load_all_people())

async def background_load_all_people():
    """Background task to load ALL people from the database"""
    try:
        # Small delay to ensure app is fully started
        await asyncio.sleep(BACKGROUND_LOAD_DELAY)
        
        if people_cache["is_loading"]:
            return
            
        people_cache["is_loading"] = True
        people_cache["last_error"] = None
        start_time = time.time()
        
        print("🔄 BACKGROUND: Starting to load ALL people...")
        
        all_people_data = []
        total_count = await people_collection.count_documents({})
        people_cache["total_in_database"] = total_count
        print(f"📊 BACKGROUND: Total people in database: {total_count}")
        
        # Load in large batches for efficiency
        batch_size = 5000
        page = 1
        total_loaded = 0
        
        while True:
            try:
                skip = (page - 1) * batch_size
                
                # Minimal projection for signup form
                projection = {
                    "_id": 1,
                    "Name": 1,
                    "Surname": 1,
                    "Email": 1,
                    "Number": 1,
                    "Leader @1": 1,
                    "Leader @12": 1,
                    "Leader @144": 1,
                    "Leader @1728": 1
                }
                
                cursor = people_collection.find({}, projection).skip(skip).limit(batch_size)
                batch_data = await cursor.to_list(length=batch_size)
                
                if not batch_data:
                    break
                
                # Transform batch data
                transformed_batch = []
                for person in batch_data:
                    transformed_batch.append({
                        "_id": str(person["_id"]),
                        "Name": person.get("Name", ""),
                        "Surname": person.get("Surname", ""),
                        "Email": person.get("Email", ""),
                        "Number": person.get("Number", ""),
                        "Leader @1": person.get("Leader @1", ""),
                        "Leader @12": person.get("Leader @12", ""),
                        "Leader @144": person.get("Leader @144", ""),
                        "Leader @1728": person.get("Leader @1728", ""),
                        "FullName": f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
                    })
                
                all_people_data.extend(transformed_batch)
                total_loaded += len(transformed_batch)
                
                # Update progress
                progress = (total_loaded / total_count) * 100 if total_count > 0 else 100
                people_cache["load_progress"] = round(progress, 1)
                people_cache["total_loaded"] = total_loaded
                
                print(f"📦 BACKGROUND: Batch {page} - {len(transformed_batch)} people (Total: {total_loaded}/{total_count}, Progress: {progress:.1f}%)")
                
                page += 1
                
                # Small delay to prevent overwhelming the database
                await asyncio.sleep(0.1)
                
            except Exception as batch_error:
                print(f"❌ BACKGROUND: Error in batch {page}: {str(batch_error)}")
                break
        
        # Update cache with complete dataset
        people_cache["data"] = all_people_data
        people_cache["last_updated"] = datetime.utcnow().isoformat()
        people_cache["expires_at"] = (datetime.utcnow() + timedelta(minutes=CACHE_DURATION_MINUTES)).isoformat()
        people_cache["is_loading"] = False
        people_cache["load_progress"] = 100
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ BACKGROUND: Successfully loaded ALL {len(all_people_data)} people in {duration:.2f} seconds")
        print(f"💾 BACKGROUND: Cache ready with {len(all_people_data)} people")
        
    except Exception as e:
        people_cache["is_loading"] = False
        people_cache["last_error"] = str(e)
        print(f"💥 BACKGROUND: Failed to load people: {str(e)}")

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
            
            print(f"✅ CACHE HIT: Returning {len(people_cache['data'])} people")
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
            print("🔄 Cache empty, triggering background load...")
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
            print("🔄 Cache expired, returning stale data while refreshing...")
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
        print(f"❌ Error in cache endpoint: {str(e)}")
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
        # Don't reset existing data - just trigger background refresh
        if not people_cache["is_loading"]:
            print("🔄 Manual cache refresh triggered")
            asyncio.create_task(background_load_all_people())
            
        return {
            "success": True,
            "message": "Cache refresh triggered",
            "is_loading": people_cache["is_loading"],
            "current_progress": people_cache["load_progress"]
        }
        
    except Exception as e:
        print(f"❌ Error refreshing cache: {str(e)}")
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

# Update the signup endpoint to use the background-loaded cache
@app.post("/signup")
async def signup(user: UserCreate):
    logger.info(f"Signup attempt: {user.email}")
    
    # Normalize email
    email = user.email.lower().strip()
    
    # Check if user already exists in Users collection ONLY
    existing = await db["Users"].find_one({"email": email})
    if existing:
        logger.warning(f"Signup failed - email already registered: {email}")
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash password
    hashed = hash_password(user.password)
    
    # Create user document
    user_dict = {
        "name": user.name,
        "surname": user.surname,
        "date_of_birth": user.date_of_birth,
        "home_address": user.home_address,
        "invited_by": user.invited_by,
        "phone_number": user.phone_number,
        "email": email,
        "gender": user.gender,
        "password": hashed,
        "confirm_password": hashed,
        "role": "user",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    # Insert user into Users collection
    user_result = await db["Users"].insert_one(user_dict)
    logger.info(f"User created successfully: {email}")
    
    # ✅ USE BACKGROUND-LOADED CACHE FOR LEADER ASSIGNMENT
    inviter_full_name = user.invited_by.strip()
    leader1 = ""
    leader12 = ""
    leader144 = ""
    leader1728 = ""
    
    if inviter_full_name:
        print(f"🔍 Looking for inviter in background cache: '{inviter_full_name}'")
        
        # Search in background-loaded cache (contains ALL people)
        cached_inviter = None
        for person in people_cache["data"]:
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            if (full_name.lower() == inviter_full_name.lower() or 
                person.get('Name', '').lower() == inviter_full_name.lower()):
                cached_inviter = person
                break
        
        if cached_inviter:
            print(f"✅ Found inviter in background cache: {cached_inviter.get('FullName')}")
            
            # Get the inviter's leader hierarchy from cache
            inviter_leader1 = cached_inviter.get("Leader @1", "")
            inviter_leader12 = cached_inviter.get("Leader @12", "")
            inviter_leader144 = cached_inviter.get("Leader @144", "")
            inviter_leader1728 = cached_inviter.get("Leader @1728", "")
            
            # Determine what level the inviter is at and set leaders accordingly
            if inviter_leader1728:
                leader1 = inviter_leader1
                leader12 = inviter_leader12
                leader144 = inviter_leader144
                leader1728 = inviter_full_name
            elif inviter_leader144:
                leader1 = inviter_leader1
                leader12 = inviter_leader12
                leader144 = inviter_full_name
                leader1728 = ""
            elif inviter_leader12:
                leader1 = inviter_leader1
                leader12 = inviter_full_name
                leader144 = ""
                leader1728 = ""
            elif inviter_leader1:
                leader1 = inviter_full_name
                leader12 = ""
                leader144 = ""
                leader1728 = ""
            else:
                leader1 = inviter_full_name
                leader12 = ""
                leader144 = ""
                leader1728 = ""
            
            logger.info(f"Leader hierarchy set for {email}: L1={leader1}, L12={leader12}, L144={leader144}, L1728={leader1728}")
        else:
            print(f"⚠️ Inviter '{inviter_full_name}' not found in background cache")
            # Fallback: set inviter as Leader @1
            leader1 = inviter_full_name
    
    # Create corresponding person record in People collection
    person_doc = {
        "Name": user.name.strip(),
        "Surname": user.surname.strip(),
        "Email": email,
        "Number": user.phone_number.strip(),
        "Address": user.home_address.strip(),
        "Gender": user.gender.strip(),
        "Birthday": user.date_of_birth,
        "InvitedBy": inviter_full_name,
        "Leader @1": leader1,
        "Leader @12": leader12,
        "Leader @144": leader144,
        "Leader @1728": leader1728,
        "Stage": "Win",
        "Date Created": datetime.utcnow().isoformat(),
        "UpdatedAt": datetime.utcnow().isoformat(),
        "user_id": str(user_result.inserted_id)
    }
    
    try:
        person_result = await people_collection.insert_one(person_doc)
        logger.info(f"Person record created successfully for: {email} (ID: {person_result.inserted_id})")
        
        # ✅ ADD THE NEW PERSON TO BACKGROUND CACHE
        new_person_cache_entry = {
            "_id": str(person_result.inserted_id),
            "Name": user.name.strip(),
            "Surname": user.surname.strip(),
            "Email": email,
            "Number": user.phone_number.strip(),
            "Leader @1": leader1,
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @1728": leader1728,
            "FullName": f"{user.name.strip()} {user.surname.strip()}".strip()
        }
        people_cache["data"].append(new_person_cache_entry)
        print(f"✅ Added new person to background cache: {new_person_cache_entry['FullName']}")
        
    except Exception as e:
        logger.error(f"Failed to create person record for {email}: {e}")
    
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

async def user_has_cell(user_email: str) -> bool:
    """Check if a user has at least one cell event"""
    cell = await events_collection.find_one({
        "Event Type": "Cells",
        "$or": [
            {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
            {"email": {"$regex": f"^{user_email}$", "$options": "i"}},
        ]
    })
    return bool(cell)

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
    
def get_current_week_identifier():
    """Get current week identifier in format YYYY-WW"""
    now = datetime.utcnow()
    year = now.isocalendar()[0]
    week = now.isocalendar()[1]
    return f"{year}-W{week:02d}"

# EVENTS ENDPOINTS----------------------------


# POST ENDOINT
@app.post("/events")
async def create_event(event: EventCreate):
    """Create a new event"""
    try:
        event_data = event.dict()
        if not event_data.get("UUID"):
            event_data["UUID"] = str(uuid.uuid4())
        
        event_type_name = event_data.get("eventTypeName")
        if not event_type_name:
            raise HTTPException(status_code=400, detail="eventTypeName is required")
        
        event_type = await events_collection.find_one({
            "name": event_type_name,  
            "isEventType": True
        })
        
        if not event_type:
            raise HTTPException(status_code=400, detail=f"Event type '{event_type_name}' not found")
        
        event_data["eventTypeId"] = event_type["UUID"]  
        event_data["eventTypeName"] = event_type["name"]  
        
        event_data.pop("eventType", None)

        # ✅ FIX: Ensure date is properly saved
        if event_data.get("date"):
            if isinstance(event_data["date"], str):
                try:
                    event_data["date"] = datetime.fromisoformat(event_data["date"].replace("Z", "+00:00"))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid date format")
        else:
            event_data["date"] = datetime.utcnow()

        # ✅ FIX: Set the day field properly
        if event_data.get("recurring_day") and len(event_data["recurring_day"]) > 0:
            event_data["day"] = event_data["recurring_day"][0]  # Use first recurring day
        else:
            event_data["day"] = "One-time"  # Default for one-time events

        # ✅ FIX: Ensure leader fields are properly saved
        event_data.setdefault("eventLeaderName", event_data.get("eventLeader", ""))
        event_data.setdefault("eventLeaderEmail", event_data.get("userEmail", ""))
        event_data.setdefault("leader1", event_data.get("leader1", ""))
        event_data.setdefault("leader12", event_data.get("leader12", ""))

        # Defaults
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data.get("attendees", []))
        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data["status"] = "open"
        
        event_data["isTicketed"] = event_data.get("isTicketed", False)
        event_data["isGlobal"] = event_data.get("isGlobal", False)
        event_data["hasPersonSteps"] = event_data.get("hasPersonSteps", False)
        
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

        event_type_display = event_data.get("eventTypeName", "").lower()
        if "cell" in event_type_display or event_data.get("hasPersonSteps"):
            event_data["leader1"] = event_data.get("leader1", "")
            event_data["leader12"] = event_data.get("leader12", "")

        print(f"🔍 DEBUG - Event data being saved: {event_data}")  # Add this for debugging
        
        result = await events_collection.insert_one(event_data)
        
        # ✅ FIX: Return the complete event data
        created_event = await events_collection.find_one({"_id": result.inserted_id})
        
        print(f"✅ Event created: {result.inserted_id}")
        print(f"📊 Event details - Leader: {created_event.get('eventLeader')}, Email: {created_event.get('eventLeaderEmail')}, Day: {created_event.get('day')}, Date: {created_event.get('date')}")
        
        return {
            "message": "Event created", 
            "id": str(result.inserted_id),
            "event": {
                "id": str(created_event["_id"]),
                "eventName": created_event.get("eventName"),
                "eventLeader": created_event.get("eventLeader"),
                "eventLeaderName": created_event.get("eventLeaderName"),
                "eventLeaderEmail": created_event.get("eventLeaderEmail"),
                "leader1": created_event.get("leader1"),
                "leader12": created_event.get("leader12"),
                "day": created_event.get("day"),
                "date": created_event.get("date"),
                "location": created_event.get("location"),
                "eventTypeName": created_event.get("eventTypeName")
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creating event: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event: {str(e)}")


# GET ENDPOINT 


@app.get("/events/cells")
async def get_cell_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query('2025-10-27'),
    leader_at_12_view: Optional[bool] = Query(None),
    show_personal_cells: Optional[bool] = Query(None),
    show_all_authorized: Optional[bool] = Query(None),
    include_subordinate_cells: Optional[bool] = Query(None)
):
    """
    ✅ DEDICATED: Get ONLY cell events with recurring logic and no future dates
    """
    try:
        print(f"\n🔍 GET /events/cells - User: {current_user.get('email')}")
        print(f"📋 Query params - search: {search}, personal: {personal}, status: {status}")

        user_role = current_user.get("role", "user").lower()
        email = current_user.get("email", "")
        
        # Get today's date
        timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(timezone)
        today = now.date()
        
        # Parse start_date
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else datetime.strptime("2025-10-27", "%Y-%m-%d").date()
        except Exception as e:
            print(f"⚠️ Error parsing start_date: {e}")
            start_date_obj = datetime.strptime("2025-10-27", "%Y-%m-%d").date()

        print(f"📅 CELLS ONLY - Date range: {start_date_obj} to {today}")

        # ✅ BASE QUERY: ONLY CELL EVENTS
        query = {
            "$or": [
                {"Event Type": {"$regex": "Cells", "$options": "i"}},
                {"eventType": {"$regex": "Cells", "$options": "i"}}
            ]
        }

        # ✅ APPLY USER-SPECIFIC FILTERING
        user_email = current_user.get("email", "").lower()
        
        # Personal filter - show only current user's events
        if personal or show_personal_cells:
            print(f"🔐 Applying PERSONAL filter for user: {user_email}")
            query["$or"] = [
                {"Email": {"$regex": user_email, "$options": "i"}},
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]
        
        # Leader at 12 view logic
        elif user_role == "leader at 12" and leader_at_12_view:
            print("👑 Leader at 12 view activated")
            if show_personal_cells:
                # Show only leader's own cells
                query["$or"] = [
                    {"Email": {"$regex": user_email, "$options": "i"}},
                    {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                    {"leader1": {"$regex": user_email, "$options": "i"}}
                ]
            elif include_subordinate_cells or show_all_authorized:
                # Show leader's cells + disciples' cells (existing logic)
                pass  # Keep the base cell query
        
        # Regular user - always show only their events
        elif user_role == "user":
            print(f"👤 Regular user - showing personal events: {user_email}")
            query["$or"] = [
                {"Email": {"$regex": user_email, "$options": "i"}},
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]

        # ✅ APPLY SEARCH FILTER
        if search and search.strip():
            search_term = search.strip()
            print(f"🔍 Applying search filter: {search_term}")
            search_query = {
                "$or": [
                    {"Event Name": {"$regex": search_term, "$options": "i"}},
                    {"eventName": {"$regex": search_term, "$options": "i"}},
                    {"Leader": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderName": {"$regex": search_term, "$options": "i"}},
                    {"Email": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderEmail": {"$regex": search_term, "$options": "i"}},
                    {"leader1": {"$regex": search_term, "$options": "i"}}
                ]
            }
            query = {"$and": [query, search_query]}

        print(f"🎯 Final query: {query}")

        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": {
                        "event_name": {"$ifNull": ["$Event Name", "$eventName"]},
                        "leader_email": {"$ifNull": ["$Email", "$eventLeaderEmail"]},
                        "day": {"$ifNull": ["$Day", "$day"]}
                    },
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"Event Name": 1}}
        ]

        events = await events_collection.aggregate(pipeline).to_list(length=None)
        print(f"📊 Found {len(events)} unique cell templates")

        # Day mapping
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        cell_instances = []
        seen_instance_keys = set()

        # ✅ GENERATE ONLY PAST CELL INSTANCES
        for event in events:
            try:
                event_name = event.get("Event Name") or event.get("eventName", "")
                day_name_raw = event.get("Day") or event.get("day") or ""
                day_name = str(day_name_raw).strip().lower()
                
                if not day_name:
                    continue

                # Find target day number
                target_day_number = None
                if day_name in day_mapping:
                    target_day_number = day_mapping[day_name]
                else:
                    common_abbreviations = {
                        'mon': 0, 'tue': 1, 'tues': 1, 'wed': 2, 
                        'thu': 3, 'thurs': 3, 'fri': 4, 'sat': 5, 'sun': 6
                    }
                    if day_name in common_abbreviations:
                        target_day_number = common_abbreviations[day_name]
                
                if target_day_number is None:
                    continue

                # Generate instances from start_date to today (NO FUTURE DATES)
                current_date = start_date_obj
                days_until_first = (target_day_number - current_date.weekday()) % 7
                first_occurrence = current_date + timedelta(days=days_until_first)

                instance_date = first_occurrence
                while instance_date <= today:  # ✅ NO FUTURE DATES
                    instance_key = f"{event.get('_id')}_{instance_date.isoformat()}"
                    
                    if instance_key not in seen_instance_keys:
                        seen_instance_keys.add(instance_key)
                        
                        # Calculate week and attendance
                        year, week, _ = instance_date.isocalendar()
                        week_identifier = f"{year}-W{week:02d}"
                        attendance_data = event.get("attendance", {})
                        week_attendance = attendance_data.get(week_identifier, {})
                        
                        did_not_meet = week_attendance.get("status") == "did_not_meet"
                        weekly_attendees = week_attendance.get("attendees", [])
                        
                        # Reverted: Prefer explicit week status, but FALLBACK to attendance-derived status
                        if did_not_meet:
                            event_status = "did_not_meet"
                        elif week_attendance.get("status"):
                            # Use explicit status if set
                            event_status = str(week_attendance.get("status")).lower()
                        else:
                            # No explicit status: derive from attendees if present, otherwise mark as overdue/incomplete
                            if isinstance(weekly_attendees, list) and len(weekly_attendees) > 0:
                                event_status = "complete"
                            else:
                                # If the instance date is in the past, mark as incomplete, otherwise leave as open
                                event_status = "incomplete" if instance_date < today else "open"
                        
                        # Apply status filter
                        if status and status != 'all' and status != event_status:
                            instance_date += timedelta(days=7)
                            continue
                        
                        # ✅ REMOVE displayDate and originatedId fields
                        instance = {
                            "_id": f"{event.get('_id')}_{instance_date.isoformat()}",
                            "UUID": event.get("UUID", ""), 
                            "eventName": event_name,
                            "eventType": "Cells",
                            "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                            "eventLeaderEmail": event.get("Email") or event.get("eventLeaderEmail", ""),
                            "leader1": event.get("leader1", ""),
                            "leader12": event.get("Leader @12") or event.get("Leader at 12", ""),
                            "day": day_name.capitalize(),
                            "date": instance_date.isoformat(),
                            # ❌ REMOVED: "displayDate": instance_date.strftime("%d - %m - %Y"),
                            "location": event.get("Location") or event.get("location", ""),
                            "attendees": weekly_attendees,
                            "status": event_status,
                            "Status": event_status.replace("_", " ").title(),
                            "_is_overdue": instance_date < today and event_status == "incomplete",
                            "is_recurring": True,
                            "week_identifier": week_identifier,
                            "original_event_id": str(event.get("_id"))
                        }
                        
                        cell_instances.append(instance)
                        print(f"  ✅ Cell instance for {instance_date} (status: {event_status})")
                    
                    instance_date += timedelta(days=7)

            except Exception as e:
                print(f"❌ Error processing cell event: {str(e)}")
                continue

        # Sort and paginate
        cell_instances.sort(key=lambda x: x['date'], reverse=True)
        total_count = len(cell_instances)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated_events = cell_instances[skip:skip + limit]

        print(f"🎯 Returning {len(paginated_events)} cell instances (page {page}/{total_pages})")

        return {
            "events": paginated_events,
            "total_events": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }

    except Exception as e:
        print(f"❌ ERROR in /events/cells: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

 # =----- Get other event types ---------------

@app.get("/events/other")
async def get_other_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query('2025-10-27'),
    end_date: Optional[str] = Query(None)
):
    """
    ✅ DEDICATED: Get Global Events and other non-cell events with their actual dates
    """
    try:
        print(f"\n🔍 GET /events/other - User: {current_user.get('email')}, Event Type: {event_type}")
        print(f"📋 Query params - search: {search}, personal: {personal}, status: {status}")

        user_role = current_user.get("role", "user").lower()
        email = current_user.get("email", "")
        
        # Get date range
        timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(timezone)
        today = now.date()
        
        # Parse dates
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else datetime.strptime("2025-10-27", "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else today + timedelta(days=30)
        except Exception as e:
            print(f"⚠️ Error parsing dates: {e}")
            start_date_obj = datetime.strptime("2025-10-27", "%Y-%m-%d").date()
            end_date_obj = today + timedelta(days=30)

        print(f"📅 OTHER EVENTS - Date range: {start_date_obj} to {end_date_obj}")

        # ✅ BASE QUERY: NON-CELL EVENTS
        query = {
            "$nor": [
                {"Event Type": {"$regex": "Cells", "$options": "i"}},
                {"eventType": {"$regex": "Cells", "$options": "i"}}
            ]
        }

        # ✅ APPLY USER-SPECIFIC FILTERING
        user_email = current_user.get("email", "").lower()
        
        # Personal filter - show only current user's events
        if personal:
            print(f"🔐 Applying PERSONAL filter for user: {user_email}")
            query["$or"] = [
                {"Email": {"$regex": user_email, "$options": "i"}},
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]
        
        # Regular user - always show only their events
        elif user_role == "user":
            print(f"👤 Regular user - showing personal events: {user_email}")
            query["$or"] = [
                {"Email": {"$regex": user_email, "$options": "i"}},
                {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                {"leader1": {"$regex": user_email, "$options": "i"}}
            ]

        # Filter by specific event type if provided
        if event_type and event_type.lower() != 'all':
            query["$or"] = [
                {"Event Type": {"$regex": f"^{event_type}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{event_type}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{event_type}$", "$options": "i"}}
            ]

        # ✅ APPLY SEARCH FILTER
        if search and search.strip():
            search_term = search.strip()
            print(f"🔍 Applying search filter: {search_term}")
            search_query = {
                "$or": [
                    {"Event Name": {"$regex": search_term, "$options": "i"}},
                    {"eventName": {"$regex": search_term, "$options": "i"}},
                    {"Leader": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderName": {"$regex": search_term, "$options": "i"}},
                    {"Email": {"$regex": search_term, "$options": "i"}},
                    {"eventLeaderEmail": {"$regex": search_term, "$options": "i"}},
                    {"leader1": {"$regex": search_term, "$options": "i"}}
                ]
            }
            query = {"$and": [query, search_query]}

        print(f"🎯 Final query: {query}")

        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": {
                        "event_name": {"$ifNull": ["$Event Name", "$eventName"]},
                        "leader_email": {"$ifNull": ["$Email", "$eventLeaderEmail"]}
                    },
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"date": 1}}
        ]

        events = await events_collection.aggregate(pipeline).to_list(length=None)
        print(f"📊 Found {len(events)} other events")

        other_events = []

        # ✅ PROCESS OTHER EVENTS WITH THEIR ACTUAL DATES
        for event in events:
            try:
                event_name = event.get("Event Name") or event.get("eventName", "")
                event_type_value = event.get("Event Type") or event.get("eventType", "Event")
                
                # Get the actual day value from database
                day_name_raw = event.get("Day") or event.get("day") or event.get("eventDay") or ""
                day_name = str(day_name_raw).strip()
                actual_day_value = day_name.capitalize() if day_name else "One-time"

                # Get event date
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
                        print(f"⚠️ Error parsing date '{event_date_field}': {e}")
                        continue
                else:
                    continue

                # ✅ Filter by date range (can include future dates)
                if event_date < start_date_obj or event_date > end_date_obj:
                    continue

                # Get attendance data
                attendance_data = event.get("attendance", {})
                event_date_iso = event_date.isoformat()
                event_attendance = attendance_data.get(event_date_iso, {})
                
                did_not_meet = event_attendance.get("status") == "did_not_meet"
                weekly_attendees = event_attendance.get("attendees", [])
                
                # Reverted: Prefer explicit event attendance status, but FALLBACK to attendance-derived status
                if did_not_meet:
                    event_status = "did_not_meet"
                elif event_attendance.get("status"):
                    # Use explicit status if set
                    event_status = str(event_attendance.get("status")).lower()
                else:
                    # No explicit status: derive from attendees if present, otherwise mark as overdue/incomplete
                    if isinstance(weekly_attendees, list) and len(weekly_attendees) > 0:
                        event_status = "complete"
                    else:
                        event_status = "incomplete" if event_date < today else "open"

                # Apply status filter
                if status and status != 'all' and status != event_status:
                    continue

                # ✅ REMOVE displayDate and originatedId fields
                instance = {
                    "_id": str(event.get("_id")),
                    "UUID": event.get("UUID", ""),
                    "eventName": event_name,
                    "eventType": event_type_value,
                    "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                    "eventLeaderEmail": event.get("Email") or event.get("eventLeaderEmail", ""),
                    "leader1": event.get("leader1", ""),
                    "leader12": event.get("Leader @12") or event.get("Leader at 12", ""),
                    "day": actual_day_value,
                    "date": event_date.isoformat(),
                    # ❌ REMOVED: "displayDate": event_date.strftime("%d - %m - %Y"),
                    "location": event.get("Location") or event.get("location", ""),
                    "attendees": weekly_attendees,
                    "status": event_status,
                    "Status": event_status.replace("_", " ").title(),
                    "_is_overdue": event_date < today and event_status == "incomplete",
                    "is_recurring": False,
                    "original_event_id": str(event.get("_id"))
                }
                
                other_events.append(instance)
                print(f"  ✅ Other event: {event_name} on {event_date} (Day: {actual_day_value})")

            except Exception as e:
                print(f"❌ Error processing other event: {str(e)}")
                continue

        # Sort and paginate
        other_events.sort(key=lambda x: x['date'], reverse=False)
        total_count = len(other_events)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated_events = other_events[skip:skip + limit]

        print(f"🎯 Returning {len(paginated_events)} other events (page {page}/{total_pages})")

        return {
            "events": paginated_events,
            "total_events": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }

    except Exception as e:
        print(f"❌ ERROR in /events/other: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

#------------------ MIGRATION ENDPOINTS --------------------
@app.post("/migrate-event-types-uuids")
async def migrate_event_types_uuids():
    """🟢 ONE-TIME: Add UUIDs to event types that don't have them"""
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
            print(f"🟢 Added UUID to event type: {event_type['name']}")
        
        return {
            "message": f"✅ Added UUIDs to {migrated_count} event types",
            "migrated_count": migrated_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


# ASSIGN LEADER 
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
    
# EVENTS TYPES SECTION---------------------------------------------------
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
    """
    ✅ FIXED: Get ONLY event type documents (not regular events)
    Returns documents where isEventType = True
    """
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
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
from urllib.parse import unquote
from datetime import datetime



@app.put("/event-types/{event_type_name}")
async def update_event_type(
    event_type_name: str = Path(..., description="Name of the event type to update"),
    updated_data: EventTypeCreate = Body(...)
):
    try:
        # Decode the URL-encoded event type name
        decoded_event_type_name = unquote(event_type_name)
        
        print(f"🔍 [EVENT-TYPE UPDATE] Looking for event type to update: '{decoded_event_type_name}'")
        
        # Check if event type exists
        existing_event_type = await events_collection.find_one({
            "name": decoded_event_type_name, 
            "isEventType": True
        })
        
        if not existing_event_type:
            print(f"❌ [EVENT-TYPE UPDATE] Event type '{decoded_event_type_name}' not found")
            raise HTTPException(status_code=404, detail=f"Event type '{decoded_event_type_name}' not found")

        # If name is being changed, check for duplicates
        new_name = updated_data.name.strip().title()
        name_changed = new_name != decoded_event_type_name
        
        print(f"📝 [EVENT-TYPE UPDATE] Name change check: '{decoded_event_type_name}' -> '{new_name}' (changed: {name_changed})")
        
        if name_changed:
            duplicate = await events_collection.find_one({
                "name": new_name,
                "isEventType": True
            })
            if duplicate:
                print(f"❌ [EVENT-TYPE UPDATE] Duplicate event type found: '{new_name}'")
                raise HTTPException(status_code=400, detail="Event type with this name already exists")

        # Prepare update data
        update_data = updated_data.dict()
        update_data["name"] = new_name
        update_data["updatedAt"] = datetime.utcnow()

        # Remove None values and protect immutable fields
        update_data = {k: v for k, v in update_data.items() if v is not None}
        
        immutable_fields = ["createdAt", "UUID", "_id"]
        for field in immutable_fields:
            update_data.pop(field, None)

        print(f"📝 [EVENT-TYPE UPDATE] Updating event type '{decoded_event_type_name}' with data: {update_data}")

        # ✅ CRITICAL: Update all related events with the new event type name FIRST
        events_updated_count = 0
        if name_changed:
            print(f"🔄 [EVENT-TYPE UPDATE] Event type name changed from '{decoded_event_type_name}' to '{new_name}'")
            
            # Count events that will be updated
            events_count = await events_collection.count_documents({
                "eventType": decoded_event_type_name
            })
            print(f"📊 [EVENT-TYPE UPDATE] Found {events_count} events to update with new event type name")
            
            if events_count > 0:
                # Get sample of events before update for debugging
                events_before = await events_collection.find(
                    {"eventType": decoded_event_type_name}
                ).to_list(length=5)
                print(f"🔍 [EVENT-TYPE UPDATE] Sample events before update: {[{'name': e.get('eventName'), 'type': e.get('eventType')} for e in events_before]}")
                
                # Update all events with the old event type name
                events_update_result = await events_collection.update_many(
                    {"eventType": decoded_event_type_name},
                    {"$set": {
                        "eventType": new_name,
                        "updatedAt": datetime.utcnow()
                    }}
                )
                events_updated_count = events_update_result.modified_count
                print(f"✅ [EVENT-TYPE UPDATE] Updated {events_updated_count} events with new event type name")
                
                # Verify events were actually updated
                if events_updated_count > 0:
                    updated_events = await events_collection.find(
                        {"eventType": new_name}
                    ).to_list(length=5)
                    print(f"🔍 [EVENT-TYPE UPDATE] Sample events after update: {[{'name': e.get('eventName'), 'type': e.get('eventType')} for e in updated_events]}")
                else:
                    print("❌ [EVENT-TYPE UPDATE] WARNING: No events were updated with the new event type name!")
                    
                    # Debug: Check what events actually exist
                    all_events_with_old_name = await events_collection.find(
                        {"eventType": decoded_event_type_name}
                    ).to_list(length=None)
                    print(f"🐛 [EVENT-TYPE UPDATE] Debug - Events still with old name: {len(all_events_with_old_name)}")
                    for event in all_events_with_old_name:
                        print(f"🐛 [EVENT-TYPE UPDATE] Event: {event.get('eventName')} - Type: {event.get('eventType')}")
            else:
                print(f"ℹ️ [EVENT-TYPE UPDATE] No events found with event type '{decoded_event_type_name}'")

        # Now update the event type itself
        result = await events_collection.update_one(
            {"_id": existing_event_type["_id"]},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            print(f" [EVENT-TYPE UPDATE] No changes made to event type '{decoded_event_type_name}'")
            existing_event_type["_id"] = str(existing_event_type["_id"])
            return existing_event_type

        updated_event_type = await events_collection.find_one({"_id": existing_event_type["_id"]})
        updated_event_type["_id"] = str(updated_event_type["_id"])
        
        print(f" [EVENT-TYPE UPDATE] Successfully updated event type to: {updated_event_type['name']}")
        print(f" [EVENT-TYPE UPDATE] Summary - Events updated: {events_updated_count}, Event type: {updated_event_type['name']}")
        
        return updated_event_type

    except HTTPException:
        raise
    except Exception as e:
        print(f" [EVENT-TYPE UPDATE] Error updating event type: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating event type: {str(e)}")

@app.delete("/event-types/{event_type_name}")
async def delete_event_type(event_type_name: str = Path(..., description="Name of the event type to delete")):
    try:
        # Decode the URL-encoded event type name
        decoded_event_type_name = unquote(event_type_name)
        
        print(f"🔍 Looking for event type: '{decoded_event_type_name}'")
        
        # Check if event type exists (case-sensitive exact match)
        event_type = await events_collection.find_one({
            "name": decoded_event_type_name, 
            "isEventType": True
        })
        
        if not event_type:
            print(f"❌ Event type '{decoded_event_type_name}' not found in database")
            # Return success since it's already gone from frontend perspective
            return {
                "message": f"Event type '{decoded_event_type_name}' not found or already deleted",
                "deleted_events_count": 0
            }

        print(f"✅ Found event type: {event_type['name']}")

        # Find all events with this event type
        events_count = await events_collection.count_documents({
            "eventType": decoded_event_type_name
        })
        
        # Delete all events that have this type
        if events_count > 0:
            result = await events_collection.delete_many({
                "eventType": decoded_event_type_name
            })
            deleted_events_count = result.deleted_count
            print(f"🗑️ Deleted {deleted_events_count} events with type '{decoded_event_type_name}'")
        else:
            deleted_events_count = 0

        # Delete the event type document
        delete_result = await events_collection.delete_one({
            "_id": event_type["_id"]
        })
        
        print(f"✅ Deleted event type: {delete_result.deleted_count} document(s) removed")

        return {
            "message": f"Deleted event type '{decoded_event_type_name}' and {deleted_events_count} associated event(s)",
            "deleted_events_count": deleted_events_count
        }

    except Exception as e:
        print(f"❌ Error deleting event type: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting event type: {str(e)}")


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

@app.get("/check-leader-at-12/{user_email}")
async def check_leader_at_12(user_email: str, current_user: dict = Depends(get_current_user)):
    """Check if a user is a Leader at 12 based on events data"""
    try:
        # Find events where this user appears as Leader at 12
        leader_events = await events_collection.find({
            "$or": [
                {"Leader at 12": {"$regex": f".*{current_user.get('name', '')}.*", "$options": "i"}},
                {"Leader @12": {"$regex": f".*{current_user.get('name', '')}.*", "$options": "i"}},
                {"leader12": {"$regex": f".*{current_user.get('name', '')}.*", "$options": "i"}}
            ]
        }).to_list(length=5)

        is_leader_at_12 = len(leader_events) > 0
        
        return {
            "is_leader_at_12": is_leader_at_12,
            "leader_name": current_user.get('name', ''),
            "found_events_count": len(leader_events),
            "sample_events": [
                {
                    "event_name": event.get("Event Name"),
                    "leader_at_12": event.get("Leader at 12") or event.get("Leader @12") or event.get("leader12")
                }
                for event in leader_events[:3]
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
    ✅ FIXED: Proper status detection that works with weekly attendance tracking
    """
    current_week = get_current_week_identifier()
    
    print(f"🔍 Checking status for: {event.get('Event Name', 'Unknown')}")
    print(f"   Current week: {current_week}")
    
    # Check if explicitly marked as did not meet
    if event.get("did_not_meet", False):
        print(f"✅ Marked as 'did_not_meet'")
        return "did_not_meet"
    
    # Check weekly attendance data first
    if "attendance" in event and current_week in event["attendance"]:
        week_data = event["attendance"][current_week]
        week_status = week_data.get("status", "incomplete")
        
        print(f"📊 Found week data - Status: {week_status}")
        
        if week_status == "complete":
            checked_in_count = len([a for a in week_data.get("attendees", []) if a.get("checked_in", False)])
            if checked_in_count > 0:
                print(f"✅ Week marked complete with {checked_in_count} checked-in attendees")
                return "complete"
            else:
                print(f"⚠️ Week marked complete but no checked-in attendees")
                return "incomplete"
        elif week_status == "did_not_meet":
            return "did_not_meet"
    
    # Fallback: Check main attendees array (for backward compatibility)
    attendees = event.get("attendees", [])
    has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
    
    if has_attendees:
        print(f"✅ Found {len(attendees)} attendees in main array")
        return "complete"
    
    print(f"❌ No attendance data found - marking as incomplete")
    return "incomplete"

def parse_event_date(event_date_field, default_date: date) -> date:
    """
    ✅ FIXED: Parse event date from various formats including "DD - MM - YYYY"
    """
    if not event_date_field:
        return default_date
        
    if isinstance(event_date_field, datetime):
        return event_date_field.date()
    elif isinstance(event_date_field, date):
        return event_date_field
    elif isinstance(event_date_field, str):
        try:
            # Try ISO format first
            return datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                # Try "DD - MM - YYYY" format (like "30 - 10 - 2025")
                if " - " in event_date_field:
                    day, month, year = event_date_field.split(" - ")
                    parsed_date = datetime(int(year), int(month), int(day)).date()
                    print(f"✅ Parsed date '{event_date_field}' -> {parsed_date}")
                    return parsed_date
                # Try other common formats
                return datetime.strptime(event_date_field, "%Y-%m-%d").date()
            except Exception as e:
                print(f"⚠️ Could not parse date '{event_date_field}': {e}")
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

def get_day_order(day: str) -> int:
    """Return sort order for days of week"""
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    return day_map.get(day.lower().strip(), 999)

def calculate_this_week_event_date(
    event_day_name: str, 
    today_date: date) -> date:
    """
    Calculates the specific calendar date for a recurring event (cell)
    in the *current* Monday-Sunday week, based on today's date.
    """
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    event_day_num = day_map.get(event_day_name.lower().strip(), -1)
    
    if event_day_num == -1:
        # Invalid day name, return a date far in the past to ensure it's filtered out
        return date.min 

    # today_date.weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = today_date.weekday()
    
    # Calculate the date of the most recent Monday (start of the week)
    week_start_date = today_date - timedelta(days=days_since_monday)
    
    # Calculate the event's date within this Monday-Sunday week
    event_date = week_start_date + timedelta(days=event_day_num)
    
    return event_date

def get_next_occurrences_for_range(
    day_name: str,
    start_date: date,
    end_date: date
) -> List[date]:
    """
    Generate all occurrences of a specific day of the week within a date range.
    
    Args:
        day_name: Name of the day (e.g., "Monday", "Tuesday")
        start_date: Start of the range
        end_date: End of the range
    
    Returns:
        List of dates that fall on the specified day
    """
    day_mapping = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    day_name_lower = day_name.lower().strip()
    target_weekday = day_mapping.get(day_name_lower)
    
    if target_weekday is None:
        print(f"⚠️ Invalid day name: '{day_name}'")
        return []
    
    occurrences = []
    current_date = start_date
    
    # Find the first occurrence of the target day
    days_until_target = (target_weekday - current_date.weekday()) % 7
    first_occurrence = current_date + timedelta(days=days_until_target)
    
    # Generate all occurrences
    while first_occurrence <= end_date:
        occurrences.append(first_occurrence)
        first_occurrence += timedelta(days=7)  # Move to next week
    
    return occurrences

def should_show_cell_for_user(
    cell_doc: dict, 
    user_email: str, 
    user_name: str, 
    is_admin: bool, 
    calc_start_date: date, 
    calc_end_date: date, 
    min_visible_date: date
) -> List[dict]:
    """
    Generate cell instances for a recurring event within a date range.
    Only shows instances from min_visible_date onwards.
    """
    
    event_day = cell_doc.get("Day")
    if not event_day:
        return []
    
    # 1. Generate all dates in the large calculation range
    occurrence_dates = get_next_occurrences_for_range(
        event_day, 
        calc_start_date, 
        calc_end_date
    )
    
    if not occurrence_dates:
        print(f"⚠️ No occurrences generated for day '{event_day}'")
        return []
    
    cell_instances = []
    today_date = date.today()
    
    for occ_date in occurrence_dates:
        
        # 2. CRITICAL FILTER: Hide anything before the minimum required visible date (2025-10-27)
        if occ_date < min_visible_date:
            continue
            
        instance = cell_doc.copy()
        
        # Convert ObjectId to string for JSON serialization
        if "_id" in instance:
            instance["_id"] = str(instance["_id"])
        
        # Set the mandatory date fields
        instance["date"] = occ_date.isoformat()  # ✅ Convert to ISO string
        instance["Date Of Event"] = occ_date.strftime('%d-%m-%Y')  # Used for display
        
        # Add event metadata for frontend
        instance["eventName"] = instance.get("Event Name", "")
        instance["eventType"] = instance.get("Event Type", instance.get("eventType", "Cells"))
        instance["eventLeaderName"] = instance.get("Leader", "")
        instance["eventLeaderEmail"] = instance.get("Email", "")
        instance["leader1"] = instance.get("leader1", "")
        instance["leader12"] = instance.get("Leader @12", instance.get("Leader at 12", ""))
        instance["day"] = event_day.capitalize()
        
        # 3. Status Logic
        if occ_date < today_date:
            # Past event: Check if completed
            did_not_meet = instance.get("did_not_meet", False)
            attendees = instance.get("attendees", [])
            has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
            
            if did_not_meet:
                instance["status"] = "did_not_meet"
            elif has_attendees:
                instance["status"] = "complete"
            else:
                instance["status"] = "incomplete"  # Overdue
        else:
            # Today or Future event
            instance["status"] = "incomplete"  # Not yet due
            
        cell_instances.append(instance)
    
    return cell_instances
    
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
    
# ===== HELPER FUNCTION: Should Include Event =====
def should_include_event_fixed(event_date: date, status: str, today_date: date, is_admin: bool = False) -> bool:
    """
    ✅ FIXED: Only show events from October 20, 2025 onwards
    """
    # Define the start date filter
    start_date = date(2025, 10, 20)  # October 20, 2025
    
    # Only include events that occur on or after October 20, 2025
    if event_date < start_date:
        print(f"⏭️ Filtered out - event date {event_date} is before {start_date}")
        return False
    
    # For regular users, only show today and future incomplete events
    if not is_admin:
        if status == 'incomplete':
            return event_date >= today_date
        else:
            return event_date >= today_date
    
    # Admin sees all events from Oct 20, 2025 onwards
    return True


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
    """
    Debug what cells a user should see
    *** MODIFIED: Filters out future cells based on recurring day. ***
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date() # Today is 2025-11-03
        
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
        
        # NOTE: Using find().to_list() is fine here since the list size is likely small
        cells = await events_collection.find(query).to_list(length=None)
        
        # Categorize by status
        incomplete_cells = []
        complete_cells = []
        did_not_meet_cells = []
        
        for cell in cells:
            event_day_name = cell.get("Day")
            
            # 1. Calculate the specific date this recurring event is due THIS week
            if not event_day_name:
                continue
            
            event_date = calculate_this_week_event_date(event_day_name, today_date)
            
            # 2. Apply the CRITICAL visibility filter: ONLY show if the day has arrived
            # This is the line that solves the issue shown in your image.
            if not should_show_cell_today(event_date, today_date):
                continue # Skip all future cells (like Wednesday/Thursday on a Monday)
            
            # If the code reaches here, the cell is due today or was due earlier this week.
            
            # 3. Process status (assuming get_actual_event_status is available)
            status = get_actual_event_status(cell, today_date)
            
            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "email": cell.get("Email"),
                "day": event_day_name,
                "date_of_week": event_date.isoformat(), # The specific date it was/is due
                "leader_at_12": cell.get("Leader at 12"),
                "status": status
            }
            
            if status == "incomplete":
                incomplete_cells.append(cell_info)
            elif status == "complete":
                complete_cells.append(cell_info)
            elif status == "did_not_meet":
                did_not_meet_cells.append(cell_info)
        
        # Sort incomplete cells (or all lists) by day order
        incomplete_cells.sort(key=lambda x: get_day_order(x.get("day", "")))
        complete_cells.sort(key=lambda x: get_day_order(x.get("day", "")))
        did_not_meet_cells.sort(key=lambda x: get_day_order(x.get("day", "")))


        return {
            "user_email": email,
            "user_name": user_name,
            "today_date": today_date.isoformat(),
            "total_cells_to_track": len(cells),
            "cells_visible_today": len(incomplete_cells) + len(complete_cells) + len(did_not_meet_cells),
            "breakdown": {
                "incomplete_cells_visible": {
                    "count": len(incomplete_cells),
                    "cells": incomplete_cells
                },
                "complete_cells_visible_and_past_due": {
                    "count": len(complete_cells),
                    "cells": complete_cells
                },
                "did_not_meet_cells_visible_and_past_due": {
                    "count": len(did_not_meet_cells),
                    "cells": did_not_meet_cells
                }
            },
            "note": "Only cells due on or before today's date are visible. Future cells are excluded.",
            "status": "success"
        }
        
    except Exception as e:
        print(f"Error in debug_user_hierarchy: {str(e)}") 
        return {"error": str(e)}

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
    

@app.get("/check-leader-at-12/{user_email}")
async def check_leader_at_12(user_email: str, current_user: dict = Depends(get_current_user)):
    """Check if a user is a Leader at 12 based on events data"""
    try:
        # Use the current user's name from token, not the email parameter
        current_user_name = current_user.get("name", "").strip()
        current_user_email = current_user.get("email", "").strip()
        
        print(f"🔍 Checking if {current_user_name} ({current_user_email}) is a Leader at 12")
        
        if not current_user_name:
            return {
                "is_leader_at_12": False,
                "leader_name": "",
                "found_events_count": 0,
                "reason": "No user name found in token"
            }

        # Find events where this user appears as Leader at 12
        leader_events = await events_collection.find({
            "$or": [
                {"Leader at 12": {"$regex": f".*{current_user_name}.*", "$options": "i"}},
                {"Leader @12": {"$regex": f".*{current_user_name}.*", "$options": "i"}},
                {"leader12": {"$regex": f".*{current_user_name}.*", "$options": "i"}}
            ],
            "Event Type": "Cells"
        }).to_list(length=10)

        is_leader_at_12 = len(leader_events) > 0
        
        print(f"✅ Leader at 12 check: {is_leader_at_12} (found {len(leader_events)} events)")
        
        return {
            "is_leader_at_12": is_leader_at_12,
            "leader_name": current_user_name,
            "found_events_count": len(leader_events),
            "sample_events": [
                {
                    "event_name": event.get("Event Name", "Unknown"),
                    "leader_at_12": event.get("Leader at 12") or event.get("Leader @12") or event.get("leader12", "")
                }
                for event in leader_events[:3]
            ]
        }
        
    except Exception as e:
        print(f"❌ Error checking Leader at 12: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/events/status-counts")
async def get_admin_events_status_counts(
    current_user: dict = Depends(get_current_user),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    search: Optional[str] = Query(None, description="Search by event name or leader"),
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)")
):
    """Get status counts for events - Admin only"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # ✅ ADD DATE FILTER - Default to October 20, 2025
        default_start_date = '2025-10-20'
        start_date_filter = start_date if start_date else default_start_date
        
        print(f"📊 Status counts - Date filter: {start_date_filter}")
        
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
        
        # ✅ CONVERT START DATE TO DATE OBJECT
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        for event in events:
            # ✅ FILTER BY DATE - Only include events from start_date onwards
            event_date = parse_event_date(event.get("Date Of Event"), today_date)
            if event_date < start_date_obj:
                continue  # Skip events before start date
            
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
        
        print(f"📊 Status counts result: incomplete={incomplete_count}, complete={complete_count}, did_not_meet={did_not_meet_count}")
        
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": len(events),
            "date_range": {
                "start_date": start_date_filter,
                "end_date": today_date.isoformat()
            }
        }
        
    except Exception as e:
        logging.error(f"Error in status counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/registrant/events/status-counts")
async def get_registrant_events_status_counts(
    current_user: dict = Depends(get_current_user),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    search: Optional[str] = Query(None, description="Search by event name or leader"),
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)")
):
    """Get status counts for events - Registrant only"""
    if current_user.get("role") != "registrant":
        raise HTTPException(status_code=403, detail="Registrant access required")
    
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        # ✅ ADD DATE FILTER
        start_date_filter = start_date if start_date else '2025-10-20'
        
        # Registrants only see their own events
        query = {
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        }
        
        # Add event type filter
        if event_type and event_type != 'all':
            query["Event Type"] = event_type
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"].extend([
                {"Event Name": search_regex},
                {"Leader": search_regex},
                {"Leader at 12": search_regex}
            ])
        
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
        
        # ✅ CONVERT START DATE TO DATE OBJECT
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        for event in events:
            # ✅ FILTER BY DATE
            event_date = parse_event_date(event.get("Date Of Event"), today_date)
            if event_date < start_date_obj:
                continue
            
            did_not_meet = event.get("did_not_meet", False)
            attendees = event.get("attendees", [])
            has_attendees = len(attendees) > 0
            
            if did_not_meet:
                did_not_meet_count += 1
            elif has_attendees:
                complete_count += 1
            else:
                incomplete_count += 1
        
        print(f"📊 REGISTRANT Status counts: incomplete={incomplete_count}, complete={complete_count}, did_not_meet={did_not_meet_count}")
        
        return {
            "incomplete": incomplete_count,
            "complete": complete_count,
            "did_not_meet": did_not_meet_count,
            "total": len(events),
            "date_range": {
                "start_date": start_date_filter,
                "end_date": today_date.isoformat()
            }
        }
        
    except Exception as e:
        logging.error(f"Error in registrant status counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/registrant/events")
async def get_registrant_events(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None)
):
    """Get events for registrant - optimized version"""
    if current_user.get("role") != "registrant":
        raise HTTPException(status_code=403, detail="Registrant access required")
    
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # ✅ DATE FILTER
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"📅 Registrant {email} - Fetching events from {start_date_obj}")

        # ✅ SIMPLE QUERY - Only registrant's own events
        query = {
            "Event Type": "Cells",
            "Email": {"$regex": f"^{email}$", "$options": "i"}
        }
        
        # Add event type filter
        if event_type and event_type != 'all':
            query["eventType"] = event_type
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"Leader": search_regex}
            ]
        
        print(f"🔍 Query: {query}")
        
        # Fetch events
        cursor = events_collection.find(query)
        all_events = await cursor.to_list(length=None)
        
        print(f"📊 Found {len(all_events)} raw events")
        
        # Process events
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        processed_events = []
        
        for event in all_events:
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
                
                # ✅ FILTER BY DATE RANGE
                if most_recent_occurrence < start_date_obj or most_recent_occurrence > today_date:
                    continue
                
                # Get leader info
                leader_name = event.get("Leader", "").strip()
                leader_at_12 = event.get("Leader @12", event.get("Leader at 12", "")).strip()
                
                # Determine status
                did_not_meet = event.get("did_not_meet", False)
                attendees = event.get("attendees", [])
                has_attendees = len(attendees) > 0 if isinstance(attendees, list) else False
                
                if did_not_meet:
                    cell_status = "did_not_meet"
                elif has_attendees:
                    cell_status = "complete"
                else:
                    cell_status = "incomplete"
                
                # Build event object
                final_event = {
                    "_id": str(event.get("_id", "")),
                    "eventName": event_name,
                    "eventType": "Cells",
                    "eventLeaderName": leader_name,
                    "eventLeaderEmail": str(event.get("Email", "")).strip(),
                    "leader1": "",
                    "leader12": leader_at_12,
                    "leader144": event.get("Leader @144", event.get("Leader at 144", "")),
                    "day": day.capitalize(),
                    "date": most_recent_occurrence.isoformat(),
                    "location": event.get("Location", ""),
                    "attendees": event.get("attendees", []) if isinstance(event.get("attendees", []), list) else [],
                    "did_not_meet": did_not_meet,
                    "status": cell_status,
                    "Status": cell_status.replace("_", " ").title(),
                    "_is_overdue": most_recent_occurrence < today_date
                }
                
                processed_events.append(final_event)
                
            except Exception as e:
                print(f"⚠️ Error processing event {event.get('_id')}: {str(e)}")
                continue
        
        print(f"✅ Processed {len(processed_events)} events")
        
        # Filter by status AFTER processing
        if status and status != 'all':
            processed_events = [e for e in processed_events if e["status"] == status]
        
        # Sort by date
        processed_events.sort(key=lambda x: (x['date'], x['eventLeaderName'].lower()))
        
        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
        
        return {
            "events": paginated_events,
            "total_events": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }
        
    except Exception as e:
        print(f"❌ ERROR in get_registrant_events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/registrant/events/cells-debug")
async def get_registrant_cell_events_debug(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None)
):
    """Registrant cells endpoint with pagination and deduplication"""
    try:
        email = current_user.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # ✅ USE PROVIDED START DATE OR DEFAULT TO OCT 20, 2025
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"📅 Registrant - Cells from {start_date_obj} to {today_date}, Page {page}")
        print(f"🔍 Search: '{search}', Status: '{status}', Personal: {personal}, Event Type: '{event_type}', Start Date: '{start_date_filter}'")

        # Build query - Registrants only see their own events
        query = {
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        }
        
        # Add event type filter if provided
        if event_type and event_type != 'all':
            query["eventType"] = event_type
        
        # Add search filter if provided
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"].extend([
                {"Event Name": search_regex},
                {"Leader": search_regex},
                {"Leader at 12": search_regex}
            ])
        
        # 🔥 FETCH ALL CELLS AND DEDUPLICATE IN PYTHON
        cursor = events_collection.find(query)
        all_cells_raw = await cursor.to_list(length=None)
        
        print(f"📊 Found {len(all_cells_raw)} cells before deduplication and date filtering")
        
        # Deduplicate using Python
        seen_cells = set()
        all_cells = []
        
        for cell in all_cells_raw:
            event_name = (cell.get("Event Name") or "").strip().lower()
            email = (cell.get("Email") or "").strip().lower()
            day = (cell.get("Day") or "").strip().lower()
            
            if not event_name:
                continue
            
            cell_key = f"{event_name}|{email}|{day}"
            
            if cell_key not in seen_cells:
                seen_cells.add(cell_key)
                all_cells.append(cell)
        
        print(f"📊 After deduplication: {len(all_cells)} unique cells")
        
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
                
                if most_recent_occurrence < start_date_obj or most_recent_occurrence > today_date:
                    continue
                
                # Get leader info
                leader_name = event.get("Leader", "").strip()
                leader_at_12 = event.get("Leader @12", event.get("Leader at 12", "")).strip()
                leader_at_144 = event.get("Leader @144", event.get("Leader at 144", ""))
                
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
                    "leader1": "",
                    "leader12": leader_at_12,
                    "leader144": leader_at_144,
                    "day": day.capitalize(),
                    "date": most_recent_occurrence.isoformat(),
                    "location": event.get("Location", ""),
                    "attendees": event.get("attendees", []) if isinstance(event.get("attendees", []), list) else [],
                    "did_not_meet": did_not_meet,
                    "status": cell_status,
                    "Status": status_display,
                    "_is_overdue": most_recent_occurrence < today_date
                }
                
                processed_events.append(final_event)
                
            except Exception as e:
                print(f"⚠️ Error processing event {event.get('_id')}: {str(e)}")
                continue
        
        print(f"Processed {len(processed_events)} events after date filtering")
        
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
        }
        
        print(f"📊 Status counts - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
        
        if status and status != 'all':
            processed_events = [e for e in processed_events if e["status"] == status]
            print(f" Filtered to {len(processed_events)} events with status '{status}'")
        
        # Sort by date
        processed_events.sort(key=lambda x: (x['date'], x['eventLeaderName'].lower()))
        
        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
        
        print(f"✅ Returning page {page}/{total_pages}: {len(paginated_events)} events")
        
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
        print(f"❌ ERROR in get_registrant_cell_events_debug: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")
    

@app.get("/events/global")
async def get_global_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None)
):
    """
    Get Global Events (like Sunday Service)
    Shows events where isGlobal = True
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # Parse start_date filter
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"🔍 Fetching Global Events from {start_date_obj}")
        
        # Build query for Global Events
        query = {
            "isGlobal": True,
            "eventTypeName": "Global Events"
        }
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
        
        print(f"📋 Query for Global Events: {query}")
        
        # Fetch events
        cursor = events_collection.find(query).sort("date", -1)
        all_events = await cursor.to_list(length=None)
        
        print(f"📊 Found {len(all_events)} raw global events")
        
        # Process events
        processed_events = []
        
        for event in all_events:
            try:
                print(f"📝 Processing event {event.get('_id')}: {event.get('eventName', event.get('Event Name', 'Unknown'))}")
                
                # Parse event date
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
                
                print(f"  📅 Event date: {event_date}, Start date filter: {start_date_obj}")
                
                # Filter by date range
                if event_date < start_date_obj:
                    print(f"  ⏭️  Skipped - before date range")
                    continue
                
                # Get event details
                event_name = event.get("Event Name") or event.get("eventName", "")
                leader_name = event.get("Leader") or event.get("eventLeader", "")
                location = event.get("Location") or event.get("location", "")
                
                # Determine status - FIXED: Use explicit status field from database, not inferred from attendees
                # This prevents events from being automatically marked "complete" just because attendees were checked in
                did_not_meet = event.get("did_not_meet", False)
                
                # Check for explicit status field first (set via close/update API call)
                stored_status = event.get("status") or event.get("Status")
                
                print(f"  🔄 Status determination: did_not_meet={did_not_meet}, stored_status={stored_status}")
                
                if did_not_meet:
                    event_status = "did_not_meet"
                    status_display = "Did Not Meet"
                elif stored_status:
                    # Use the explicit status from the database
                    event_status = str(stored_status).lower()
                    status_display = str(stored_status).replace("_", " ").title()
                else:
                    # Default to "open" for events without an explicit status
                    # (This ensures new events start as open, not derived from attendees)
                    event_status = "open"
                    status_display = "Open"
                
                print(f"  ✓ Final status: {event_status}")
                
                # Apply status filter
                if status and status != 'all' and status != event_status:
                    print(f"  ⏭️  Skipped - status filter: requested={status}, actual={event_status}")
                    continue
                
                # Build event object
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
                    "attendees": event.get("attendees", []) if isinstance(event.get("attendees", []), list) else [],
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
                    "updated_at": event.get("updated_at")
                }
                
                processed_events.append(final_event)
                print(f"  ✅ Event added to processed list")
                
            except Exception as e:
                print(f"⚠️ Error processing global event {event.get('_id')}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"✅ Processed {len(processed_events)} global events after filtering")
        
        # Sort by date (most recent first)
        processed_events.sort(key=lambda x: x['date'], reverse=True)
        
        # Calculate status counts
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
        }
        
        print(f"📊 Global Events Status - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
        
        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
        
        print(f"✅ Returning page {page}/{total_pages}: {len(paginated_events)} global events")
        
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
        print(f"❌ ERROR in get_global_events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching global events: {str(e)}")
    
    
@app.get("/events/global/status-counts")
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
        
        # Parse start_date filter
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        # Build query
        query = {
            "isGlobal": True,
            "eventType": "Global Events"
        }
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
        
        # Fetch events
        cursor = events_collection.find(query)
        all_events = await cursor.to_list(length=None)
        
        # Calculate counts
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
        
        for event in all_events:
            try:
                # Parse event date
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
                
                # Filter by date range
                if event_date < start_date_obj:
                    continue
                
                # Determine status
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
        print(f"❌ ERROR in global events status counts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/admin/migrate-persistent-attendees")
async def migrate_persistent_attendees(current_user: dict = Depends(get_current_user)):
    """Migrate old attendee data to persistent_attendees format"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    
    try:
        # Find all cell events
        cursor = events_collection.find({"Event Type": "Cells"})
        updated = 0
        
        async for event in cursor:
            event_id = event["_id"]
            
            # Check if already has persistent_attendees
            if event.get("persistent_attendees"):
                continue
            
            # Get attendees from latest week
            attendance = event.get("attendance", {})
            if not attendance:
                # Try old attendees field
                old_attendees = event.get("attendees", [])
                if old_attendees:
                    await events_collection.update_one(
                        {"_id": event_id},
                        {"$set": {"persistent_attendees": old_attendees}}
                    )
                    updated += 1
                continue
            
            # Get most recent week's attendees
            sorted_weeks = sorted(attendance.keys(), reverse=True)
            if sorted_weeks:
                latest_week = sorted_weeks[0]
                latest_attendees = attendance[latest_week].get("attendees", [])
                
                if latest_attendees:
                    await events_collection.update_one(
                        {"_id": event_id},
                        {"$set": {"persistent_attendees": latest_attendees}}
                    )
                    updated += 1
        
        return {
            "message": f"Migrated {updated} events",
            "updated": updated
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status(current_user: dict = Depends(get_current_user)):
    """Check if user is a leader OR has a cell"""
    try:
        user_email = current_user.get("email")
        user_role = current_user.get("role", "").lower()
        
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
        
        print(f"🔍 Checking access for: {user_email}, role: {user_role}")
        
        # ✅ CRITICAL: Check if user has a cell (for regular users)
        if user_role == "user":
            has_cell = await user_has_cell(user_email)
            print(f"   User has cell: {has_cell}")
            
            if not has_cell:
                print(f"   ❌ User {user_email} has no cell - denying Events page access")
                return {"isLeader": False, "hasCell": False, "canAccessEvents": False}
            else:
                print(f"   ✅ User {user_email} has cell - granting Events page access")
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
                print(f"   ✅ {user_email} is a leader")
                return {"isLeader": True, "hasCell": True, "canAccessEvents": True}
        
        # Fallback for admin/registrant
        if user_role in ["admin", "registrant"]:
            print(f"   ✅ {user_email} is {user_role} - granting access")
            return {"isLeader": True, "hasCell": True, "canAccessEvents": True}

        print(f"   ❌ {user_email} is not a leader and has no special role")
        return {"isLeader": False, "hasCell": False, "canAccessEvents": False}

    except Exception as e:
        print(f"❌ Error checking leader status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/admin/cleanup-duplicate-cells")
async def cleanup_duplicate_cells(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    
    # Find duplicates and keep only the oldest one
    pipeline = [
        {"$match": {"Event Type": "Cells"}},
        {
            "$group": {
                "_id": {
                    "event_name": "$Event Name",
                    "email": "$Email",
                    "day": "$Day"
                },
                "docs": {"$push": "$_id"},
                "count": {"$sum": 1}
            }
        },
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = await events_collection.aggregate(pipeline).to_list(length=None)
    
    deleted_count = 0
    for dup in duplicates:
        # Keep first, delete rest
        ids_to_delete = dup["docs"][1:]
        result = await events_collection.delete_many({
            "_id": {"$in": ids_to_delete}
        })
        deleted_count += result.deleted_count
    
    return {"message": f"Deleted {deleted_count} duplicate cells"}
    
@app.get("/admin/events/missing-leaders")
async def get_missing_leaders(current_user: dict = Depends(get_current_user)):
    """Find all Leaders at 12 that don't exist in People database"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        pipeline = [
            {"$match": {"Event Type": "Cells"}},
            {"$group": {
                "_id": {"$ifNull": ["$Leader at 12", "$Leader @12"]},
                "event_count": {"$sum": 1}
            }},
            {"$match": {"_id": {"$ne": None, "$ne": ""}}},
            {"$sort": {"event_count": -1}}
        ]
        
        cursor = events_collection.aggregate(pipeline)
        event_leaders = []
        async for result in cursor:
            name = result.get("_id", "").strip()
            if name:
                event_leaders.append({
                    "name": name,
                    "event_count": result.get("event_count", 0)
                })
        
        print(f"🔍 Found {len(event_leaders)} unique Leader at 12 names in events")
        
        # Check which ones exist in People
        missing_leaders = []
        found_leaders = []
        
        for leader_info in event_leaders:
            name = leader_info["name"]
            
            # Try to find in People collection
            person = await people_collection.find_one({
                "$or": [
                    {"Name": {"$regex": f"^{name}$", "$options": "i"}},
                    {"$expr": {
                        "$regexMatch": {
                            "input": {"$concat": ["$Name", " ", "$Surname"]},
                            "regex": f"^{name}$",
                            "options": "i"
                        }
                    }}
                ]
            })
            
            if not person:
                missing_leaders.append(leader_info)
            else:
                found_leaders.append({
                    **leader_info,
                    "gender": person.get("Gender", "Unknown"),
                    "full_name": f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
                })
        
        print(f"✅ Found {len(found_leaders)} leaders in People database")
        print(f"Missing {len(missing_leaders)} leaders from People database")
        
        return {
            "total_leaders_in_events": len(event_leaders),
            "found_in_people": len(found_leaders),
            "missing_from_people": len(missing_leaders),
            "found_leaders": found_leaders[:20],  
            "missing_leaders": missing_leaders,  
            "message": f"Found {len(found_leaders)} leaders, {len(missing_leaders)} need to be added to People database"
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    
# ✅ ADD INDEXES FOR FASTER QUERIES
@app.on_event("startup")
async def create_indexes_on_startup():
    print("📌 Creating MongoDB indexes for faster queries...")
    
    try:
        # Compound index for common queries
        await events_collection.create_index(
            [
                ("Event Type", 1),
                ("Email", 1),
                ("Day", 1),
                ("Event Name", 1)
            ],
            name="fast_lookup_idx"
        )
        
        # Index for leader searches
        await events_collection.create_index(
            [("Leader", 1), ("Leader at 12", 1)],
            name="leader_search_idx"
        )
        
        # Index for people collection
        await people_collection.create_index(
            [("Name", 1), ("Surname", 1), ("Gender", 1)],
            name="people_lookup_idx"
        )
        
        print(" Indexes created successfully")
    except Exception as e:
        print(f"⚠️ Error creating indexes: {e}")
    

@app.put("/events/{event_id}")
async def update_event(event_id: str, event_data: dict):
    """
    ✅ FIXED: Update event by _id or UUID
    """
    try:
        print(f"🔍 Attempting to update event with ID: {event_id}")
        print(f"📥 Received data: {event_data}")
        
        # ✅ Try to find event by _id first (MongoDB ObjectId)
        event = None
        
        # Try as MongoDB ObjectId
        if ObjectId.is_valid(event_id):
            try:
                event = await events_collection.find_one({"_id": ObjectId(event_id)})
                if event:
                    print(f"✅ Found event by _id: {event_id}")
            except Exception as e:
                print(f"⚠️ Could not find by ObjectId: {e}")
        
        # If not found, try by UUID
        if not event:
            event = await events_collection.find_one({"UUID": event_id})
            if event:
                print(f"✅ Found event by UUID: {event_id}")
        
        # If still not found, return 404
        if not event:
            print(f"❌ Event not found with identifier: {event_id}")
            raise HTTPException(
                status_code=404, 
                detail=f"Event not found with identifier: {event_id}"
            )
        
        # ✅ Prepare update data
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
        
        # ✅ Add update timestamp
        update_data['updated_at'] = datetime.utcnow()
        
        print(f"📝 Updating with data: {update_data}")
        
        # ✅ Perform the update
        result = await events_collection.update_one(
            {"_id": event["_id"]},  # Always use the found event's _id
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            print(f"⚠️ No changes made to event {event_id}")
        else:
            print(f"✅ Event {event_id} updated successfully")
        
        # ✅ Fetch and return the updated event
        updated_event = await events_collection.find_one({"_id": event["_id"]})
        updated_event["_id"] = str(updated_event["_id"])
        
        return updated_event
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error updating event: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error updating event: {str(e)}"
        )

@app.post("/admin/events/bulk-assign-all-leaders")
async def bulk_assign_all_leaders_comprehensive(current_user: dict = Depends(get_current_user)):
    """
    🔥 COMPREHENSIVE: Bulk assign Leader @1 for ALL cell events
    This ensures every cell event has the correct Leader @1 from People database
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        print("\n" + "="*80)
        print("🚀 STARTING BULK LEADER @1 ASSIGNMENT FOR ALL CELL EVENTS")
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
        
        print(f"📊 Found {len(cell_events)} cell events to process\n")
        
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
                print(f"   ⏭️  SKIPPED - No Leader @12 found")
                skipped_count += 1
                results["skipped"].append({
                    "event_name": event_name,
                    "event_leader": event_leader,
                    "reason": "No Leader @12"
                })
                continue
            
            print(f"   🔍 Looking up Leader @1 for '{leader_at_12}'...")
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
                print(f"   ❌ FAILED - Could not find Leader @1 for '{leader_at_12}'")
        
        print("\n" + "="*80)
        print("📊 BULK ASSIGNMENT COMPLETE")
        print("="*80)
        print(f"✅ Updated: {updated_count}")
        print(f"❌ Failed: {failed_count}")
        print(f"⏭️  Skipped: {skipped_count}")
        print(f"📋 Total Processed: {len(cell_events)}")
        print("="*80 + "\n")
        
        return {
            "success": True,
            "message": f"✅ Successfully assigned Leader @1 to {updated_count} events. {failed_count} failed, {skipped_count} skipped.",
            "summary": {
                "total_processed": len(cell_events),
                "updated": updated_count,
                "failed": failed_count,
                "skipped": skipped_count
            },
            "results": results
        }
        
    except Exception as e:
        print(f"\n❌ ERROR in bulk assign: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error assigning leaders: {str(e)}")

@app.post("/admin/events/fix-all-leaders-at-1")
async def fix_all_leaders_at_1(current_user: dict = Depends(get_current_user)):
    """
    🔥 FIXED: Assign Leader @1 based on EVENT LEADER's gender
    This assigns Gavin/Vicky based on who is leading the cell
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        print("\n" + "="*80)
        print("🔥 FIXING ALL LEADERS @1 BASED ON EVENT LEADER'S GENDER")
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
                print(f"[{idx}/{len(all_events)}] ⏭️ Skipping {event_name} - No leader")
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
                print(f"   ❌ Leader '{leader_name}' not found in People database")
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
                print(f"   ⚠️ Unknown gender: '{gender}'")
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
            print(f"   ✅ Assigned Leader @1: {leader_at_1}")
        
        print("\n" + "="*80)
        print("📊 SUMMARY")
        print("="*80)
        print(f"✅ Updated: {updated_count}")
        print(f"❌ Failed: {failed_count}")
        print(f"⏭️  Skipped: {skipped_count}")
        print(f"📋 Total: {len(all_events)}")
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
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))      


@app.get("/admin/events/verify-leaders")
async def verify_leaders_assignment(current_user: dict = Depends(get_current_user)):
    """
    🔍 Verify Leader @1 assignments in cell events
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


@app.get("/admin/events/cells-debug")
async def get_admin_cell_events_debug(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None)  # ✅ ADD START DATE PARAMETER
):
    """Optimized admin cells endpoint with pagination and deduplication"""
    try:
        role = current_user.get("role", "")
        if role.lower() != "admin":
            raise HTTPException(status_code=403, detail="Only admins can access this endpoint")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # ✅ USE PROVIDED START DATE OR DEFAULT TO OCT 20, 2025
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"📅 Admin - Cells from {start_date_obj} to {today_date}, Page {page}")
        print(f"🔍 Search: '{search}', Status: '{status}', Personal: {personal}, Event Type: '{event_type}', Start Date: '{start_date_filter}'")

        # Build match filter
        match_filter = {"Event Type": "Cells"}
        
        # Add event type filter if provided
        if event_type and event_type != 'all':
            match_filter["eventType"] = event_type
            print(f"🎯 Filtering by event type: {event_type}")
        
        # Add personal filtering logic
        if personal:
            user_email = current_user.get("email", "")
            print(f"🎯 PERSONAL FILTER ACTIVATED for user: {user_email}")
            
            # Find user's name from their cell
            user_cell = await events_collection.find_one({
                "Event Type": "Cells",
                "$or": [
                    {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                    {"email": {"$regex": f"^{user_email}$", "$options": "i"}},
                ]
            })
            
            user_name = user_cell.get("Leader", "").strip() if user_cell else ""
            print(f"✅ User name found: '{user_name}'")
            
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
            print(f"🔍 Personal query conditions: {len(personal_conditions)} conditions")
        
        # Add search filter if provided (only if not in personal mode)
        elif search and search.strip():
            search_term = search.strip()
            print(f"✅ Applying search filter for: '{search_term}'")
            
            match_filter["$or"] = [
                {"Event Name": {"$regex": search_term, "$options": "i"}},
                {"Leader": {"$regex": search_term, "$options": "i"}},
                {"Email": {"$regex": search_term, "$options": "i"}},
                {"Leader at 12": {"$regex": search_term, "$options": "i"}},
                {"Leader @12": {"$regex": search_term, "$options": "i"}},
            ]
        
        # 🔥 FETCH ALL CELLS AND DEDUPLICATE IN PYTHON
        cursor = events_collection.find(match_filter)
        all_cells_raw = await cursor.to_list(length=None)
        
        print(f"📊 Found {len(all_cells_raw)} cells before deduplication and date filtering")
        
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
                print(f"⚠️ Skipping duplicate: {event_name} ({email}) on {day}")
        
        print(f"📊 After deduplication: {len(all_cells)} unique cells")
        
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
                print(f"⚠️ Error fetching leaders from People collection: {str(e)}")
        
        print(f"🔍 Found {len(leader_at_1_map)} leaders with Leader @1")
        
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
                
                # ✅ FILTER BY DATE RANGE (Oct 20, 2025 to today)
                if most_recent_occurrence < start_date_obj or most_recent_occurrence > today_date:
                    print(f"⏭️ Skipping {event_name} - date {most_recent_occurrence} outside range {start_date_obj} to {today_date}")
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
                    "attendees": event.get("attendees", []) if isinstance(event.get("attendees", []), list) else [],
                    "did_not_meet": did_not_meet,
                    "status": cell_status,
                    "Status": status_display,
                    "_is_overdue": most_recent_occurrence < today_date
                }
                
                processed_events.append(final_event)
                
            except Exception as e:
                print(f"⚠️ Error processing event {event.get('_id')}: {str(e)}")
                continue
        
        print(f"✅ Processed {len(processed_events)} events after date filtering")
        
        # Calculate status counts from ALL processed events
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
        }
        
        print(f"📊 Status counts - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
        
        # Filter by status AFTER counting
        if status and status != 'all':
            processed_events = [e for e in processed_events if e["status"] == status]
            print(f"✅ Filtered to {len(processed_events)} events with status '{status}'")
        
        # Sort by date
        processed_events.sort(key=lambda x: (x['date'], x['eventLeaderName'].lower()))
        
        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]
        
        print(f"✅ Returning page {page}/{total_pages}: {len(paginated_events)} events")
        
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
        print(f"❌ ERROR in get_admin_cell_events_debug: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")      

    
@app.get("/events/cells-user-fixed")
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
    """✅ FIXED: Shows cells with proper deduplication"""
    try:
        email = current_user.get("email")
        role = current_user.get("role", "user").lower()
        
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        start_date_obj = datetime.strptime(start_date or "2025-10-20", "%Y-%m-%d").date()
        
        print(f"🔍 Fetching cells for user: {email} (role: {role})")
        print(f"📅 Date range: {start_date_obj} onwards")
        print(f"🎯 Personal filter: {personal}")

        # Build query based on role and personal filter
        query = {"Event Type": "Cells"}
        
        # Apply role-based filtering
        if role == "admin" and not personal:
            # Admin with "View All" - no email filter
            print("👑 ADMIN VIEW ALL - Showing all cells")
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

        # ✅ USE AGGREGATION WITH $GROUP TO REMOVE DUPLICATES
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
        
        print(f"📊 Found {len(all_cells_raw)} unique cells after deduplication")

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
                target_weekday = day_mapping[day]
                base_date = max(start_date_obj, today_date)
                base_weekday = base_date.weekday()
                days_until = (target_weekday - base_weekday) % 7
                next_occurrence = base_date + timedelta(days=days_until)

                # Get leader info
                leader_name = event.get("Leader", "").strip()
                leader_at_12 = event.get("Leader @12", event.get("Leader at 12", "")).strip()
                
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
                    "did_not_meet": did_not_meet,
                    "status": status_val,
                    "Status": status_val.replace("_", " ").title(),
                    "_is_overdue": next_occurrence < today_date
                }
                
                processed_events.append(final_event)
                
            except Exception as e:
                print(f"⚠️ Error processing event {event.get('_id')}: {str(e)}")
                continue

        # Sort by date
        processed_events.sort(key=lambda x: x['date'])

        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]

        print(f"✅ Returning {len(paginated_events)} events (page {page} of {total_pages})")

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
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")
    
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

async def get_leader_at_1_for_leader_at_12(leader_at_12_name: str) -> str:
    """
    ✅ FIXED: Get Leader @1 based on Leader @12's gender from People database
    """
    if not leader_at_12_name or not leader_at_12_name.strip():
        return ""
    
    cleaned_name = leader_at_12_name.strip()
    print(f"🔍 Looking up Leader @1 for Leader @12: '{cleaned_name}'")
    
    try:
        # Try multiple search strategies to find the person
        search_queries = [
            # Exact full name match
            {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
            # Case-insensitive name match
            {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            # First name only (if full name has space)
            {"Name": {"$regex": f"^{cleaned_name.split()[0]}$", "$options": "i"}} if " " in cleaned_name else None,
        ]
        
        # Remove None queries
        search_queries = [q for q in search_queries if q is not None]
        
        person = None
        for query in search_queries:
            person = await people_collection.find_one(query)
            if person:
                print(f"   ✅ Found person using query: {query}")
                break
        
        if not person:
            print(f"   ❌ Person '{cleaned_name}' NOT found in database")
            return ""
        
        # Get gender
        gender = (person.get("Gender") or "").lower().strip()
        person_full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
        
        print(f"   ✅ Found person: {person_full_name}")
        print(f"   📋 Gender: '{gender}'")
        
        # SIMPLE GENDER-BASED ASSIGNMENT
        if gender in ["female", "f", "woman", "lady", "girl"]:
            print(f"   ✅ Assigned: Vicky Enslin (female)")
            return "Vicky Enslin"
        elif gender in ["male", "m", "man", "gentleman", "boy"]:
            print(f"   ✅ Assigned: Gavin Enslin (male)")
            return "Gavin Enslin"
        else:
            print(f"   ⚠️ Unknown gender: '{gender}' - cannot assign Leader @1")
            return ""
            
    except Exception as e:
        print(f"   ❌ Error looking up leader: {str(e)}")
        return ""

# @app.get("/debug/all-leader-at-12-people")
# async def debug_all_leader_at_12_people():
#     """Get all people who ARE Leaders at 12 (have people under them)"""
#     try:
#         # Find people who appear as Leader @12 in other people's records
#         pipeline = [
#             {
#                 "$match": {
#                     "Leader @12": {"$exists": True, "$ne": ""}
#                 }
#             },
#             {
#                 "$group": {
#                     "_id": "$Leader @12",
#                     "count": {"$sum": 1},
#                     "people_they_lead": {
#                         "$push": {
#                             "name": {"$concat": ["$Name", " ", "$Surname"]},
#                             "email": "$Email"
#                         }
#                     }
#                 }
#             },
#             {
#                 "$lookup": {
#                     "from": "people",
#                     "localField": "_id",
#                     "foreignField": "Name",
#                     "as": "leader_details"
#                 }
#             },
#             {
#                 "$unwind": {
#                     "path": "$leader_details",
#                     "preserveNullAndEmptyArrays": True
#                 }
#             },
#             {
#                 "$project": {
#                     "leader_at_12_name": "$_id",
#                     "number_of_people_they_lead": "$count",
#                     "leader_gender": "$leader_details.Gender",
#                     "leader_has_leader_at_1": "$leader_details.Leader @1",
#                     "sample_people_they_lead": {"$slice": ["$people_they_lead", 3]},
#                     "_id": 0
#                 }
#             },
#             {
#                 "$sort": {"number_of_people_they_lead": -1}
#             }
#         ]
        
#         results = await people_collection.aggregate(pipeline).to_list(length=50)
        
#         return {
#             "total_leader_at_12_people": len(results),
#             "leaders_at_12": results
#         }
        
#     except Exception as e:
#         return {"error": str(e)}

@app.post("/admin/events/fix-all-missing-leader-at-1")
async def fix_all_missing_leader_at_1(current_user: dict = Depends(get_current_user)):
    """
    ✅ UPDATED:
    Find ALL Cell events where the event leader is a Leader @12 (in people collection)
    and assign the correct Leader @1 (Vicky or Gavin) based on gender.
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        # 1️⃣ Find all Cell events missing leader1 (Leader @1)
        cell_events = await events_collection.find({
            "Event Type": {"$regex": "^Cells$", "$options": "i"},
            "$or": [
                {"leader1": {"$exists": False}},
                {"leader1": ""},
                {"leader1": None}
            ]
        }).to_list(length=None)

        updated_count = 0
        failed_count = 0
        results = []

        print(f"🔍 Found {len(cell_events)} events missing Leader at 1")

        for event in cell_events:
            event_id = event["_id"]
            event_name = event.get("Event Name", "")
            leader_name = event.get("Leader", "").strip()
            leader_email = event.get("Email", "").strip()

            if not leader_name and not leader_email:
                failed_count += 1
                continue

            # 2️⃣ Find corresponding person in people collection
            person = await people_collection.find_one({
                "$or": [
                    {"Email": {"$regex": f"^{leader_email}$", "$options": "i"}},
                    {"Name": {"$regex": f"^{leader_name}$", "$options": "i"}}
                ]
            })

            if not person:
                print(f"❌ Person not found for {leader_name} ({leader_email})")
                failed_count += 1
                continue

            gender = str(person.get("Gender", "")).lower()

            # 3️⃣ Determine correct Leader @1 based on gender
            if gender == "female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "male":
                leader_at_1 = "Gavin Enslin"
            else:
                print(f"⚠️ Gender unknown for {leader_name}")
                failed_count += 1
                continue

            # 4️⃣ Update the event in MongoDB
            await events_collection.update_one(
                {"_id": event_id},
                {"$set": {"leader1": leader_at_1}}
            )

            updated_count += 1
            results.append({
                "event_name": event_name,
                "leader_name": leader_name,
                "gender": gender,
                "assigned_leader_at_1": leader_at_1,
                "status": "✅ updated"
            })

            print(f"✅ Updated {event_name}: {leader_name} ({gender}) → {leader_at_1}")

        return {
            "message": f"✅ Fixed {updated_count} events, {failed_count} failed",
            "updated_count": updated_count,
            "failed_count": failed_count,
            "total_processed": len(cell_events),
            "results": results[:25]
        }

    except Exception as e:
        print(f"❌ Error fixing leaders: {e}")
        raise HTTPException(status_code=500, detail=f"Error fixing leaders: {str(e)}")


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
    return ""  # ✅ ADDED MISSING RETURN STATEMENT

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

async def get_leader_at_1_for_event_leader(event_leader_name: str) -> str:
    """
    ✅ ENHANCED: Get Leader @1 based on event leader's position in hierarchy
    - If event leader IS a Leader at 12 (appears as Leader at 12 in other events), assign Gavin/Vicky
    - Otherwise, return empty
    """
    if not event_leader_name or not event_leader_name.strip():
        return ""
    
    cleaned_name = event_leader_name.strip()
    
    # Skip if already Gavin/Vicky
    if cleaned_name.lower() in ["gavin enslin", "vicky enslin"]:
        return ""
    
    print(f"🔍 Checking if event leader '{cleaned_name}' is a Leader at 12...")
    
    # Check if this person appears as Leader at 12 in ANY event
    is_leader_at_12 = await events_collection.find_one({
        "$or": [
            {"Leader at 12": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            {"Leader @12": {"$regex": f"^{cleaned_name}$", "$options": "i"}}
        ]
    })
    
    if is_leader_at_12:
        print(f"   ✅ {cleaned_name} IS a Leader at 12 - looking up their gender")
        # Now get their gender to assign Gavin/Vicky
        return await get_leader_at_1_for_leader_at_12(cleaned_name)
    
    print(f"   ❌ {cleaned_name} is NOT a Leader at 12")
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

@app.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: dict = Body(...)
):
    """
    ✅ FIXED: Proper attendee persistence across recurring events
    """
    try:
        print(f"🔍 DEBUG: Raw submission data received:")
        print(f"   Event ID: {event_id}")
        print(f"   Did not meet: {submission.get('did_not_meet')}")
        print(f"   Attendees count: {len(submission.get('attendees', []))}")
        print(f"   All attendees count: {len(submission.get('all_attendees', []))}")
        
        # Convert to Pydantic model for validation
        try:
            submission_model = AttendanceSubmission(**submission)
            print("✅ Pydantic validation passed")
        except Exception as validation_error:
            print(f"❌ Pydantic validation failed: {validation_error}")
            raise HTTPException(status_code=422, detail=str(validation_error))

        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        event_name = event.get("Event Name", "Unknown")
        current_week = get_current_week_identifier()
        
        print(f"🎯 SUBMIT ATTENDANCE for: {event_name} (Week: {current_week})")
        
        # ✅ Initialize attendance tracking if not exists
        if "attendance" not in event:
            event["attendance"] = {}
        
        # ✅ Get Common Attendee Pool (persistent list)
        common_attendee_pool = submission_model.all_attendees
        
        # ✅ CRITICAL: If no pool provided, preserve existing persistent data
        if not common_attendee_pool:
            common_attendee_pool = event.get("persistent_attendees", [])
        
        # ✅ Convert Pydantic models to dictionaries for MongoDB
        common_attendee_pool_dict = [attendee.model_dump() for attendee in common_attendee_pool]
        
        # ✅ Get checked-in attendees for this week
        checked_in_attendees = []
        
        if submission_model.did_not_meet:
            # Case 1: Did Not Meet
            weekly_attendance_entry = {
                "status": "did_not_meet",
                "attendees": [],
                "submitted_at": datetime.utcnow(),
                "submitted_by": submission_model.leaderEmail,
                "common_attendee_pool_count": len(common_attendee_pool_dict)
            }
            print(f"🔴 MARKING AS DID NOT MEET for week {current_week}")
            
            main_update_fields = {
                "Status": "Did Not Meet",
                "status": "did_not_meet",
                "did_not_meet": True,
            }
        
        else:
            # Case 2: Attendance Captured
            for att in submission_model.attendees:
                attendee_data = att.model_dump()
                attendee_data.update({
                    "checked_in": True,
                    "check_in_date": datetime.utcnow().isoformat()
                })
                
                if submission_model.isTicketed:
                    attendee_data.update({
                        "priceTier": att.priceTier,
                        "price": att.price,
                        "ageGroup": att.ageGroup,
                        "memberType": att.memberType,
                        "paymentMethod": att.paymentMethod,
                        "paid": att.paid,
                        "owing": att.owing
                    })
                
                checked_in_attendees.append(attendee_data)
            
            # ✅ VALIDATION: Only mark as complete if we have checked-in attendees
            if len(checked_in_attendees) == 0:
                print(f"⚠️ No attendees checked in - keeping as incomplete")
                weekly_attendance_entry = {
                    "status": "incomplete",
                    "attendees": [],
                    "submitted_at": datetime.utcnow(),
                    "submitted_by": submission_model.leaderEmail,
                    "common_attendee_pool_count": len(common_attendee_pool_dict)
                }
                
                main_update_fields = {
                    "Status": "Incomplete",
                    "status": "incomplete",
                    "did_not_meet": False,
                }
            else:
                weekly_attendance_entry = {
                    "status": "complete",
                    "attendees": checked_in_attendees,
                    "submitted_at": datetime.utcnow(),
                    "submitted_by": submission_model.leaderEmail,
                    "common_attendee_pool_count": len(common_attendee_pool_dict)
                }
                print(f"✅ MARKING AS COMPLETE with {len(checked_in_attendees)} attendees")
                
                main_update_fields = {
                    "Status": "Complete",
                    "status": "complete",
                    "did_not_meet": False,
                }
        
        # ✅ Build update data with proper attendee persistence
        update_data = {
            "Date Captured": datetime.now().strftime("%d %B %Y"),
            "updated_at": datetime.utcnow(),
            **main_update_fields,
            # ✅ CRITICAL: Always save the persistent attendees list
            "persistent_attendees": common_attendee_pool_dict,
            # ✅ Store this week's attendance data
            f"attendance.{current_week}": weekly_attendance_entry
        }
        
        print(f"📤 Saving: {len(common_attendee_pool_dict)} persistent attendees, {len(checked_in_attendees)} checked-in attendees")
        
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        print(f"📝 Database result: matched={result.matched_count}, modified={result.modified_count}")
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
        
        return {
            "message": "Attendance submitted successfully",
            "event_id": event_id,
            "status": weekly_attendance_entry["status"],
            "did_not_meet": submission_model.did_not_meet,
            "checked_in_count": len(checked_in_attendees),
            "persistent_attendees_count": len(common_attendee_pool_dict),
            "week": current_week,
            "success": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in submit_attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")
    
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
        
        # Get persistent attendees first
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
async def submit_attendance_handler(event_id: str, submission: AttendanceSubmission):
    """
    ✅ FIXED: Properly saves attendance with weekly tracking AND ensures event appears in filters
    """
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        current_week = get_current_week_identifier()
        
        # Initialize attendance tracking if not exists
        if "attendance" not in event:
            event["attendance"] = {}
        
        if submission.did_not_meet:
            # ✅ Case 1: Did Not Meet
            event["attendance"][current_week] = {
                "status": "did_not_meet",
                "attendees": [],
                "submitted_at": datetime.utcnow(),
                "submitted_by": submission.leaderEmail
            }
            
            update_data = {
                "Status": "Did Not Meet",
                "status": "did_not_meet",
                "attendees": [],  # Clear main attendees array
                "total_attendance": 0,
                "Date Captured": datetime.now().strftime("%d %B %Y"),
                "updated_at": datetime.utcnow(),
                "attendance": event["attendance"],
                # ✅ CRITICAL: Ensure these fields are set for filtering
                "did_not_meet": True,
                "is_recurring": event.get("is_recurring", True)
            }
        else:
            # ✅ Case 2: Attendance Captured
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
            
            # ✅ Save to current week's attendance
            event["attendance"][current_week] = {
                "status": "complete",
                "attendees": attendees_list,
                "submitted_at": datetime.utcnow(),
                "submitted_by": submission.leaderEmail
            }
            
            # ✅ Update main event fields
            update_data = {
                "Status": "Complete",
                "status": "complete",
                "did_not_meet": False,
                "attendees": attendees_list,  # Update main array
                "total_attendance": len(attendees_list),
                "Date Captured": datetime.now().strftime("%d %B %Y"),
                "updated_at": datetime.utcnow(),
                "attendance": event["attendance"],
                # ✅ CRITICAL: Ensure these fields are set for filtering
                "is_recurring": event.get("is_recurring", True),
                # ✅ Update persistent_attendees for the attendance modal
                "persistent_attendees": attendees_list
            }
        
        # Save to database
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        print(f"✅ Attendance saved for event {event_id}: {update_data['status']}")
        
        return {
            "message": "Success",
            "success": True,
            "status": "did_not_meet" if submission.did_not_meet else "complete",
            "event_id": event_id,
            "week": current_week
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
    

# @app.get("/leaders/position/{level}")
# async def get_leaders_by_position(level: int):
#     """Return a list of people who are leaders at the specified level: 12, 144, or 1728."""
#     try:
#         if level not in [12, 144, 1728]:
#             raise HTTPException(status_code=400, detail="Invalid leadership level")

#         field_map = {
#             12: "Leader @12",
#             144: "Leader @144",
#             1728: "Leader @ 1728"
#         }

#         field_name = field_map[level]

#         people = await people_collection.find({field_name: {"$exists": True, "$ne": ""}}).to_list(length=None)

#         result = []
#         for person in people:
#             leader_name = person.get(field_name, "").strip()
#             if leader_name:
#                 result.append({
#                     "name": leader_name,
#                     "person_id": str(person["_id"])
#                 })

#         return {"leaders": result}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.get("/leaders/cells-for/{email}")
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



# @app.get("/test/user-cells/{email}")
# async def test_user_cells(email: str):
#     """
#     Debug endpoint to see what cells a regular user should see.
#     Tests the exact logic used in /events/cells-user
#     """
#     try:
#         timezone = pytz.timezone("Africa/Johannesburg")
#         today = datetime.now(timezone)
#         today_date = today.date()
        
#         # Step 1: Find user's cell by email
#         user_cell = await events_collection.find_one({
#             "Email": {"$regex": f"^{email}$", "$options": "i"}
#         })
        
#         user_name = ""
#         if user_cell:
#             user_name = user_cell.get("Leader", "").strip()
        
#         # Step 2: Build the same query as the endpoint
#         query_conditions = [
#             {"Email": {"$regex": f"^{email}$", "$options": "i"}},
#         ]
        
#         if user_name:
#             query_conditions.extend([
#                 {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
#                 {"Leader at 12": {"$regex": f"{user_name}", "$options": "i"}},
#             ])
        
#         query = {
#             "Event Type": "Cells",
#             "$or": query_conditions
#         }
        
#         # Step 3: Find all matching cells
#         all_cells = await events_collection.find(query).to_list(length=None)
        
#         # Step 4: Process each cell
#         cells_by_category = {
#             "own_cells_by_email": [],
#             "own_cells_by_name": [],
#             "supervised_cells": [],
#             "filtered_out_overdue": [],
#             "filtered_out_complete": []
#         }
        
#         for cell in all_cells:
#             cell_email = cell.get("Email", "").lower().strip()
#             cell_leader = cell.get("Leader", "").strip()
#             cell_name = cell.get("Event Name", "")
#             event_date = parse_event_date(cell.get("Date Of Event"), today_date)
#             status = get_actual_event_status(cell, today_date)
            
#             cell_info = {
#                 "event_name": cell_name,
#                 "leader": cell_leader,
#                 "email": cell_email,
#                 "leader_at_12": cell.get("Leader at 12"),
#                 "day": cell.get("Day"),
#                 "date": str(event_date),
#                 "status": status
#             }
            
#             # Check if should be included
#             should_include = should_include_event(event_date, status, today_date, is_admin=False)
            
#             if not should_include:
#                 if status == 'incomplete':
#                     cells_by_category["filtered_out_overdue"].append(cell_info)
#                 else:
#                     cells_by_category["filtered_out_complete"].append(cell_info)
#                 continue
            
#             # Categorize included cells
#             if cell_email == email.lower():
#                 cells_by_category["own_cells_by_email"].append(cell_info)
#             elif user_name and cell_leader.lower() == user_name.lower():
#                 cells_by_category["own_cells_by_name"].append(cell_info)
#             else:
#                 cells_by_category["supervised_cells"].append(cell_info)
        
#         total_visible = (
#             len(cells_by_category["own_cells_by_email"]) +
#             len(cells_by_category["own_cells_by_name"]) +
#             len(cells_by_category["supervised_cells"])
#         )
        
#         return {
#             "user_email": email,
#             "user_name_found": user_name or "NOT FOUND",
#             "today": today.strftime("%Y-%m-%d"),
#             "query_used": {
#                 "Event Type": "Cells",
#                 "OR_conditions": [
#                     f"Email matches {email}",
#                     f"Leader matches {user_name}" if user_name else "SKIPPED - no name",
#                     f"Leader at 12 contains {user_name}" if user_name else "SKIPPED - no name"
#                 ]
#             },
#             "total_cells_found": len(all_cells),
#             "total_visible_to_user": total_visible,
#             "cells_breakdown": cells_by_category,
#             "issue_diagnosis": {
#                 "has_cell_record": bool(user_cell),
#                 "has_name_in_database": bool(user_name),
#                 "email_query_working": len(cells_by_category["own_cells_by_email"]) > 0,
#                 "name_query_working": len(cells_by_category["own_cells_by_name"]) > 0,
#             }
#         }
        
#     except Exception as e:
#         logging.error(f"Error in test_user_cells: {e}", exc_info=True)
#         return {"error": str(e)}
    
@app.on_event("startup")
async def create_indexes_on_startup():
    print("📌 Creating MongoDB indexes for events...")
    await events_collection.create_index(
        [("Event Type", 1), ("Email", 1), ("Leader", 1), ("Day", 1)],

        name="event_type_email_leader_day_idx"
    )


@app.get("/ping")
async def ping():
    return JSONResponse(content={"message": "Server is alive 🚀"}, status_code=200)

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
        
        print(f"✅ Added UUIDs to {updated_count} events")
        
        return {
            "message": f"Successfully added UUIDs to {updated_count} events",
            "updated_count": updated_count
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
#  END OF EVENTS------------------------------------------------------------------------------


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

@app.put("/profile/{user_id}")
async def update_profile(
    user_id: str, 
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """Complete working profile update endpoint"""
    try:
        print(f"🎯 PROFILE UPDATE ENDPOINT CALLED")
        print(f"🔐 User ID from URL: {user_id}")
        print(f"🔐 Current User ID from Token: {current_user.get('user_id')}")
        
        # Check authorization
        token_user_id = current_user.get("user_id")
        if not token_user_id or token_user_id != user_id:
            print(f"❌ AUTHORIZATION FAILED: {token_user_id} != {user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Get and parse request body
        body = await request.body()
        body_str = body.decode('utf-8')
        print(f"📦 RAW REQUEST BODY: {body_str}")
        
        try:
            update_data = json.loads(body_str)
        except json.JSONDecodeError as e:
            print(f"❌ JSON PARSE ERROR: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

        print(f"📋 PARSED UPDATE DATA: {update_data}")

        # Check if user exists
        existing_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not existing_user:
            print(f"❌ USER NOT FOUND: {user_id}")
            raise HTTPException(status_code=404, detail="User not found")

        # Build update payload - handle all possible field names
        update_payload = {}
        
        # Field mapping from frontend to database
        field_mapping = {
            # Direct mappings
            "name": "name",
            "surname": "surname", 
            "email": "email",
            "date_of_birth": "date_of_birth",
            "home_address": "home_address",
            "phone_number": "phone_number",
            "invited_by": "invited_by",
            "gender": "gender",
            "profile_picture": "profile_picture",
            
            # Alternative field names from frontend
            "dob": "date_of_birth",
            "address": "home_address",
            "invitedBy": "invited_by", 
            "phone": "phone_number"
        }
        
        # Map all fields
        for frontend_field, db_field in field_mapping.items():
            if frontend_field in update_data:
                value = update_data[frontend_field]
                if value is not None and value != "":
                    # Normalize gender values
                    if db_field == "gender":
                        value = normalize_gender_value(value)
                    
                    update_payload[db_field] = value
                    print(f"✅ Mapping {frontend_field} -> {db_field}: {value}")

        # Add update timestamp
        update_payload["updated_at"] = datetime.utcnow().isoformat()
        
        print(f"🚀 FINAL UPDATE PAYLOAD: {update_payload}")

        if not update_payload:
            print("⚠️ No fields to update")
            return {
                "message": "No changes to update",
                "user": format_user_response(existing_user)
            }

        # Perform the update
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)}, 
            {"$set": update_payload}
        )

        print(f"📊 UPDATE RESULT - matched: {result.matched_count}, modified: {result.modified_count}")

        # Fetch and return updated user
        updated_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found after update")

        response_data = format_user_response(updated_user)
        print(f"✅ UPDATE SUCCESSFUL: {response_data}")

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

def normalize_gender_value(gender):
    """Normalize gender values to consistent format"""
    if not gender:
        return gender
    
    gender = str(gender).strip()
    gender_map = {
        'male': 'Male',
        'female': 'Female',
        'm': 'Male',
        'f': 'Female',
        'Male': 'Male',
        'Female': 'Female',
        'Other': 'Other',
        'Prefer not to say': 'Prefer not to say'
    }
    
    return gender_map.get(gender, gender)

def format_user_response(user):
    """Format user document for response"""
    return {
        "id": str(user["_id"]),
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "date_of_birth": user.get("date_of_birth", ""),
        "home_address": user.get("home_address", ""),
        "invited_by": user.get("invited_by", ""),
        "phone_number": user.get("phone_number", ""),
        "email": user.get("email", ""),
        "gender": normalize_gender_value(user.get("gender", "")),
        "role": user.get("role", "user"),
        "profile_picture": user.get("profile_picture", ""),
    }

# Debug endpoint
@app.put("/profile/{user_id}/debug")
async def debug_profile_update(
    user_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """Debug endpoint to see what's happening"""
    try:
        body = await request.body()
        body_str = body.decode('utf-8')
        
        return {
            "message": "Debug info",
            "user_id_from_url": user_id,
            "user_id_from_token": current_user.get("user_id"),
            "authorized": current_user.get("user_id") == user_id,
            "raw_body": body_str,
            "current_user_email": current_user.get("email")
        }
    except Exception as e:
        return {"error": str(e)}

# Test endpoint
@app.get("/profile/{user_id}/test")
async def test_profile_access(
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Test if profile access works"""
    return {
        "message": "Profile test",
        "user_id": user_id,
        "current_user": current_user.get("user_id"),
        "authorized": current_user.get("user_id") == user_id
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
            query["Birthday"] = dob
        if location:
            query["Address"] = {"$regex": location, "$options": "i"}
        if leader:
            query["$or"] = [
                {"Leader @1": {"$regex": leader, "$options": "i"}},
                {"Leader @12": {"$regex": leader, "$options": "i"}},
                {"Leader @144": {"$regex": leader, "$options": "i"}},
                {"Leader @1728": {"$regex": leader, "$options": "i"}}
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
                "Number": person.get("Number", ""),
                "Email": person.get("Email", ""),
                "Address": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "Birthday": person.get("Birthday", ""),
                "InvitedBy": person.get("InvitedBy", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", ""),
                "Stage": person.get("Stage", "Win"),
                "Date Created": person.get("Date Created") or datetime.utcnow().isoformat(),
                "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
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
        print(f"Error fetching people: {e}")
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
            "Number": person.get("Number", ""),
            "Email": person.get("Email", ""),
            "Address": person.get("Address", ""),
            "Gender": person.get("Gender", ""),
            "Birthday": person.get("Birthday", ""),
            "InvitedBy": person.get("InvitedBy", ""),
            "Leader @1": person.get("Leader @1", ""),
            "Leader @12": person.get("Leader @12", ""),
            "Leader @144": person.get("Leader @144", ""),
            "Leader @1728": person.get("Leader @1728", ""),
            "Stage": person.get("Stage", "Win"),
            "Date Created": person.get("Date Created") or datetime.utcnow().isoformat(),
            "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
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
        "Number": data.get("Number") or data.get("number", ""),
        "Email": data.get("Email") or data.get("email", ""),
        "Address": data.get("Address") or data.get("address", ""),
        "Birthday": data.get("Birthday") or data.get("birthday") or data.get("dob", ""),
        "Gender": data.get("Gender") or data.get("gender", ""),
        "InvitedBy": data.get("InvitedBy") or data.get("invitedBy", ""),
        "Leader @1": data.get("Leader @1") or data.get("leader1", ""),
        "Leader @12": data.get("Leader @12") or data.get("leader12", ""),
        "Leader @144": data.get("Leader @144") or data.get("leader144", ""),
        "Leader @1728": data.get("Leader @1728") or data.get("leader1728", ""),
        "Stage": data.get("Stage") or data.get("stage", "Win"),
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
            "Number": updated_person.get("Number", ""),
            "Email": updated_person.get("Email", ""),
            "Address": updated_person.get("Address", ""),
            "Gender": updated_person.get("Gender", ""),
            "Birthday": updated_person.get("Birthday", ""),
            "InvitedBy": updated_person.get("InvitedBy", ""),
            "Leader @1": updated_person.get("Leader @1", ""),
            "Leader @12": updated_person.get("Leader @12", ""),
            "Leader @144": updated_person.get("Leader @144", ""),
            "Leader @1728": updated_person.get("Leader @1728", ""),
            "Stage": updated_person.get("Stage", "Win"),
            "UpdatedAt": updated_person.get("UpdatedAt"),
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
        leader1 = person_data.leaders[0] if len(person_data.leaders) > 0 else ""
        leader12 = person_data.leaders[1] if len(person_data.leaders) > 1 else ""
        leader144 = person_data.leaders[2] if len(person_data.leaders) > 2 else ""
        leader1728 = person_data.leaders[3] if len(person_data.leaders) > 3 else ""

        # Prepare the document
        person_doc = {
            "Name": person_data.name.strip(),
            "Surname": person_data.surname.strip(),
            "Email": email,
            "Number": person_data.number.strip(),
            "Address": person_data.address.strip(),
            "Gender": person_data.gender.strip(),
            "Birthday": person_data.dob.strip(),
            "InvitedBy": person_data.invitedBy.strip(),
            "Leader @1": leader1,
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @1728": leader1728,
            "Stage": person_data.stage or "Win",
            "Date Created": datetime.utcnow().isoformat(),
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
            "Number": person_doc["Number"],
            "Gender": person_doc["Gender"],
            "Birthday": person_doc["Birthday"],
            "Address": person_doc["Address"],
            "InvitedBy": person_doc["InvitedBy"],
            "Leader @1": person_doc["Leader @1"],
            "Leader @12": person_doc["Leader @12"],
            "Leader @144": person_doc["Leader @144"],
            "Leader @1728": person_doc["Leader @1728"],
            "Stage": person_doc["Stage"],
            "Date Created": person_doc["Date Created"],
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


@app.get("/people/search-fast")
async def search_people_fast(
    query: str = Query(..., min_length=2),
    limit: int = Query(25, le=50)
):
    """
    FAST search endpoint for autocomplete - optimized for signup form
    Uses simple regex matching and returns minimal fields
    """
    try:
        if not query or len(query) < 2:
            return {"results": []}
        
        # Simple regex search on name fields - much faster than complex queries
        search_regex = {"$regex": query.strip(), "$options": "i"}
        
        # Only fetch essential fields for autocomplete
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1, 
            "Email": 1,
            "Phone": 1,
            "Leader @1": 1,
            "Leader @12": 1,
            "Leader @144": 1,
            "Leader @1728": 1
        }
        
        cursor = people_collection.find({
            "$or": [
                {"Name": search_regex},
                {"Surname": search_regex},
                {"Email": search_regex},
                {"$expr": {
                    "$regexMatch": {
                        "input": {"$concat": ["$Name", " ", "$Surname"]},
                        "regex": query.strip(),
                        "options": "i"
                    }
                }}
            ]
        }, projection).limit(limit)
        
        results = []
        async for person in cursor:
            results.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Phone": person.get("Phone", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"results": results}
        
    except Exception as e:
        print(f"Error in fast search: {e}")
        return {"results": []}

@app.get("/people/all-minimal")
async def get_all_people_minimal():
    """
    Get all people with minimal fields for client-side caching
    Much faster than full document fetch
    """
    try:
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Phone": 1
        }
        
        cursor = people_collection.find({}, projection).limit(1000)  # Reasonable limit
        
        people = []
        async for person in cursor:
            people.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Phone": person.get("Phone", "")
            })
        
        return {"people": people}
        
    except Exception as e:
        print(f"Error fetching minimal people: {e}")
        return {"people": []}

@app.get("/people/leaders-only")
async def get_leaders_only():
    """
    Get only people who are leaders (have people under them)
    Optimized for signup form where we mostly need leaders
    """
    try:
        # Find people who appear as leaders in other people's records
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"Leader @1": {"$exists": True, "$ne": ""}},
                        {"Leader @12": {"$exists": True, "$ne": ""}},
                        {"Leader @144": {"$exists": True, "$ne": ""}},
                        {"Leader @1728": {"$exists": True, "$ne": ""}}
                    ]
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "Name": 1,
                    "Surname": 1,
                    "Email": 1,
                    "Phone": 1,
                    "Leader @1": 1,
                    "Leader @12": 1,
                    "Leader @144": 1,
                    "Leader @1728": 1
                }
            },
            {"$limit": 500}  # Leaders only, so smaller set
        ]
        
        cursor = people_collection.aggregate(pipeline)
        leaders = []
        
        async for person in cursor:
            leaders.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Phone": person.get("Phone", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"leaders": leaders}
        
    except Exception as e:
        print(f"Error fetching leaders: {e}")
        return {"leaders": []}


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
     
# --- GET all task types ---
@app.get("/tasktypes", response_model=List[TaskTypeOut])
async def get_task_types():
    try:
        cursor = tasktypes_collection.find().sort("name", 1)
        types = []
        async for t in cursor:
            types.append(task_type_serializer(t))
        return types
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- POST new task type ---
@app.post("/tasktypes", response_model=TaskTypeOut)
async def create_task_type(task: TaskTypeIn):
    try:
        # Check if already exists
        existing = await tasktypes_collection.find_one({"name": task.name})
        if existing:
            raise HTTPException(status_code=400, detail="Task type already exists.")

        new_task = {"name": task.name}
        result = await tasktypes_collection.insert_one(new_task)
        created = await tasktypes_collection.find_one({"_id": result.inserted_id})
        return task_type_serializer(created)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Helper to convert ObjectId to string
def serialize_doc(doc):
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

# --- Update route ---
@app.put("/tasks/{task_id}")
async def update_task(task_id: str, updated_task: dict):
    try:
        obj_id = ObjectId(task_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid task ID")
    
    # Check if task exists
    task = await db["tasks"].find_one({"_id": obj_id})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Prepare update data - only include fields that should be updated
    update_data = {}
    
    # Map frontend fields to backend fields
    if "name" in updated_task:
        update_data["name"] = updated_task["name"]
    
    if "taskType" in updated_task:
        update_data["taskType"] = updated_task["taskType"]
    
    if "contacted_person" in updated_task:
        update_data["contacted_person"] = updated_task["contacted_person"]
    
    if "followup_date" in updated_task:
        # Ensure it's a proper datetime string or convert it
        try:
            if isinstance(updated_task["followup_date"], str):
                update_data["followup_date"] = updated_task["followup_date"]
            else:
                update_data["followup_date"] = updated_task["followup_date"]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    
    if "status" in updated_task:
        update_data["status"] = updated_task["status"]
    
    if "type" in updated_task:
        update_data["type"] = updated_task["type"]
    
    if "assignedfor" in updated_task:
        update_data["assignedfor"] = updated_task["assignedfor"]
    
    # Add updated timestamp
    update_data["updated_at"] = datetime.utcnow().isoformat()
    
    # Update the task
    try:
        result = await db["tasks"].update_one(
            {"_id": obj_id}, 
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            # Check if task actually exists but nothing changed
            if result.matched_count > 0:
                # Task exists but no changes were made
                updated_task_in_db = await db["tasks"].find_one({"_id": obj_id})
                return {"updatedTask": serialize_doc(updated_task_in_db)}
            else:
                raise HTTPException(status_code=404, detail="Task not found")
        
        # Fetch and return the updated task
        updated_task_in_db = await db["tasks"].find_one({"_id": obj_id})
        return {"updatedTask": serialize_doc(updated_task_in_db)}
        
    except Exception as e:
        print(f"Error updating task: {str(e)}")  # Log the error
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

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
    

#consolidation

async def get_event_summary_stats(event_id: str):
    """Get consolidation and new people statistics for an event"""
    try:
        consolidations_collection = db["consolidations"]
        
        # Get all consolidations for this event
        event_consolidations = await consolidations_collection.find({
            "event_id": event_id
        }).to_list(length=None)
        
        # Count by decision type
        first_time_count = sum(1 for c in event_consolidations if c.get("decision_type") == "first_time")
        recommitment_count = sum(1 for c in event_consolidations if c.get("decision_type") == "recommitment")
        
        # Get event to count total attendees
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        total_attendees = len(event.get("attendees", [])) if event else 0
        
        # Count new people (attendees not in people collection)
        new_people_count = 0
        if event:
            for attendee in event.get("attendees", []):
                email = attendee.get("email") or attendee.get("person_email")
                if email:
                    existing_person = await people_collection.find_one({
                        "Email": {"$regex": f"^{email}$", "$options": "i"}
                    })
                    if not existing_person:
                        new_people_count += 1
        
        return {
            "total_attendees": total_attendees,
            "first_time_decisions": first_time_count,
            "recommitments": recommitment_count,
            "total_decisions": first_time_count + recommitment_count,
            "new_people": new_people_count,
            "decision_rate": round(((first_time_count + recommitment_count) / total_attendees) * 100, 1) if total_attendees > 0 else 0
        }
    except Exception as e:
        print(f"Error calculating event stats: {e}")
        return {}
    
@app.post("/consolidations")
async def create_consolidation(
    consolidation: ConsolidationCreate,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new consolidation record and associated task assigned to the leader
    """
    try:
        consolidation_id = str(ObjectId())
        
        print(f"🎯 Creating consolidation for: {consolidation.person_name} {consolidation.person_surname}")
        print(f"👥 Assigned to leader: {consolidation.assigned_to} (email: {consolidation.assigned_to_email})")
        
        # 1. Create or find the person
        person_email = consolidation.person_email
        if not person_email:
            person_email = f"{consolidation.person_name.lower()}.{consolidation.person_surname.lower()}@consolidation.temp"
        
        existing_person = await people_collection.find_one({
            "$or": [
                {"Email": person_email},
                {"Name": consolidation.person_name, "Surname": consolidation.person_surname}
            ]
        })
        
        person_id = None
        if existing_person:
            person_id = str(existing_person["_id"])
            print(f"✅ Found existing person: {person_id}")
            # Update existing person
            update_data = {
                "Stage": "Consolidate",
                "UpdatedAt": datetime.utcnow().isoformat(),
                "DecisionType": consolidation.decision_type.value,
                "DecisionDate": consolidation.decision_date,
            }
            
            # Safely handle decision history
            existing_history = existing_person.get("DecisionHistory", [])
            if consolidation.decision_type == DecisionType.RECOMMITMENT:
                existing_history.append({
                    "type": "recommitment",
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id
                })
                update_data["DecisionHistory"] = existing_history
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0) + 1
                update_data["LastDecisionDate"] = consolidation.decision_date
            else:
                existing_history.append({
                    "type": "first_time", 
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id
                })
                update_data["DecisionHistory"] = existing_history
                update_data["FirstDecisionDate"] = consolidation.decision_date
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0)
            
            await people_collection.update_one(
                {"_id": ObjectId(person_id)},
                {"$set": update_data}
            )
        else:
            # Create new person
            person_doc = {
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Stage": "Consolidate",
                "DecisionType": consolidation.decision_type.value,
                "DecisionDate": consolidation.decision_date,
                "Date Created": datetime.utcnow().isoformat(),
                "UpdatedAt": datetime.utcnow().isoformat(),
                "InvitedBy": current_user.get("email", ""),
                "Leader @1": consolidation.leaders[0] if len(consolidation.leaders) > 0 else "",
                "Leader @12": consolidation.leaders[1] if len(consolidation.leaders) > 1 else "",
                "Leader @144": consolidation.leaders[2] if len(consolidation.leaders) > 2 else "",
                "Leader @1728": consolidation.leaders[3] if len(consolidation.leaders) > 3 else "",
            }
            
            # Add consolidation-specific fields
            decision_history = [{
                "type": consolidation.decision_type.value,
                "date": consolidation.decision_date,
                "consolidation_id": consolidation_id
            }]
            
            person_doc["DecisionHistory"] = decision_history
            person_doc["TotalRecommitments"] = 1 if consolidation.decision_type == DecisionType.RECOMMITMENT else 0
            
            if consolidation.decision_type == DecisionType.FIRST_TIME:
                person_doc["FirstDecisionDate"] = consolidation.decision_date
            else:
                person_doc["LastDecisionDate"] = consolidation.decision_date
            
            result = await people_collection.insert_one(person_doc)
            person_id = str(result.inserted_id)
            print(f"✅ Created new person: {person_id}")

        # 2. FIND OR CREATE LEADER'S USER ACCOUNT
        leader_email = consolidation.assigned_to_email
        leader_user_id = None
        
        if not leader_email:
            # Try to find leader's email from people collection
            leader_person = await people_collection.find_one({
                "$or": [
                    {"Name": consolidation.assigned_to},
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, consolidation.assigned_to]}}
                ]
            })
            if leader_person:
                leader_email = leader_person.get("Email")
                print(f"✅ Found leader email from people collection: {leader_email}")
        
        if leader_email:
            # Find leader's user account
            leader_user = await users_collection.find_one({"email": leader_email})
            if leader_user:
                leader_user_id = str(leader_user["_id"])
                print(f"✅ Found leader user account: {leader_email} (ID: {leader_user_id})")
            else:
                print(f"⚠️ Leader {consolidation.assigned_to} has no user account with email: {leader_email}")
        else:
            print(f"⚠️ Could not find email for leader: {consolidation.assigned_to}")

        # 3. Create task assigned to the leader
        decision_display_name = "First Time Decision" if consolidation.decision_type == DecisionType.FIRST_TIME else "Recommitment"
        
        # Use leader's email for assignment, fallback to leader's name
        assigned_for = leader_email if leader_email else consolidation.assigned_to
        
        task_doc = {
            "name": f"Consolidation: {consolidation.person_name} {consolidation.person_surname} ({decision_display_name})",
            "taskType": "consolidation",
            "description": f"Follow up with {consolidation.person_name} {consolidation.person_surname} who made a {decision_display_name.lower()} on {consolidation.decision_date}",
            "followup_date": datetime.utcnow().isoformat(),
            "status": "Open",
            "assignedfor": assigned_for,  # Assign to leader's email or name
            "assigned_to_email": leader_email,
            "assigned_to_user_id": leader_user_id,
            "type": "followup",
            "priority": "high",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "decision_type": consolidation.decision_type.value,
            "decision_display_name": decision_display_name,
            "contacted_person": {
                "name": f"{consolidation.person_name} {consolidation.person_surname}",
                "email": person_email,
                "phone": consolidation.person_phone or ""
            },
            "created_at": datetime.utcnow().isoformat(),
            "created_by": current_user.get("email", ""),
            "is_consolidation_task": True,
            "leader_assigned": consolidation.assigned_to
        }

        task_result = await tasks_collection.insert_one(task_doc)
        task_id = str(task_result.inserted_id)
        print(f"✅ Created consolidation task: {task_id} assigned to {assigned_for}")

        # 4. Add to event attendees
        if consolidation.event_id and ObjectId.is_valid(consolidation.event_id):
            attendee_record = {
                "id": person_id,
                "name": consolidation.person_name,
                "fullName": f"{consolidation.person_name} {consolidation.person_surname}",
                "email": person_email,
                "phone": consolidation.person_phone or "",
                "decision": consolidation.decision_type.value,
                "decision_display": decision_display_name,
                "time": datetime.utcnow().isoformat(),
                "is_consolidation": True,
                "consolidation_id": consolidation_id,
                "assigned_leader": consolidation.assigned_to,
                "assigned_leader_email": leader_email
            }

            await events_collection.update_one(
                {"_id": ObjectId(consolidation.event_id)},
                {
                    "$push": {"attendees": attendee_record},
                    "$inc": {"total_attendance": 1}
                }
            )
            print(f"✅ Added to event attendees: {consolidation.event_id}")

        # 5. Create consolidation record
        consolidation_doc = {
            "_id": ObjectId(consolidation_id),
            "person_id": person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "person_email": person_email,
            "person_phone": consolidation.person_phone,
            "decision_type": consolidation.decision_type.value,
            "decision_display_name": decision_display_name,
            "decision_date": consolidation.decision_date,
            "assigned_to": consolidation.assigned_to,
            "assigned_to_email": leader_email,
            "assigned_to_user_id": leader_user_id,
            "event_id": consolidation.event_id,
            "notes": consolidation.notes,
            "created_by": current_user.get("email", ""),
            "created_at": datetime.utcnow().isoformat(),
            "status": "active",
            "task_id": task_id
        }

        consolidations_collection = db["consolidations"]
        await consolidations_collection.insert_one(consolidation_doc)
        print(f"✅ Created consolidation record: {consolidation_id}")

        return {
            "message": f"{decision_display_name} recorded successfully and assigned to {consolidation.assigned_to}",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "task_id": task_id,
            "decision_type": consolidation.decision_type.value,
            "assigned_to": consolidation.assigned_to,
            "assigned_to_email": leader_email,
            "leader_user_id": leader_user_id,
            "success": True
        }

    except Exception as e:
        print(f"❌ Error creating consolidation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creating consolidation: {str(e)}")

@app.get("/tasks/leader/{leader_email}")
async def get_leader_tasks(
    leader_email: str,
    current_user: dict = Depends(get_current_user)
):
    """Get all consolidation tasks assigned to a specific leader"""
    try:
        # Find consolidation tasks assigned to this leader
        tasks = await tasks_collection.find({
            "is_consolidation_task": True,
            "$or": [
                {"assigned_to_email": leader_email},
                {"assignedfor": leader_email},
                {"assignedfor": {"$regex": f"^{leader_email}$", "$options": "i"}},
                {"leader_assigned": {"$regex": f"^{leader_email}$", "$options": "i"}}
            ]
        }).to_list(length=None)
        
        # Format response
        formatted_tasks = []
        for task in tasks:
            task["_id"] = str(task["_id"])
            formatted_tasks.append(task)
        
        return {
            "leader_email": leader_email,
            "total_tasks": len(formatted_tasks),
            "tasks": formatted_tasks
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/consolidations")
async def get_consolidations(
    assigned_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    perPage: int = Query(50, ge=1),
    current_user: dict = Depends(get_current_user)
):
    """
    Get consolidation records with filtering
    """
    try:
        query = {}
        
        if assigned_to:
            query["assigned_to"] = assigned_to
        if status:
            query["status"] = status
        
        consolidations_collection = db["consolidations"]
        skip = (page - 1) * perPage
        
        cursor = consolidations_collection.find(query).skip(skip).limit(perPage)
        consolidations = []
        
        async for consolidation in cursor:
            consolidation["_id"] = str(consolidation["_id"])
            # Get person details
            person = await people_collection.find_one({"_id": ObjectId(consolidation["person_id"])})
            if person:
                consolidation["person_details"] = {
                    "name": person.get("Name", ""),
                    "surname": person.get("Surname", ""),
                    "email": person.get("Email", ""),
                    "phone": person.get("Number", ""),
                    "stage": person.get("Stage", "")
                }
            consolidations.append(consolidation)
        
        total = await consolidations_collection.count_documents(query)
        
        return {
            "consolidations": consolidations,
            "total": total,
            "page": page,
            "perPage": perPage
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/consolidations/{consolidation_id}")
async def update_consolidation(
    consolidation_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update consolidation status or details
    """
    try:
        if not ObjectId.is_valid(consolidation_id):
            raise HTTPException(status_code=400, detail="Invalid consolidation ID")
        
        consolidations_collection = db["consolidations"]
        consolidation = await consolidations_collection.find_one({"_id": ObjectId(consolidation_id)})
        
        if not consolidation:
            raise HTTPException(status_code=404, detail="Consolidation not found")
        
        # Update consolidation
        update_data["updated_at"] = datetime.utcnow().isoformat()
        await consolidations_collection.update_one(
            {"_id": ObjectId(consolidation_id)},
            {"$set": update_data}
        )
        
        # If status is completed, update person's stage
        if update_data.get("status") == "completed":
            await people_collection.update_one(
                {"_id": ObjectId(consolidation["person_id"])},
                {"$set": {"Stage": "Disciple", "UpdatedAt": datetime.utcnow().isoformat()}}
            )
            
            # Also update the associated task
            if consolidation.get("task_id"):
                await tasks_collection.update_one(
                    {"_id": ObjectId(consolidation["task_id"])},
                    {"$set": {"status": "completed"}}
                )
        
        return {"message": "Consolidation updated successfully", "success": True}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/consolidations/stats")
async def get_consolidation_stats(
    period: str = Query("monthly", regex="^(daily|weekly|monthly|yearly)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get consolidation statistics"""
    try:
        stats_collection = db["consolidation_stats"]
        
        if period == "daily":
            date_key = datetime.utcnow().date().isoformat()
            query = {"date": date_key, "type": "daily"}
        elif period == "weekly":
            week_key = datetime.utcnow().strftime("%Y-W%U")
            query = {"week": week_key, "type": "weekly"}
        elif period == "monthly":
            month_key = datetime.utcnow().strftime("%Y-%m")
            query = {"month": month_key, "type": "monthly"}
        else:  # yearly
            year_key = datetime.utcnow().strftime("%Y")
            query = {"year": year_key, "type": "yearly"}
        
        stats = await stats_collection.find_one(query)
        
        if not stats:
            return {
                "period": period,
                "total_consolidations": 0,
                "first_time_count": 0,
                "recommitment_count": 0,
                "first_time_percentage": 0,
                "recommitment_percentage": 0
            }
        
        total = stats.get("total_consolidations", 0)
        first_time = stats.get("first_time_count", 0)
        recommitment = stats.get("recommitment_count", 0)
        
        return {
            "period": period,
            "total_consolidations": total,
            "first_time_count": first_time,
            "recommitment_count": recommitment,
            "first_time_percentage": round((first_time / total) * 100, 1) if total > 0 else 0,
            "recommitment_percentage": round((recommitment / total) * 100, 1) if total > 0 else 0
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/consolidations/person/{person_id}")
async def get_person_consolidation_history(
    person_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get consolidation history for a specific person"""
    try:
        if not ObjectId.is_valid(person_id):
            raise HTTPException(status_code=400, detail="Invalid person ID")
        
        # Get person details
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        
        # Get all consolidations for this person
        consolidations_collection = db["consolidations"]
        consolidations = await consolidations_collection.find({
            "person_id": person_id
        }).sort("decision_date", -1).to_list(length=None)
        
        for consolidation in consolidations:
            consolidation["_id"] = str(consolidation["_id"])
        
        return {
            "person_details": {
                "name": person.get("Name", ""),
                "surname": person.get("Surname", ""),
                "email": person.get("Email", ""),
                "phone": person.get("Number", ""),
                "first_decision_date": person.get("FirstDecisionDate"),
                "last_decision_date": person.get("LastDecisionDate"),
                "total_recommitments": person.get("TotalRecommitments", 0),
                "current_stage": person.get("Stage", "")
            },
            "consolidation_history": consolidations,
            "total_consolidations": len(consolidations)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/events/{event_id}/consolidations")
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

@app.get("/events/{event_id}/new-people")
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