import os
from datetime import datetime, timedelta, time, date
from bson import ObjectId
import re
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
from typing import Dict, List, Optional
import asyncio

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

@app.get("/")
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

CACHE_DURATION_MINUTES = 1440  
BACKGROUND_LOAD_DELAY = 2  
@app.on_event("startup")
async def startup_event():
    """Start background loading of all people on startup"""
    print(" Starting background load of ALL people...")
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
        
        print("BACKGROUND: Starting to load ALL people...")
        
        all_people_data = []
        total_count = await people_collection.count_documents({})
        people_cache["total_in_database"] = total_count
        print(f"BACKGROUND: Total people in database: {total_count}")
        
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
                    "Gender":1, #getting gender as well
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
                        "Gender": person.get("Gender",""), #getting gender
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
                
                print(f"BACKGROUND: Batch {page} - {len(transformed_batch)} people (Total: {total_loaded}/{total_count}, Progress: {progress:.1f}%)")
                
                page += 1
                
                # Small delay to prevent overwhelming the database
                await asyncio.sleep(0.1)
                
            except Exception as batch_error:
                print(f"BACKGROUND: Error in batch {page}: {str(batch_error)}")
                break
        
        # Update cache with complete dataset
        people_cache["data"] = all_people_data
        people_cache["last_updated"] = datetime.utcnow().isoformat()
        people_cache["expires_at"] = (datetime.utcnow() + timedelta(minutes=CACHE_DURATION_MINUTES)).isoformat()
        people_cache["is_loading"] = False
        people_cache["load_progress"] = 100
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"BACKGROUND: Successfully loaded ALL {len(all_people_data)} people in {duration:.2f} seconds")
        print(f"BACKGROUND: Cache ready with {len(all_people_data)} people")
        
    except Exception as e:
        people_cache["is_loading"] = False
        people_cache["last_error"] = str(e)
        print(f"BACKGROUND: Failed to load people: {str(e)}")

# Update your cache endpoint to return the expected structure
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

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "cache_status": {
            "has_data": len(people_cache["data"]) > 0,
            "data_count": len(people_cache["data"]),
            "is_loading": people_cache["is_loading"],
            "last_updated": people_cache["last_updated"]
        }
    }

@app.get("/people/simple")
async def get_people_simple(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000)
):
    """Simple people endpoint as fallback"""
    try:
        skip = (page - 1) * per_page
        
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Number": 1,
            "Gender": 1, #getting gender is
            "Leader @1": 1,
            "Leader @12": 1,
            "Leader @144": 1
        }
        
        cursor = people_collection.find({}, projection).skip(skip).limit(per_page)
        people_list = await cursor.to_list(length=per_page)
        
        formatted_people = []
        for person in people_list:
            formatted_people.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Gender": person.get("Gender",""),
                "Number": person.get("Number", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "FullName": f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            })
        
        total_count = await people_collection.count_documents({})
        
        return {
            "success": True,
            "results": formatted_people,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_count": total_count,
                "has_more": (skip + len(formatted_people)) < total_count
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "results": []
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
    
    # USE BACKGROUND-LOADED CACHE FOR LEADER ASSIGNMENT
    inviter_full_name = user.invited_by.strip()
    leader1 = ""
    leader12 = ""
    leader144 = ""
    leader1728 = ""
    
    if inviter_full_name:
        print(f"Looking for inviter in background cache: '{inviter_full_name}'")
        
        # Search in background-loaded cache (contains ALL people)
        cached_inviter = None
        for person in people_cache["data"]:
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            if (full_name.lower() == inviter_full_name.lower() or 
                person.get('Name', '').lower() == inviter_full_name.lower()):
                cached_inviter = person
                break
        
        if cached_inviter:
            print(f"Found inviter in background cache: {cached_inviter.get('FullName')}")
            # checking if gender is matching so it's not it can just assign them a leader at 12
            isGenderMatching = cached_inviter.get("Gender", "") == user.gender.capitalize()

            print(cached_inviter)
            print(isGenderMatching)
            print(cached_inviter.get("Gender", ""),user.gender.capitalize())

            if  not isGenderMatching:
                if user.gender == "male":
                    leader1 = "Gavin Enslin"
                else:
                    leader1 = "Vicky Enslin"
                leader12 = ""   
                leader144 = ""
                leader1728 = ""     
            else: #if gender is matching should go on as usual
                # Get the inviter's leader hierarchy from cache
                inviter_leader1 = cached_inviter.get("Leader @1", "")
                inviter_leader12 = cached_inviter.get("Leader @12", "")
                inviter_leader144 = cached_inviter.get("Leader @144", "")
                inviter_leader1728 = cached_inviter.get("Leader @1728", "")
                print(cached_inviter,inviter_leader1,inviter_leader12,inviter_leader144,inviter_leader1728)
                
                
                # Determine what level the inviter is at and set leaders accordingly
                if inviter_leader1728:
                    print("1")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader144:
                    print("2")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader12:
                    print("3")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_full_name
                    leader1728 = ""
                elif inviter_leader1:
                    print("4")
                    leader1 = inviter_leader1
                    leader12 = inviter_full_name
                    leader144 = ""
                    leader1728 = ""
                else:
                    print("5")
                    leader1 = user.gender
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
                
                logger.info(f"Leader hierarchy set for {email}: L1={leader1}, L12={leader12}, L144={leader144}, L1728={leader1728}")
        else:
            print("6")
            print(f"Inviter '{inviter_full_name}' not found in background cache")
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
        
        # ADD THE NEW PERSON TO BACKGROUND CACHE
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
        print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")
        
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
    person = await people_collection.find_one({"Email":user.email}) or {}


    full_name = f"{person.get('Name') or ''} {person.get('Surname') or ''}"
    print("FULL NAME",full_name)
    is_Leader = await events_collection.find_one({"$or":[{"Email":user.email,"Event Type":"Cells"},{"Leader":full_name,"Event Type":"Cells"}]})
    is_Leader = bool(is_Leader)
    

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
    },
    "leaders":{
        'leaderAt1':person.get("Leader @1",""),
        'leaderAt12':person.get("Leader @12",""),
        'leaderAt144':person.get("Leader @144",""),
    },
    "isLeader": is_Leader
    }

@app.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, background_tasks: BackgroundTasks):
    logger.info(f"Forgot password requested for email: {payload.email}")
    
    # Find the user by email
    user = await users_collection.find_one({"email": payload.email})
    
    if not user:
        logger.info(f"Forgot password - email not found: {payload.email}")
        return {"message": "If your email exists, a reset link has been sent."}

    # Create a reset token valid for 1 hour
    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1)
    )
    
    reset_link = f"https://new-active-teams.netlify.app/reset-password?token={reset_token}"
    recipient_name = user.get("name", "there")  # Default to "there" if name missing

    logger.info(f"Reset link generated for {payload.email}")

    # Add background task with all required arguments
    background_tasks.add_task(send_reset_email, payload.email, recipient_name, reset_link)
    logger.info(f"Reset email task scheduled for {payload.email}")

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
        
        print(f"Looking for event type: '{event_type_name}'")
        
        # CRITICAL FIX: Handle "CELLS" and "ALL CELLS" explicitly
        if event_type_name.upper() in ["CELLS", "ALL CELLS"]:
            # For CELLS, we don't need to look up an event type - it's built-in
            event_data["eventTypeId"] = "CELLS_BUILT_IN"
            event_data["eventTypeName"] = "CELLS"
            event_data["hasPersonSteps"] = True  # Enable leader fields
            event_data["isGlobal"] = False
            print(f"Using built-in CELLS event type with leader fields enabled")
        else:
            # For other event types, look them up in the database
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

        # Use the day value from frontend
        print(f"Using day value from frontend: {event_data.get('day')}")

        # Ensure leader fields are properly saved
        event_data.setdefault("eventLeaderName", event_data.get("eventLeader", ""))
        event_data.setdefault("eventLeaderEmail", event_data.get("eventLeaderEmail", ""))
        
        # CRITICAL: Keep leader fields for CELLS events
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
            "message": "Event created successfully", 
            "id": str(result.inserted_id),
            "event": {
                "id": str(created_event["_id"]),
                "eventName": created_event.get("eventName"),
                "eventLeader": created_event.get("eventLeader"),
                "eventLeaderName": created_event.get("eventLeaderName"),
                "eventLeaderEmail": created_event.get("eventLeaderEmail"),
                "day": created_event.get("day"),
                "date": created_event.get("date"),
                "location": created_event.get("location"),
                "eventTypeName": created_event.get("eventTypeName"),
                "isGlobal": created_event.get("isGlobal"),
                "hasPersonSteps": created_event.get("hasPersonSteps"),
                "leader1": created_event.get("leader1"),
                "leader12": created_event.get("leader12")
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
    isLeaderAt12: Optional[bool] = Query(None) #getting if leader at 12 from frontend
):
    """
    Get cell events with proper separation between personal and disciples' cells
    """
    try:
        print("=" * 100)
        print("GET /events/cells REQUEST")
        print(isLeaderAt12)
        role = current_user.get("role", "user").lower()
        user_email = current_user.get("email", "")


        #using from person found in people 
        person = await people_collection.find_one({"Email":user_email})  
        first_name = person.get('Name', '').strip()
        surname = person.get('Surname', '').strip()
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

        # BASE QUERY
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

        # SEARCH FILTER
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
                    {"LeaderAt12": {"$regex": search_term, "$options": "i"}},
                ]
            }) 

        # ROLE-BASED FILTERING
        
        # ADMIN MODE
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

        # LEADER AT 12 MODE - CASE INSENSITIVE FIELD NAMES
        elif is_actual_leader_at_12 and leader_at_12_view:
            print(f"LEADER AT 12 MODE")
            
            # Check what view we want
            want_personal_view = (show_personal_cells or personal)
            want_disciples_view = show_all_authorized
            
            print(f"   View preferences - Personal: {want_personal_view}, Disciples: {want_disciples_view}")
            
            if want_personal_view:
                # PERSONAL: Only their own cells
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
                # VIEW ALL: Only disciples' cells (where they are Leader at 12)
                print("   VIEW ALL MODE: Disciples' cells only (where user is Leader at 12)")
                
                # DYNAMIC SOLUTION: Build comprehensive name variations for ANY Leader at 12
                name_variations = []
                
                # 1. Current user's full name from profile
                name_variations.append(user_name)
                
                # 2. Try different name format combinations
                name_variations.extend([
                    f"{first_name} {surname}",  # Standard format
                    first_name,  # Just first name
                    user_name.replace(" ", ""),  # No spaces
                    user_name.replace(" ", "-"),  # Hyphenated
                ])
                
                # 3. Handle hyphenated names dynamically
                if "-" in first_name:
                    hyphen_parts = first_name.split("-")
                    # Try different combinations
                    name_variations.extend([
                        f"{hyphen_parts[0]} {surname}",  # First part only
                        f"{hyphen_parts[0]}-{surname}",  # First part hyphenated with surname
                        f"{first_name} {surname}",  # Full hyphenated name
                    ])
                    # Also try each part of the hyphenated name
                    for part in hyphen_parts:
                        if part and part.strip():
                            name_variations.append(f"{part} {surname}")
                
                # 4. Handle multiple names (e.g., "Nash Bobo Mbankumuna")
                name_parts = user_name.split()
                if len(name_parts) > 2:
                    # Try first + last name
                    name_variations.append(f"{name_parts[0]} {name_parts[-1]}")
                    # Try first name only
                    name_variations.append(name_parts[0])
                    # Try all combinations for names with middle names
                    for i in range(1, len(name_parts)):
                        combined_name = " ".join(name_parts[:i+1])
                        name_variations.append(combined_name)
                
                # 5. Remove duplicates and empty values
                name_variations = list(set([name for name in name_variations if name and name.strip()]))
                
                print(f"   Leader '{user_name}' - Trying to match Leader at 12 with variations: {name_variations}")
                
                # Build OR conditions for all name variations
                or_conditions = []
                
                for name_variant in name_variations:
                    safe_name = re.escape(name_variant)
                    print("LEADER AT 12 NAME",safe_name)    
                    
                    # CRITICAL FIX: Try ALL possible field name variations with different cases
                    # Lowercase field names
                    or_conditions.extend([
                        {"leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                    ])
                    
                    # Uppercase field names  
                    or_conditions.extend([
                        {"Leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"Leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"Leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                    ])
                    
                    # CamelCase field names
                    or_conditions.extend([
                        {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                        {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                    ])
                    
                    # Also try partial matches for flexibility
                    # Lowercase
                    or_conditions.extend([
                        {"leader at 12": {"$regex": safe_name, "$options": "i"}},
                        {"leader @12": {"$regex": safe_name, "$options": "i"}},
                        {"leader12": {"$regex": safe_name, "$options": "i"}},
                    ])
                    
                    # Uppercase
                    or_conditions.extend([
                        {"Leader at 12": {"$regex": safe_name, "$options": "i"}},
                        {"Leader @12": {"$regex": safe_name, "$options": "i"}},
                        {"Leader12": {"$regex": safe_name, "$options": "i"}},
                    ])
                    
                    # CamelCase
                    or_conditions.extend([
                        {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
                        {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
                    ])
                
                if or_conditions:
                    query["$and"].append({"$or": or_conditions})
                    print(f"   Searching for cells where Leader at 12 matches any of: {name_variations}")
                    print(f"   Total OR conditions: {len(or_conditions)}")
                    
                    # DEBUG: Check what cells we're actually finding
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
                        # Check ALL possible field names for Leader at 12
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
                # Default fallback - show personal cells
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

        # REGULAR USERS
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

        # EXECUTE QUERY
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
        
        # Generate instances (weekly occurrences)
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
                    continue

                target_day = day_mapping[day_name]
                days_until = (target_day - start_date_obj.weekday()) % 7
                instance_date = start_date_obj + timedelta(days=days_until)
                
                while instance_date <= today:
                    year, week, _ = instance_date.isocalendar()
                    week_id = f"{year}-W{week:02d}"
                    
                    attendance = event.get("attendance", {}).get(week_id, {})
                    attendees = attendance.get("attendees", [])
                    did_not_meet = attendance.get("status") == "did_not_meet"
                    
                    has_checked_in = any(a.get("checked_in", False) for a in attendees)
                    
                    if did_not_meet or event.get("did_not_meet"):
                        event_status = "did_not_meet"
                    elif has_checked_in:
                        event_status = "complete"
                    else:
                        event_status = "incomplete"
                    
                    if status and status != event_status:
                        instance_date += timedelta(days=7)
                        continue
                    
                    instance = {
                        "_id": f"{event.get('_id')}_{instance_date.isoformat()}",
                        "UUID": event.get("UUID", ""),
                        "eventName": event.get("Event Name") or event.get("eventName") or event.get("EventName", ""),
                        "eventType": "Cells",
                        "eventLeaderName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", ""),
                        "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("EventLeaderEmail") or event.get("Email", ""),
                        "leader1": event.get("leader1") or event.get("Leader @1") or event.get("Leader at 1", ""),
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
                        "location": event.get("Location") or event.get("location", ""),
                        "attendees": attendees,
                        "persistent_attendees": event.get("persistent_attendees", []),
                        "hasPersonSteps": True,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "_is_overdue": instance_date < today and event_status == "incomplete",
                        "is_recurring": True,
                        "week_identifier": week_id,
                        "original_event_id": str(event.get("_id"))
                    }
                    
                    cell_instances.append(instance)
                    instance_date += timedelta(days=7)

            except Exception as e:
                print(f"Error processing event: {str(e)}")
                continue

        cell_instances.sort(key=lambda x: x['date'], reverse=True)
        
        total_count = len(cell_instances)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated = cell_instances[skip:skip + limit]

        print(f"RESULTS: {len(paginated)} events (page {page}/{total_pages}, total: {total_count})")
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


async def is_user_leader_at_12(user_email: str, user_name: str) -> bool:
    try:
        # Check user role first
        user = await users_collection.find_one({"email": user_email})
        if user:
            role = user.get("role", "").lower()
            if "leader at 12" in role or "leader@12" in role or "leader @12" in role:
                print(f"   User {user_name} has Leader at 12 role: {role}")
                return True
        
        # Build dynamic name variations for ANY user
        name_variations = []
        
        if user_name:
            parts = user_name.split()
            first_name = parts[0] if parts else ""
            surname = parts[-1] if len(parts) > 1 else ""
            
            # Basic variations
            name_variations.append(user_name)
            name_variations.append(f"{first_name} {surname}")
            name_variations.append(first_name)
            
            # Handle hyphenated first names
            if "-" in first_name:
                hyphen_parts = first_name.split("-")
                name_variations.extend([
                    f"{hyphen_parts[0]} {surname}",
                    f"{first_name} {surname}",
                ])
                for part in hyphen_parts:
                    if part and part.strip():
                        name_variations.append(f"{part} {surname}")
            
            # Handle multiple names (e.g., "Nash Bobo Mbankumuna")
            if len(parts) > 2:
                name_variations.append(f"{parts[0]} {parts[-1]}")
                for i in range(1, len(parts)):
                    combined_name = " ".join(parts[:i+1])
                    name_variations.append(combined_name)
        
        # Remove duplicates and empty values
        name_variations = list(set([name for name in name_variations if name and name.strip()]))
        
        print(f"   Checking if {user_name} is Leader at 12 with name variations: {name_variations}")
        
        # Check if user is listed as Leader at 12 in any cell - CASE INSENSITIVE FIELDS
        or_conditions = []
        for name_variant in name_variations:
            safe_name = re.escape(name_variant)
            
            # Try ALL possible field name variations with different cases
            # Lowercase field names
            or_conditions.extend([
                {"leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
            
            # Uppercase field names  
            or_conditions.extend([
                {"Leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
            
            # CamelCase field names
            or_conditions.extend([
                {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
            
            # Also try partial matches for flexibility
            # Lowercase
            or_conditions.extend([
                {"leader at 12": {"$regex": safe_name, "$options": "i"}},
                {"leader @12": {"$regex": safe_name, "$options": "i"}},
                {"leader12": {"$regex": safe_name, "$options": "i"}},
            ])
            
            # Uppercase
            or_conditions.extend([
                {"Leader at 12": {"$regex": safe_name, "$options": "i"}},
                {"Leader @12": {"$regex": safe_name, "$options": "i"}},
                {"Leader12": {"$regex": safe_name, "$options": "i"}},
            ])
            
            # CamelCase
            or_conditions.extend([
                {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
                {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
            ])
        
        if or_conditions:
            full_query = {
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
            
            cells_count = await events_collection.count_documents(full_query)
            print(f"   User {user_name} has {cells_count} cells where they are Leader at 12")
            
            # DEBUG: Show which cells were found
            if cells_count > 0:
                found_cells = await events_collection.find(full_query).to_list(length=5)
                print(f"   Sample cells where {user_name} is Leader at 12:")
                for i, cell in enumerate(found_cells):
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
                    print(f"      {i+1}. {cell.get('Event Name', 'N/A')} - Leader@12: {leader_at_12}")
            
            return cells_count > 0
        else:
            print(f"   User {user_name} is NOT a Leader at 12 (no name variations generated)")
            return False
        
    except Exception as e:
        print(f"Error checking if user is leader at 12: {str(e)}")
        return False


    
@app.get("/debug-leader-at-12-cells")
async def debug_leader_at_12_cells(current_user: dict = Depends(get_current_user)):
    try:
        user_email = current_user.get("email", "")
        first_name = current_user.get('name', '').strip()
        surname = current_user.get('surname', '').strip()
        user_name = f"{first_name} {surname}".strip()
        
        print(f"DEBUG LEADER AT 12 for: {user_name} ({user_email})")
        
        # 1. Check what name variations we're generating
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
                f"{first_name} {surname}",
            ])
            for part in hyphen_parts:
                if part and part.strip():
                    name_variations.append(f"{part} {surname}")
        
        name_variations = list(set([name for name in name_variations if name and name.strip()]))
        
        print(f"Name variations being tried: {name_variations}")
        
        # 2. Check personal cells (where user is leader)
        personal_query = {
            "$or": [
                {"eventLeaderEmail": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                {"Leader": {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}},
            ]
        }
        
        personal_cells = await events_collection.find(personal_query).to_list(length=20)
        print(f"PERSONAL cells found: {len(personal_cells)}")
        for cell in personal_cells:
            print(f"   - {cell.get('Event Name')} | Leader: {cell.get('Leader')}")
        
        # 3. Check disciples' cells (where user is Leader at 12)
        or_conditions = []
        for name_variant in name_variations:
            safe_name = re.escape(name_variant)
            or_conditions.extend([
                {"Leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": safe_name, "$options": "i"}},
                {"Leader @12": {"$regex": safe_name, "$options": "i"}},
                {"leader12": {"$regex": safe_name, "$options": "i"}},
            ])
        
        disciples_query = {"$or": or_conditions}
        disciples_cells = await events_collection.find(disciples_query).to_list(length=50)
        
        print(f"DISCIPLES cells found: {len(disciples_cells)}")
        for cell in disciples_cells:
            leader_at_12 = cell.get('Leader at 12') or cell.get('Leader @12') or cell.get('leader12', 'N/A')
            print(f"   - {cell.get('Event Name')} | Leader: {cell.get('Leader')} | Leader@12: {leader_at_12}")
        
        return {
            "user_name": user_name,
            "name_variations": name_variations,
            "personal_cells_count": len(personal_cells),
            "disciples_cells_count": len(disciples_cells),
            "personal_cells": [{"name": c.get('Event Name'), "leader": c.get('Leader')} for c in personal_cells[:10]],
            "disciples_cells": [{"name": c.get('Event Name'), "leader": c.get('Leader'), "leader_at_12": c.get('Leader at 12') or c.get('Leader @12') or c.get('leader12')} for c in disciples_cells[:10]]
        }
        
    except Exception as e:
        print(f"DEBUG ERROR: {str(e)}")
        return {"error": str(e)}
        
@app.get("/debug/leader-disciples/{user_email}")
async def debug_leader_disciples(user_email: str):
    """Debug endpoint to see who a leader has invited - CHECK EVENTS COLLECTION ONLY"""
    try:
        print(f"Debugging leader disciples for: {user_email}")
        
        # Find the leader in EVENTS collection (where cells exist)
        leader_cell = await events_collection.find_one({
            "$or": [
                {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}}
            ],
            "Event Type": "Cells"
        })
        
        if not leader_cell:
            return {"error": f"Leader not found with email: {user_email} in events collection"}
        
        leader_name = leader_cell.get("Leader", "").strip()
        print(f"Found leader in events collection: {leader_name}")
        
        # Find all people this leader invited (from people collection)
        disciples = await people_collection.find({
            "invited_by": {"$regex": f".*{re.escape(leader_name)}.*", "$options": "i"}
        }).to_list(length=None)
        
        print(f"Found {len(disciples)} disciples invited by {leader_name}")
        
        # Find cells for these disciples in EVENTS collection
        disciple_emails = [d.get("email", "").lower() for d in disciples if d.get("email")]
        disciple_names = [f"{d.get('Name', '')} {d.get('Surname', '')}".strip() for d in disciples if d.get('Name')]
        
        disciple_cells = []
        
        # Find cells by disciple emails
        for disciple_email in disciple_emails:
            if disciple_email and disciple_email.strip():
                cells = await events_collection.find({
                    "Event Type": "Cells",
                    "$or": [
                        {"Email": {"$regex": f"^{re.escape(disciple_email)}$", "$options": "i"}},
                        {"eventLeaderEmail": {"$regex": f"^{re.escape(disciple_email)}$", "$options": "i"}}
                    ]
                }).to_list(length=None)
                disciple_cells.extend(cells)
        
        # Find cells by disciple names
        for disciple_name in disciple_names:
            if disciple_name and disciple_name.strip():
                cells = await events_collection.find({
                    "Event Type": "Cells",
                    "Leader": {"$regex": f"^{re.escape(disciple_name)}$", "$options": "i"}
                }).to_list(length=None)
                disciple_cells.extend(cells)
        
        # Find cells where the leader is explicitly Leader at 12
        leader_at_12_cells = await events_collection.find({
            "Event Type": "Cells",
            "$or": [
                {"Leader at 12": {"$regex": f"^{re.escape(leader_name)}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{re.escape(leader_name)}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{re.escape(leader_name)}$", "$options": "i"}}
            ]
        }).to_list(length=None)
        
        # Find ALL cells in the database (for comparison)
        all_cells = await events_collection.find({
            "Event Type": "Cells"
        }).to_list(length=50)  # First 50 only
        
        return {
            "leader_info": {
                "name": leader_name,
                "email": user_email,
                "own_cell": leader_cell.get("Event Name")
            },
            "disciples": [
                {
                    "name": f"{d.get('Name', '')} {d.get('Surname', '')}",
                    "email": d.get("email"),
                    "invited_by": d.get("invited_by")
                }
                for d in disciples
            ],
            "disciple_cells": [
                {
                    "event_name": cell.get("Event Name"),
                    "leader": cell.get("Leader"),
                    "leader_email": cell.get("Email"),
                    "leader_at_12": cell.get("Leader at 12") or cell.get("Leader @12", "")
                }
                for cell in disciple_cells
            ],
            "leader_at_12_cells": [
                {
                    "event_name": cell.get("Event Name"),
                    "leader": cell.get("Leader"),
                    "leader_email": cell.get("Email"),
                    "leader_at_12": cell.get("Leader at 12") or cell.get("Leader @12", "")
                }
                for cell in leader_at_12_cells
            ],
            "all_cells_sample": [
                {
                    "event_name": cell.get("Event Name"),
                    "leader": cell.get("Leader"),
                    "leader_email": cell.get("Email"),
                    "leader_at_12": cell.get("Leader at 12") or cell.get("Leader @12", "")
                }
                for cell in all_cells
            ],
            "summary": {
                "total_disciples": len(disciples),
                "total_disciple_cells": len(disciple_cells),
                "total_leader_at_12_cells": len(leader_at_12_cells),
                "total_cells_in_database": len(all_cells)
            }
        }
        
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/debug/leader-at-12-detailed/{user_email}")
async def debug_leader_at_12_detailed(user_email: str):
    try:
        # Find the user from events database
        user_cell = await events_collection.find_one({
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
            ]
        })
        
        if not user_cell:
            return {"error": f"No cell found for user email: {user_email}"}
        
        user_name = user_cell.get("Leader", "").strip()
        
        if not user_name:
            return {"error": f"No leader name found for user: {user_email}"}
        
        print(f"DEBUG: Analyzing Leader at 12 for user: {user_name} ({user_email})")
        
        # Build the EXACT same query that the main endpoint uses
        query = {
            "Event Type": "Cells",
            "$or": []
        }
        
        # Their own cells
        query["$or"].extend([
            {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
            {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
            {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
        ])
        
        # Cells where they are Leader at 12 (enhanced matching)
        leader_fields = ["Leader @12", "Leader at 12", "leader12"]
        
        for field in leader_fields:
            # Exact match
            query["$or"].append({field: {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}})
            # Word boundary match
            query["$or"].append({field: {"$regex": f"\\b{re.escape(user_name)}\\b", "$options": "i"}})
            # Contains match
            query["$or"].append({field: {"$regex": user_name, "$options": "i"}})
            
            # First name variations
            if " " in user_name:
                first_name = user_name.split()[0]
                last_name = user_name.split()[-1]
                
                query["$or"].append({field: {"$regex": f"^{re.escape(first_name)}.*{re.escape(last_name)}$", "$options": "i"}})
                query["$or"].append({field: {"$regex": f"\\b{re.escape(first_name)}\\b", "$options": "i"}})
        
        # Execute query
        cells_cursor = events_collection.find(query)
        all_cells = await cells_cursor.to_list(length=None)
        
        # Also get ALL cells with Leader at 12 data for comparison
        all_leader12_cells = await events_collection.find({
            "Event Type": "Cells",
            "$or": [
                {"Leader @12": {"$exists": True}},
                {"Leader at 12": {"$exists": True}},
                {"leader12": {"$exists": True}}
            ]
        }).to_list(length=None)
        
        # Categorize results
        own_cells = []
        disciple_cells = []
        
        for cell in all_cells:
            cell_leader = cell.get("Leader", "")
            cell_leader_email = cell.get("Email", "")
            leader_at_12 = cell.get("Leader at 12") or cell.get("Leader @12", "") or cell.get("leader12", "")
            
            cell_info = {
                "event_name": cell.get("Event Name"),
                "leader": cell_leader,
                "leader_email": cell_leader_email,
                "leader_at_12": leader_at_12,
                "day": cell.get("Day"),
                "is_own_cell": (
                    cell_leader_email.lower() == user_email.lower() or
                    cell_leader.lower() == user_name.lower()
                ),
                "query_match_reason": "Unknown"
            }
            
            # Determine why this cell matched
            if cell_leader_email.lower() == user_email.lower():
                cell_info["query_match_reason"] = "Email exact match"
            elif cell_leader.lower() == user_name.lower():
                cell_info["query_match_reason"] = "Leader name exact match"
            elif any([
                leader_at_12 and user_name.lower() in leader_at_12.lower(),
                cell.get("Leader @12") and user_name.lower() in cell.get("Leader @12", "").lower(),
                cell.get("leader12") and user_name.lower() in cell.get("leader12", "").lower()
            ]):
                cell_info["query_match_reason"] = "Leader at 12 match"
            
            if cell_info["is_own_cell"]:
                own_cells.append(cell_info)
            else:
                disciple_cells.append(cell_info)
        
        # Find cells that SHOULD match but don't
        missing_cells = []
        for cell in all_leader12_cells:
            leader_at_12 = cell.get("Leader at 12") or cell.get("Leader @12", "") or cell.get("leader12", "")
            if leader_at_12 and user_name.lower() in leader_at_12.lower():
                # Check if this cell is not in our results
                cell_in_results = any(
                    result_cell.get("event_name") == cell.get("Event Name")
                    for result_cell in own_cells + disciple_cells
                )
                if not cell_in_results:
                    missing_cells.append({
                        "event_name": cell.get("Event Name"),
                        "leader": cell.get("Leader"),
                        "leader_at_12": leader_at_12,
                        "reason": "Cell has user as Leader at 12 but not returned in query"
                    })
        
        return {
            "user_name": user_name,
            "user_email": user_email,
            "query_used": query,
            "summary": {
                "total_cells_found": len(all_cells),
                "own_cells": len(own_cells),
                "disciple_cells": len(disciple_cells),
                "total_cells_in_db_with_leader12": len(all_leader12_cells),
                "potential_missing_cells": len(missing_cells)
            },
            "own_cells": own_cells,
            "disciple_cells": disciple_cells,
            "missing_cells": missing_cells,
            "all_leader12_cells_sample": [
                {
                    "event_name": cell.get("Event Name"),
                    "leader": cell.get("Leader"),
                    "leader_at_12": cell.get("Leader at 12") or cell.get("Leader @12", "") or cell.get("leader12", ""),
                    "matches_user": any([
                        cell.get("Leader at 12") and user_name.lower() in (cell.get("Leader at 12") or "").lower(),
                        cell.get("Leader @12") and user_name.lower() in (cell.get("Leader @12") or "").lower(),
                        cell.get("leader12") and user_name.lower() in (cell.get("leader12") or "").lower()
                    ])
                }
                for cell in all_leader12_cells[:20]
            ]
        }
        
    except Exception as e:
        return {"error": str(e)}
    
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
                
                # Get the day value from multiple possible fields
                day_name_raw = event.get("Day") or event.get("day") or event.get("eventDay") or ""
                day_name = str(day_name_raw).strip()

                # Get event date for date range filtering
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

                # For other events, use the wider date range to include historical events
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
    
#------------------ MIGRATION ENDPOINTS --------------------
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
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
from urllib.parse import unquote

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
        
        print(f"🗑️ DELETE EVENT TYPE: {decoded_event_type_name}, force={force}")
        
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
            print(f"❌ Event type '{decoded_event_type_name}' not found")
            raise HTTPException(
                status_code=404, 
                detail=f"Event type '{decoded_event_type_name}' not found"
            )
        
        actual_identifier = (
            existing_event_type.get("name") or 
            existing_event_type.get("eventType") or 
            existing_event_type.get("eventTypeName")
        )
        
        print(f"✅ Found event type: {actual_identifier}")
        
        # Find events using this type - COMPREHENSIVE SEARCH
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
                # Ensure we're finding actual events, not event type definitions
                {"isEventType": {"$ne": True}},
                # Events should have at least one of these identifying fields
                {"$or": [
                    {"eventName": {"$exists": True}},
                    {"Event Name": {"$exists": True}},
                    {"date": {"$exists": True}},
                    {"Date Of Event": {"$exists": True}}
                ]}
            ]
        }
        
        print(f"📊 Searching for events with query: {events_query}")
        
        events_using_type = await events_collection.find(events_query).to_list(length=None)
        events_count = len(events_using_type)
        
        print(f"📊 Found {events_count} events using '{actual_identifier}'")
        
        if events_count > 0:
            # Show detailed information about the events
            event_details = []
            for event in events_using_type[:20]:  # Show up to 20 events
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
                print(f"   📌 Event: {detail['name']} (ID: {detail['id']}, Status: {detail['status']})")
            
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
            print(f"✅ Event type '{actual_identifier}' deleted successfully")
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
        print(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, 
            detail=f"Error deleting event type: {str(e)}"
        )

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
        
        print(f"🔍 DIAGNOSTIC: Checking usage of event type: {decoded_name}")
        
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
        
        print(f"✅ Found event type definition: {actual_name}")
        
        # Find ALL events using this type
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
        
        print(f"📊 Found {len(events)} events using '{actual_name}'")
        
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
            print(f"   📌 {detail['eventName']} - {detail['date']} - Status: {detail['status']}")
        
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



@app.get("/test/leader12-debug/{email}")
async def test_leader12_debug(email: str):
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
    
@app.get("/debug/event-status/{event_id}")
async def debug_event_status(event_id: str):
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
    day_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    return day_map.get(day.lower().strip(), 999)

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

def get_next_occurrences_for_range(
    day_name: str,
    start_date: date,
    end_date: date
) -> List[date]:
    
    day_mapping = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    day_name_lower = day_name.lower().strip()
    target_weekday = day_mapping.get(day_name_lower)
    
    if target_weekday is None:
        print(f"Invalid day name: '{day_name}'")
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
    
    event_day = cell_doc.get("Day")
    if not event_day:
        return []
    
    occurrence_dates = get_next_occurrences_for_range(
        event_day, 
        calc_start_date, 
        calc_end_date
    )
    
    if not occurrence_dates:
        print(f"No occurrences generated for day '{event_day}'")
        return []
    
    cell_instances = []
    today_date = date.today()
    
    for occ_date in occurrence_dates:
        
        if occ_date < min_visible_date:
            continue
            
        instance = cell_doc.copy()
        
        if "_id" in instance:
            instance["_id"] = str(instance["_id"])
        
        # Set the mandatory date fields
        instance["date"] = occ_date.isoformat()  # Convert to ISO string
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
    try:
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
    
def should_include_event_fixed(event_date: date, status: str, today_date: date, is_admin: bool = False) -> bool:
    start_date = date(2025, 11, 30) 
    
    if event_date < start_date:
        print(f"Filtered out - event date {event_date} is before {start_date}")
        return False
    
    if not is_admin:
        if status == 'incomplete':
            return event_date >= today_date
        else:
            return event_date >= today_date
    
    return True


def parse_time(time_str):
    if not time_str:
        return 19, 0  
    
    try:
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
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        today_day = today.strftime("%A").lower()
        
        print(f"DEBUG: Today is {today_day} ({today_date})")
        
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
    
        incomplete_cells = []
        complete_cells = []
        did_not_meet_cells = []
        
        for cell in cells:
            event_day_name = cell.get("Day")
            
            if not event_day_name:
                continue
            
            event_date = calculate_this_week_event_date(event_day_name, today_date)
            
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
    FIXED: Shows cells for TODAY'S day of the week (recurring schedule)
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
        logging.info(f"TODAY: {today_day_name.upper()} ({today_date})")
        logging.info(f"Fetching cells for {today_day_name}")
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
        
        query = {
            "Event Type": "Cells",
            "Day": {"$regex": f"^{today_day_name}$", "$options": "i"},
            "$or": query_conditions
        }

        logging.info(f"Query: Cells where Day = '{today_day_name}'")

        cursor = events_collection.find(query)
        
        events = []
        seen_keys = set()

        async for event in cursor:
            event_name = event.get("Event Name", "")
            event_email = event.get("Email", "").lower().strip()
            recurring_day = event.get("Day", "").strip().lower()
            
            # Verify it's today's day
            if recurring_day != today_day_name:
                logging.warning(f"Skipping {recurring_day} cell: {event_name}")
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
        logging.info(f"Returning {len(events)} cells for {today_day_name}")
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
        logging.error(f"Error: {e}", exc_info=True)
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
        
        print(f"Checking if {current_user_name} ({current_user_email}) is a Leader at 12")
        
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
        
        print(f"Leader at 12 check: {is_leader_at_12} (found {len(leader_events)} events)")
        
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
        print(f"Error checking Leader at 12: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/check-leader-at-12-status")
async def check_leader_at_12_status(current_user: dict = Depends(get_current_user)):
    """
    Fixed Leader at 12 detection - matches the exact field names in your events
    """
    try:
        user_email = current_user.get("email", "")
        user_name = f"{current_user.get('name', '').strip()} {current_user.get('surname', '').strip()}".strip()
        
        if not user_name:
            user_name = current_user.get("name", "") or current_user.get("username", "")
        
        print(f"FIXED Leader at 12 check for: '{user_name}' ({user_email})")
        
        # CRITICAL FIX: Match the exact field names from your events data
        # Your events have both "Leader at 12" and "Leader @12" fields
        oversees_cells_query = {
            "$or": [
                {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{user_name}$", "$options": "i"}},
                # Also check if they are in the attendance data as Leader @12
                {"attendance.Leader @12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"attendance.leader12": {"$regex": f"^{user_name}$", "$options": "i"}},
            ],
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
            ]
        }
        
        print(f"Query for oversees cells: {oversees_cells_query}")
        
        oversees_cells = await events_collection.find(oversees_cells_query).to_list(length=None)
        
        # Also check if user has their own cell
        own_cell_query = {
            "$or": [
                {"eventLeaderEmail": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"Email": {"$regex": f"^{user_email}$", "$options": "i"}},
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
            ],
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
            ]
        }
        
        own_cells = await events_collection.find(own_cell_query).to_list(length=None)
        
        # Check user profile
        user_profile = await users_collection.find_one({"email": user_email})
        has_leader_at_12_role = False
        leader12_field = ""
        
        if user_profile:
            user_role = user_profile.get("role", "").lower()
            leader12_field = user_profile.get("leader12", "")
            has_leader_at_12_role = any(phrase in user_role for phrase in [
                "leader at 12", "leader@12", "leader @12", "leader at12"
            ])
        
        # Determine Leader at 12 status
        oversees_other_cells = len(oversees_cells) > 0
        has_own_cell = len(own_cells) > 0
        
        # A Leader at 12 oversees other cells (may or may not have their own cell)
        is_leader_at_12 = oversees_other_cells or has_leader_at_12_role or bool(leader12_field)
        
        print(f"FIXED Leader at 12 Results:")
        print(f"   - User: '{user_name}'")
        print(f"   - Oversees other cells: {oversees_other_cells} ({len(oversees_cells)} cells)")
        print(f"   - Has own cell: {has_own_cell} ({len(own_cells)} cells)")
        print(f"   - Has leader at 12 role: {has_leader_at_12_role}")
        print(f"   - Has leader12 field: {bool(leader12_field)} ('{leader12_field}')")
        print(f"   - Final is_leader_at_12: {is_leader_at_12}")
        
        # Detailed debug output
        if oversees_cells:
            print(f"   - Cells overseen as Leader at 12:")
            for cell in oversees_cells:
                leader_at_12 = cell.get("Leader at 12") or cell.get("Leader @12") or cell.get("leader12")
                leader_at_12_attendance = ""
                if cell.get("attendance"):
                    leader_at_12_attendance = cell["attendance"].get("Leader @12") or cell["attendance"].get("leader12")
                
                # print(f"     * '{cell.get('Event Name')}'")
                # print(f"       Main Leader at 12: '{leader_at_12}'")
                if leader_at_12_attendance:
                    print(f"       Attendance Leader @12: '{leader_at_12_attendance}'")
        
        # if own_cells:
        #     print(f"   - Own cells:")
        #     for cell in own_cells:
        #         # print(f"     * '{cell.get('Event Name')}' - Leader: '{cell.get('Leader')}'")
        
        return {
            "is_leader_at_12": is_leader_at_12,
            "user_name": user_name,
            "user_email": user_email,
            "has_own_cell": has_own_cell,
            "own_cells_count": len(own_cells),
            "oversees_cells_count": len(oversees_cells),
            "has_leader_at_12_role": has_leader_at_12_role,
            "has_leader12_field": bool(leader12_field),
            "leader12_field": leader12_field,
            "own_cells": [cell.get("Event Name") for cell in own_cells],
            "overseen_cells": [cell.get("Event Name") for cell in oversees_cells],
            "message": f"'{user_name}' {'IS' if is_leader_at_12 else 'is NOT'} a Leader at 12"
        }
        
    except Exception as e:
        print(f"Error in fixed leader at 12 check: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/debug-leader-at-12/{user_email}")
async def debug_leader_at_12(user_email: str):
    """
    Debug endpoint to check why a specific user is/isn't being recognized as Leader at 12
    """
    try:
        user_profile = await users_collection.find_one({"email": user_email})
        if not user_profile:
            return {"error": "User not found"}
        
        user_name = f"{user_profile.get('name', '').strip()} {user_profile.get('surname', '').strip()}".strip()
        
        print(f"DEBUG Leader at 12 for: '{user_name}' ({user_email})")
        
        # Check all events where this user might be Leader at 12
        query = {
            "$or": [
                {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"attendance.Leader @12": {"$regex": f"^{user_name}$", "$options": "i"}},
            ]
        }
        
        events = await events_collection.find(query).to_list(length=None)
        
        debug_results = []
        for event in events:
            event_data = {
                "event_name": event.get("Event Name"),
                "leader_at_12": event.get("Leader at 12"),
                "leader_@12": event.get("Leader @12"), 
                "leader12": event.get("leader12"),
                "attendance_leader_@12": event.get("attendance", {}).get("Leader @12"),
                "attendance_leader12": event.get("attendance", {}).get("leader12"),
                "exact_match_leader_at_12": event.get("Leader at 12") == user_name,
                "exact_match_leader_@12": event.get("Leader @12") == user_name,
            }
            debug_results.append(event_data)
        
        return {
            "user_name": user_name,
            "user_email": user_email,
            "user_role": user_profile.get("role"),
            "leader12_field": user_profile.get("leader12"),
            "total_events_found": len(events),
            "debug_results": debug_results,
            "is_leader_at_12": len(events) > 0
        }
        
    except Exception as e:
        print(f"Debug error: {str(e)}")
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
        default_start_date = '2025-11-30'
        start_date_filter = start_date if start_date else default_start_date
        
        print(f"Status counts - Date filter: {start_date_filter}")
        
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
        
        # CONVERT START DATE TO DATE OBJECT
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        for event in events:
            # FILTER BY DATE - Only include events from start_date onwards
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
        
        print(f"Status counts result: incomplete={incomplete_count}, complete={complete_count}, did_not_meet={did_not_meet_count}")
        
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

        # ADD DATE FILTER
        start_date_filter = start_date if start_date else '2025-11-30'
        
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
        
        # CONVERT START DATE TO DATE OBJECT
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        for event in events:
            # FILTER BY DATE
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
        
        print(f"REGISTRANT Status counts: incomplete={incomplete_count}, complete={complete_count}, did_not_meet={did_not_meet_count}")
        
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
        
        # DATE FILTER
        start_date_filter = start_date if start_date else '2025-11-30'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"Registrant {email} - Fetching events from {start_date_obj}")

        # SIMPLE QUERY - Only registrant's own events
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
        
        print(f"Query: {query}")
        
        # Fetch events
        cursor = events_collection.find(query)
        all_events = await cursor.to_list(length=None)
        
        print(f"Found {len(all_events)} raw events")
        
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
                
                # FILTER BY DATE RANGE
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
                    "attendees": attendees if isinstance(attendees, list) else [],
                    "did_not_meet": did_not_meet,
                    "status": cell_status,
                    "Status": cell_status.replace("_", " ").title(),
                    "_is_overdue": most_recent_occurrence < today_date
                }
                
                processed_events.append(final_event)
                
            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {str(e)}")
                continue
        
        print(f"Processed {len(processed_events)} events")
        
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
        print(f"ERROR in get_registrant_events: {str(e)}")
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
        
        start_date_filter = start_date if start_date else '2025-11-30'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"Registrant - Cells from {start_date_obj} to {today_date}, Page {page}")
        print(f"Search: '{search}', Status: '{status}', Personal: {personal}, Event Type: '{event_type}', Start Date: '{start_date_filter}'")

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
        
        print(f"Found {len(all_cells_raw)} cells before deduplication and date filtering")
        
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
        
        print(f"After deduplication: {len(all_cells)} unique cells")
        
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
        
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
        }
        
        print(f"Status counts - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
        
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
        print(f"ERROR in get_registrant_cell_events_debug: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")
    
# @app.get("/events/global")
# async def get_global_events(
#     current_user: dict = Depends(get_current_user),
#     page: int = Query(1, ge=1),
#     limit: int = Query(25, ge=1, le=100),
#     status: Optional[str] = Query(None),
#     search: Optional[str] = Query(None),
#     start_date: Optional[str] = Query(None)
# ):
#     """
#     Get Global Events (like Sunday Service)
#     Shows events where isGlobal = True
#     """
#     try:
#         timezone = pytz.timezone("Africa/Johannesburg")
#         today = datetime.now(timezone)
#         today_date = today.date()
        
#         # Parse start_date filter
#         start_date_filter = start_date if start_date else '2025-10-20'
#         start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
#         print(f"Fetching Global Events from {start_date_obj}")
        
#         # Build query for Global Events
#         query = {
#             "isGlobal": True,
#             "eventTypeName": "Global Events"
#         }
        
#         # Add search filter
#         if search and search.strip():
#             search_regex = {"$regex": search.strip(), "$options": "i"}
#             query["$or"] = [
#                 {"Event Name": search_regex},
#                 {"eventName": search_regex},
#                 {"Leader": search_regex},
#                 {"Location": search_regex}
#             ]
        
#         print(f"Query for Global Events: {query}")
        
#         # Fetch events
#         cursor = events_collection.find(query).sort("date", -1)
#         all_events = await cursor.to_list(length=None)
        
#         print(f"Found {len(all_events)} raw global events")
        
#         # Process events
#         processed_events = []
        
#         for event in all_events:
#             try:
#                 print(f"Processing event {event.get('_id')}: {event.get('eventName', event.get('Event Name', 'Unknown'))}")
                
#                 # Parse event date
#                 event_date_field = event.get("date")
#                 if isinstance(event_date_field, datetime):
#                     event_date = event_date_field.date()
#                 elif isinstance(event_date_field, str):
#                     try:
#                         event_date = datetime.fromisoformat(
#                             event_date_field.replace("Z", "+00:00")
#                         ).date()
#                     except Exception:
#                         event_date = today_date
#                 else:
#                     event_date = today_date
                
#                 print(f"  Event date: {event_date}, Start date filter: {start_date_obj}")
                
#                 # Filter by date range
#                 if event_date < start_date_obj:
#                     print(f"   Skipped - before date range")
#                     continue
                
#                 # Get event details
#                 event_name = event.get("Event Name") or event.get("eventName", "")
#                 leader_name = event.get("Leader") or event.get("eventLeader", "")
#                 location = event.get("Location") or event.get("location", "")
                
#                 # Determine status - FIXED: Use explicit status field from database, not inferred from attendees
#                 # This prevents events from being automatically marked "complete" just because attendees were checked in
#                 did_not_meet = event.get("did_not_meet", False)
                
#                 # Check for explicit status field first (set via close/update API call)
#                 stored_status = event.get("status") or event.get("Status")
                
#                 print(f"  Status determination: did_not_meet={did_not_meet}, stored_status={stored_status}")
                
#                 if did_not_meet:
#                     event_status = "did_not_meet"
#                     status_display = "Did Not Meet"
#                 elif stored_status:
#                     # Use the explicit status from the database
#                     event_status = str(stored_status).lower()
#                     status_display = str(stored_status).replace("_", " ").title()
#                 else:
#                     # Default to "open" for events without an explicit status
#                     # (This ensures new events start as open, not derived from attendees)
#                     event_status = "open"
#                     status_display = "Open"
                
#                 print(f"  ✓ Final status: {event_status}")
                
#                 # Apply status filter
#                 if status and status != 'all' and status != event_status:
#                     print(f"   Skipped - status filter: requested={status}, actual={event_status}")
#                     continue
                
#                 # Build event object
#                 final_event = {
#                     "_id": str(event.get("_id", "")),
#                     "eventName": event_name,
#                     "eventType": "Global Events",
#                     "eventLeaderName": leader_name,
#                     "eventLeaderEmail": event.get("Email") or event.get("userEmail", ""),
#                     "day": event.get("Day", ""),
#                     "date": event_date.isoformat(),
#                     "time": event.get("time", ""),
#                     "location": location,
#                     "description": event.get("description", ""),
#                     "attendees": event.get("attendees", []) if isinstance(event.get("attendees", []), list) else [],
#                     "did_not_meet": did_not_meet,
#                     "status": event_status,
#                     "Status": status_display,
#                     "_is_overdue": event_date < today_date and event_status == "incomplete",
#                     "isGlobal": True,
#                     "isTicketed": event.get("isTicketed", False),
#                     "priceTiers": event.get("priceTiers", []),
#                     "total_attendance": event.get("total_attendance", 0),
#                     "UUID": event.get("UUID", ""),
#                     "created_at": event.get("created_at"),
#                     "updated_at": event.get("updated_at")
#                 }
                
#                 processed_events.append(final_event)
#                 print(f"  Event added to processed list")
                
#             except Exception as e:
#                 print(f"Error processing global event {event.get('_id')}: {str(e)}")
#                 import traceback
#                 traceback.print_exc()
#                 continue
        
#         print(f"Processed {len(processed_events)} global events after filtering")
        
#         # Sort by date (most recent first)
#         processed_events.sort(key=lambda x: x['date'], reverse=True)
        
#         # Calculate status counts
#         status_counts = {
#             "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
#             "complete": sum(1 for e in processed_events if e["status"] == "complete"),
#             "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet")
#         }
        
#         print(f"Global Events Status - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}")
        
#         # Pagination
#         total = len(processed_events)
#         total_pages = (total + limit - 1) // limit if total > 0 else 1
#         start_idx = (page - 1) * limit
#         end_idx = start_idx + limit
#         paginated_events = processed_events[start_idx:end_idx]
        
#         print(f"Returning page {page}/{total_pages}: {len(paginated_events)} global events")
        
#         return {
#             "events": paginated_events,
#             "total_events": total,
#             "total_pages": total_pages,
#             "current_page": page,
#             "page_size": limit,
#             "status_counts": status_counts,
#             "date_range": {
#                 "start_date": start_date_filter,
#                 "end_date": today_date.isoformat()
#             }
#         }
        
#     except Exception as e:
#         print(f"ERROR in get_global_events: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         raise HTTPException(status_code=500, detail=f"Error fetching global events: {str(e)}")
    
@app.get("/events/global")
async def get_global_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    last_updated: Optional[str] = Query(None)  # NEW: Track last update time
):
    """
    Get Global Events (like Sunday Service) with real-time updates
    Shows events where isGlobal = True
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        
        # Parse start_date filter
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
        
        print(f"Fetching Global Events from {start_date_obj}")
        
        # Build query for Global Events
        query = {
            "isGlobal": True,
            "eventTypeName": "Global Events"
        }
        
        # NEW: Filter by last_updated if provided (for real-time updates)
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
        
        # Add search filter
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
        
        print(f"Query for Global Events: {query}")
        
        # Fetch events
        cursor = events_collection.find(query).sort([("created_at", -1), ("date", -1)])
        all_events = await cursor.to_list(length=None)
        
        print(f"Found {len(all_events)} raw global events")
        
        # Get the latest update timestamp for real-time polling
        latest_timestamp = None
        if all_events:
            # Find the most recently created or updated event
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
                print(f"🕒 Latest event timestamp: {latest_timestamp}")
        
        # Process events
        processed_events = []
        new_events_count = 0
        
        for event in all_events:
            try:
                print(f"Processing event {event.get('_id')}: {event.get('eventName', event.get('Event Name', 'Unknown'))}")
                
                # Check if this is a new event (for real-time tracking)
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
                
                print(f"  Event date: {event_date}, Start date filter: {start_date_obj}")
                
                # Filter by date range
                if event_date < start_date_obj:
                    print(f"   Skipped - before date range")
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
                
                print(f"  Status determination: did_not_meet={did_not_meet}, stored_status={stored_status}")
                
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
                    print(f"   Skipped - status filter: requested={status}, actual={event_status}")
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
                    "updated_at": event.get("updated_at"),
                    "_is_new": is_new_event  # NEW: Flag for new events in real-time updates
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
        
        # Sort by date (most recent first)
        processed_events.sort(key=lambda x: x['date'], reverse=True)
        
        # Calculate status counts
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet"),
            "open": sum(1 for e in processed_events if e["status"] == "open")
        }
        
        print(f"Global Events Status - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}, Open: {status_counts['open']}")
        
        # Pagination
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
            # NEW: Real-time update fields
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
        print(f"ERROR in global events status counts: {str(e)}")
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

@app.post("/admin/migrate-persistent-attendees")
async def migrate_persistent_attendees(current_user: dict = Depends(get_current_user)):
    """Migrate old attendee data to persistent_attendees format"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    
    try:
        print("\n" + "="*80)
        print("STARTING MIGRATION: persistent_attendees")
        print("="*80 + "\n")
        
        # Find all cell events
        cursor = events_collection.find({
            "$or": [
                {"Event Type": "Cells"},
                {"eventType": "Cells"}
            ]
        })
        
        updated = 0
        skipped_already_migrated = 0
        skipped_no_data = 0
        migration_details = []
        
        async for event in cursor:
            event_id = event["_id"]
            event_name = event.get("Event Name", "Unknown")
            
            # Check if already has persistent_attendees
            if event.get("persistent_attendees") and len(event.get("persistent_attendees", [])) > 0:
                skipped_already_migrated += 1
                print(f"  SKIP: {event_name} (already has persistent_attendees)")
                continue
            
            print(f"\nProcessing: {event_name}")
            
            # Strategy 1: Try to get from attendance records (preferred)
            attendance = event.get("attendance", {})
            if attendance:
                # Get most recent week's attendees
                sorted_weeks = sorted(attendance.keys(), reverse=True)
                if sorted_weeks:
                    latest_week = sorted_weeks[0]
                    latest_attendees = attendance[latest_week].get("attendees", [])
                    
                    if latest_attendees and len(latest_attendees) > 0:
                        # Clean the attendee data
                        cleaned_attendees = []
                        for att in latest_attendees:
                            cleaned_attendees.append({
                                "id": att.get("id", ""),
                                "name": att.get("name") or att.get("fullName", ""),
                                "fullName": att.get("fullName") or att.get("name", ""),
                                "email": att.get("email", ""),
                                "phone": att.get("phone", ""),
                                "leader12": att.get("leader12", ""),
                                "leader144": att.get("leader144", ""),
                            })
                        
                        await events_collection.update_one(
                            {"_id": event_id},
                            {"$set": {"persistent_attendees": cleaned_attendees}}
                        )
                        
                        updated += 1
                        migration_details.append({
                            "event": event_name,
                            "source": f"attendance[{latest_week}]",
                            "attendees_count": len(cleaned_attendees)
                        })
                        
                        print(f" MIGRATED from week {latest_week}: {len(cleaned_attendees)} attendees")
                        continue
            
            # Strategy 2: Try old attendees field (fallback)
            old_attendees = event.get("attendees", [])
            if old_attendees and len(old_attendees) > 0:
                # Clean the attendee data
                cleaned_attendees = []
                for att in old_attendees:
                    cleaned_attendees.append({
                        "id": att.get("id", ""),
                        "name": att.get("name") or att.get("fullName", ""),
                        "fullName": att.get("fullName") or att.get("name", ""),
                        "email": att.get("email", ""),
                        "phone": att.get("phone", ""),
                        "leader12": att.get("leader12", ""),
                        "leader144": att.get("leader144", ""),
                    })
                
                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": {"persistent_attendees": cleaned_attendees}}
                )
                
                updated += 1
                migration_details.append({
                    "event": event_name,
                    "source": "old attendees field",
                    "attendees_count": len(cleaned_attendees)
                })
                
                print(f"  MIGRATED from old attendees field: {len(cleaned_attendees)} attendees")
                continue
            
            # No data to migrate
            skipped_no_data += 1
            print(f"    SKIP: No attendee data found")
        
        print("\n" + "="*80)
        print(" MIGRATION COMPLETE")
        print("="*80)
        print(f" Updated: {updated} events")
        print(f"⏭  Skipped (already migrated): {skipped_already_migrated} events")
        print(f"  Skipped (no data): {skipped_no_data} events")
        print(f" Total processed: {updated + skipped_already_migrated + skipped_no_data}")
        print("="*80 + "\n")
        
        return {
            "success": True,
            "message": f" Migrated {updated} events successfully",
            "summary": {
                "updated": updated,
                "skipped_already_migrated": skipped_already_migrated,
                "skipped_no_data": skipped_no_data,
                "total_processed": updated + skipped_already_migrated + skipped_no_data
            },
            "migration_details": migration_details[:20] 
        }
    
    except Exception as e:
        print(f"\n MIGRATION ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status(current_user: dict = Depends(get_current_user)):
    """Check if user is a leader OR has a cell"""
    try:
        user_email = current_user.get("email")
        user_role = current_user.get("role", "").lower()
        
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
        
        print(f"Checking access for: {user_email}, role: {user_role}")
        
        # CRITICAL: Check if user has a cell (for regular users)
        if user_role == "user":
            has_cell = await user_has_cell(user_email)
            print(f"   User has cell: {has_cell}")
            
            if not has_cell:
                print(f"   User {user_email} has no cell - denying Events page access")
                return {"isLeader": False, "hasCell": False, "canAccessEvents": False}
            else:
                print(f"   User {user_email} has cell - granting Events page access")
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
                print(f"   {user_email} is a leader")
                return {"isLeader": True, "hasCell": True, "canAccessEvents": True}
        
        # Fallback for admin/registrant
        if user_role in ["admin", "registrant"]:
            print(f"   {user_email} is {user_role} - granting access")
            return {"isLeader": True, "hasCell": True, "canAccessEvents": True}

        print(f"   {user_email} is not a leader and has no special role")
        return {"isLeader": False, "hasCell": False, "canAccessEvents": False}

    except Exception as e:
        print(f"Error checking leader status: {str(e)}")
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
        
        print(f"Found {len(event_leaders)} unique Leader at 12 names in events")
        
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
        
        print(f"Found {len(found_leaders)} leaders in People database")
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
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    
# ADD INDEXES FOR FASTER QUERIES
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
        print(f"Error creating indexes: {e}")
    

@app.put("/events/{event_id}")
async def update_event(event_id: str, event_data: dict):
    """
    FIXED: Update event by _id or UUID
    """
    try:
        print(f"Attempting to update event with ID: {event_id}")
        print(f"📥 Received data: {event_data}")
        
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


@app.get("/debug/leader-check/{leader_name}")
async def debug_leader_check(leader_name: str):
    """Debug endpoint to check why a specific leader isn't getting Leader at 1"""
    try:
        print(f"DEBUG: Checking leader: {leader_name}")
        
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
            
            print(f"Found person: {person_name}")
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
            print(f"Person not found in database")
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
        
        # 🔥 FETCH ALL CELLS AND DEDUPLICATE IN PYTHON
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
    """FIXED: Shows cells with proper deduplication"""
    try:
        email = current_user.get("email")
        role = current_user.get("role", "user").lower()
        
        if not email:
            raise HTTPException(status_code=400, detail="User email not found")

        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
        start_date_obj = datetime.strptime(start_date or "2025-10-20", "%Y-%m-%d").date()
        
        print(f"Fetching cells for user: {email} (role: {role})")
        print(f"Date range: {start_date_obj} onwards")
        print(f"Personal filter: {personal}")

        # Build query based on role and personal filter
        query = {"Event Type": "Cells"}
        
        # Apply role-based filtering
        if role == "admin" and not personal:
            # Admin with "View All" - no email filter
            print("ADMIN VIEW ALL - Showing all cells")
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

        # USE AGGREGATION WITH $GROUP TO REMOVE DUPLICATES
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
        
        print(f"Found {len(all_cells_raw)} unique cells after deduplication")

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
                
                # FIX: Get persistent_attendees from the event
                persistent_attendees = event.get("persistent_attendees", [])
                
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
                    "persistent_attendees": persistent_attendees,  # ADD THIS
                    "did_not_meet": did_not_meet,
                    "status": status_val,
                    "Status": status_val.replace("_", " ").title(),
                    "_is_overdue": next_occurrence < today_date
                }
                
                processed_events.append(final_event)
                print(f"Processed {event_name}: {len(persistent_attendees)} persistent attendees")
                
            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {str(e)}")
                continue

        # Sort by date
        processed_events.sort(key=lambda x: x['date'])

        # Pagination
        total = len(processed_events)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_events = processed_events[start_idx:end_idx]

        print(f"Returning {len(paginated_events)} events (page {page} of {total_pages})")

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
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")
    
@app.get("/debug/leader-check/{leader_name}")
async def debug_leader_check(leader_name: str):
    """Debug endpoint to check why a specific leader isn't getting Leader at 1"""
    try:
        print(f"DEBUG: Checking leader: {leader_name}")
        
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
            
            print(f"Found THE PERSON: {person_name} using {search_method}")
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
            print(f"Person '{cleaned_name}' not found in database with any search method")
            return {
                "leader_name": leader_name,
                "found_in_database": False,
                "assigned_leader_at_1": "NOT FOUND - CANNOT ASSIGN"
            }
        
    except Exception as e:
        return {"error": str(e)}

async def get_leader_at_1_for_leader_at_12(leader_at_12_name: str) -> str:
    """
    FIXED: Get Leader @1 based on Leader @12's gender from People database
    """
    if not leader_at_12_name or not leader_at_12_name.strip():
        return ""
    
    cleaned_name = leader_at_12_name.strip()
    print(f"Looking up Leader @1 for Leader @12: '{cleaned_name}'")
    
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
                print(f"   Found person using query: {query}")
                break
        
        if not person:
            print(f"   Person '{cleaned_name}' NOT found in database")
            return ""
        
        # Get gender
        gender = (person.get("Gender") or "").lower().strip()
        person_full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
        
        print(f"   Found person: {person_full_name}")
        print(f"   Gender: '{gender}'")
        
        # SIMPLE GENDER-BASED ASSIGNMENT
        if gender in ["female", "f", "woman", "lady", "girl"]:
            print(f"   Assigned: Vicky Enslin (female)")
            return "Vicky Enslin"
        elif gender in ["male", "m", "man", "gentleman", "boy"]:
            print(f"   Assigned: Gavin Enslin (male)")
            return "Gavin Enslin"
        else:
            print(f"   Unknown gender: '{gender}' - cannot assign Leader @1")
            return ""
            
    except Exception as e:
        print(f"   Error looking up leader: {str(e)}")
        return ""

@app.post("/admin/events/fix-all-missing-leader-at-1")
async def fix_all_missing_leader_at_1(current_user: dict = Depends(get_current_user)):
    """
    UPDATED:
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

        print(f"Found {len(cell_events)} events missing Leader at 1")

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
                print(f"Person not found for {leader_name} ({leader_email})")
                failed_count += 1
                continue

            gender = str(person.get("Gender", "")).lower()

            # 3️⃣ Determine correct Leader @1 based on gender
            if gender == "female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "male":
                leader_at_1 = "Gavin Enslin"
            else:
                print(f"Gender unknown for {leader_name}")
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
                "status": "updated"
            })

            print(f"Updated {event_name}: {leader_name} ({gender}) → {leader_at_1}")

        return {
            "message": f"Fixed {updated_count} events, {failed_count} failed",
            "updated_count": updated_count,
            "failed_count": failed_count,
            "total_processed": len(cell_events),
            "results": results[:25]
        }

    except Exception as e:
        print(f"Error fixing leaders: {e}")
        raise HTTPException(status_code=500, detail=f"Error fixing leaders: {str(e)}")


async def get_leader_at_1_for_leader_at_144(leader_at_144_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 144.
    This should come from their Leader at 12
    """
    if not leader_at_144_name:
        return ""
    
    print(f"Getting Leader at 1 for Leader @144: {leader_at_144_name}")
    
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
        print(f"Leader @144 {leader_at_144_name} has Leader @12: {leader_at_12_name}")
        return await get_leader_at_1_for_leader_at_12(leader_at_12_name)
    
    print(f"Could not find Leader @12 for Leader @144: {leader_at_144_name}")
    return ""  # ADDED MISSING RETURN STATEMENT

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
    ENHANCED: Get Leader @1 based on event leader's position in hierarchy
    - If event leader IS a Leader at 12 (appears as Leader at 12 in other events), assign Gavin/Vicky
    - Otherwise, return empty
    """
    if not event_leader_name or not event_leader_name.strip():
        return ""
    
    cleaned_name = event_leader_name.strip()
    
    # Skip if already Gavin/Vicky
    if cleaned_name.lower() in ["gavin enslin", "vicky enslin"]:
        return ""
    
    print(f"Checking if event leader '{cleaned_name}' is a Leader at 12...")
    
    # Check if this person appears as Leader at 12 in ANY event
    is_leader_at_12 = await events_collection.find_one({
        "$or": [
            {"Leader at 12": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            {"Leader @12": {"$regex": f"^{cleaned_name}$", "$options": "i"}}
        ]
    })
    
    if is_leader_at_12:
        print(f"   {cleaned_name} IS a Leader at 12 - looking up their gender")
        # Now get their gender to assign Gavin/Vicky
        return await get_leader_at_1_for_leader_at_12(cleaned_name)
    
    print(f"   {cleaned_name} is NOT a Leader at 12")
    return ""
    
@app.get("/current-user/leader-at-1")
async def get_current_user_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Get the current user's recommended Leader at 1"""
    try:
        user_name = current_user.get("name", "").strip()
        user_email = current_user.get("email", "").strip()
        
        print(f"Getting Leader at 1 for user: {user_name} ({user_email})")
        
        if not user_name and not user_email:
            print("No user name or email found in token")
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
            print("No search conditions available")
            return {"leader_at_1": ""}
        
        query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
        
        print(f"Search query: {query}")
        
        # Try to find the user in people collection
        person = await people_collection.find_one(query)
        
        if not person:
            print(f"User not found in people database with any search criteria")
            # Try one more fallback: search by partial name match
            if user_name:
                fallback_person = await people_collection.find_one({
                    "Name": {"$regex": user_name, "$options": "i"}
                })
                if fallback_person:
                    print(f"Found user with fallback search: {fallback_person.get('Name')}")
                    person = fallback_person
        
        if not person:
            return {"leader_at_1": ""}
        
        print(f"Found user in people database: {person.get('Name')} {person.get('Surname', '')}")
        print(f"User data - Leader @12: {person.get('Leader @12')}, Leader @144: {person.get('Leader @144')}, Leader @1728: {person.get('Leader @ 1728')}")
        
        # Get Leader at 1 based on the user's position in hierarchy
        leader_at_1 = ""
        
        # Check if user is a Leader at 12
        if person.get("Leader @12"):
            print(f"User {person.get('Name')} is a Leader @12")
            leader_at_1 = await get_leader_at_1_for_leader_at_12(person.get("Name"))
        # Check if user is a Leader at 144  
        elif person.get("Leader @144"):
            print(f"User {person.get('Name')} is a Leader @144")
            leader_at_1 = await get_leader_at_1_for_leader_at_144(person.get("Name"))
        # Check if user is a Leader at 1728
        elif person.get("Leader @ 1728"):
            print(f"User {person.get('Name')} is a Leader @1728")
            leader_at_1 = await get_leader_at_1_for_leader_at_1728(person.get("Name"))
        else:
            print(f"User {person.get('Name')} has no leadership position")
        
        print(f"Recommended Leader at 1 for {person.get('Name')}: {leader_at_1}")
        return {"leader_at_1": leader_at_1}
        
    except Exception as e:
        print(f"Error getting current user leader at 1: {e}")
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
        print(f"DEBUG: Finding EXACT leader: {leader_name}")
        
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

# @app.get("/debug/test-leader-assignment/{leader_name}")
# async def debug_test_leader_assignment(leader_name: str):
#     """Test the actual leader assignment logic used in build_event_object"""
#     try:
#         print(f"TESTING ACTUAL LOGIC for: {leader_name}")
        
#         # This is the EXACT logic from build_event_object
#         leader_at_1 = ""
        
#         if leader_name:
#             print(f"   Leader at 12 provided: {leader_name}")
#             leader_at_1 = await get_leader_at_1_for_leader_at_12(leader_name)
#             print(f"   → Leader @1 assigned: {leader_at_1}")
        
#         return {
#             "leader_at_12_input": leader_name,
#             "leader_at_1_output": leader_at_1,
#             "success": bool(leader_at_1)
#         }
        
#     except Exception as e:
#         return {"error": str(e)}

@app.get("/debug/debug-search/{leader_name}")
async def debug_debug_search(leader_name: str):
    """Debug the exact search logic in get_leader_at_1_for_leader_at_12"""
    try:
        print(f"DEBUGGING SEARCH for: {leader_name}")
        
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
    submission: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Fixed: Handles frontend data structure properly with proper variable scope
    """
    try:
        print(f"SUBMIT ATTENDANCE STARTED")
        print(f"📥 Event ID: {event_id}")
        print(f"Submission keys: {list(submission.keys())}")

        # EXTRACT OBJECTID FROM COMPOSITE ID
        actual_event_id = event_id
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                print(f"Extracted ObjectId: {actual_event_id}")
            else:
                raise HTTPException(status_code=400, detail="Invalid event ID format")

        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            print(f"Event not found: {actual_event_id}")
            raise HTTPException(status_code=404, detail="Event not found")
        
        event_name = event.get("Event Name", "Unknown")
        current_week = get_current_week_identifier()
        print(f"Found event: {event_name}")

        # EXTRACT DATA FROM SUBMISSION
        attendees_data = submission.get('attendees', [])
        if not attendees_data and 'payload' in submission:
            attendees_data = submission['payload'].get('attendees', [])
        
        print(f"👥 Attendees data type: {type(attendees_data)}, length: {len(attendees_data)}")

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
                        "leader144": attendee.get("leader144", "")
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
                        "check_in_date": datetime.utcnow().isoformat()
                    }
                    checked_in_attendees.append(attendee_data)

        # HANDLE DID_NOT_MEET LOGIC
        if did_not_meet:
            print(f"Marking as 'Did Not Meet'")
            weekly_attendance_entry = {
                "status": "did_not_meet",
                "attendees": [],
                "submitted_at": datetime.utcnow(),
                "submitted_by": current_user.get('email', ''),
                "persistent_attendees_count": len(persistent_attendees_dict)
            }
            
            main_update_fields = {
                "Status": "Did Not Meet",
                "status": "did_not_meet",
                "did_not_meet": True,
            }
        else:
            # HANDLE REGULAR ATTENDANCE
            if len(checked_in_attendees) == 0:
                print(f"No attendees checked in - marking as incomplete")
                weekly_attendance_entry = {
                    "status": "incomplete",
                    "attendees": [],
                    "submitted_at": datetime.utcnow(),
                    "submitted_by": current_user.get('email', ''),
                    "persistent_attendees_count": len(persistent_attendees_dict)
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
                    "submitted_by": current_user.get('email', ''),
                    "persistent_attendees": persistent_attendees_dict,
                }
                print(f"MARKING AS COMPLETE with {len(checked_in_attendees)} attendees")
                
                main_update_fields = {
                    "Status": "Complete",
                    "status": "complete",
                    "did_not_meet": False,
                }

        # PREPARE UPDATE DATA
        update_data = {
            "Date Captured": datetime.now().strftime("%d %B %Y"),
            "updated_at": datetime.utcnow(),
            **main_update_fields,
            "persistent_attendees": persistent_attendees_dict,
            f"attendance.{current_week}": weekly_attendance_entry
        }

        print(f"Saving to database:")
        print(f"   - Event: {event_name}")
        print(f"   - Week: {current_week}")
        print(f"   - Status: {weekly_attendance_entry.get('status', 'unknown')}")
        print(f"   - Persistent attendees: {len(persistent_attendees_dict)}")
        print(f"   - Checked-in this week: {len(checked_in_attendees)}")

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
            "success": True
        }

        print(f"ATTENDANCE SUBMISSION SUCCESSFUL")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR in submit_attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

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
        
        print(f"Added UUIDs to {updated_count} events")
        
        return {
            "message": f"Successfully added UUIDs to {updated_count} events",
            "updated_count": updated_count
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
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
        print(f"PROFILE UPDATE ENDPOINT CALLED")
        print(f"User ID from URL: {user_id}")
        print(f"Current User ID from Token: {current_user.get('user_id')}")
        
        # Check authorization
        token_user_id = current_user.get("user_id")
        if not token_user_id or token_user_id != user_id:
            print(f"AUTHORIZATION FAILED: {token_user_id} != {user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Get and parse request body
        body = await request.body()
        body_str = body.decode('utf-8')
        print(f"RAW REQUEST BODY: {body_str}")
        
        try:
            update_data = json.loads(body_str)
        except json.JSONDecodeError as e:
            print(f"JSON PARSE ERROR: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

        print(f"PARSED UPDATE DATA: {update_data}")

        # Check if user exists
        existing_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not existing_user:
            print(f"USER NOT FOUND: {user_id}")
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
                    print(f"Mapping {frontend_field} -> {db_field}: {value}")

        # Add update timestamp
        update_payload["updated_at"] = datetime.utcnow().isoformat()
        
        print(f"🚀 FINAL UPDATE PAYLOAD: {update_payload}")

        if not update_payload:
            print("No fields to update")
            return {
                "message": "No changes to update",
                "user": format_user_response(existing_user)
            }

        # Perform the update
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)}, 
            {"$set": update_payload}
        )

        print(f"UPDATE RESULT - matched: {result.matched_count}, modified: {result.modified_count}")

        # Fetch and return updated user
        updated_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found after update")

        response_data = format_user_response(updated_user)
        print(f"UPDATE SUCCESSFUL: {response_data}")

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        print(f"UNEXPECTED ERROR: {str(e)}")
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
        
        print(f"Creating consolidation for: {consolidation.person_name} {consolidation.person_surname}")
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
            print(f"Found existing person: {person_id}")
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
            # 🚨 FIX: Create new person with ALL required fields
            person_doc = {
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Gender": "",  # Add default gender
                "Address": "",  # Add default address
                "Birthday": "",  # Add default birthday
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
            
            # 🚨 CRITICAL: Add the new person to the background cache
            new_person_cache_entry = {
                "_id": person_id,
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Gender": "",
                "Leader @1": consolidation.leaders[0] if len(consolidation.leaders) > 0 else "",
                "Leader @12": consolidation.leaders[1] if len(consolidation.leaders) > 1 else "",
                "Leader @144": consolidation.leaders[2] if len(consolidation.leaders) > 2 else "",
                "Leader @1728": consolidation.leaders[3] if len(consolidation.leaders) > 3 else "",
                "FullName": f"{consolidation.person_name.strip()} {consolidation.person_surname.strip()}".strip()
            }
            people_cache["data"].append(new_person_cache_entry)
            print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")

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
                print(f"Found leader email from people collection: {leader_email}")
        
        if leader_email:
            # Find leader's user account
            leader_user = await users_collection.find_one({"email": leader_email})
            if leader_user:
                leader_user_id = str(leader_user["_id"])
                print(f"Found leader user account: {leader_email} (ID: {leader_user_id})")
            else:
                print(f"Leader {consolidation.assigned_to} has no user account with email: {leader_email}")
        else:
            print(f"Could not find email for leader: {consolidation.assigned_to}")

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
        print(f"Created consolidation task: {task_id} assigned to {assigned_for}")

        # 4. 🚨 CRITICAL FIX: Add to event consolidations array ONLY, NOT attendees
        if consolidation.event_id and ObjectId.is_valid(consolidation.event_id):
            consolidation_record = {
                "id": consolidation_id,
                "person_id": person_id,
                "person_name": consolidation.person_name,
                "person_surname": consolidation.person_surname,
                "person_email": person_email,
                "person_phone": consolidation.person_phone or "",
                "decision_type": consolidation.decision_type.value,
                "decision_display_name": decision_display_name,
                "assigned_to": consolidation.assigned_to,
                "assigned_to_email": leader_email,
                "created_at": datetime.utcnow().isoformat(),
                "type": "consolidation",
                "status": "active",
                "notes": consolidation.notes
            }

            # 🚨 FIX: Only add to consolidations array, NOT attendees
            await events_collection.update_one(
                {"_id": ObjectId(consolidation.event_id)},
                {
                    "$push": {"consolidations": consolidation_record},
                    "$set": {"updated_at": datetime.utcnow().isoformat()}
                }
            )
            print(f"Added to event consolidations: {consolidation.event_id}")

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
        print(f"Created consolidation record: {consolidation_id}")

        # 6. 🚨 UPDATE PEOPLE COUNT IN CACHE
        # Refresh the total people count in cache
        total_people_count = await people_collection.count_documents({})
        print(f"Updated total people count: {total_people_count}")

        return {
            "message": f"{decision_display_name} recorded successfully and assigned to {consolidation.assigned_to}",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "task_id": task_id,
            "decision_type": consolidation.decision_type.value,
            "assigned_to": consolidation.assigned_to,
            "assigned_to_email": leader_email,
            "leader_user_id": leader_user_id,
            "people_count_updated": total_people_count,
            "success": True
        }

    except Exception as e:
        print(f"Error creating consolidation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creating consolidation: {str(e)}")


# === ADD THIS AT THE END OF main.py (no import needed) ===

@app.get("/api/users")
async def get_all_users():
    try:
        users_cursor = users_collection.find({}, {"password": 0})
        users_list = await users_cursor.to_list(length=1000)

        formatted_users = []
        for user in users_list:
            full_name = f"{user.get('name', '')} {user.get('surname', '')}".strip()
            if not full_name:
                full_name = user.get("email", "").split("@")[0]

            formatted_users.append({
                "_id": str(user["_id"]),
                "email": user.get("email", ""),
                "name": user.get("name", ""),
                "surname": user.get("surname", ""),
                "fullName": full_name,
                "role": user.get("role", "member"),
                "phone": user.get("phone", ""),
                "avatar": user.get("avatar"),
                "created_at": user.get("created_at")
            })

        return {
            "success": True,
            "count": len(formatted_users),
            "users": formatted_users
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch users: {str(e)}") 
    
@app.get("/tasks/all")
async def get_all_tasks(
        current_user: dict = Depends(get_current_user)
    ):
        """
        Dedicated endpoint: Get ALL tasks for every user
        Only accessible to leaders, admins, and managers
        Used by StatsDashboard & Admin panels
        """
        try:
            # Permission check — only leaders can see all tasks
            role = current_user.get("role", "").lower()
            if role not in ["admin", "leader", "manager"]:
                return {
                    "error": "Access denied. You must be a leader or admin to view all tasks.",
                    "status": "failed"
                }, 403

            timezone = pytz.timezone("Africa/Johannesburg")
            cursor = tasks_collection.find({})  # No filter → ALL tasks
            all_tasks = []

            async for task in cursor:
                # Safely parse followup_date
                followup_raw = task.get("followup_date")
                followup_dt = None
                if followup_raw:
                    if isinstance(followup_raw, datetime):
                        followup_dt = followup_raw
                    else:
                        try:
                            dt_str = str(followup_raw).replace("Z", "+00:00")
                            followup_dt = datetime.fromisoformat(dt_str)
                        except:
                            try:
                                followup_dt = datetime.fromisoformat(str(followup_raw))
                            except:
                                logging.warning(f"Invalid date format in task {task['_id']}: {followup_raw}")

                    if followup_dt:
                        if followup_dt.tzinfo is None:
                            followup_dt = pytz.utc.localize(followup_dt)
                        followup_dt = followup_dt.astimezone(timezone)

                # Resolve full user info for legacy assignedfor (email string)
                assigned_to = None
                if task.get("assignedTo") and isinstance(task["assignedTo"], dict):
                    assigned_to = task["assignedTo"]
                elif task.get("assignedfor"):
                    user = await users_collection.find_one(
                        {"email": {"$regex": f"^{task['assignedfor'].strip()}$", "$options": "i"}},
                        {"name": 1, "surname": 1, "email": 1, "phone": 1}
                    )
                    if user:
                        assigned_to = {
                            "_id": str(user["_id"]),
                            "name": user.get("name", ""),
                            "surname": user.get("surname", ""),
                            "email": user.get("email", ""),
                            "phone": user.get("phone", "")
                        }

                all_tasks.append({
                    "_id": str(task["_id"]),
                    "name": task.get("name", "Unnamed Task"),
                    "taskType": task.get("taskType", ""),
                    "followup_date": followup_dt.isoformat() if followup_dt else None,
                    "status": task.get("status", "Open"),
                    "assignedfor": task.get("assignedfor", ""),
                    "assignedTo": assigned_to,  # Fully resolved user
                    "type": task.get("type", "call"),
                    "contacted_person": task.get("contacted_person", {}),
                    "isRecurring": bool(task.get("recurring_day")),
                    "createdAt": task.get("createdAt", datetime.utcnow()).isoformat() if task.get("createdAt") else None,
                })

            # Sort newest first
            all_tasks.sort(key=lambda x: x["followup_date"] or "9999-12-31", reverse=True)

            return {
                "total_tasks": len(all_tasks),
                "tasks": all_tasks,
                "status": "success",
                "fetched_by": current_user.get("email"),
                "role": current_user.get("role"),
                "timestamp": datetime.now(timezone).isoformat(),
                "message": "All tasks loaded successfully"
            }

        except Exception as e:
            logging.error(f"Error in /tasks/all: {e}", exc_info=True)
            return {
                "error": "Failed to fetch all tasks",
                "details": str(e),
                "status": "failed"
            }, 500        

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
    
# @app.get("/service-checkin/real-time-data")
# async def get_service_checkin_real_time_data(
#     event_id: str = Query(..., description="Event ID to get real-time data for"),
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Get real-time data for service check-in with all three data types
#     """
#     try:
#         print(f"🔍 Getting real-time data for event: {event_id}")
        
#         if not ObjectId.is_valid(event_id):
#             raise HTTPException(status_code=400, detail="Invalid event ID")

#         # Get the event
#         event = await events_collection.find_one({"_id": ObjectId(event_id)})
#         if not event:
#             raise HTTPException(status_code=404, detail="Event not found")

#         # Extract the three data types from the event
#         attendees = event.get("attendees", [])
#         new_people = event.get("new_people", [])
#         consolidations = event.get("consolidations", [])

#         # Counts for stats cards
#         present_count = len([a for a in attendees if a.get("checked_in", False)])
#         new_people_count = len(new_people)
#         consolidation_count = len(consolidations)

#         print(f"📊 Real-time stats - Present: {present_count}, New: {new_people_count}, Consolidations: {consolidation_count}")

#         return {
#             "success": True,
#             "event_id": event_id,
#             "event_name": event.get("eventName", "Unknown Event"),
#             "present_attendees": attendees,
#             "new_people": new_people,
#             "consolidations": consolidations,
#             "present_count": present_count,
#             "new_people_count": new_people_count,
#             "consolidation_count": consolidation_count,
#             "total_attendance": len(attendees)
#         }

#     except Exception as e:
#         print(f"❌ Error getting real-time data: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {str(e)}")
    
@app.get("/service-checkin/real-time-data")
async def get_service_checkin_real_time_data(
    event_id: str = Query(..., description="Event ID to get real-time data for"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get real-time data for service check-in with all three data types
    - FIXED: Returns ACTUAL counts from database
    """
    try:
        print(f"Getting real-time data for event: {event_id}")
        
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Get the event FRESH from database
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Extract the three data types from the event - COUNT PROPERLY
        attendees = event.get("attendees", [])
        new_people = event.get("new_people", [])
        consolidations = event.get("consolidations", [])

        # Counts for stats cards - COUNT ACTUAL CHECKED-IN PEOPLE
        present_count = len([a for a in attendees if a.get("checked_in", False) or a.get("is_checked_in", False)])
        new_people_count = len(new_people)
        consolidation_count = len(consolidations)

        print(f"Real-time stats - Present: {present_count}, New: {new_people_count}, Consolidations: {consolidation_count}")

        return {
            "success": True,
            "event_id": event_id,
            "event_name": event.get("eventName", "Unknown Event"),
            "present_attendees": attendees,
            "new_people": new_people,
            "consolidations": consolidations,
            "present_count": present_count,  # ACTUAL COUNT FROM DB
            "new_people_count": new_people_count,  # ACTUAL COUNT FROM DB
            "consolidation_count": consolidation_count,  # ACTUAL COUNT FROM DB
            "total_attendance": len(attendees),
            "refreshed_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        print(f"Error getting real-time data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {str(e)}")

# @app.post("/service-checkin/checkin")
# async def service_checkin_person(
#     checkin_data: dict = Body(...),
#     current_user: dict = Depends(get_current_user)
# ):
#     """
#     Service Check-in:
#     - attendee:   Existing person in the People database
#     - new_person: Visitor NOT in database (only recorded for event)
#     - consolidation: Decision/Follow-up
#     """
#     try:
#         event_id = checkin_data.get("event_id")
#         person_data = checkin_data.get("person_data", {})
#         checkin_type = checkin_data.get("type", "attendee")

#         if not event_id or not ObjectId.is_valid(event_id):
#             raise HTTPException(status_code=400, detail="Invalid event ID")

#         # Get event
#         event = await events_collection.find_one({"_id": ObjectId(event_id)})
#         if not event:
#             raise HTTPException(status_code=404, detail="Event not found")

#         now = datetime.utcnow().isoformat()

#         # ============================================================
#         # 1️⃣ ATTENDEE — Must exist in People database
#         # ============================================================
#         if checkin_type == "attendee":
#             person_id = person_data.get("id") or person_data.get("_id")
#             if not person_id or not ObjectId.is_valid(person_id):
#                 raise HTTPException(
#                     status_code=400,
#                     detail="Valid person ID is required for attendee check-in"
#                 )

#             # Find person
#             existing = await people_collection.find_one({"_id": ObjectId(person_id)})
#             if not existing:
#                 raise HTTPException(
#                     status_code=404,
#                     detail="Person does not exist — add them first using /people"
#                 )

#             # Prevent duplicate check-in
#             already_checked = await events_collection.find_one({
#                 "_id": ObjectId(event_id),
#                 "attendees.id": str(existing["_id"])
#             })
#             if already_checked:
#                 raise HTTPException(
#                     status_code=400,
#                     detail=f"{existing.get('Name')} is already checked in"
#                 )

#             attendee_record = {
#                 "id": str(existing["_id"]),
#                 "name": existing.get("Name", ""),
#                 "surname": existing.get("Surname", ""),
#                 "email": existing.get("Email", ""),
#                 "phone": existing.get("Number", ""),
#                 "time": now,
#                 "type": "attendee"
#             }

#             await events_collection.update_one(
#                 {"_id": ObjectId(event_id)},
#                 {
#                     "$push": {"attendees": attendee_record},
#                     "$inc": {"total_attendance": 1},
#                     "$set": {"updated_at": now}
#                 }
#             )

#             return {
#                 "message": f"{existing.get('Name')} checked in",
#                 "type": "attendee",
#                 "attendee": attendee_record,
#                 "success": True
#             }

#         # ============================================================
#         # 2️⃣ NEW PERSON — Visitors NOT in database
#         # ============================================================
#         elif checkin_type == "new_person":

#             new_person_id = f"new_{secrets.token_urlsafe(8)}"

#             new_person_record = {
#                 "id": new_person_id,
#                 "name": person_data.get("name", ""),
#                 "surname": person_data.get("surname", ""),
#                 "email": person_data.get("email", ""),
#                 "phone": person_data.get("phone", ""),
#                 "gender": person_data.get("gender", ""),
#                 "invitedBy": person_data.get("invitedBy", ""),
#                 "added_at": now,
#                 "type": "new_person",
#                 "needs_database_entry": True,  # Tells the team to add them via /people later
#                 "is_checked_in": True,
#                 "notes": "Visitor - add to database later if needed"
#             }

#             await events_collection.update_one(
#                 {"_id": ObjectId(event_id)},
#                 {
#                     "$push": {"new_people": new_person_record},
#                     "$set": {"updated_at": now}
#                 }
#             )

#             return {
#                 "message": "Visitor added to event",
#                 "type": "new_person",
#                 "new_person": new_person_record,
#                 "success": True
#             }

#         # ============================================================
#         # 3️⃣ CONSOLIDATION — Follow-up decisions
#         # ============================================================
#         elif checkin_type == "consolidation":

#             consolidation_id = f"con_{secrets.token_urlsafe(8)}"

#             consolidation_record = {
#                 "id": consolidation_id,
#                 "person_name": person_data.get("person_name", ""),
#                 "person_surname": person_data.get("person_surname", ""),
#                 "person_email": person_data.get("person_email", ""),
#                 "person_phone": person_data.get("person_phone", ""),
#                 "decision_type": person_data.get("decision_type", "first_time"),
#                 "decision_display_name": person_data.get("decision_display_name", ""),
#                 "assigned_to": person_data.get("assigned_to", ""),
#                 "notes": person_data.get("notes", ""),
#                 "created_at": now,
#                 "type": "consolidation",
#                 "status": "active"
#             }

#             await events_collection.update_one(
#                 {"_id": ObjectId(event_id)},
#                 {
#                     "$push": {"consolidations": consolidation_record},
#                     "$set": {"updated_at": now}
#                 }
#             )

#             return {
#                 "message": "Decision recorded",
#                 "type": "consolidation",
#                 "consolidation": consolidation_record,
#                 "success": True
#             }

#         # ============================================================
#         # ❌ INVALID TYPE
#         # ============================================================
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Invalid type — must be attendee, new_person, or consolidation"
#             )

#     except HTTPException:
#         raise
#     except Exception as e:
#         print("Error in check-in:", e)
#         raise HTTPException(status_code=500, detail="Check-in failed")
@app.post("/service-checkin/checkin")
async def service_checkin_person(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Service Check-in - FIXED: Returns ACTUAL counts after operation
    """
    try:
        event_id = checkin_data.get("event_id")
        person_data = checkin_data.get("person_data", {})
        checkin_type = checkin_data.get("type", "attendee")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Get event FRESH from database
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        now = datetime.utcnow().isoformat()

        # ============================================================
        # 1️⃣ ATTENDEE — Must exist in People database
        # ============================================================
        if checkin_type == "attendee":
            person_id = person_data.get("id") or person_data.get("_id")
            if not person_id or not ObjectId.is_valid(person_id):
                raise HTTPException(
                    status_code=400,
                    detail="Valid person ID is required for attendee check-in"
                )

            # Find person
            existing = await people_collection.find_one({"_id": ObjectId(person_id)})
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail="Person does not exist — add them first using /people"
                )

            # Prevent duplicate check-in
            already_checked = await events_collection.find_one({
                "_id": ObjectId(event_id),
                "attendees.id": str(existing["_id"])
            })
            if already_checked:
                raise HTTPException(
                    status_code=400,
                    detail=f"{existing.get('Name')} is already checked in"
                )

            attendee_record = {
                "id": str(existing["_id"]),
                "name": existing.get("Name", ""),
                "surname": existing.get("Surname", ""),
                "email": existing.get("Email", ""),
                "phone": existing.get("Number", ""),
                "time": now,
                "checked_in": True,  # IMPORTANT: Mark as checked in
                "type": "attendee"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"attendees": attendee_record},
                    "$inc": {"total_attendance": 1},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            updated_attendees = updated_event.get("attendees", [])
            present_count = len([a for a in updated_attendees if a.get("checked_in", False)])

            return {
                "message": f"{existing.get('Name')} checked in",
                "type": "attendee",
                "attendee": attendee_record,
                "present_count": present_count,  # ACTUAL COUNT FROM DB
                "success": True
            }

        # ============================================================
        # 2️⃣ NEW PERSON — Visitors NOT in database
        # ============================================================
        elif checkin_type == "new_person":
            new_person_id = f"new_{secrets.token_urlsafe(8)}"

            new_person_record = {
                "id": new_person_id,
                "name": person_data.get("name", ""),
                "surname": person_data.get("surname", ""),
                "email": person_data.get("email", ""),
                "phone": person_data.get("phone", ""),
                "gender": person_data.get("gender", ""),
                "invitedBy": person_data.get("invitedBy", ""),
                "added_at": now,
                "type": "new_person",
                "needs_database_entry": True,
                "is_checked_in": True,
                "notes": "Visitor - add to database later if needed"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"new_people": new_person_record},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            new_people_count = len(updated_event.get("new_people", []))

            return {
                "message": "Visitor added to event",
                "type": "new_person",
                "new_person": new_person_record,
                "new_people_count": new_people_count,  # ACTUAL COUNT FROM DB
                "success": True
            }

        # ============================================================
        # 3️⃣ CONSOLIDATION — Follow-up decisions
        # ============================================================
        elif checkin_type == "consolidation":
            consolidation_id = f"con_{secrets.token_urlsafe(8)}"

            consolidation_record = {
                "id": consolidation_id,
                "person_name": person_data.get("person_name", ""),
                "person_surname": person_data.get("person_surname", ""),
                "person_email": person_data.get("person_email", ""),
                "person_phone": person_data.get("person_phone", ""),
                "decision_type": person_data.get("decision_type", "first_time"),
                "decision_display_name": person_data.get("decision_display_name", ""),
                "assigned_to": person_data.get("assigned_to", ""),
                "notes": person_data.get("notes", ""),
                "created_at": now,
                "type": "consolidation",
                "status": "active"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"consolidations": consolidation_record},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            consolidation_count = len(updated_event.get("consolidations", []))

            return {
                "message": "Decision recorded",
                "type": "consolidation",
                "consolidation": consolidation_record,
                "consolidation_count": consolidation_count,  # ACTUAL COUNT FROM DB
                "success": True
            }

        # ============================================================
        # INVALID TYPE
        # ============================================================
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid type — must be attendee, new_person, or consolidation"
            )

    except HTTPException:
        raise
    except Exception as e:
        print("Error in check-in:", e)
        raise HTTPException(status_code=500, detail="Check-in failed")

@app.delete("/service-checkin/remove")
async def remove_from_service_checkin(
    removal_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Remove a person from any of the three data types in an event
    - attendees, new_people, or consolidations
    """
    try:
        event_id = removal_data.get("event_id")
        person_id = removal_data.get("person_id")
        data_type = removal_data.get("type")  # attendees, new_people, consolidations

        print(f"🗑️ Removing from service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        # Build the update query
        update_query = {
            "$pull": {data_type: {"id": person_id}},
            "$set": {"updated_at": datetime.utcnow().isoformat()}
        }

        # If removing from attendees, also decrement total_attendance
        if data_type == "attendees":
            update_query["$inc"] = {"total_attendance": -1}

        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            update_query
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found in specified list")

        print(f"✅ Successfully removed from {data_type}")

        # Get updated counts for real-time sync
        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        
        # Calculate updated counts
        present_count = len([a for a in updated_event.get("attendees", []) if a.get("checked_in", False)])
        new_people_count = len(updated_event.get("new_people", []))
        consolidation_count = len(updated_event.get("consolidations", []))

        return {
            "success": True,
            "message": f"Person removed from {data_type} successfully",
            "updated_counts": {
                "present_count": present_count,
                "new_people_count": new_people_count,
                "consolidation_count": consolidation_count
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error removing from service check-in: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error removing person: {str(e)}")
    
@app.put("/service-checkin/update")
async def update_service_checkin_person(
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update a person in any of the three data types
    """
    try:
        event_id = update_data.get("event_id")
        person_id = update_data.get("person_id")
        data_type = update_data.get("type")  # attendees, new_people, consolidations
        update_fields = update_data.get("update_fields", {})

        print(f"✏️ Updating service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        # Build the update query
        set_fields = {}
        for field, value in update_fields.items():
            set_fields[f"{data_type}.$.{field}"] = value

        set_fields["updated_at"] = datetime.utcnow().isoformat()

        result = await events_collection.update_one(
            {
                "_id": ObjectId(event_id),
                f"{data_type}.id": person_id
            },
            {
                "$set": set_fields
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found or no changes made")

        print(f"Successfully updated in {data_type}")

        return {
            "success": True,
            "message": f"Person updated in {data_type} successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating service check-in: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating person: {str(e)}")
    
    
@app.post("/events/{event_id}/initialize-structure")
async def initialize_event_structure(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Initialize a new event with the three-type structure
    """
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Check if structure already exists
        if "new_people" in event and "consolidations" in event:
            return {
                "success": True,
                "message": "Event already has the new structure",
                "already_initialized": True
            }

        # Initialize the three arrays if they don't exist
        update_data = {
            "attendees": event.get("attendees", []),
            "new_people": event.get("new_people", []),
            "consolidations": event.get("consolidations", []),
            "updated_at": datetime.utcnow().isoformat()
        }

        # Ensure total_attendance exists
        if "total_attendance" not in event:
            update_data["total_attendance"] = len(update_data["attendees"])

        await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )

        print(f"Event structure initialized: {event_id}")

        return {
            "success": True,
            "message": "Event structure initialized successfully",
            "already_initialized": False,
            "attendees_count": len(update_data["attendees"]),
            "new_people_count": len(update_data["new_people"]),
            "consolidations_count": len(update_data["consolidations"])
        }

    except Exception as e:
        print(f"Error initializing event structure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error initializing event: {str(e)}")
    
@app.post("/admin/migrate-all-events-structure")
async def migrate_all_events_structure(current_user: dict = Depends(get_current_user)):
    """
    Migrate ALL events to the new three-type structure
    Admin only
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        print("Starting migration of all events to new structure...")
        
        # Get all events
        all_events = await events_collection.find({}).to_list(length=None)
        migrated_count = 0
        results = []

        for event in all_events:
            try:
                event_id = event["_id"]
                
                # Skip events that already have the new structure
                if "new_people" in event and "consolidations" in event:
                    continue

                # Transform existing data
                old_attendees = event.get("attendees", [])
                new_attendees = []
                new_people = []
                consolidations = []

                for attendee in old_attendees:
                    if isinstance(attendee, dict):
                        # Check if this is a consolidation (has decision field)
                        if attendee.get("decision") or attendee.get("is_consolidation"):
                            consolidation_record = {
                                "id": attendee.get("id", f"consolidation_{secrets.token_urlsafe(8)}"),
                                "person_name": attendee.get("name", ""),
                                "person_surname": attendee.get("surname", ""),
                                "person_email": attendee.get("email", ""),
                                "person_phone": attendee.get("phone", ""),
                                "decision_type": attendee.get("decision", "first_time"),
                                "decision_display_name": attendee.get("decision_display", 
                                    "First Time Decision" if attendee.get("decision") == "first_time" else "Recommitment"),
                                "assigned_to": attendee.get("assigned_leader", ""),
                                "assigned_to_email": attendee.get("assigned_leader_email", ""),
                                "created_at": attendee.get("time", datetime.utcnow().isoformat()),
                                "type": "consolidation",
                                "status": "active"
                            }
                            consolidations.append(consolidation_record)
                        else:
                            # Regular attendee
                            attendee_record = {
                                "id": attendee.get("id", f"attendee_{secrets.token_urlsafe(8)}"),
                                "name": attendee.get("name", ""),
                                "fullName": attendee.get("fullName", attendee.get("name", "")),
                                "email": attendee.get("email", ""),
                                "phone": attendee.get("phone", ""),
                                "leader12": attendee.get("leader12", ""),
                                "time": attendee.get("time", datetime.utcnow().isoformat()),
                                "checked_in": attendee.get("checked_in", True),
                                "type": "attendee"
                            }
                            new_attendees.append(attendee_record)

                update_data = {
                    "attendees": new_attendees,
                    "new_people": new_people,
                    "consolidations": consolidations,
                    "updated_at": datetime.utcnow().isoformat()
                }

                if "total_attendance" not in event:
                    update_data["total_attendance"] = len(new_attendees)

                await events_collection.update_one(
                    {"_id": event_id},
                    {"$set": update_data}
                )

                migrated_count += 1
                results.append({
                    "event_id": str(event_id),
                    "event_name": event.get("eventName", "Unknown"),
                    "attendees": len(new_attendees),
                    "consolidations": len(consolidations)
                })

                print(f"Migrated: {event.get('eventName', 'Unknown')}")

            except Exception as e:
                print(f"Error migrating event {event.get('eventName')}: {str(e)}")
                continue

        print(f"Migration complete! Migrated {migrated_count} events")

        return {
            "success": True,
            "message": f"Migrated {migrated_count} events to new structure",
            "migrated_count": migrated_count,
            "total_events": len(all_events),
            "results": results
        }

    except Exception as e:
        print(f"Error in bulk migration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error migrating events: {str(e)}")
    
    
@app.patch("/events/{event_id}")
async def update_event_status(
    event_id: str = Path(...),
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update event status to complete - properly handles PATCH requests
    """
    try:
        print(f"🔧 UPDATE EVENT STATUS: {event_id}")
        print(f"📥 Update data: {update_data}")
        
        # Check if this is a status update to "complete"
        new_status = update_data.get("status")
        if new_status != "complete":
            return JSONResponse(
                status_code=400,
                content={"error": "Only status update to 'complete' is allowed"}
            )
        
        # Find the event
        if not ObjectId.is_valid(event_id):
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid event ID format"}
            )
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            return JSONResponse(
                status_code=404,
                content={"error": "Event not found"}
            )
        
        event_name = event.get("Event Name") or event.get("eventName", "Unknown Event")
        print(f"Found event: {event_name}")
        
        # Prepare update data
        update_payload = {
            "status": "complete",
            "Status": "Complete",
            "updated_at": datetime.utcnow().isoformat(),
            "closed_at": datetime.utcnow().isoformat(),
            "closed_by": current_user.get("email", ""),
            "closed_by_name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
        }
        
        # Update the event
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_payload}
        )
        
        if result.modified_count == 0:
            print(f"No changes made to event {event_id}")
            # Still return success as the event might already be complete
            return {
                "message": f"Event '{event_name}' status updated to complete",
                "event": {
                    "id": event_id,
                    "eventName": event_name,
                    "status": "complete"
                }
            }
        
        print(f"Event '{event_name}' successfully closed")
        
        # Return the updated event data
        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        
        response_data = {
            "message": f"Event '{event_name}' closed successfully!",
            "event": {
                "id": event_id,
                "eventName": event_name,
                "status": "complete",
                "closed_at": update_payload["closed_at"],
                "closed_by": update_payload["closed_by"]
            }
        }
        
        return response_data
        
    except Exception as e:
        print(f"ERROR updating event status: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Event may still be open in the database. Please check. Error: {str(e)}"}
        )

# Also add PUT method for compatibility with your frontend fallback
@app.put("/events/{event_id}")
async def update_event_status_put(
    event_id: str = Path(...),
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    PUT endpoint for event status update (fallback for frontend)
    """
    return await update_event_status(event_id, update_data, current_user)