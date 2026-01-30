# Annotated portion (lines 1-400) of main.py
# Each comment explains the purpose of the following line or small group of lines.

# Import OS module for environment variables and file paths
import os
# Import datetime utilities used across the app
from datetime import datetime, timedelta, date
# Import time module for simple timing operations
import time
# Import ObjectId type for MongoDB document ids
from bson import ObjectId
# Import regular expressions
import re
# Import FastAPI building blocks and request artifacts
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks, File, UploadFile
# Import JSON response helper
from fastapi.responses import JSONResponse
# Import CORS middleware so frontend domains can call API
from fastapi.middleware.cors import CORSMiddleware
# Import Pydantic models and types from auth.models (used to validate payloads)
from auth.models import EventCreate,DecisionType, UserProfile, ConsolidationCreate, UserProfileUpdate, CheckIn, UncaptureRequest, UserCreate,UserCreater,  UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel, PersonCreate, EventTypeCreate, UserListResponse, UserList, MessageResponse, PermissionUpdate, RoleUpdate, AttendanceSubmission, TaskUpdate, EventUpdate ,TaskTypeIn ,TaskTypeOut , LeaderStatusResponse, UserProfile, AttendanceSubmission
# Import utility functions from auth.utils used by endpoints
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token , task_type_serializer, get_current_user 
# Import math for numeric checks
import math
# Import secrets for secure token generation
import secrets
# Import database handles and collections
from database import db, events_collection, people_collection, users_collection, tasks_collection ,tasktypes_collection
# Import email sending helper
from auth.email_utils import send_reset_email
# Typing helpers
from typing import Optional, List,  Optional,  Dict
# Counter for aggregations
from collections import Counter
# Logging
import logging
# Timezone handling
import pytz
# For base64 encoding/decoding
import base64
# UUID generation
import uuid
# Security scheme for endpoints that require bearer auth
from fastapi.security import HTTPBearer
oauth2_scheme = HTTPBearer()
# Password hashing context (passlib) - additional use in file
from passlib.context import CryptContext
# JSON utilities
import json
# URL handling
from urllib.parse import unquote
# Exception tracebacks
import traceback
# Async utilities
import asyncio
# Background scheduler for periodic tasks
from apscheduler.schedulers.background import BackgroundScheduler


# Create FastAPI application instance
app = FastAPI()

# Configure CORS middleware to allow specific origins and headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://teams.theactivechurch.org",
        "http://localhost:8000",
        "http://localhost:5173",  
        "https://new-active-teams.netlify.app",
        "https://activeteams.netlify.app",
        "https://activeteamsbackend2.0.onrender.com"
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

# Root endpoint to verify app is running
@app.get("/")
def root():
    # Simple JSON response verifying service is live
    return {"message": "App is live on Render!"}

# Helper to sanitize documents and replace NaN/Infinity floats with None
def sanitize_document(doc):
    """Recursively sanitize document to replace NaN/Infinity float values with None."""
    for k, v in doc.items():
        if isinstance(v, float):
            # Replace non-finite floats with None
            if math.isnan(v) or math.isinf(v):
                doc[k] = None
        elif isinstance(v, dict):
            # Recurse into nested dicts
            sanitize_document(v)
        elif isinstance(v, list):
            # Iterate through lists and sanitize elements
            for i in range(len(v)):
                if isinstance(v[i], dict):
                    sanitize_document(v[i])
                elif isinstance(v[i], float) and (math.isnan(v[i]) or math.isinf(v[i])):
                    v[i] = None
    return doc

# --- Password hashing setup ---
# Configure passlib context to use bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Local wrappers for hashing/verification (duplicate of utils but kept for local use)
def hash_password(password: str) -> str:
    # Return bcrypt hash of provided password
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    # Verify provided plain password against stored hash
    return pwd_context.verify(plain_password, hashed_password)

# Setup basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth")

# Re-declare security and pwd_context variables (appear duplicated in file)
oauth2_scheme = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT and refresh expiration config read from environment with defaults
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

# Cache duration in minutes and a small delay used by background loader
CACHE_DURATION_MINUTES = 1440  
BACKGROUND_LOAD_DELAY = 2  

# Startup event to trigger background loading of all people
@app.on_event("startup")
async def startup_event():
    # Kick off background task to load people into in-memory cache
    print(" Starting background load of ALL people...")
    asyncio.create_task(background_load_all_people())

# Background task that loads all people into memory in batches
async def background_load_all_people():
    """Background task to load ALL people from the database"""
    try:
        # Small delay to ensure app is fully started
        await asyncio.sleep(BACKGROUND_LOAD_DELAY)
       
        if people_cache["is_loading"]:
            # If load already in progress, exit early
            return
           
        people_cache["is_loading"] = True
        people_cache["last_error"] = None
        start_time = time.time()
       
        print("BACKGROUND: Starting to load ALL people...")
       
        all_people_data = []
        # Count total documents to compute progress
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
               
                # Query mongo with projection and pagination
                cursor = people_collection.find({}, projection).skip(skip).limit(batch_size)
                batch_data = await cursor.to_list(length=batch_size)
               
                if not batch_data:
                    # No more records - break loop
                    break
               
                # Transform batch data into lighter dicts for cache
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
                # Log batch-level errors and break out
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
        # On overall failure, clear loading flag and store last error
        people_cache["is_loading"] = False
        people_cache["last_error"] = str(e)
        print(f"BACKGROUND: Failed to load people: {str(e)}")

# NOTE: This annotated file is the first part. I will continue with the next portion on your confirmation.
