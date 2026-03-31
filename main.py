
import os
from datetime import datetime, timedelta, date, timezone
import time
from bson import ObjectId
import re
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from auth.models import EventCreate,DecisionType, UserProfile, ConsolidationCreate, UserProfileUpdate, CheckIn, UncaptureRequest, UserCreate,UserCreater,  UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel,TaskTypeUpdate, PersonCreate, EventTypeCreate, UserListResponse, UserList, MessageResponse, PermissionUpdate, RoleUpdate, AttendanceSubmission, TaskUpdate, EventUpdate ,TaskTypeIn ,TaskTypeOut , LeaderStatusResponse, UserProfile,  OrganizationCreate, OrganizationUpdate, OrganizationResponse, OrganizationList, PeopleResponse, PeopleList
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token , task_type_serializer, get_current_user 
import math
import secrets
from database import db, events_collection, people_collection, users_collection, tasks_collection ,tasktypes_collection,consolidations_collection, organizations_collection, org_config_collection
from auth.email_utils import send_reset_email
from typing import  List,  Optional,  Dict
from collections import Counter
import logging
import pytz
import base64
import uuid
from fastapi.security import HTTPBearer
oauth2_scheme = HTTPBearer()
from passlib.context import CryptContext
import json
from urllib.parse import unquote
import traceback
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from time import sleep
from supreme_admin import router as supreme_admin_router
app = FastAPI()
app.include_router(supreme_admin_router)
import pandas as pd
import io

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
ORG_ID_MAP = {
    "active-church": "active-teams",
    "active church": "active-teams",
}

def get_org_from_user(current_user: dict):
    if current_user.get("role") == "super_admin":
        return None, None, set(), True
    raw = (
        current_user.get("org_id") or
        current_user.get("Organization") or
        current_user.get("Organisation") or
        current_user.get("organization") or
        current_user.get("organisation") or ""
    ).strip()
    if not raw:
        return None, None, set(), False
    slug = raw.lower().replace(" ", "-")
    slug = ORG_ID_MAP.get(slug, slug)
    human = slug.replace("-", " ")
    aliases = {raw.lower(), slug, human}
    if "active" in slug:
        aliases.update({"active-teams", "active church", "active teams", "active-church"})
    return slug, raw, aliases, False

def build_org_query(current_user: dict) -> dict:
    _, _, aliases, is_super_admin = get_org_from_user(current_user)
    if is_super_admin or not aliases:
        return {}
    alias_list = list(aliases)
    fields = [
        "org_id", "Org_id", "orgId", "OrgId",
        "Organization", "Organisation", "organization", "organisation", "church_id",
    ]
    return {"$or": [{f: {"$in": alias_list}} for f in fields]}

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

def serialize_doc(doc: dict) -> dict:
    """Recursively convert ObjectId values to strings for JSON serialization."""
    if not isinstance(doc, dict):
        return doc
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = serialize_doc(v)
        elif isinstance(v, list):
            out[k] = [serialize_doc(i) if isinstance(i, dict) else str(i) if isinstance(i, ObjectId) else i for i in v]
        else:
            out[k] = v
    return out

DB_NAME = os.getenv("DB_NAME", "active-teams-db")
consolidations_collection = db.get_collection("consolidations")


def get_database_client():
    """Return a Mongo client instance compatible with existing `db` usage."""
    try:
        client = getattr(db, "client", None)
        if client:
            return client
    except Exception:
        pass
    from motor.motor_asyncio import AsyncIOMotorClient
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    return AsyncIOMotorClient(mongo_uri)


def convert_datetime_to_iso(doc: dict) -> dict:
    """Recursively convert datetime values in a document to ISO strings."""
    if not isinstance(doc, dict):
        return doc
    out = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = convert_datetime_to_iso(v)
        elif isinstance(v, list):
            new_list = []
            for item in v:
                if isinstance(item, dict):
                    new_list.append(convert_datetime_to_iso(item))
                elif isinstance(item, datetime):
                    new_list.append(item.isoformat())
                else:
                    new_list.append(item)
            out[k] = new_list
        else:
            out[k] = v
    return out


def get_exact_date_identifier(target_date: date) -> str:
    """Return canonical date identifier used for attendance keys (YYYY-MM-DD)."""
    try:
        sa = pytz.timezone("Africa/Johannesburg")
        if isinstance(target_date, datetime):
            dt = target_date.astimezone(sa)
            return dt.date().isoformat()
        if isinstance(target_date, date):
            return target_date.isoformat()
    except Exception:
        pass
    return str(target_date)


async def user_has_cell(user_email: str) -> bool:
    """Return True if the user (email) has at least one cell event."""
    if not user_email:
        return False
    try:
        sample = await events_collection.find_one({
            "$or": [
                {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                {"email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}}
            ],
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
            ]
        })
        return bool(sample)
    except Exception:
        return False


def build_event_object(event: dict, timezone, today_date: date) -> dict:
    """Minimal helper to build event object for user-cell endpoints."""
    try:
        raw_date = event.get("date") or event.get("Date Of Event")
        event_date = None
        if isinstance(raw_date, str):
            try:
                event_date = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date()
            except Exception:
                try:
                    event_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
                except Exception:
                    event_date = today_date
        elif isinstance(raw_date, datetime):
            event_date = raw_date.date()
        else:
            event_date = today_date

        status = "incomplete"
        attendance = event.get("attendance", {}) or {}
        exact_key = event_date.isoformat()
        date_entry = attendance.get(exact_key, {})
        if date_entry:
            st = (date_entry.get("status") or "").lower()
            if st:
                status = st
            elif date_entry.get("attendees"):
                status = "complete"

        if event.get("did_not_meet", False):
            status = "did_not_meet"
        elif (event.get("attendees") or []):
            status = "complete"

        return {
            "_id": str(event.get("_id")),
            "eventName": event.get("Event Name") or event.get("eventName", ""),
            "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
            "eventLeaderEmail": event.get("Email") or event.get("eventLeaderEmail", ""),
            "day": (event.get("Day") or event.get("day") or "").capitalize(),
            "date": event_date.isoformat(),
            "display_date": event_date.strftime("%d - %m - %Y"),
            "status": status,
            "Status": status.replace("_", " ").title(),
            "attendees": date_entry.get("attendees", []) if isinstance(date_entry, dict) else [],
            "persistent_attendees": event.get("persistent_attendees", []),
            "is_recurring": bool(event.get("recurring_day") or event.get("recurring_days"))
        }
    except Exception:
        return {
            "_id": str(event.get("_id")),
            "eventName": event.get("Event Name") or event.get("eventName", ""),
            "status": "incomplete",
            "date": today_date.isoformat(),
            "display_date": today_date.strftime("%d - %m - %Y"),
            "attendees": [],
            "persistent_attendees": event.get("persistent_attendees", []),
        }


# --- Password hashing setup ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth")

oauth2_scheme = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))

# Cache storage
people_cache = {
    "data": [],
    "last_updated": None,
    "expires_at": None,
    "is_loading": False,
    "background_task": None,
    "load_progress": 0,
    "total_loaded": 0,
    "last_error": None,
    "total_in_database": 0,
    "version": 1,
    "is_valid": True,
    "pending_refresh": False,
    "refresh_queue": []
}

CACHE_DURATION_MINUTES = 1440  
BACKGROUND_LOAD_DELAY = 2  
BATCH_SIZE = 5000 

# Lightweight cache for organizations (used by /organizations)
organizations_cache = {
    "data": [],
    "last_loaded": None,
    "expires_at": None,
}

ORGANIZATIONS_CACHE_TTL_MINUTES = 10

async def get_organizations_cached() -> list[dict]:
    """Return organizations from a short-lived in-memory cache for fast lookups."""
    now = datetime.utcnow()

    # Fast path: cache still valid
    expires_at = organizations_cache.get("expires_at")
    if organizations_cache["data"] and expires_at:
        try:
            if now < datetime.fromisoformat(expires_at):
                return organizations_cache["data"]
        except Exception:
            # If parsing fails, fall through and reload
            pass

    # Slow path: load from MongoDB
    cursor = organizations_collection.find(
        {},
        {"_id": 1, "name": 1, "tag": 1, "description": 1, "created_at": 1},
    ).sort("name", 1)
    orgs = await cursor.to_list(length=500)
    for org in orgs:
        org["_id"] = str(org["_id"])

    organizations_cache["data"] = orgs
    organizations_cache["last_loaded"] = now.isoformat()
    organizations_cache["expires_at"] = (now + timedelta(minutes=ORGANIZATIONS_CACHE_TTL_MINUTES)).isoformat()

    return orgs

PEOPLE_PROJECTION = {
    "_id": 1,
    "Name": 1, "Surname": 1, "Email": 1, "Number": 1,
    "Gender": 1, "Birthday": 1, "Address": 1, "InvitedBy": 1, "Stage": 1,
    "org_id": 1, "Org_id": 1, "orgId": 1, "OrgId": 1,
    "Organisation": 1, "Organization": 1, "organisation": 1, "organization": 1,
    "church_id": 1,
    "LeaderId": 1, "LeaderPath": 1,
    "DateCreated": 1, "Date Created": 1, "UpdatedAt": 1,
}

def get_leader_level(index_from_top: int) -> int:
    """
    Derive the leader level from its position in the reversed path.
    index_from_top=0 → root leader → level 1
    index_from_top=1 → level 12
    index_from_top=2 → level 144   (12^2)
    index_from_top=3 → level 1728  (12^3)
    index_from_top=4 → level 20736 (12^4)
    ... expands infinitely with no hardcoded ceiling
    """
    if index_from_top == 0:
        return 1
    return 12 ** index_from_top

def build_id_to_name_map(people_docs: list) -> dict:
    mapping = {}
    for p in people_docs:
        pid = str(p.get("_id", ""))
        if pid:
            mapping[pid] = f"{p.get('Name', '')} {p.get('Surname', '')}".strip()
    return mapping

def build_id_to_full_map(people_docs: list) -> dict:
    """Return {str(ObjectId): {id, name, email, phone}} for every person doc."""
    mapping = {}
    for p in people_docs:
        pid = str(p.get("_id", ""))
        if pid:
            mapping[pid] = {
                "id":    pid,
                "name":  f"{p.get('Name', '')} {p.get('Surname', '')}".strip(),
                "email": p.get("Email", "") or "",
                "phone": p.get("Number", "") or "",
            }
    return mapping

def resolve_leaders(leader_path: list, id_to_full: dict) -> list:
    """
    LeaderPath stored root-first: [vicky_id, bernice_id, keren_id]
    index 0 = root = level 1
    index 1 = level 12
    index 2 = level 144
    """
    if not leader_path:
        return []

    leaders = []
    for idx, lid in enumerate(leader_path):
        if not lid:
            continue
        info = id_to_full.get(str(lid)) or {"id": str(lid), "name": "", "email": "", "phone": ""}
        if not info.get("name"):
            continue  # skip empty entries
        leaders.append({
            "level": get_leader_level(idx),
            "id":    str(lid),
            "name":  info.get("name", ""),
            "email": info.get("email", ""),
            "phone": info.get("phone", ""),
        })
    return leaders

async def _build_full_id_map_from_db() -> dict:
    """Fetch Name+Surname+Email+Number for ALL people and return id→full map."""
    docs = await people_collection.find(
        {}, {"_id": 1, "Name": 1, "Surname": 1, "Email": 1, "Number": 1}
    ).to_list(length=None)
    return build_id_to_full_map(docs)

def transform_person_full(p, id_to_full: dict = None):
    """
    Like transform_person but also falls back to legacy Leader @N string fields
    if LeaderPath is empty. This ensures the cache always has leaders[] populated.
    """
    def oid(v):
        return str(v) if v else None

    raw_path  = p.get("LeaderPath", [])
    path_strs = [oid(x) for x in raw_path if x]
    leader_id = oid(p.get("LeaderId")) or (path_strs[0] if path_strs else None)

    # Try resolving from LeaderPath first
    leaders = resolve_leaders(path_strs, id_to_full or {})

    # Fallback: if leaders is empty, build from legacy Leader @N flat keys
    if not leaders:
        LEGACY_LEVELS = [
            ("Leader @1",    1),
            ("leader1",      1),
            ("Leader @12",   12),
            ("leader12",     12),
            ("Leader @144",  144),
            ("leader144",    144),
            ("Leader @1728", 1728),
            ("leader1728",   1728),
        ]
        seen_levels = set()
        for field, level in LEGACY_LEVELS:
            name = p.get(field, "").strip()
            if name and level not in seen_levels:
                leaders.append({
                    "level": level,
                    "id":    "",
                    "name":  name,
                    "email": "",
                    "phone": "",
                })
                seen_levels.add(level)
        # sort root first
        leaders.sort(key=lambda x: x["level"])

    return {
        "_id":          oid(p.get("_id")),
        "Name":         p.get("Name") or "",
        "Surname":      p.get("Surname") or "",
        "Email":        p.get("Email") or "",
        "Number":       p.get("Number") or "",
        "Gender":       p.get("Gender") or "",
        "Birthday":     p.get("Birthday") or "",
        "Address":      p.get("Address") or "",
        "InvitedBy":    p.get("InvitedBy") or "",
        "Stage":        p.get("Stage") or "Win",
        "DateCreated":  p.get("DateCreated") or p.get("Date Created") or "",
        "UpdatedAt":    p.get("UpdatedAt") or "",
        "org_id":       p.get("org_id") or p.get("Org_id") or p.get("orgId") or oid(p.get("church_id")) or "",
        "Organisation": p.get("Organisation") or p.get("Organization") or "",
        "Organization": p.get("Organization") or p.get("Organisation") or "",
        "leaders":      leaders,
        "LeaderId":     leader_id,
        "LeaderPath":   path_strs,
    }
    

async def invalidate_people_cache(operation_type: str, details: dict = None):
    """
    Invalidate the people cache and trigger background rehydration.
    Operation types: 'create', 'update', 'delete'
    """
    try:
        print(f"CACHE INVALIDATION: {operation_type.upper()} operation detected on people collection")
        
        people_cache["is_valid"] = False
        people_cache["pending_refresh"] = True
        
        if details:
            people_cache["refresh_queue"].append({
                "operation": operation_type,
                "details": details,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        stale_data = people_cache["data"].copy() if people_cache["data"] else []
        
        if not people_cache["is_loading"]:
            print(f"Triggering background cache refresh after {operation_type} operation...")
            people_cache["background_task"] = asyncio.create_task(
                background_refresh_people_cache(stale_data)
            )
        
        return {
            "cache_invalidated": True,
            "operation": operation_type,
            "current_data_size": len(stale_data),
            "refresh_triggered": not people_cache["is_loading"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        print(f"Cache invalidation error: {str(e)}")
        return {"error": str(e)}


async def background_refresh_people_cache(stale_data: list = None):
    try:
        people_cache["is_loading"]    = True
        people_cache["last_error"]    = None
        people_cache["load_progress"] = 0
        people_cache["total_loaded"]  = 0

        import time as _time
        start = _time.time()
        print("BACKGROUND REFRESH: starting...")

        FULL_PROJECTION = {
            "_id": 1,
            "Name": 1, "Surname": 1, "Email": 1, "Number": 1,
            "Gender": 1, "Birthday": 1, "Address": 1, "InvitedBy": 1, "Stage": 1,
            "org_id": 1, "Org_id": 1, "orgId": 1, "OrgId": 1,
            "Organisation": 1, "Organization": 1, "organisation": 1, "organization": 1,
            "church_id": 1,
            "LeaderId": 1, "LeaderPath": 1,
            "DateCreated": 1, "Date Created": 1, "UpdatedAt": 1,
            "Leader @1": 1, "Leader @12": 1, "Leader @144": 1, "Leader @1728": 1,
            "leader1": 1, "leader12": 1, "leader144": 1, "leader1728": 1,
        }

        # ── Fetch ALL docs in one go (no sleep, no batching) ──────────────
        # Motor streams the cursor efficiently — no need to paginate
        all_raw = await people_collection.find(
            {}, FULL_PROJECTION
        ).to_list(length=None)  # None = no limit, loads everything at once

        total_count = len(all_raw)
        people_cache["total_in_database"] = total_count
        print(f"BACKGROUND REFRESH: fetched {total_count} raw docs in {_time.time()-start:.2f}s")

        # ── Build id→full map in one pass (no second DB query) ───────────
        id_to_full = build_id_to_full_map(all_raw)
        print(f"BACKGROUND REFRESH: built id map with {len(id_to_full)} entries")

        # ── Transform everything in one pass, no sleep ────────────────────
        # Process in chunks so we can update progress without sleeping
        CHUNK = 2000
        all_people = []
        
        for i in range(0, total_count, CHUNK):
            chunk = all_raw[i : i + CHUNK]
            transformed = [transform_person_full(p, id_to_full=id_to_full) for p in chunk]
            all_people.extend(transformed)
            
            progress = min(100, round(len(all_people) / total_count * 100, 1))
            people_cache["load_progress"] = progress
            people_cache["total_loaded"]  = len(all_people)
            people_cache["data"]          = all_people.copy()  # partial results available immediately
            
            # Yield control to event loop without sleeping
            await asyncio.sleep(0)  # was 0.05 — just yields, doesn't actually wait

        people_cache["data"]            = all_people
        people_cache["last_updated"]    = datetime.utcnow().isoformat()
        people_cache["expires_at"]      = (datetime.utcnow() + timedelta(minutes=CACHE_DURATION_MINUTES)).isoformat()
        people_cache["is_loading"]      = False
        people_cache["load_progress"]   = 100
        people_cache["is_valid"]        = True
        people_cache["pending_refresh"] = False
        people_cache["version"]        += 1
        people_cache["refresh_queue"]   = []

        print(f"BACKGROUND REFRESH COMPLETE: {len(all_people)} people in {_time.time()-start:.2f}s")
        return {"success": True, "loaded_count": len(all_people), "cache_version": people_cache["version"]}

    except Exception as e:
        people_cache["is_loading"]      = False
        people_cache["last_error"]      = str(e)
        people_cache["pending_refresh"] = False
        print(f"BACKGROUND REFRESH FAILED: {e}")
        return {"error": str(e)}

async def background_load_all_people():
    """
    Startup wrapper — waits for app to fully start then delegates to
    background_refresh_people_cache (single source of truth).
    """
    await asyncio.sleep(BACKGROUND_LOAD_DELAY)
    await background_refresh_people_cache()

async def background_load_all_people():
    await background_refresh_people_cache()

@app.on_event("startup")
async def startup_event():
    """Kick off a single background load of all people on startup."""
    print("Starting background load of ALL people...")
    asyncio.create_task(background_load_all_people())


@app.get("/cache/people")
async def get_cached_people(current_user: dict = Depends(get_current_user)):
    """
    Returns org-filtered people from the in-memory cache.
    Cache entries are already in the dynamic `leaders` array shape
    (built by transform_person in background_refresh_people_cache).
    No Leader @N string fields are present.
    """
    try:
        current_time = datetime.now(timezone.utc)

        # ── resolve org from JWT ──────────────────────────────────────────────
        _, _, aliases, is_super_admin = get_org_from_user(current_user)
        org_label = current_user.get("Organization") or current_user.get("org_id") or "ALL"

        # ── in-memory org filter ──────────────────────────────────────────────
        def filter_by_org(data: list) -> list:
            if is_super_admin or not aliases:
                return data
            result = []
            for p in data:
                person_org = (
                    p.get("org_id") or p.get("Org_id") or p.get("orgId") or
                    p.get("Organization") or p.get("Organisation") or
                    p.get("organization") or p.get("organisation") or ""
                ).strip().lower()
                if person_org and person_org in aliases:
                    result.append(p)
            return result

        # ── parse expires_at safely (stored as ISO string or datetime) ────────
        def parse_expires(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
            try:
                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                return None

        expires_at = parse_expires(people_cache.get("expires_at"))

        # ── 1. fresh valid cache ──────────────────────────────────────────────
        if (
            people_cache["data"] and
            people_cache["is_valid"] and
            expires_at and
            current_time < expires_at
        ):
            filtered = filter_by_org(people_cache["data"])
            print(f"CACHE HIT: {len(filtered)} people (org={org_label})")
            return {
                "success":       True,
                "cached_data":   filtered,
                "cached_at":     people_cache["last_updated"],
                "expires_at":    people_cache["expires_at"],
                "source":        "cache",
                "total_count":   len(filtered),
                "is_complete":   True,
                "load_progress": 100,
                "cache_version": people_cache["version"],
                "is_valid":      True,
                "organization":  org_label,
            }

        # ── 2. cache still loading — return partial ───────────────────────────
        if people_cache["is_loading"]:
            filtered = filter_by_org(people_cache["data"] or [])
            return {
                "success":           True,
                "cached_data":       filtered,
                "cached_at":         people_cache["last_updated"],
                "source":            "loading",
                "total_count":       len(filtered),
                "is_complete":       False,
                "load_progress":     people_cache["load_progress"],
                "loaded_so_far":     people_cache["total_loaded"],
                "total_in_database": people_cache["total_in_database"],
                "message":           f"Loading... {people_cache['load_progress']}% complete",
                "cache_version":     people_cache["version"],
                "is_valid":          people_cache["is_valid"],
                "organization":      org_label,
            }

        # ── 3. stale cache — serve stale, trigger refresh ─────────────────────
        if people_cache["data"] and not people_cache["is_valid"]:
            print("Cache stale — returning stale data while refreshing...")
            if not people_cache["is_loading"] and people_cache["pending_refresh"]:
                asyncio.create_task(
                    background_refresh_people_cache(people_cache["data"].copy())
                )
            filtered = filter_by_org(people_cache["data"])
            return {
                "success":        True,
                "cached_data":    filtered,
                "cached_at":      people_cache["last_updated"],
                "expires_at":     people_cache["expires_at"],
                "source":         "stale_cache",
                "total_count":    len(filtered),
                "is_complete":    True,
                "message":        "Stale data (refresh in progress)",
                "cache_version":  people_cache["version"],
                "is_valid":       False,
                "refresh_queued": people_cache["pending_refresh"],
                "organization":   org_label,
            }

        # ── 4. cache empty — trigger load ─────────────────────────────────────
        if not people_cache["data"] and not people_cache["is_loading"]:
            print("Cache empty — triggering background load...")
            asyncio.create_task(background_refresh_people_cache())
            return {
                "success":       True,
                "cached_data":   [],
                "cached_at":     None,
                "source":        "triggered_load",
                "total_count":   0,
                "is_complete":   False,
                "message":       "Background loading started...",
                "load_progress": 0,
                "cache_version": people_cache["version"],
                "organization":  org_label,
            }

        # ── 5. fallback ───────────────────────────────────────────────────────
        filtered = filter_by_org(people_cache["data"] or [])
        return {
            "success":       True,
            "cached_data":   filtered,
            "cached_at":     people_cache["last_updated"],
            "source":        "fallback",
            "total_count":   len(filtered),
            "is_complete":   bool(filtered),
            "cache_version": people_cache["version"],
            "organization":  org_label,
        }

    except Exception as e:
        print(f"Error in /cache/people: {str(e)}")
        return {
            "success":     False,
            "error":       str(e),
            "cached_data": [],
            "total_count": 0,
        }

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
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
    """
    Simple paginated people endpoint as fallback.
    Uses updated schema fields — old Leader @N fields are no longer fetched.
    """
    try:
        skip = (page - 1) * per_page
        cursor = people_collection.find({}, PEOPLE_PROJECTION).skip(skip).limit(per_page)
        people_list = await cursor.to_list(length=per_page)
       
        formatted_people = [transform_person_full(p) for p in people_list]
       
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
    Manually refresh the people cache.
    """
    try:
        if not people_cache["is_loading"]:
            print("Manual cache refresh triggered")
            current_data = people_cache["data"].copy() if people_cache["data"] else None
            asyncio.create_task(background_refresh_people_cache(current_data))
            
            return {
                "success": True,
                "message": "Cache refresh triggered",
                "is_loading": True,
                "current_progress": people_cache["load_progress"],
                "current_cache_size": len(people_cache["data"]) if people_cache["data"] else 0
            }
        else:
            return {
                "success": True,
                "message": "Cache refresh already in progress",
                "is_loading": True,
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
    """Get detailed cache status and loading progress."""
    total_in_db = await people_collection.count_documents({})
    cache_size = len(people_cache["data"])

    return {
        "cache": {
            "size": cache_size,
            "last_updated": people_cache["last_updated"],
            "expires_at": people_cache["expires_at"],
            "is_loading": people_cache["is_loading"],
            "load_progress": people_cache["load_progress"],
            "total_loaded": people_cache["total_loaded"],
            "last_error": people_cache["last_error"],
            "is_valid": people_cache["is_valid"],
            "pending_refresh": people_cache["pending_refresh"],
            "version": people_cache["version"],
            "refresh_queue_size": len(people_cache["refresh_queue"])
        },
        "database": {
            "total_people": total_in_db,
            "coverage_percentage": round((cache_size / total_in_db) * 100, 1) if total_in_db > 0 else 0
        },
        "is_complete": cache_size >= total_in_db if total_in_db > 0 else True
    }
  
@app.post("/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, background_tasks: BackgroundTasks):
    logger.info(f"Forgot password requested for email: {payload.email}")
   
    # Find the user by email
    user = await users_collection.find_one({"email": payload.email})
   
    if not user:
        logger.info(f"Forgot password - email not found: {payload.email}")
        return {"message": "If your email exists, a reset link has been sent."}

    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1)
    )
   
    reset_link = f"https://teams.theactivechurch.org/reset-password?token={reset_token}"
    recipient_name = user.get("name", "there") 

    logger.info(f"Reset link generated for {payload.email}")

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

    # Derive org_id
    org_name = (
        user.get("org_id") or
        user.get("organization") or
        "active-teams"
    )
    org_id = org_name.lower().replace(" ", "-")

    token = create_access_token(
        {
            "user_id": str(user["_id"]),
            "email": user["email"],
            "role": user.get("role", "user"),
            "org_id": org_id,    
        },
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
            "org_id": org_id,    
        }},
    )
    logger.info(f"Refresh token rotated for user: {user['email']}")
    return {
        "access_token": token,
        "token_type": "bearer",
        "refresh_token_id": new_refresh_token_id,
        "refresh_token": new_refresh_plain,
    }


# In login endpoint
@app.post("/login")
async def login(user: UserLogin):
    logger.info(f"Login attempt: {user.email}")
    existing = await users_collection.find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        logger.warning(f"Login failed: {user.email}")
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Ensure user ID is properly formatted
    user_id = str(existing["_id"])
    
    # Get organization with proper case handling
    organization = existing.get("Organization") or existing.get("organization", "")
    
    # Get or create org_id
    org_id = existing.get("org_id")
    if not org_id and organization:
        org_id = organization.lower().replace(" ", "-")
    if not org_id:
        org_id = "active-teams"
    
    # Create token with user_id
    access_token = create_access_token({
        "user_id": user_id,  # Important: use "user_id" as key
        "sub": user_id,      # Also include sub for compatibility
        "email": existing["email"],
        "role": existing.get("role", "user"),
        "is_supreme_admin": existing.get("is_supreme_admin", False),
        "Organization": organization,
        "org_id": org_id
    })
    
    logger.info(f"Token created for user {user_id}")
    
    # Return response with user ID
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user_id,
            "_id": user_id,  # Include both for compatibility
            "email": existing["email"],
            "name": existing.get("name", ""),
            "surname": existing.get("surname", ""),
            "role": existing.get("role", "user"),
            "organization": organization,
            "org_id": org_id,
            "is_supreme_admin": existing.get("is_supreme_admin", False)
        }
    }

@app.post("/signup")
async def signup(user: UserCreate):
    logger.info(f"Signup attempt: {user.email}")
   
    # Normalize email
    email = user.email.lower().strip()
   
    # Check if user already exists in Users collection ONLY
    existing = await users_collection.find_one({"email": email})
    if existing:
        logger.warning(f"Signup failed - email already registered: {email}")
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash password
    hashed = hash_password(user.password)
   

    # ---- Resolve organization & org_tag dynamically from DB ----
    organization = (user.organization or "").strip()
    org_tag = ""
    if organization:
        org_doc = await organizations_collection.find_one(
            {"name": {"$regex": f"^{re.escape(organization)}$", "$options": "i"}}
        )
        if org_doc:
            org_tag = org_doc.get("tag", organization)
        else:
            org_tag = organization  # fallback: use name as tag if not found

    # Create base user document (leader fields will be added after hierarchy is calculated)
    user_dict = {
        "name": user.name,
        "surname": user.surname,
        "date_of_birth": user.date_of_birth,
        "home_address": user.home_address,
        "phone_number": user.phone_number,
        "email": email,
        "gender": user.gender,
        "password": hashed,
        "confirm_password": hashed,
        # Default role for all new signups so route guards recognize them
        "role": "user",
        "Organization": organization,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }

    inviter_full_name = user.invited_by.strip()
    inviter_person = None
    inviter_person_id: Optional[ObjectId] = None
    leader1 = ""
    leader12 = ""
    leader144 = ""
    leader1728 = ""

    async def _find_person_by_full_name(full_name: str):
        full_name = (full_name or "").strip()
        if not full_name:
            return None
        parts = [p for p in full_name.split(" ") if p]
        first = parts[0]
        last = " ".join(parts[1:]) if len(parts) > 1 else ""

        if last:
            return await people_collection.find_one(
                {
                    "Name": {"$regex": f"^{re.escape(first)}$", "$options": "i"},
                    "Surname": {"$regex": f"^{re.escape(last)}$", "$options": "i"},
                },
                {"_id": 1, "LeaderId": 1, "LeaderPath": 1, "Name": 1, "Surname": 1},
            )

        return await people_collection.find_one(
            {"Name": {"$regex": f"^{re.escape(first)}$", "$options": "i"}},
            {"_id": 1, "LeaderId": 1, "LeaderPath": 1, "Name": 1, "Surname": 1},
        )

    def _normalize_object_id_list(value):
        if not isinstance(value, list):
            return []
        out = []
        for v in value:
            if isinstance(v, ObjectId):
                out.append(v)
            elif isinstance(v, str):
                try:
                    out.append(ObjectId(v))
                except Exception:
                    continue
        return out

    def _build_leader_path_from_leader_doc(leader_doc):
        if not leader_doc or not leader_doc.get("_id"):
            return []
        leader_id = leader_doc["_id"]
        base_path = _normalize_object_id_list(leader_doc.get("LeaderPath"))
        if not base_path or base_path[-1] != leader_id:
            base_path.append(leader_id)
        return base_path
   
    if inviter_full_name:
        print(f"Looking for inviter in background cache: '{inviter_full_name}'")

        # Prefer the inviter's ObjectId if the frontend provided it.
        if getattr(user, "invited_by_id", None):
            try:
                inviter_person = await people_collection.find_one(
                    {"_id": ObjectId(user.invited_by_id)},
                    {"_id": 1, "LeaderId": 1, "LeaderPath": 1, "Name": 1, "Surname": 1, "Gender": 1,
                     "Leader @1": 1, "Leader @12": 1, "Leader @144": 1, "Leader @1728": 1},
                )
                if inviter_person:
                    inviter_person_id = inviter_person["_id"]
            except Exception:
                inviter_person = None
                inviter_person_id = None
       
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
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
                else:
                    print("5")
                    leader1 = inviter_leader1
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
               
                logger.info(f"Leader hierarchy set for {email}: L1={leader1}, L12={leader12}, L144={leader144}, L1728={leader1728}")
        else:
            # Fallback: set inviter as Leader @1
            leader1 = inviter_full_name

        # If we didn't resolve inviter by id, try resolve by name (best-effort).
        if inviter_person is None:
            inviter_person = await _find_person_by_full_name(inviter_full_name)
            if inviter_person:
                inviter_person_id = inviter_person["_id"]
   
    # ---- Dynamic Leadership Logic: ONLY for Active Church ----
    if organization and organization.lower() != "active church":
        logger.info(f"Organization '{organization}' is not 'Active Church'. Clearing leadership hierarchy for {email}.")
        leader1 = ""
        leader12 = ""
        leader144 = ""
        leader1728 = ""
    elif not organization:
        # Default to Active Church if no organization specified.
        pass

    # ---- ObjectId-based leader hierarchy (LeaderId + LeaderPath) ----
    leader_id_obj: Optional[ObjectId] = None
    leader_path: list[ObjectId] = []

    if organization and organization.lower() != "active church":
        leader_id_obj = None
        leader_path = []
    else:
        # Prefer the selected inviter as the direct leader when available.
        if inviter_person_id:
            leader_id_obj = inviter_person_id
            leader_path = _build_leader_path_from_leader_doc(inviter_person) if inviter_person else [inviter_person_id]
        else:
            # Fall back to resolving the computed Leader @1 full name to a People ObjectId.
            if leader1:
                leader1_doc = await _find_person_by_full_name(leader1)
                if leader1_doc and leader1_doc.get("_id"):
                    leader_id_obj = leader1_doc["_id"]
                    leader_path = _build_leader_path_from_leader_doc(leader1_doc)

    # Attach ObjectId-based leader fields onto the user record before inserting.
    user_dict["LeaderId"] = leader_id_obj
    user_dict["LeaderPath"] = leader_path

    # Insert user into Users collection (now includes LeaderId + LeaderPath)
    user_result = await db["Users"].insert_one(user_dict)
    logger.info(f"User created successfully: {email}")

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
        "LeaderId": leader_id_obj,
        "LeaderPath": leader_path,
        "Organization": organization,
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
            "Leader @12": "",
            "Leader @144": "",
            "Leader @1728": "",
            "FullName": f"{user.name.strip()} {user.surname.strip()}".strip()
        }
        people_cache["data"].append(new_person_cache_entry)
        print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")
       
    except Exception as e:
        logger.error(f"Failed to create person record for {email}: {e}")
   
    return {"message": "User created successfully", "Organization": organization,}


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


# ====================================================================
# ORGANIZATIONS ENDPOINTS  
# ====================================================================

@app.get("/organizations")
async def list_organizations():
    """Return all organizations stored in the database."""
    try:
        # Use in-memory cache for very fast responses
        orgs = await get_organizations_cached()
        return {"success": True, "organizations": orgs, "total": len(orgs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch organizations: {str(e)}")


@app.get("/organizations/{org_id}")
async def get_organization(org_id: str):
    """Return a single organization by ID."""
    if not ObjectId.is_valid(org_id):
        raise HTTPException(status_code=400, detail="Invalid organization ID")
    org = await organizations_collection.find_one({"_id": ObjectId(org_id)})
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    org["_id"] = str(org["_id"])
    return {"success": True, "organization": org}


@app.post("/organizations")
async def create_organization(data: dict = Body(...)):
    """
    Create a new organization.
    Body: { "name": "City Church", "tag": "City Church", "description": "..." }
    - 'name'  is the display name (must be unique, case-insensitive)
    - 'tag'   is the badge label shown on user profiles (defaults to name)
    """
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Organization name is required")

    
    existing = await organizations_collection.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
    )
    if existing:
        raise HTTPException(status_code=409, detail=f"Organization '{name}' already exists")

    tag = (data.get("tag") or name).strip()
    doc = {
        "name": name,
        "description": (data.get("description") or "").strip(),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    result = await organizations_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    logger.info(f"Organization created: {name} (tag={tag})")
    # Invalidate org cache so new org appears immediately
    organizations_cache["data"] = []
    organizations_cache["last_loaded"] = None
    organizations_cache["expires_at"] = None
    return {"success": True, "message": "Organization created", "organization": doc}


@app.put("/organizations/{org_id}")
async def update_organization(org_id: str, data: dict = Body(...)):
    """
    Update an existing organization's name.
    This will ALSO update all users with the old organization name to the new name.
    """
    if not ObjectId.is_valid(org_id):
        raise HTTPException(status_code=400, detail="Invalid organization ID")

    existing = await organizations_collection.find_one({"_id": ObjectId(org_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Organization not found")

    old_name = existing.get("name", "")
    update_fields = {}
    
    if "name" in data and data["name"]:
        update_fields["name"] = data["name"].strip()
    
    if "address" in data:
        update_fields["address"] = data["address"].strip()
    if "phone" in data:
        update_fields["phone"] = data["phone"].strip()
    if "email" in data:
        update_fields["email"] = data["email"].strip()

    if not update_fields:
        return {"success": False, "message": "No fields to update"}

    update_fields["updated_at"] = datetime.utcnow().isoformat()
    
    # Update the organization
    await organizations_collection.update_one(
        {"_id": ObjectId(org_id)}, 
        {"$set": update_fields}
    )
    if "name" in update_fields and update_fields["name"] != old_name:
        new_name = update_fields["name"]
        print(f" Organization name changed from '{old_name}' to '{new_name}'")
        print(f" Updating all users with organization '{old_name}' to '{new_name}'...")
        
        update_result = await users_collection.update_many(
            {
                "$or": [
                    # Exact match
                    {"organization": old_name},
                    {"Organization": old_name},
                    
                    {"organization": old_name.lower()},
                    {"Organization": old_name.lower()},
                    
                    {"organization": old_name.upper()},
                    {"Organization": old_name.upper()},
                    
                    # Partial matches with regex (case insensitive)
                    {"organization": {"$regex": f"^{re.escape(old_name)}$", "$options": "i"}},
                    {"Organization": {"$regex": f"^{re.escape(old_name)}$", "$options": "i"}}
                ]
            },
            {
                "$set": {
                    "organization": new_name,
                    "updated_at": datetime.utcnow().isoformat()
                }
            }
        )
        
        print(f" Updated {update_result.modified_count} users from '{old_name}' to '{new_name}'")
 
        additional_update = await users_collection.update_many(
            {
                "$or": [
                    {"org": old_name},
                    {"church": old_name},
                    {"org": {"$regex": f"^{re.escape(old_name)}$", "$options": "i"}},
                    {"church": {"$regex": f"^{re.escape(old_name)}$", "$options": "i"}}
                ]
            },
            {
                "$set": {
                    "organization": new_name,
                    "updated_at": datetime.utcnow().isoformat()
                }
            }
        )
        
        if additional_update.modified_count > 0:
            print(f" Updated {additional_update.modified_count} additional users from other fields")

    organizations_cache["data"] = []
    organizations_cache["last_loaded"] = None
    organizations_cache["expires_at"] = None

    # Get updated organization
    updated = await organizations_collection.find_one({"_id": ObjectId(org_id)})
    updated["_id"] = str(updated["_id"])

    return {
        "success": True, 
        "message": "Organization updated successfully", 
        "organization": updated
    }

@app.delete("/organizations/{org_id}")
async def delete_organization(org_id: str):
    """Delete an organization. Existing users keep their old org/tag strings (no auto-wipe)."""
    if not ObjectId.is_valid(org_id):
        raise HTTPException(status_code=400, detail="Invalid organization ID")
    result = await organizations_collection.delete_one({"_id": ObjectId(org_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Organization not found")
    logger.info(f"Organization deleted: {org_id}")

    # Invalidate org cache so deletions are reflected
    organizations_cache["data"] = []
    organizations_cache["last_loaded"] = None
    organizations_cache["expires_at"] = None

    return {"success": True, "message": "Organization deleted"}

SAST_TZ = pytz.timezone('Africa/Johannesburg')
   
def is_recurring_event(event: dict) -> bool:
    """Check if event has recurring days configured"""
    recurring_days = event.get("recurring_day") or event.get("recurring_days") or []
    
    # Handle different formats
    if isinstance(recurring_days, str):
        recurring_days = [recurring_days] if recurring_days else []
    
    return len(recurring_days) > 1  

def generate_current_week_instances(event: dict) -> list:

    """
    Generate instances ONLY for the current week, up to today
    - If today is Wednesday, only show Mon, Tue, Wed
    - Don't show Thu, Fri, Sat, Sun until those days arrive
    """
    instances = []
    
    # Get recurring days
    recurring_days = event.get("recurring_day") or event.get("recurring_days") or []
    if isinstance(recurring_days, str):
        recurring_days = [recurring_days] if recurring_days else []
    
    # Need at least 2 days to be recurring
    if len(recurring_days) <= 1:
        return instances
    
    # Day name to weekday number
    day_mapping = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    
    # Convert day names to numbers
    target_weekdays = [day_mapping[day.lower().strip()] for day in recurring_days if day.lower().strip() in day_mapping]
    
    if not target_weekdays:
        return instances
    
    # Get today's date
    timezone = pytz.timezone("Africa/Johannesburg")
    today = datetime.now(timezone).date()
    
    # Get start of this week (Monday)
    days_since_monday = today.weekday()  # 0=Mon, 1=Tue, ..., 6=Sun
    week_start = today - timedelta(days=days_since_monday)
    
    print(f" Generating instances for: {event.get('Event Name', event.get('eventName'))}")
    print(f"   Recurring days: {recurring_days}")
    print(f"   Week: {week_start} to {today}")
    
    # Get event time
    event_date_field = event.get("date") or event.get("Date Of Event")
    if isinstance(event_date_field, datetime):
        original_time = event_date_field.time()
    elif isinstance(event_date_field, str):
        try:
            dt = datetime.fromisoformat(event_date_field.replace("Z", "+00:00"))
            original_time = dt.time()
        except:
            original_time = datetime.strptime("09:00", "%H:%M").time()
    else:
        original_time = datetime.strptime("09:00", "%H:%M").time()
    
    # Generate instances from week_start to TODAY ONLY
    current_date = week_start
    while current_date <= today:
        # Is this day one of the recurring days?
        if current_date.weekday() in target_weekdays:
            # Get day name
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_name = days[current_date.weekday()]
            
            # Get attendance for this specific date
            attendance_data = event.get("attendance", {})
            instance_date_iso = current_date.isoformat()
            instance_attendance = attendance_data.get(instance_date_iso, {})
            
            # Status logic
            did_not_meet = instance_attendance.get("status") == "did_not_meet"
            weekly_attendees = instance_attendance.get("attendees", [])
            has_attendees = len(weekly_attendees) > 0
            
            if did_not_meet:
                event_status = "did_not_meet"
            elif has_attendees:
                event_status = "complete"
            else:
                event_status = "incomplete"
            
            exact_date_str = current_date.strftime("%Y-%m-%d") 
            
            # Create instance
            instance = {
                "_id": f"{event.get('_id')}_{instance_date_iso}",
                "UUID": event.get("UUID", ""),
                "eventName": event.get("Event Name") or event.get("eventName", ""),
                "eventType": event.get("Event Type") or event.get("eventType") or event.get("eventTypeName", ""),
                "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                "leader1": event.get("leader1", ""),
                "leader12": event.get("Leader @12") or event.get("Leader at 12") or event.get("leader12", ""),
                "day": day_name,
                "date": instance_date_iso,
                "display_date": current_date.strftime("%d - %m - %Y"),
                "location": event.get("Location") or event.get("location", ""),
                "attendees": weekly_attendees,
                "hasPersonSteps": event.get("hasPersonSteps", False),
                "status": event_status,
                "Status": event_status.replace("_", " ").title(),
                "did_not_meet": did_not_meet,
                "_is_overdue": current_date < today and event_status == "incomplete",
                "is_recurring": True,
                # "recurring_days": recurring_days, 
                "week_identifier": week_id,
                "original_event_id": str(event.get("_id"))
            }
            
            # Add persistent_attendees for cells
            if event.get("hasPersonSteps"):
                instance["persistent_attendees"] = event.get("persistent_attendees", [])
            
            instances.append(instance)
            print(f"    {current_date} ({day_name}) - Status: {event_status}")
        
        # Next day
        current_date += timedelta(days=1)
    
    print(f"   Total: {len(instances)} instances (only up to today)")
    return instances


def get_current_week_identifier():
    """Get current week identifier in format YYYY-WW using South Africa timezone"""
    try:
        sa_timezone = pytz.timezone("Africa/Johannesburg")
        now = datetime.now(sa_timezone)
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"
    except Exception as e:
        print(f"Error getting week identifier: {e}")
        now = datetime.utcnow()
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"

DAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

def get_monday(date_obj: datetime) -> datetime:
    # Monday = 0
    return date_obj - timedelta(days=date_obj.weekday())
   

# Events Section  ----------------------------------------------
SAST_TZ = pytz.timezone('Africa/Johannesburg')
# South African timezone
def normalize_time(time_value: str) -> str:
    """
    Normalize time to HH:MM.
    NO timezone conversion.
    """
    if not time_value or not isinstance(time_value, str):
        return time_value

    try:
        # Defensive: ISO string sent accidentally
        if "T" in time_value:
            time_value = time_value.split("T")[1][:5]

        parts = time_value.split(":")
        if len(parts) >= 2:
            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    except Exception:
        pass

    return time_value

def parse_date_to_sast(date_input):
    """
    Parse any date input and convert to SAST timezone-aware datetime.
    """
    try:
        if not date_input:
            return None

        # If already a datetime object
        if isinstance(date_input, datetime):
            dt = date_input

        # If input is a string
        elif isinstance(date_input, str):
            # Remove 'Z' if present and parse as ISO format
            date_str = date_input.replace('Z', '+00:00')
            dt = datetime.fromisoformat(date_str)

        else:
            return None

        # If naive datetime (no timezone), assume it's SAST
        if dt.tzinfo is None:
            dt = SAST_TZ.localize(dt)
        else:
            # Convert to SAST
            dt = dt.astimezone(SAST_TZ)

        return dt

    except Exception as e:
        print(f"Error parsing date: {e}")
        return None

def format_display_date(dt):
    """
    Format datetime to DD - MM - YYYY
    """
    if not dt:
        return ""

    if isinstance(dt, str):
        dt = parse_date_to_sast(dt)

    return dt.strftime("%d - %m - %Y") if dt else ""

@app.post("/events")
async def create_event(event: EventCreate, current_user: dict = Depends(get_current_user)):
    try:
        event_data = event.dict()
        event_data["_id"] = ObjectId()

        if not event_data.get("UUID"):
            event_data["UUID"] = str(uuid.uuid4())

        event_type_name = event_data.get("eventTypeName")
        if not event_type_name:
            raise HTTPException(status_code=400, detail="eventTypeName is required")

        org_id = current_user.get("org_id", "active-teams")
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")
        
        # Make sure organization is properly set (only uppercase)
        if not organization:
            organization = "Active Church"  # Default organization
        
        # Set ONLY the uppercase Organization field
        event_data["org_id"] = org_id
        event_data["Organization"] = organization  # Uppercase O only

        # Check if it's a CELLS type
        if event_type_name.upper() in ["CELLS", "ALL CELLS"]:
            event_data["eventTypeId"] = "CELLS_BUILT_IN"
            event_data["eventTypeName"] = "CELLS"
            event_data["hasPersonSteps"] = True
            event_data["isGlobal"] = False
            event_data["status"] = "incomplete"
        else:
            # Try to find the event type - first by name only (without org filter)
            event_type = await events_collection.find_one({
                "$or": [
                    {"name": {"$regex": f"^{event_type_name}$", "$options": "i"}},
                    {"eventType": {"$regex": f"^{event_type_name}$", "$options": "i"}},
                    {"eventTypeName": {"$regex": f"^{event_type_name}$", "$options": "i"}}
                ],
                "isEventType": True
            })
            
            # If found, check if it's global or belongs to the user's org
            if event_type:
                is_global = event_type.get("isGlobal", False)
                event_org_id = event_type.get("org_id", "")
                
                # If it's global OR belongs to user's org, use it
                if is_global or event_org_id == org_id:
                    print(f"Found event type: {event_type_name} (global={is_global})")
                    event_data["eventTypeId"] = event_type.get("UUID")
                    event_data["eventTypeName"] = event_type.get("name")
                    event_data["isGlobal"] = event_type.get("isGlobal", False)
                    event_data["hasPersonSteps"] = event_type.get("hasPersonSteps", False)
                    event_data["isTicketed"] = event_type.get("isTicketed", False)
                    event_data["status"] = "open"
                else:
                    # Event type exists but belongs to different org - create as custom
                    print(f"Event type '{event_type_name}' belongs to org {event_org_id}, user org is {org_id} - using as custom")
                    event_data["eventTypeId"] = None
                    event_data["eventTypeName"] = event_type_name
                    event_data["isGlobal"] = False
                    event_data["hasPersonSteps"] = False
                    event_data["isTicketed"] = False
                    event_data["status"] = "open"
            else:
                # Event type not found, use default
                print(f"Event type '{event_type_name}' not found, using default")
                event_data["eventTypeId"] = None
                event_data["eventTypeName"] = event_type_name
                event_data["isGlobal"] = False
                event_data["hasPersonSteps"] = False
                event_data["isTicketed"] = False
                event_data["status"] = "open"

        print(f"Using day value from frontend: {event_data.get('day')}")

        if event_data.get("time") or event_data.get("Time"):
            raw_time = event_data.get("time") or event_data.get("Time")
            print(f"Raw time received from frontend: {raw_time}")
            clean_time = normalize_time(raw_time)
            event_data["time"] = clean_time
            event_data["Time"] = clean_time
            print(f"Time stored as: {clean_time}")

        event_data.pop("eventType", None)

        if not event_data.get("eventLeaderEmail"):
            raise HTTPException(status_code=400, detail="eventLeaderEmail is required")

        for key in ["userEmail", "email"]:
            event_data.pop(key, None)

        recurring_days = event_data.get("recurring_day", [])
        if isinstance(recurring_days, str):
            recurring_days = [recurring_days]
        recurring_days = [d.strip() for d in recurring_days if d and d.strip()]
        event_data["recurring_day"] = recurring_days

        if not recurring_days:
            event_data["day"] = event_data.get("day", "One-time")
        else:
            event_data["day"] = recurring_days[0]

        event_data.setdefault("eventLeaderName", event_data.get("eventLeader", ""))
        if event_data.get("hasPersonSteps"):
            event_data.setdefault("leader1", "")
            event_data.setdefault("leader12", "")
            event_data.setdefault("persistent_attendees", [])

        if event_data.get("isTicketed") and event_data.get("priceTiers"):
            event_data["priceTiers"] = [
                {k: (float(v) if k == "price" else v) for k, v in tier.items()}
                for tier in event_data["priceTiers"]
            ]
        else:
            event_data["priceTiers"] = []

        if event_data.get("isGlobal"):
            for field in ["leader1", "leader12"]:
                if field in event_data and not event_data[field]:
                    del event_data[field]

        event_data["created_at"] = datetime.utcnow()
        event_data["updated_at"] = datetime.utcnow()
        event_data.setdefault("attendees", [])
        event_data["total_attendance"] = len(event_data["attendees"])

        reference_date = event_data.get("date")
        if isinstance(reference_date, str):
            try:
                reference_dt = datetime.strptime(reference_date, "%Y-%m-%d")
                reference_date = reference_dt.date()
            except Exception:
                try:
                    reference_date = datetime.fromisoformat(reference_date.replace("Z", "00:00")).date()
                except Exception:
                    reference_date = datetime.now().date()
        elif isinstance(reference_date, datetime):
            reference_date = reference_date.date()
        else:
            reference_date = datetime.now().date()

        if recurring_days:
            first_day_lower = recurring_days[0].lower().strip()
            if first_day_lower in DAY_INDEX:
                target_weekday = DAY_INDEX[first_day_lower]
                days_until = (target_weekday - reference_date.weekday()) % 7
                first_event_date = reference_date + timedelta(days=days_until)
            else:
                first_event_date = reference_date

            event_data["date"] = first_event_date.isoformat()
            event_data["day"] = recurring_days[0].capitalize()
            event_data["recurring_day"] = recurring_days
            event_data["attendance"] = {}

            try:
                event_data["Date Of Event"] = datetime.combine(first_event_date, datetime.min.time()).isoformat() + "Z"
            except Exception:
                event_data["Date Of Event"] = first_event_date.isoformat()

            print(f"[RECURRING CREATE] Single doc -> day: {event_data['day']}, date: {event_data['date']}, eventName: {event_data.get('eventName') or event_data.get('Event Name')}, Organization: {event_data['Organization']}")

            result = await events_collection.insert_one(event_data)
            print(f"[RECURRING CREATE] Inserted _id: {result.inserted_id}")

            return {
                "success": True,
                "message": "Recurring event created successfully",
                "created_event_ids": [str(result.inserted_id)],
                "id": str(result.inserted_id),
                "count": 1
            }

        result = await events_collection.insert_one(event_data)
        created_event = await events_collection.find_one({"_id": result.inserted_id})

        return {
            "success": True,
            "message": "Event created successfully",
            "id": str(result.inserted_id),
            "event": {**created_event, "_id": str(created_event["_id"])}
        }

    except Exception as e:
        print(f"Error creating event: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/event-types")
async def get_event_types(current_user: dict = Depends(get_current_user)):
    try:
        org_id = current_user.get("org_id") or (current_user.get("organization", "").lower().replace(" ", "-")) or "active-teams"
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

        print(f"GET EVENT TYPES — user: {current_user.get('email')} | org_id: {org_id}")

        event_types = []

        if org_id == "active-teams":
            event_types.append({
                "_id": "CELLS_BUILT_IN",
                "id": "CELLS_BUILT_IN",
                "name": "CELLS",
                "eventTypeName": "CELLS",
                "isBuiltIn": True,
                "isEventType": True,
                "isGlobal": False,
                "org_id": org_id
            })

        cursor = events_collection.find({
            "isEventType": True,
            "$or": [
                {"org_id": org_id},
                {"Organization": {"$regex": current_user.get("Organization", ""), "$options": "i"}}
            ]
        }).sort("createdAt", 1)

        async for et in cursor:
            et["_id"] = str(et["_id"])
            if et.get("eventTypeName", "").upper() == "CELLS" or et.get("name", "").upper() == "CELLS":
                continue
            event_types.append(et)

        print(f"Found {len(event_types)} event types for org: {org_id}")
        return event_types

    except Exception as e:
        print(f"Error fetching event types: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    
@app.post("/admin/backfill-event-leaders")
async def backfill_event_leaders(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    
    try:
        updated = 0
        skipped = 0
        not_found = 0

        cursor = events_collection.find(
            {"isEventType": {"$ne": True}},
            {"_id": 1, "Email": 1, "eventLeaderEmail": 1, "EventLeaderEmail": 1}
        )

        async for event in cursor:
            leader_email = (
                event.get("Email") or
                event.get("eventLeaderEmail") or
                event.get("EventLeaderEmail") or
                ""
            ).strip().lower()

            if not leader_email:
                skipped += 1
                continue

            person = await people_collection.find_one(
                {"Email": {"$regex": f"^{re.escape(leader_email)}$", "$options": "i"}},
                {"_id": 1, "LeaderId": 1, "LeaderPath": 1}
            )

            if not person:
                not_found += 1
                continue

            await events_collection.update_one(
                {"_id": event["_id"]},
                {"$set": {
                    "PersonId": person["_id"],
                    "LeaderId": person.get("LeaderId"),
                    "LeaderPath": person.get("LeaderPath", []),
                }}
            )
            updated += 1

        print(f"Backfill complete: updated={updated}, skipped={skipped}, not_found={not_found}")
        return {
            "success": True,
            "updated": updated,
            "skipped": skipped,
            "not_found": not_found
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def convert_event_for_display(event):
    """
    Convert event from database format to display format
    Times are already in SAST in DB, so no conversion needed
    """
    if not event:
        return event
    
    # Ensure display_date is present
    if event.get('date') and not event.get('display_date'):
        sast_dt = parse_date_to_sast(event['date'])
        if sast_dt:
            event['display_date'] = format_display_date(sast_dt)

    if event.get('Time') and not event.get('time'):
        event['time'] = event['Time']
    elif event.get('time') and not event.get('Time'):
        event['Time'] = event['time']
    return event

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
    isLeaderAt12: Optional[bool] = Query(None),
    firstName: Optional[str] = Query(None),
    userSurname: Optional[str] = Query(None),
    must_paginate: Optional[bool] = Query(True)
):
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        org_config = await org_config_collection.find_one({"_id": org_id})
        recurring_type = org_config.get("recurring_event_type", "Cells") if org_config else "Cells"

        user_email = current_user.get("email", "")
        role = current_user.get("role", "").lower().strip()
        is_actual_leader_at_12 = (
            role == "leaderat12" or
            "leaderat12" in role or
            "leader at 12" in role or
            "leader@12" in role
        )

        if recurring_type.lower() != "cells":
            return {
                "events": [],
                "total_events": 0,
                "total_pages": 1,
                "current_page": 1,
                "page_size": 25,
            }

        user_name_from_frontend = f"{firstName or ''} {userSurname or ''}".strip()

        person = await people_collection.find_one(
            {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
            {"_id": 1, "Name": 1, "Surname": 1}
        )

        user_person_id = None

        if person:
            user_person_id = person.get("_id")
            db_first = person.get("Name", "").strip()
            db_surname = person.get("Surname", "").strip()
            user_name_from_db = f"{db_first} {db_surname}".strip()
        else:
            user_name_from_db = ""

        user_name_from_token = current_user.get("name", "")

        if user_name_from_frontend:
            user_name = user_name_from_frontend
        elif user_name_from_db:
            user_name = user_name_from_db
        else:
            user_name = user_name_from_token

        print(f"User name resolved as: {user_name}")

        query = {
            "$and": [
                {
                    "$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}},
                        {"EventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeId": "CELLS_BUILT_IN"},
                        {"hasPersonSteps": True},
                    ]
                },
                {"isEventType": {"$ne": True}},
                {
                    "$or": [
                        {"org_id": org_id},
                        {"Organization": {"$regex": re.escape(organization), "$options": "i"}}
                    ]
                },
                {
                    "$or": [
                        {"is_active": True},
                        {"is_active": {"$exists": False}}
                    ]
                },
            ]
        }

        if search and search.strip():
            search_term = search.strip()
            query["$and"].append({
                "$or": [
                    {"Event Name": {"$regex": search_term, "$options": "i"}},
                    {"eventName": {"$regex": search_term, "$options": "i"}},
                    {"Leader": {"$regex": search_term, "$options": "i"}},
                    {"Email": {"$regex": search_term, "$options": "i"}},
                    {"Leader at 12": {"$regex": search_term, "$options": "i"}},
                    {"Leader @12": {"$regex": search_term, "$options": "i"}},
                ]
            })

        def create_name_conditions(target_name, fields):
            conditions = []
            if not target_name:
                return conditions
            clean_name = target_name.strip()
            for field in fields:
                conditions.append({field: {"$regex": f"^{re.escape(clean_name)}$", "$options": "i"}})
                conditions.append({field: {"$regex": re.escape(clean_name), "$options": "i"}})
                title_name = clean_name.title()
                conditions.append({field: {"$regex": f"^{re.escape(title_name)}$", "$options": "i"}})
                name_parts = clean_name.split()
                if len(name_parts) > 0:
                    first_name = name_parts[0].strip()
                    conditions.append({field: {"$regex": re.escape(first_name), "$options": "i"}})
            return conditions

        if role == "admin":
            if personal or show_personal_cells:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

        elif is_actual_leader_at_12 and leader_at_12_view:
            want_personal_view = (show_personal_cells or personal)
            want_disciples_view = (show_all_authorized or include_subordinate_cells)

            if want_personal_view and not want_disciples_view:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

            elif want_disciples_view and not want_personal_view:
                conditions = []
                if user_person_id:
                    conditions.append({"LeaderPath": user_person_id})
                leader_at_12_fields = [
                    "Leader at 12", "Leader @12", "leader12",
                    "Leader12", "LeaderAt12", "leader at 12", "leader @12"
                ]
                for field in leader_at_12_fields:
                    conditions.append({field: {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}})
                    conditions.append({field: {"$regex": re.escape(user_name), "$options": "i"}})
                print(f"Disciples query conditions count: {len(conditions)}")
                if conditions:
                    query["$and"].append({"$or": conditions})
                else:
                    query["$and"].append({"_id": "nonexistent_id"})

            else:
                name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName"]
                name_conditions = create_name_conditions(user_name, name_fields)
                email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
                email_conditions = create_name_conditions(user_email, email_fields)
                query["$and"].append({"$or": name_conditions + email_conditions})

        elif role == "leader144":
            name_fields = ["Leader", "eventLeader", "eventLeaderName", "EventLeaderName",
                           "leader144", "Leader at 144", "Leader @144"]
            name_conditions = create_name_conditions(user_name, name_fields)
            email_fields = ["eventLeaderEmail", "EventLeaderEmail", "Email"]
            email_conditions = create_name_conditions(user_email, email_fields)
            leader_path_condition = []
            if user_person_id:
                leader_path_condition = [{"leaderLeaderPath": user_person_id}]
            query["$and"].append({"$or": name_conditions + email_conditions + leader_path_condition})

        elif role in ["user", "registrant", "leader"]:
            conditions = []
            if user_name:
                clean_name = user_name.strip()
                for field in ["Leader", "eventLeaderName", "EventLeaderName"]:
                    conditions.append({field: {"$regex": f"^{re.escape(clean_name)}$", "$options": "i"}})
            if user_email:
                clean_email = user_email.strip().lower()
                for field in ["eventLeaderEmail", "EventLeaderEmail", "Email"]:
                    conditions.append({field: {"$regex": f"^{re.escape(clean_email)}$", "$options": "i"}})
            if user_person_id:
                conditions.append({"leaderLeaderPath": user_person_id})
            if conditions:
                query["$and"].append({"$or": conditions})
            else:
                query["$and"].append({"_id": "nonexistent_id"})

        print(f"Final query for cells: {query}")

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

        sa_timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(sa_timezone).date()

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

                target_weekday = day_mapping.get(day_name)
                if target_weekday is None:
                    continue

                max_weeks = 1 if status == "incomplete" else 4

                days_since_monday = today.weekday()
                week_start = today - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)

                for week_back in range(0, max_weeks):
                    instance_date = current_week_instance - timedelta(weeks=week_back)

                    if instance_date > today:
                        continue
                    if instance_date < start_date_obj:
                        continue

                    exact_date = instance_date.isoformat()
                    attendance_data = event.get("attendance", {})
                    attendance = attendance_data.get(exact_date, {})

                    if not attendance:
                        for key, value in attendance_data.items():
                            if isinstance(value, dict):
                                if value.get("event_date_exact") == exact_date:
                                    attendance = value
                                    break
                                event_date_iso = value.get("event_date_iso")
                                if event_date_iso and exact_date in event_date_iso:
                                    attendance = value
                                    break
                        if not attendance:
                            legacy_week_key = instance_date.strftime("%G-W%V")
                            legacy_attendance = attendance_data.get(legacy_week_key, {})
                            if legacy_attendance:
                                attendance = legacy_attendance
                                try:
                                    await events_collection.update_one(
                                        {"_id": event["_id"]},
                                        {"$set": {f"attendance.{exact_date}": legacy_attendance}}
                                    )
                                except Exception as migrate_error:
                                    print(f"Legacy attendance migration skipped: {migrate_error}")

                    if not attendance:
                        event_status = "incomplete"
                        attendees = []
                        did_not_meet = False
                    else:
                        att_status = attendance.get("status", "").lower()
                        attendees = attendance.get("attendees", [])
                        if att_status == "did_not_meet":
                            event_status = "did_not_meet"
                            did_not_meet = True
                        elif att_status == "complete" or len(attendees) > 0:
                            event_status = "complete"
                            did_not_meet = False
                        else:
                            event_status = "incomplete"
                            did_not_meet = False

                    if status and status != 'all' and event_status != status:
                        continue

                    is_overdue = instance_date < today and event_status == "incomplete"

                    leaderAt1 = event.get("leader1") or event.get("Leader @1") or event.get("Leader at 1", "")

                    if not leaderAt1:
                        leaderPipeline = [
                            {"$project": {"Gender": 1, "fullName": {"$concat": ["$Name", " ", "$Surname"]}}},
                            {"$match": {"fullName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", "")}},
                            {"$limit": 1}
                        ]
                        peopleFullnames = await people_collection.aggregate(leaderPipeline).to_list(length=None)
                        if peopleFullnames and len(peopleFullnames) > 0:
                            eventLeader = peopleFullnames[0]
                            if eventLeader:
                                gender = eventLeader.get("Gender", "")
                                leaderAt1 = await get_top_leader_dynamic(gender, org_id)

                    leaderAt12 = (
                        event.get("Leader at 12") or
                        event.get("Leader @12") or
                        event.get("leader12") or
                        event.get("Leader12") or
                        event.get("LeaderAt12") or
                        event.get("leader at 12") or
                        event.get("leader @12") or
                        ""
                    )

                    instance = {
                        "_id": f"{event.get('_id')}_{exact_date}",
                        "UUID": event.get("UUID", ""),
                        "eventName": event.get("Event Name") or event.get("eventName") or event.get("EventName", ""),
                        "eventType": "Cells",
                        "eventLeaderName": event.get("Leader") or event.get("eventLeaderName") or event.get("EventLeaderName", ""),
                        "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("EventLeaderEmail") or event.get("Email", ""),
                        "leader1": leaderAt1,
                        "leader12": leaderAt12,
                        "day": day_name.capitalize(),
                        "date": exact_date,
                        "display_date": instance_date.strftime("%d - %m - %Y"),
                        "location": event.get("Location") or event.get("location", ""),
                        "attendees": attendees,
                        "persistent_attendees": event.get("persistent_attendees", []),
                        "hasPersonSteps": True,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "did_not_meet": did_not_meet,
                        "_is_overdue": is_overdue,
                        "is_recurring": True,
                        "original_event_id": str(event.get("_id")),
                        "attendance": attendance,
                        "is_active": event.get("is_active", ""),
                    }
                    if event.get("time"):
                        instance["time"] = event.get("time")
                    if event.get("Time"):
                        instance["Time"] = event.get("Time")

                    cell_instances.append(instance)

            except Exception as e:
                print(f"Error processing event {event.get('_id')}: {e}")
                continue

        if must_paginate:
            total_count = len(cell_instances)
            total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
            skip = (page - 1) * limit
            paginated = cell_instances[skip:skip + limit]
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
                    "is_leader_at_12": is_actual_leader_at_12,
                    "view_mode": "personal" if (personal or show_personal_cells) else "all"
                }
            }
        else:
            print("SENDING ALL EVENTS")
            return {"events": cell_instances}

    except Exception as e:
        print(f"Error in /events/cells: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/eventsdata")
async def get_other_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=500),
    status: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(None),
    start_date: Optional[str] = Query("2025-10-10"),
    end_date: Optional[str] = Query(None),
    show_all_dates: Optional[bool] = Query(False)
):
    try:
        print(f"GET /eventsdata - User: {current_user.get('email')}, Event Type: {event_type}")
        print(f"Query params - status: {status}, personal: {personal}, search: {search}")

        user_role = current_user.get("role", "user").lower()
        user_email = current_user.get("email", "").lower().strip()
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()

        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

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
            "$and": [
                {
                    "$or": [
                        {"org_id": org_id},
                        {"Organization": {"$regex": re.escape(organization), "$options": "i"}}
                    ]
                },
                {
                    "$nor": [
                        {"Event Type": {"$regex": "Cells", "$options": "i"}},
                        {"eventType": {"$regex": "Cells", "$options": "i"}},
                        {"eventTypeName": {"$regex": "Cells", "$options": "i"}}
                    ]
                }
            ]
        }

        if user_role not in ["admin", "leaderat12", "registrant"]:
            visibility_filter = {
                "$or": [
                    {"isGlobal": True},
                    {"isGlobal": "true"},
                    {"eventLeaderEmail": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"userEmail": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"leader1": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}},
                    {"eventLeaderName": {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}},
                    {"Leader": {"$regex": f"^{re.escape(user_name)}$", "$options": "i"}},
                ]
            }
            query["$and"].append(visibility_filter)

        if personal:
            print(f"Applying PERSONAL filter for user: {user_email}")
            query["$and"].append({
                "$or": [
                    {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                    {"leader1": {"$regex": user_email, "$options": "i"}}
                ]
            })
        elif user_role == "user":
            print(f"Regular user - showing personal events: {user_email}")
            query["$and"].append({
                "$or": [
                    {"eventLeaderEmail": {"$regex": user_email, "$options": "i"}},
                    {"leader1": {"$regex": user_email, "$options": "i"}}
                ]
            })

        if event_type and event_type.lower() != 'all':
            print(f"Filtering by event type: '{event_type}'")
            if event_type.lower() not in ["all", "cells"]:
                query["$and"].append({
                    "$or": [
                        {"Event Type": {"$regex": f"^{event_type}$", "$options": "i"}},
                        {"eventType": {"$regex": f"^{event_type}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{event_type}$", "$options": "i"}}
                    ]
                })

        if search and search.strip():
            search_term = search.strip()
            print(f"Applying search filter: '{search_term}'")
            safe_search_term = re.escape(search_term)
            query["$and"].append({
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
            })

        print(f"Final query: {query}")

        cursor = events_collection.find(query)
        events = await cursor.to_list(length=3000)
        print(f"Found {len(events)} other events")

        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }

        other_events = []

        for event in events:
            try:
                event_name = event.get("Event Name") or event.get("eventName", "")
                event_type_value = event.get("Event Type") or event.get("eventType", "Event")
                recurring_days = event.get("recurring_day", [])
                if not isinstance(recurring_days, list):
                    recurring_days = []
                is_recurring = len(recurring_days) > 0

                # Helper function to enrich attendees with financial data
                def enrich_attendees_with_financials(attendees_list):
                    enriched = []
                    for att in attendees_list:
                        if not isinstance(att, dict):
                            continue
                        # Calculate financials if missing
                        price = att.get("price", 0)
                        paid = att.get("paid", att.get("paidAmount", 0))
                        
                        if paid >= price:
                            owing = 0
                            change = paid - price
                        elif paid > 0 and paid < price:
                            owing = price - paid
                            change = 0
                        else:
                            owing = price
                            change = 0
                        
                        enriched_att = {
                            "id": att.get("id", ""),
                            "name": att.get("name", ""),
                            "fullName": att.get("fullName", att.get("name", "")),
                            "email": att.get("email", ""),
                            "phone": att.get("phone", ""),
                            "leader12": att.get("leader12", ""),
                            "leader144": att.get("leader144", ""),
                            "checked_in": att.get("checked_in", False),
                            "decision": att.get("decision", ""),
                            "priceName": att.get("priceName", ""),
                            "price": price,
                            "ageGroup": att.get("ageGroup", ""),
                            "paymentMethod": att.get("paymentMethod", ""),
                            "paid": paid,
                            "owing": owing,
                            "change": change,
                        }
                        enriched.append(enriched_att)
                    return enriched

                if is_recurring:
                    days_since_monday = today.weekday()
                    week_start = today - timedelta(days=days_since_monday)

                    for day_name_raw in recurring_days:
                        day_key = str(day_name_raw).strip().lower()
                        target_weekday = day_mapping.get(day_key)
                        if target_weekday is None:
                            continue

                        for week_back in range(0, 1):
                            instance_date = (week_start + timedelta(days=target_weekday)) - timedelta(weeks=week_back)
                            if instance_date > today:
                                continue
                            if instance_date < start_date_obj or instance_date > end_date_obj:
                                continue

                            exact_date_str = instance_date.isoformat()
                            days_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            actual_day_value = days_list[instance_date.weekday()]

                            attendance_data = event.get("attendance", {})
                            if not isinstance(attendance_data, dict):
                                attendance_data = {}
                            date_attendance = attendance_data.get(exact_date_str, {})
                            if not isinstance(date_attendance, dict):
                                date_attendance = {}

                            original_date_str = None
                            event_date_field = event.get("date") or event.get("Date Of Event") or event.get("eventDate")
                            if isinstance(event_date_field, datetime):
                                original_date_str = event_date_field.date().isoformat()
                            elif isinstance(event_date_field, str):
                                try:
                                    if 'T' in event_date_field:
                                        original_date_str = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date().isoformat()
                                    else:
                                        original_date_str = event_date_field[:10]
                                except:
                                    pass

                            root_attendees = event.get("attendees", [])
                            if not isinstance(root_attendees, list):
                                root_attendees = []

                            if not date_attendance and exact_date_str == original_date_str and root_attendees:
                                date_attendance = {
                                    "attendees": root_attendees,
                                    "status": str(event.get("status", "")).lower(),
                                    "new_people": event.get("new_people", []),
                                    "consolidations": event.get("consolidations", []),
                                }

                            weekly_attendees = date_attendance.get("attendees", [])
                            if not isinstance(weekly_attendees, list):
                                weekly_attendees = []
                            
                            # Enrich attendees with financial data
                            weekly_attendees = enrich_attendees_with_financials(weekly_attendees)
                            has_weekly_attendees = len(weekly_attendees) > 0

                            new_people = date_attendance.get("new_people", [])
                            if not isinstance(new_people, list):
                                new_people = []
                            consolidations = date_attendance.get("consolidations", [])
                            if not isinstance(consolidations, list):
                                consolidations = []

                            att_status = str(date_attendance.get("status", "")).lower()
                            is_did_not_meet = date_attendance.get("is_did_not_meet", False)

                            if is_did_not_meet or att_status == "did_not_meet":
                                event_status = "did_not_meet"
                            elif att_status in ["open", "incomplete", "reopened", "active"]:
                                event_status = "incomplete"
                            elif has_weekly_attendees or att_status in ["complete", "closed"]:
                                event_status = "complete"
                            else:
                                event_status = "incomplete"

                            if status and status != event_status:
                                continue

                            total_attendance = len(weekly_attendees)

                            instance = {
                                "_id": f"{str(event.get('_id'))}_{exact_date_str}",
                                "UUID": event.get("UUID", ""),
                                "eventName": event_name,
                                "eventType": event_type_value,
                                "eventLeaderName": event.get("Leader") or event.get("eventLeaderName", ""),
                                "eventLeaderEmail": event.get("eventLeaderEmail") or event.get("Email", ""),
                                "leader1": event.get("leader1", ""),
                                "leader12": event.get("Leader @12") or event.get("Leader at 12", ""),
                                "day": actual_day_value,
                                "date": exact_date_str,
                                "location": event.get("Location") or event.get("location", ""),
                                "hasPersonSteps": False,
                                "status": event_status,
                                "Status": event_status.replace("_", " ").title(),
                                "_is_overdue": instance_date < today and event_status == "incomplete",
                                "is_recurring": True,
                                "recurring_days": recurring_days,
                                "original_event_id": str(event.get("_id")),
                                "isGlobal": event.get("isGlobal", False),
                                "isTicketed": event.get("isTicketed", False),
                                "priceTiers": event.get("priceTiers", []),
                                "closed_by": date_attendance.get("closed_by") or event.get("closed_by", ""),
                                "closed_at": str(date_attendance.get("closed_at") or event.get("closed_at", "")),
                                "created_at": str(event.get("created_at", "")),
                                "updated_at": str(event.get("updated_at", "") or event.get("updatedAt", "")),
                                "attendees": weekly_attendees,
                                "persistent_attendees": enrich_attendees_with_financials(event.get("persistent_attendees", [])),
                                "new_people": new_people,
                                "consolidations": consolidations,
                                "total_attendance": total_attendance,
                                "new_people_count": len(new_people),
                                "consolidation_count": len(consolidations),
                            }
                            other_events.append(instance)

                else:
                    day_name_raw = event.get("Day") or event.get("day") or event.get("eventDay") or ""
                    day_name = str(day_name_raw).strip()

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

                    if not day_name:
                        try:
                            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            day_name = days[event_date.weekday()]
                        except Exception as e:
                            print(f"Error calculating day from date: {e}")
                            day_name = "One-time"

                    actual_day_value = day_name.capitalize() if day_name else "One-time"

                    if event_date < start_date_obj or event_date > end_date_obj:
                        continue
                    if event_date > today:
                        continue

                    weekly_attendees = event.get("attendees", [])
                    if not isinstance(weekly_attendees, list):
                        weekly_attendees = []

                    if not weekly_attendees:
                        attendance_data = event.get("attendance", {})
                        if isinstance(attendance_data, dict):
                            event_date_iso = event_date.isoformat()
                            event_attendance = attendance_data.get(event_date_iso, {})
                            weekly_attendees = event_attendance.get("attendees", [])
                            if not isinstance(weekly_attendees, list):
                                weekly_attendees = []

                    # Helper function to enrich attendees with financial data
                    def enrich_attendees_with_financials(attendees_list):
                        enriched = []
                        for att in attendees_list:
                            if not isinstance(att, dict):
                                continue
                            price = att.get("price", 0)
                            paid = att.get("paid", att.get("paidAmount", 0))
                            
                            if paid >= price:
                                owing = 0
                                change = paid - price
                            elif paid > 0 and paid < price:
                                owing = price - paid
                                change = 0
                            else:
                                owing = price
                                change = 0
                            
                            enriched_att = {
                                "id": att.get("id", ""),
                                "name": att.get("name", ""),
                                "fullName": att.get("fullName", att.get("name", "")),
                                "email": att.get("email", ""),
                                "phone": att.get("phone", ""),
                                "leader12": att.get("leader12", ""),
                                "leader144": att.get("leader144", ""),
                                "checked_in": att.get("checked_in", False),
                                "decision": att.get("decision", ""),
                                "priceName": att.get("priceName", ""),
                                "price": price,
                                "ageGroup": att.get("ageGroup", ""),
                                "paymentMethod": att.get("paymentMethod", ""),
                                "paid": paid,
                                "owing": owing,
                                "change": change,
                            }
                            enriched.append(enriched_att)
                        return enriched

                    weekly_attendees = enrich_attendees_with_financials(weekly_attendees)
                    has_weekly_attendees = len(weekly_attendees) > 0

                    new_people = event.get("new_people", [])
                    if not isinstance(new_people, list):
                        new_people = []

                    consolidations = event.get("consolidations", [])
                    if not isinstance(consolidations, list):
                        consolidations = []

                    main_event_status = event.get("status", "").lower()
                    main_event_did_not_meet = event.get("did_not_meet", False)
                    main_event_complete = event.get("Status", "").lower() == "complete"

                    if main_event_did_not_meet or main_event_status == "did_not_meet":
                        event_status = "did_not_meet"
                    elif main_event_status in ["open", "incomplete", "reopened", "active"]:
                        event_status = "incomplete"
                    elif has_weekly_attendees or main_event_complete or main_event_status in ["complete", "closed"]:
                        event_status = "complete"
                    else:
                        event_status = "incomplete"

                    print(f"Event '{event_name}' - attendees: {len(weekly_attendees)}, status: {event_status}")

                    if status and status != event_status:
                        continue

                    total_attendance = event.get("total_attendance")
                    if not isinstance(total_attendance, int) or total_attendance == 0:
                        total_attendance = len(weekly_attendees)

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
                        "hasPersonSteps": False,
                        "status": event_status,
                        "Status": event_status.replace("_", " ").title(),
                        "_is_overdue": event_date < today and event_status == "incomplete",
                        "is_recurring": False,
                        "recurring_days": [],
                        "original_event_id": str(event.get("_id")),
                        "isGlobal": event.get("isGlobal", False),
                        "isTicketed": event.get("isTicketed", False),
                        "priceTiers": event.get("priceTiers", []),
                        "closed_by": event.get("closed_by", ""),
                        "closed_at": str(event.get("closed_at", "")),
                        "created_at": str(event.get("created_at", "")),
                        "updated_at": str(event.get("updated_at", "") or event.get("updatedAt", "")),
                        "attendees": weekly_attendees,
                        "persistent_attendees": enrich_attendees_with_financials(event.get("persistent_attendees", [])),
                        "new_people": new_people,
                        "consolidations": consolidations,
                        "total_attendance": total_attendance,
                        "new_people_count": len(new_people),
                        "consolidation_count": len(consolidations),
                    }
                    other_events.append(instance)

            except Exception as e:
                print(f"Error processing other event: {str(e)}")
                import traceback
                traceback.print_exc()
                continue

        other_events.sort(key=lambda x: x['date'], reverse=True)

        total_count = len(other_events)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        skip = (page - 1) * limit
        paginated_events = other_events[skip:skip + limit]

        print(f"Returning {len(paginated_events)} other events (page {page}/{total_pages})")

        return {
            "events": paginated_events,
            "total_events": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }

    except Exception as e:
        print(f"ERROR in /eventsdata: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
  
@app.get("/events/{event_id}/attendance/{week}")
async def get_weekly_attendance(
    event_id: str = Path(...),
    week: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
       
        exact_date_str = week 
        attendance_data = event.get("attendance", {}).get(exact_date_str)

        if not attendance_data:
            try:
                parsed_date = datetime.strptime(exact_date_str, "%Y-%m-%d").date()
                legacy_week_key = parsed_date.strftime("%G-W%V") 
                legacy_attendance = event.get("attendance", {}).get(legacy_week_key)
                if legacy_attendance:
                    attendance_data = legacy_attendance
                
                    await events_collection.update_one(
                        {"_id": ObjectId(event_id)},
                        {"$set": {f"attendance.{exact_date_str}": legacy_attendance}}
                    )
            except Exception as migrate_error:
                print(f"Legacy attendance migration skipped: {migrate_error}")
        
        if not attendance_data:
            return {
                "week": exact_date_str, 
                "exists": False,
                "message": "No attendance data for this week"
            }
        
        return {
            "week": exact_date_str,
            "exists": True,
            "data": attendance_data,
            "persistent_attendees": event.get("persistent_attendees", []),
            "event_statistics": {
                "total_associated_count": event.get("total_associated_count", 0),
                "last_attendance_count": event.get("last_attendance_count", 0),
                "last_decisions_count": event.get("last_decisions_count", 0)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/events/cells/{identifier}")
async def update_cell_event_working(identifier: str, event_data: dict):
    """
    SINGLE EVENT UPDATE: Update ONLY the existing event, NEVER create new ones
    """
    try:
        from datetime import datetime as dt
        
        # Find the SINGLE event by ID
        event = None
        if ObjectId.is_valid(identifier):
            event = await events_collection.find_one({"_id": ObjectId(identifier)})
        
        if not event:
            raise HTTPException(
                status_code=404,
                detail=f"Event not found with identifier: {identifier}"
            )
        
        # Prepare update fields
        update_fields = {}
        
        # Event Name mapping
        if 'eventName' in event_data or 'Event Name' in event_data:
            event_name_value = event_data.get('eventName') or event_data.get('Event Name')
            update_fields['eventName'] = event_name_value
            update_fields['Event Name'] = event_name_value
        
        # Day mapping
        if 'Day' in event_data or 'day' in event_data:
            day_value = event_data.get('Day') or event_data.get('day')
            update_fields['Day'] = day_value
            update_fields['day'] = day_value
        
        # Address/location mapping
        if 'Address' in event_data or 'location' in event_data:
            location_value = event_data.get('Address') or event_data.get('location')
            update_fields['Address'] = location_value
            update_fields['location'] = location_value
        
        # Time mapping
        if 'Time' in event_data or 'time' in event_data:
            time_value = event_data.get('Time') or event_data.get('time')
            update_fields['Time'] = time_value
            update_fields['time'] = time_value
        
        # Date mapping - Handle both formats AND display_date
        if 'date' in event_data or 'Date Of Event' in event_data:
            date_value = event_data.get('date')
            date_of_event_value = event_data.get('Date Of Event')
            
            if date_of_event_value:
                update_fields['Date Of Event'] = date_of_event_value
                if date_value:
                    update_fields['date'] = date_value
                else:
                    try:
                        dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                        update_fields['date'] = dt_obj.strftime('%Y-%m-%dT%H:%M')
                    except:
                        update_fields['date'] = date_of_event_value
                
                # Update display_date for table
                try:
                    dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    pass
            
            elif date_value:
                update_fields['date'] = date_value
                try:
                    dt_obj = dt.fromisoformat(date_value)
                    update_fields['Date Of Event'] = dt_obj.isoformat() + 'Z'
                    # Update display_date for table
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    update_fields['Date Of Event'] = date_value
        
        # Email mapping
        if 'Email' in event_data or 'eventLeaderEmail' in event_data:
            email_value = event_data.get('Email') or event_data.get('eventLeaderEmail')
            update_fields['Email'] = email_value
            update_fields['eventLeaderEmail'] = email_value
        
        # Leader mapping
        if 'Leader' in event_data or 'eventLeader' in event_data or 'eventLeaderName' in event_data:
            leader_value = event_data.get('Leader') or event_data.get('eventLeader') or event_data.get('eventLeaderName')
            update_fields['Leader'] = leader_value
            update_fields['eventLeader'] = leader_value
            update_fields['eventLeaderName'] = leader_value
        
        # Status mapping
        if 'status' in event_data or 'Status' in event_data:
            status_value = event_data.get('status') or event_data.get('Status')
            update_fields['status'] = status_value
        
        protected_fields = [
            'eventName', 'Event Name', 'Day', 'day', 'Address', 'location', 
            'Time', 'time', 'date', 'Date Of Event', 'Email', 
            'eventLeaderEmail', 'Leader', 'eventLeader', 'eventLeaderName',
            'status', 'Status',
            'persistent_attendees', 
            'attendees',             
            'attendance',           
            '_id', 'id', 'UUID',     
            'created_at',            
            'total_attendance'   
        ]
        
        # Other fields - but skip protected ones
        for key, value in event_data.items():
            if key not in protected_fields:
                update_fields[key] = value
         
        if update_fields.get("deactivation_end"):
            print("yay events!")
            update_fields["deactivation_end"] = datetime.strptime(update_fields["deactivation_end"], "%Y-%m-%dT%H:%M:%S.%f")
        
        update_fields["updated_at"] = datetime.utcnow()
        
        print(f"Updating event {identifier} with fields: {update_fields}")
        print(f"Protected fields excluded: persistent_attendees, attendees, attendance")
        
        # PERFORM THE UPDATE
        result = await events_collection.update_one(
            {"_id": event["_id"]},
            {"$set": update_fields}
        )
        
        return {
            "success": True,
            "message": "Event updated successfully",
            "modified": result.modified_count > 0,
            "event_id": str(event.get("_id"))
        }
        
    except Exception as e:
        print(f"Error updating event: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/events/person/{person_name}/event/{event_name}/day/{day_name}")
async def update_events_by_person_event_and_day(person_name: str, event_name: str, day_name: str, update_data: dict):
    """
    Update ONLY events for a specific person with a SPECIFIC event name AND SPECIFIC day
    """
    try:
        from datetime import datetime as dt
        
        decoded_person = unquote(person_name)
        decoded_event = unquote(event_name)
        decoded_day = unquote(day_name)
        
        print(f"=== UPDATE PERSON+EVENT+DAY (PRECISE) ===")
        print(f"Person: {decoded_person}")
        print(f"Event name: {decoded_event}")
        print(f"Day: {decoded_day}")
        print(f"Update data: {update_data}")
        
        # STRICT query
        strict_query = {
            "$and": [
                {
                    "$or": [
                        {"Leader": decoded_person},
                        {"eventLeader": decoded_person},
                        {"eventLeaderName": decoded_person}
                    ]
                },
                {
                    "$or": [
                        {"Event Name": decoded_event},
                        {"eventName": decoded_event}
                    ]
                },
                {
                    "$or": [
                        {"Day": decoded_day},
                        {"day": decoded_day}
                    ]
                }
            ]
        }
        
        cursor = events_collection.find(strict_query)
        matching_events = await cursor.to_list(length=None)
        
        if not matching_events:
            return {
                "success": False,
                "message": f"No {decoded_day} events found for {decoded_person} with name: {decoded_event}",
                "matched_count": 0,
                "modified_count": 0
            }
        
        print(f"Found {len(matching_events)} matching events")
        
        # Prepare update with proper field mapping
        update_fields = {}
        
        # Event Name mapping
        if 'eventName' in update_data or 'Event Name' in update_data:
            event_name_value = update_data.get('eventName') or update_data.get('Event Name')
            update_fields['eventName'] = event_name_value
            update_fields['Event Name'] = event_name_value
        
        # Day mapping
        if 'Day' in update_data or 'day' in update_data:
            day_value = update_data.get('Day') or update_data.get('day')
            update_fields['Day'] = day_value
            update_fields['day'] = day_value
        
        # Date mapping - Handle both formats AND display_date
        if 'date' in update_data or 'Date Of Event' in update_data:
            date_value = update_data.get('date')
            date_of_event_value = update_data.get('Date Of Event')
            
            if date_of_event_value:
                update_fields['Date Of Event'] = date_of_event_value
                if date_value:
                    update_fields['date'] = date_value
                else:
                    try:
                        dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                        update_fields['date'] = dt_obj.strftime('%Y-%m-%dT%H:%M')
                    except:
                        update_fields['date'] = date_of_event_value
                
                # Update display_date for table
                try:
                    dt_obj = dt.fromisoformat(date_of_event_value.replace('Z', '+00:00'))
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y')
                except:
                    pass
            
            elif date_value:
                update_fields['date'] = date_value
                try:
                    # Handle YYYY-MM-DD format (what frontend sends)
                    if len(date_value) == 10 and '-' in date_value:
                        dt_obj = dt.strptime(date_value, '%Y-%m-%d')
                    else:
                        dt_obj = dt.fromisoformat(date_value)
                    
                    update_fields['Date Of Event'] = dt_obj.isoformat() + 'Z'
                    update_fields['display_date'] = dt_obj.strftime('%d - %m - %Y') 
                except:
                    update_fields['Date Of Event'] = date_value

        
        # Time mapping
        if 'Time' in update_data or 'time' in update_data:
            time_value = update_data.get('Time') or update_data.get('time')
            
            if time_value:
                print(f"DEBUG - Time received from frontend: {time_value}")
                
                # Store exactly as received
                update_fields['Time'] = time_value
                update_fields['time'] = time_value  
                      
        # Address/Location mapping
        if 'Address' in update_data or 'location' in update_data:
            location_value = update_data.get('Address') or update_data.get('location')
            update_fields['Address'] = location_value
            update_fields['location'] = location_value
        
        # Email mapping
        if 'Email' in update_data or 'eventLeaderEmail' in update_data:
            email_value = update_data.get('Email') or update_data.get('eventLeaderEmail')
            update_fields['Email'] = email_value
            update_fields['eventLeaderEmail'] = email_value
        
        # Status mapping
        if 'status' in update_data or 'Status' in update_data:
            status_value = update_data.get('status') or update_data.get('Status')
            update_fields['status'] = status_value
            update_fields['Status'] = status_value
        
        protected_fields = [
            'eventName', 'Event Name', 'Day', 'day', 'date', 'Date Of Event', 
            'Time', 'time', 'Address', 'location', 'Email', 'eventLeaderEmail', 
            'status', 'Status',
            'persistent_attendees', 
            'attendees',            
            'attendance',           
            '_id', 'id', 'UUID',     
            'created_at',            
            'total_attendance'      
        ]
        
        for key, value in update_data.items():
            if key not in protected_fields and key not in update_fields:
                update_fields[key] = value
        
        update_fields["updated_at"] = datetime.utcnow()
        
        for key, value in update_fields.items():
            if 'time' in key.lower() or 'Time' in key:
                print(f"  {key}: {value} (type: {type(value)})")
                
        if update_fields.get("deactivation_end",""):
            print("yay!")
            update_fields["deactivation_end"] = datetime.strptime( update_fields["deactivation_end"], "%Y-%m-%dT%H:%M:%S.%f")
        print(f"Updating with: {update_fields}")
        print(f"Protected fields excluded: persistent_attendees, attendees, attendance")
        
        # Update all matching events
        result = await events_collection.update_many(
            strict_query,
            {"$set": update_fields}
        )
        
        print(f"Updated: matched {result.matched_count}, modified {result.modified_count}")
        
        # Fetch and return one updated event to verify
        updated_event = await events_collection.find_one(strict_query)

        return {
            "success": True,
            "message": f"Updated {result.modified_count} {decoded_day} events named '{decoded_event}'",
            "matched_count": len(matching_events),
            "modified_count": result.modified_count,
            "person": decoded_person,
            "original_event_name": decoded_event,
            "original_day": decoded_day,
            "new_event_name": update_fields.get('Event Name'),
            "new_day": update_fields.get('Day'),
            "sample_time_stored": updated_event.get('time') if updated_event else None
        }
        
    except Exception as e:
        print(f"Error updating events: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

#----------------Deactivate cells Endpoints------------
@app.put("/events/deactivate")
async def deactivate_event(
    cell_identifier: str = Query(..., description="Cell name or Person name"),
    weeks: int = Query(..., description="Number of weeks to deactivate (1-12)"),
    reason: Optional[str] = Query(None, description="Reason for deactivation"),
    person_name: Optional[str] = Query(None, description="Person name (if cell_identifier is a cell name)"),
    day_of_week: Optional[str] = Query(None, description="Specific day to deactivate (e.g., 'Wednesday')"),
    is_permanent_deact: bool = Query(None,description="Determines whether it is a permanent or a temporary deactivation"),
):
    try:
        current_time = datetime.utcnow()
        deactivation_end = current_time + timedelta(weeks=weeks)
        print("BOOL",is_permanent_deact)
        updates = {
            "is_active": False,
            "deactivation_start": current_time,
            "deactivation_end": datetime.strptime(str(deactivation_end),"%Y-%m-%d %H:%M:%S.%f"),
            "deactivation_reason": reason,
            "last_status_change": current_time,
            "is_permanent_deact":is_permanent_deact
        }
         
        query = {"$or": []}
        print(cell_identifier, person_name)
        
        if person_name:
            query["$or"].append({
                "$and": [
                    {"$or": [
                        {"eventName": cell_identifier},
                        {"Event Name": cell_identifier}
                    ]},
                    {"$or": [
                        {"eventLeader": person_name},
                        {"Leader": person_name},
                        {"eventLeaderName": person_name}
                    ]}
                ]
            })
        else:
            query["$or"].append({
                "$and": [
                     {"$or": [
                        {"eventName": cell_identifier},
                        {"Event Name": cell_identifier}
                    ]},
                    {"$or": [
                        {"eventLeader": cell_identifier},
                        {"Leader": cell_identifier},
                        {"eventLeaderName": cell_identifier}
                    ]}
                ]
            })
        print("QUERY", query)
        # Add day filter if specified
        if day_of_week:
            if "$or" in query and len(query["$or"]) > 0:
                for i in range(len(query["$or"])):
                    if "$and" in query["$or"][i]:
                        query["$or"][i]["$and"].append(
                            {"$or": [
                                {"Day": day_of_week},
                                {"recurring_day": day_of_week}
                            ]}
                        )
        
        print(f"DEBUG: Query length: {len(str(query))}")  
        
        result = await events_collection.update_many(query, {"$set": updates})
        
        if result.modified_count == 0:
            simple_query = {
                "$or": [
                    {"eventLeader": cell_identifier},
                    {"Leader": cell_identifier},
                    {"eventLeaderName": cell_identifier}
                ]
            }
            
            if day_of_week:
                simple_query["$or"].append({"Day": day_of_week})
                simple_query["$or"].append({"recurring_day": day_of_week})
            
            result = await events_collection.update_many(simple_query, {"$set": updates})
            
            if result.modified_count == 0:
                raise HTTPException(status_code=404, detail="No cells found")
        
        return {
            "success": True,
            "message": f"{result.modified_count} cell(s) deactivated for {weeks} week(s)",
            "weeks": weeks,
            "deactivation_end": deactivation_end.isoformat(),
            "cell_count": result.modified_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/cells/reactivate")
async def reactivate_cell(
    cell_identifier: str = Query(..., description="Cell name or Person name"),
    person_name: Optional[str] = Query(None, description="Person name (if cell_identifier is a cell name)"),
    day_of_week: Optional[str] = Query(None, description="Specific day to reactivate")
):
    try:
        current_time = datetime.utcnow()
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }
        
        query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": "cells"},
                        {"Event Type": "cells"}
                    ]
                },
                {"is_active": False}
            ]
        }
        
        if person_name:
            query["$and"].append({
                "$or": [
                    {"eventName": cell_identifier},
                    {"Event Name": cell_identifier}
                ]
            })
            query["$and"].append({
                "$or": [
                    {"eventLeader": person_name},
                    {"Leader": person_name},
                    {"eventLeaderName": person_name}
                ]
            })
        else:
            query["$and"].append({
                "$or": [
                    {"eventLeader": cell_identifier},
                    {"Leader": cell_identifier},
                    {"eventLeaderName": cell_identifier}
                ]
            })
        
        if day_of_week:
            query["$and"].append({
                "$or": [
                    {"Day": day_of_week},
                    {"recurring_day": day_of_week}
                ]
            })
        
        result = await events_collection.update_many(query, {"$set": updates})
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="No deactivated cells found")
        
        return {
            "success": True,
            "message": f"{result.modified_count} cell(s) reactivated",
            "cell_count": result.modified_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def auto_reactivate_expired_events():
    try:
        current_time = datetime.utcnow()
        
        
        query = {
            "$and": [
                {"is_active": False},
                {"deactivation_end": {"$lte": current_time, "$ne": None}},
                {"$or":[{"is_permanent_deact":{"$ne":True}}]}
            ]
        }
        
        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "last_status_change": current_time
        }
        
        result = await events_collection.update_many(query, {"$set": updates})
        print(result)
        if result.modified_count > 0:
            print(f"Auto-reactivated {result.modified_count} cells")
            
    except Exception as e:
        print(f"Auto-reactivation error: {e}")


scheduler = AsyncIOScheduler()    
scheduler.add_job(auto_reactivate_expired_events,'cron',hour=0,minute=0) 
scheduler.start()
sleep(10)
  
#------------------ MIGRATION ENDPOINTS ---------- 
@app.post("/migrate-event-types-uuids")
async def migrate_event_types_uuids():
    """ ONE-TIME: Add UUIDs to event types that don't have them"""
    try:
        import uuid
       
        # Find all event types without UUIDs
        cursor = events_collection.find({
            "isEventType": True,
            "UUID": {"$exists": False}  
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

# -----------------EVENTS TYPES SECTION--------------
@app.post("/event-types")
async def create_event_type(event_type: EventTypeCreate, current_user: dict = Depends(get_current_user)):
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        # Convert to title case (first letter of each word uppercase)
        name = event_type.name.strip().title()
        name_lower = name.lower()  # Keep lowercase version for regex checks

        # Check for reserved keywords (case insensitive)
        if re.search(r'\bcell[s]?\b', name_lower) or 'cell' in name_lower:
            raise HTTPException(
                status_code=400,
                detail="Event types containing 'cell' or 'cells' are reserved and cannot be created."
            )

        org_id = current_user.get("org_id") or (current_user.get("organization", "").lower().replace(" ", "-")) or "active-teams"
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        # Check for existing event type (case insensitive)
        existing = await events_collection.find_one({
            "$or": [
                {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
            ],
            "isEventType": True,
            "org_id": org_id
        })

        if existing:
            raise HTTPException(status_code=400, detail=f"Event type '{name}' already exists")

        event_type_data = {
            "name": name,  # Now stored in title case
            "eventType": name,  # Now stored in title case
            "eventTypeName": name,  # Now stored in title case
            "description": event_type.description.strip(),
            "isEventType": True,
            "isTicketed": event_type.isTicketed if hasattr(event_type, 'isTicketed') else False,
            "isGlobal": event_type.isGlobal if hasattr(event_type, 'isGlobal') else False,
            "hasPersonSteps": event_type.hasPersonSteps if hasattr(event_type, 'hasPersonSteps') else False,
            "org_id": org_id,
            "Organization": organization,
            "UUID": str(uuid.uuid4()),
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }

        if event_type_data.get("isGlobal") is None:
            event_type_data["isGlobal"] = "global" in name_lower

        if event_type_data.get("hasPersonSteps") is None:
            event_type_data["hasPersonSteps"] = any(
                keyword in name_lower for keyword in ["person", "individual"]
            )

        result = await events_collection.insert_one(event_type_data)
        inserted = await events_collection.find_one({"_id": result.inserted_id})
        inserted["_id"] = str(inserted["_id"])

        print(f"Created event type: {name} for org: {org_id}")

        return inserted

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating event type: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating event type: {str(e)}")

@app.get("/org-config")
async def get_org_config(current_user: dict = Depends(get_current_user)):
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        print(f"ORG CONFIG REQUEST - email: {current_user.get('email')} | org_id in token: {current_user.get('org_id')} | derived org_id: {org_id}")

        config = await org_config_collection.find_one({"_id": org_id})
        print(f"Config found: {config is not None}")  #

        if not config:
            pass

        config["org_id"] = str(config["_id"])
        config.pop("_id", None)
        return config

    except Exception as e:
        print(f"ORG CONFIG ERROR: {str(e)}") 
        import traceback
        traceback.print_exc()  
        raise HTTPException(status_code=500, detail=str(e)) 
     
@app.put("/event-types/{event_type_name}")
async def update_event_type(
    event_type_name: str,
    updated_data: EventTypeCreate = Body(...)
):
    try:
        decoded_event_type_name = unquote(event_type_name)
        
        # Check if event type exists
        existing_event_type = await events_collection.find_one({
            "name": {"$regex": f"^{decoded_event_type_name}$", "$options": "i"},
            "isEventType": True
        })
        
        if not existing_event_type:
            try:
                existing_event_type = await events_collection.find_one({
                    "_id": ObjectId(decoded_event_type_name),
                    "isEventType": True
                })
            except:
                pass
            
            if not existing_event_type:
                raise HTTPException(status_code=404, detail=f"Event type '{decoded_event_type_name}' not found")

        # Convert name to title case (first letter of each word uppercase)
        new_name = updated_data.name.strip().title()
        current_name = existing_event_type["name"]
        name_changed = new_name.lower() != current_name.lower()
        
        # Check if isGlobal is being changed
        current_is_global = existing_event_type.get("isGlobal", False)
        new_is_global = updated_data.isGlobal if updated_data.isGlobal is not None else False
        is_global_changed = current_is_global != new_is_global
        
        # Check for duplicate names (case insensitive)
        if name_changed:
            duplicate = await events_collection.find_one({
                "name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"},
                "isEventType": True,
                "_id": {"$ne": existing_event_type["_id"]}
            })
            if duplicate:
                raise HTTPException(status_code=400, detail="Event type with this name already exists")
        
        events_updated_count = 0
        if name_changed or is_global_changed:
            # Build base query
            update_query = {
                "$or": [
                    {"eventType": current_name},
                    {"eventTypeName": current_name}
                ],
                "isEventType": {"$ne": True}
            }
            
            # Build update fields
            update_fields = {
                "updatedAt": datetime.utcnow()
            }
            
            if name_changed:
                update_fields["eventType"] = new_name
                update_fields["eventTypeName"] = new_name
            
            if is_global_changed:
                # Find events that don't have explicit isGlobal set
                events_without_explicit_isglobal = await events_collection.find({
                    **update_query,
                    "$or": [
                        {"isGlobal": {"$exists": False}},
                        {"isGlobal": None},
                        {"isGlobal": ""},
                        {"isGlobal": current_is_global}
                    ]
                }).to_list(length=None)
                
                events_updated_count = len(events_without_explicit_isglobal)
                
                if events_updated_count > 0:
                    update_fields["isGlobal"] = new_is_global
            
            # Apply the update
            if name_changed or (is_global_changed and events_updated_count > 0):
                await events_collection.update_many(
                    update_query,
                    {"$set": update_fields}
                )

        # Prepare update data
        update_data_dict = updated_data.dict()
        update_data_dict["name"] = new_name
        update_data_dict["eventType"] = new_name 
        update_data_dict["eventTypeName"] = new_name  
        update_data_dict["updatedAt"] = datetime.utcnow()
        
        update_data_dict = {k: v for k, v in update_data_dict.items() if v is not None}
        
        immutable_fields = ["_id", "UUID", "createdAt", "isEventType"]
        for field in immutable_fields:
            update_data_dict.pop(field, None)

        # Update the event type document
        result = await events_collection.update_one(
            {"_id": existing_event_type["_id"]},
            {"$set": update_data_dict}
        )

        if result.modified_count == 0:
            existing_event_type["_id"] = str(existing_event_type["_id"])
            return existing_event_type

        updated_event_type = await events_collection.find_one({"_id": existing_event_type["_id"]})
        updated_event_type["_id"] = str(updated_event_type["_id"])
        
        return updated_event_type

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating event type: {str(e)}")
from urllib.parse import unquote

@app.delete("/event-types/{event_type_name}")
async def delete_event_type(
    event_type_name: str,
    force: bool = Query(False, description="Force delete even if events exist")
):
    try:
        decoded_event_type_name = unquote(event_type_name)
       
        print(f" DELETE EVENT TYPE: {decoded_event_type_name}, force={force}")
       
        existing_event_type = await events_collection.find_one({
            "$or": [
                {"name": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                {"eventType": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                {"eventTypeName": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}}
            ],
            "isEventType": True
        })
       
        if not existing_event_type:
            print(f" Event type '{decoded_event_type_name}' not found")
            raise HTTPException(
                status_code=404,
                detail=f"Event type '{decoded_event_type_name}' not found"
            )
       
        actual_identifier = (
            existing_event_type.get("name") or
            existing_event_type.get("eventType") or
            existing_event_type.get("eventTypeName")
        )
        
        # PREVENT DELETION OF "CELLS" EVENT TYPE (BUILT-IN)
        actual_identifier_lower = actual_identifier.lower()
        if any(keyword in actual_identifier_lower for keyword in ["cell", "cells"]):
            raise HTTPException(
                status_code=400,
                detail=f"'{actual_identifier}' is a reserved built-in event type and cannot be modified or deleted."
            )
       
        print(f" Found event type: {actual_identifier}")
       
        events_query = {
            "$and": [
                {
                    "$or": [
                        {"eventType": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(actual_identifier)}$", "$options": "i"}},
                        {"eventType": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                        {"eventTypeName": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}},
                        {"Event Type": {"$regex": f"^{re.escape(decoded_event_type_name)}$", "$options": "i"}}
                    ]
                },
                {"isEventType": {"$ne": True}},
                {"$or": [
                    {"eventName": {"$exists": True}},
                    {"Event Name": {"$exists": True}},
                    {"date": {"$exists": True}},
                    {"Date Of Event": {"$exists": True}}
                ]}
            ]
        }
       
        print(f" Searching for events with query: {events_query}")
       
        events_using_type = await events_collection.find(events_query).to_list(length=None)
        events_count = len(events_using_type)
       
        print(f" Found {events_count} events using '{actual_identifier}'")
       
        if events_count > 0:
            event_details = []
            for event in events_using_type[:20]: 
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
                print(f"  Event: {detail['name']} (ID: {detail['id']}, Status: {detail['status']})")
           
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
       
        result = await events_collection.delete_one({"_id": existing_event_type["_id"]})
       
        if result.deleted_count == 1:
            print(f" Event type '{actual_identifier}' deleted successfully")
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
        print(f" Unexpected error: {str(e)}")
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
       
        print(f" DIAGNOSTIC: Checking usage of event type: {decoded_name}")
       
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
       
        print(f" Found event type definition: {actual_name}")
       
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
       
        print(f" Found {len(events)} events using '{actual_name}'")
       
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
                "all_type_fields": {
                    "Event Type": event.get("Event Type"),
                    "eventType": event.get("eventType"),
                    "eventTypeName": event.get("eventTypeName")
                }
            }
            event_details.append(detail)
            print(f"   {detail['eventName']} - {detail['date']} - Status: {detail['status']}")
       
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

        unique_leaders = [dict(t) for t in {tuple(d.items()) for d in leaders}]

        # Sort by position and name for cleaner frontend usage
        unique_leaders.sort(key=lambda x: (x["position"], x["name"]))

        return {"leaders": unique_leaders}

    except Exception as e:
        print(f"Error fetching leaders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# GET CELLS BASED ON OCCURING DAYS--------------------------

logging.basicConfig(level=logging.INFO)

def get_actual_event_status(event: dict, target_date: date) -> str:

    exact_date_str = get_exact_date_identifier(target_date)
   
    print(f"Checking status for: {event.get('Event Name', 'Unknown')}")
    print(f"   Target date key: {exact_date_str}") 
   
    # Check if explicitly marked as did not meet
    if event.get("did_not_meet", False):
        print(f"Marked as 'did_not_meet'")
        return "did_not_meet"
   
    if "attendance" in event and exact_date_str in event["attendance"]:
        date_data = event["attendance"][exact_date_str] 
        date_status = date_data.get("status", "incomplete")
       
        print(f"Found date data - Status: {date_status}")  
       
        if date_status == "complete":
            checked_in_count = len([a for a in date_data.get("attendees", []) if a.get("checked_in", False)])
            if checked_in_count > 0:
                print(f" Week marked complete with {checked_in_count} checked-in attendees")
                return "complete"
            else:
                print(f" Week marked complete but no checked-in attendees")
                return "incomplete"
        elif date_status == "did_not_meet":
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
        instance["date"] = occ_date.isoformat()  
        instance["Date Of Event"] = occ_date.strftime('%d-%m-%Y')  
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
                instance["status"] = "incomplete" 
        else:
            instance["status"] = "incomplete"
           
        cell_instances.append(instance)
   
    return cell_instances
   
   
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
            hour = int(time_str)
            minute = 0
           
        return hour, minute
    except:
        return 19, 0

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

        logging.info(f"TODAY: {today_day_name.upper()} ({today_date})")
        logging.info(f"Fetching cells for {today_day_name}")

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

        for event in events:
            event.pop("_event_date", None)
            event.pop("_day_order", None)

        logging.info(f"Returning {len(events)} cells for {today_day_name}")

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

        start_date_filter = start_date if start_date else '2025-11-30'
       
        query = {
            "Event Type": "Cells",
            "$or": [
                {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                {"email": {"$regex": f"^{email}$", "$options": "i"}},
            ]
        }
       
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
                # Compute current-week instance (Monday..Sunday week)
                days_since_monday = today_date.weekday()
                week_start = today_date - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)
                # Never include a future date - if current-week instance is in future, use previous week
                if current_week_instance > today_date:
                    most_recent_occurrence = current_week_instance - timedelta(weeks=1)
                else:
                    most_recent_occurrence = current_week_instance
               
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

@app.get("/events/global")
async def get_global_events(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    last_updated: Optional[str] = Query(None)  
):
    """
    Get Global Events (like Sunday Service) with real-time updates
    Shows events where isGlobal = True
    """
    try:
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone)
        today_date = today.date()
       
        
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
       
        print(f"Fetching Global Events from {start_date_obj}")
       
        
        query = {
            "isGlobal": True,
            "eventTypeName": "Global Events"
        }
       
        
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
       
        
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
       
        print(f"Query for Global Events: {query}")
       
        
        cursor = events_collection.find(query).sort([("created_at", -1), ("date", -1)])
        all_events = await cursor.to_list(length=None)
       
        print(f"Found {len(all_events)} raw global events")
       
        
        latest_timestamp = None
        if all_events:
            
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
                print(f" Latest event timestamp: {latest_timestamp}")
       
        
        processed_events = []
        new_events_count = 0
       
        for event in all_events:
            try:
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
               
                
                if event_date < start_date_obj:
                    print(f"   Skipped - before date range")
                    continue
               
                
                event_name = event.get("Event Name") or event.get("eventName", "")
                leader_name = event.get("Leader") or event.get("eventLeader", "")
                location = event.get("Location") or event.get("location", "")
               
                
                
                did_not_meet = event.get("did_not_meet", False)
               
                
                stored_status = event.get("status") or event.get("Status")
               
                print(f"  Status determination: did_not_meet={did_not_meet}, stored_status={stored_status}")
               
                if did_not_meet:
                    event_status = "did_not_meet"
                    status_display = "Did Not Meet"
                elif stored_status:
                    
                    event_status = str(stored_status).lower()
                    status_display = str(stored_status).replace("_", " ").title()
                else:
                    
                    
                    event_status = "open"
                    status_display = "Open"
               
                print(f"  ✓ Final status: {event_status}")
               
                
                if status and status != 'all' and status != event_status:
                    print(f"   Skipped - status filter: requested={status}, actual={event_status}")
                    continue
                
                
                attendees_data = event.get("attendees", []) if isinstance(event.get("attendees", []), list) else []
                new_people_data = event.get("new_people", []) if isinstance(event.get("new_people", []), list) else []
                consolidations_data = event.get("consolidations", []) if isinstance(event.get("consolidations", []), list) else []
                
                print(f"  Data arrays - attendees: {len(attendees_data)}, new_people: {len(new_people_data)}, consolidations: {len(consolidations_data)}")
               
                
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
                    
                    "attendees": attendees_data,
                    "new_people": new_people_data,
                    "consolidations": consolidations_data,
                    
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
                    "_is_new": is_new_event,  
                    
                    "closed_by": event.get("closed_by"),
                    "closed_at": event.get("closed_at")
                }
                
                if event.get('time'):
                    final_event['time'] = event.get('time')
                if event.get('Time'):
                    final_event['Time'] = event.get('Time')
               
                processed_events.append(final_event)
                print(f"  Event added to processed list")
               
            except Exception as e:
                print(f"Error processing global event {event.get('_id')}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
       
        print(f"Processed {len(processed_events)} global events after filtering")
        print(f"🆕 New events since last update: {new_events_count}")
       
        
        processed_events.sort(key=lambda x: x['date'], reverse=True)
       
        
        status_counts = {
            "incomplete": sum(1 for e in processed_events if e["status"] == "incomplete"),
            "complete": sum(1 for e in processed_events if e["status"] == "complete"),
            "did_not_meet": sum(1 for e in processed_events if e["status"] == "did_not_meet"),
            "open": sum(1 for e in processed_events if e["status"] == "open"),
            "closed": sum(1 for e in processed_events if e["status"] == "closed")  
        }
       
        print(f"Global Events Status - Incomplete: {status_counts['incomplete']}, Complete: {status_counts['complete']}, Did Not Meet: {status_counts['did_not_meet']}, Open: {status_counts['open']}, Closed: {status_counts['closed']}")
       
        
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
       
        
        start_date_filter = start_date if start_date else '2025-10-20'
        start_date_obj = datetime.strptime(start_date_filter, "%Y-%m-%d").date()
       
        
        query = {
            "isGlobal": True,
            "eventType": "Global Events"
        }
       
        
        if search and search.strip():
            search_regex = {"$regex": search.strip(), "$options": "i"}
            query["$or"] = [
                {"Event Name": search_regex},
                {"eventName": search_regex},
                {"Leader": search_regex},
                {"Location": search_regex}
            ]
       
        
        cursor = events_collection.find(query)
        all_events = await cursor.to_list(length=None)
       
        
        incomplete_count = 0
        complete_count = 0
        did_not_meet_count = 0
       
        for event in all_events:
            try:
                
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
               
                
                if event_date < start_date_obj:
                    continue
               
                
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


@app.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status(current_user: dict = Depends(get_current_user)):
    """Check if user is a leader OR has a cell"""
    try:
        user_email = current_user.get("email")
        user_role = current_user.get("role", "").lower()
       
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
       
        print(f"Checking access for: {user_email}, role: {user_role}")
       
        # Check if user has a cell (for regular users)  roles determination 
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
@app.on_event("startup")
async def create_indexes_on_startup():
    print("Creating MongoDB indexes for faster queries...")
   
    try:
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
       
        await people_collection.create_index(
            [("Name", 1), ("Surname", 1), ("Gender", 1)],
            name="people_lookup_idx"
        )

        # Indexes for faster admin user queries
        await users_collection.create_index([("organization", 1)], name="users_org_idx")
        await users_collection.create_index([("Organization", 1)], name="users_Org_idx")
        await users_collection.create_index([("email", 1)], name="users_email_idx")
       
        print("Indexes created successfully")
    except Exception as e:
        print(f"Error creating indexes: {e}")

@app.put("/events/{event_id}")
async def update_event(event_id: str, event_data: dict, current_user: dict = Depends(get_current_user)):
    """
    FIXED: Update event by _id or UUID
    Now properly updates status for ALL users (bidirectional fix)
    """
    try:
        print(f"Attempting to update event with ID: {event_id}")
        print(f" Received data: {event_data}")
        print(f" Updated by user: {current_user.get('email')} with role: {current_user.get('role')}")
       
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
       
        # =========== FIX: Check if status is being updated ===========
        is_status_update = False
        new_status = None
        old_status = event.get('status') or event.get('Status')
        
        # Check both 'status' and 'Status' fields
        if 'status' in event_data and event_data['status'] is not None:
            new_status = event_data['status']
            is_status_update = True
            print(f"Status update detected: {old_status} -> {new_status}")
        elif 'Status' in event_data and event_data['Status'] is not None:
            new_status = event_data['Status']
            is_status_update = True
            print(f"Status update detected: {old_status} -> {new_status}")
       
        # Prepare update data
        update_data = {}
       
        # Fields that can be updated
        updatable_fields = [
            'eventName', 'day', 'location', 'date',
            'status', 'renocaming', 'eventLeader',
            'eventType', 'isTicketed', 'isGlobal',
            'priceTiers'
        ]
       
        for field in updatable_fields:
            if field in event_data and event_data[field] is not None:
                update_data[field] = event_data[field]
       
        if is_status_update and new_status:
            update_data['status'] = new_status
            update_data['Status'] = new_status
            
            update_data['last_updated_by'] = {
                "email": current_user.get('email'),
                "name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                "role": current_user.get('role'),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            print(f"Updated status fields for ALL users: {new_status}")
            print(f"Updated by: {current_user.get('email')} ({current_user.get('role')})")
            
            if new_status in ['complete', 'did_not_meet']:
                try:
                    event_date_field = (
                        event_data.get("date")
                        or event_data.get("Date Of Event")
                        or event.get("date")
                        or event.get("Date Of Event")
                    )
                    event_date = None
                    
                    if isinstance(event_date_field, datetime):
                        event_date = event_date_field.date()
                    elif isinstance(event_date_field, date):
                        event_date = event_date_field
                    elif isinstance(event_date_field, str):
                        try:
                            event_date = datetime.fromisoformat(event_date_field.replace("Z", "+00:00")).date()
                        except Exception:
                            try:
                                event_date = datetime.strptime(event_date_field, "%Y-%m-%d").date()
                            except Exception:
                                event_date = None
                    
                    if event_date is None:
                        print("Skipping attendance update: event date is missing or unparseable")
                    else:
                        exact_date_str = event_date.strftime("%Y-%m-%d")  
                        
                        attendance_field = f"attendance.{exact_date_str}.status"
                        update_data[attendance_field] = new_status
                        update_data[f"attendance.{exact_date_str}.updated_by_external"] = {
                            "email": current_user.get('email'),
                            "role": current_user.get('role'),
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        
                        print(f"Also updated date-based attendance ({exact_date_str}) to: {new_status}")
                except Exception as e:
                    print(f"Note: Could not update date-based attendance: {e}")
        
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
            
            # =========== FIX: Log the synchronization ===========
            if is_status_update:
                print(f"STATUS SYNCHRONIZED: Event {event_id} status changed to {new_status}")
                print(f"  - Changed by: {current_user.get('email')} ({current_user.get('role')})")
                print(f"  - Old status: {old_status}")
                print(f"  - New status: {new_status}")
                print(f"  - Will be visible to ALL users immediately")
       
        # Fetch and return the updated event
        updated_event = await events_collection.find_one({"_id": event["_id"]})
        updated_event["_id"] = str(updated_event["_id"])
        
        # =========== FIX: Return synchronization info ===========
        response_data = {
            **updated_event,
            "sync_info": {
                "status_updated": is_status_update,
                "new_status": new_status,
                "updated_by": current_user.get('email'),
                "updated_by_role": current_user.get('role'),
                "timestamp": datetime.utcnow().isoformat(),
                "message": "Status synchronized for ALL users" if is_status_update else "Event updated"
            }
        }
       
        return response_data
       
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
     COMPREHENSIVE: Bulk assign Leader @1 for ALL cell events
    This ensures every cell event has the correct Leader @1 from People database
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        print("\n" + "="*80)
        print(" STARTING BULK LEADER @1 ASSIGNMENT FOR ALL CELL EVENTS")
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
     FIXED: Assign Leader @1 based on EVENT LEADER's gender
    This assigns Gavin/Vicky based on who is leading the cell
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
   
    try:
        print("\n" + "="*80)
        print(" FIXING ALL LEADERS @1 BASED ON EVENT LEADER'S GENDER")
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
       
               # Compute current-week instance (Monday..Sunday week)
                # Resolve target weekday from the stored 'day' field (not a missing var)
                target_weekday = day_mapping.get(day)
                if target_weekday is None:
                    # invalid or missing day -> skip this event
                    continue
                # Use today_date (date) consistently in this function
                days_since_monday = today_date.weekday()
                week_start = today_date - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)
                 
                # Choose the most relevant occurrence not in the future
                if current_week_instance > today_date:
                    next_occurrence = current_week_instance - timedelta(weeks=1)
                else:
                    next_occurrence = current_week_instance
                # Ensure within requested start_date (don't return occurrences older than start_date_obj)
                if next_occurrence < start_date_obj:
                    # find first occurrence on/after start_date_obj (but not in the future)
                    days_since_start = start_date_obj.weekday()
                    start_week_start = start_date_obj - timedelta(days=days_since_start)
                    candidate = start_week_start + timedelta(days=target_weekday)
                    if candidate > today_date:
                        next_occurrence = candidate - timedelta(weeks=1)
                    else:
                        next_occurrence = candidate

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

        if event.get('time'):
            final_event['time'] = event.get('time')
        if event.get('Time'):
            final_event['Time'] = event.get('Time')     
            
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

            #  Determine correct Leader @1 based on gender
            if gender == "female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "male":
                leader_at_1 = "Gavin Enslin"
            else:
                print(f"Gender unknown for {leader_name}")
                failed_count += 1
                continue

            #  Update the event in MongoDB
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
            print(f"Email username part: {email_username}")
       
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

async def get_leader_at_1_for_leader_at_1728(leader_at_1728_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 1728.
    This should come from their Leader at 144 -> Leader at 12
    """
    if not leader_at_1728_name:
        return ""
   
    # FIRST: Try to find the person by Name 
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


async def update_event_status(event_id: str, new_status: str, updated_by: dict):
    """Centralized function to update event status for ALL users"""
    if new_status not in ['complete', 'incomplete', 'did_not_meet', 'cancelled']:
        raise ValueError(f"Invalid status: {new_status}")
    
    update_data = {
        "status": new_status,
        "Status": new_status,
        "updated_at": datetime.utcnow(),
        "last_updated_by": {
            "email": updated_by.get('email'),
            "name": f"{updated_by.get('name', '')} {updated_by.get('surname', '')}".strip(),
            "role": updated_by.get('role'),
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    result = await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {"$set": update_data}
    )
    
    return result

@app.get("/events/cells/optimized")
async def get_cell_events_optimized(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query('2025-11-30'),
    leader_at_12_view: Optional[bool] = Query(None),
    show_personal_cells: Optional[bool] = Query(None),
    show_all_authorized: Optional[bool] = Query(None),
):
    try:
        user_email = current_user.get("email", "")
        role = current_user.get("role", "user").lower()
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        
        is_leader_at_12 = (
            "leaderat12" in role or 
            "leader at 12" in role or
            "leader@12" in role or
            role == "leaderat12" or
            leader_at_12_view
        )
        
        query = {"Event Type": "Cells"}
        
        if search and search.strip():
            search_term = search.strip()
            query["$or"] = [
                {"Event Name": {"$regex": search_term, "$options": "i"}},
                {"Leader": {"$regex": search_term, "$options": "i"}},
                {"Email": {"$regex": search_term, "$options": "i"}},
            ]
        
        if role == "admin":
            if personal or show_personal_cells:
                query["Email"] = user_email
        elif is_leader_at_12:
            want_personal = (show_personal_cells or personal)
            want_disciples = (show_all_authorized)
            
            if want_personal and not want_disciples:
                query["Email"] = user_email
            elif want_disciples and not want_personal:
                query["Leader @12"] = user_name
                query["Email"] = {"$ne": user_email}
            else:
                query["$or"] = [
                    {"Email": user_email},
                    {"Leader @12": user_name}
                ]
        else:
            query["Email"] = user_email

        cursor = events_collection.find(query)
        all_cells = await cursor.to_list(length=None)
        
        timezone = pytz.timezone("Africa/Johannesburg")
        today = datetime.now(timezone).date()
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        day_mapping = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        
        cell_instances = []
        for cell in all_cells:
            try:
                day_name = str(cell.get("Day", "")).strip().lower()
                if day_name not in day_mapping:
                    continue
                
                target_weekday = day_mapping.get(day_name)
                if target_weekday is None:
                    continue
                attendance_data = cell.get("attendance", {})
                weeks_to_check = 1 if status == "incomplete" else 4
                # Use 'today' (a date) defined at top of this function
                days_since_monday = today.weekday()
                week_start = today - timedelta(days=days_since_monday)
                current_week_instance = week_start + timedelta(days=target_weekday)
# ...existing code...

                for week_back in range(0, weeks_to_check):
                    instance_date = current_week_instance - timedelta(weeks=week_back)
                    # Strict: skip future dates
                    if instance_date > today:
                        continue
                    if instance_date < start_date_obj:
                        continue
                     
                    exact_date_str = instance_date.isoformat()
                    
                    exact_date_str = instance_date.isoformat()
                    week_attendance = attendance_data.get(exact_date_str, {})
                    
                    if not week_attendance:
                        for key, value in attendance_data.items():
                            if isinstance(value, dict):
                                if value.get("event_date_exact") == exact_date_str:
                                    week_attendance = value
                                    break
                                event_date_iso = value.get("event_date_iso")
                                if event_date_iso and exact_date_str in event_date_iso:
                                    week_attendance = value
                                    break
                    
                    if not week_attendance or not isinstance(week_attendance, dict):
                        cell_status = "incomplete"
                        attendees = []
                        did_not_meet = False
                    else:
                        att_status = week_attendance.get("status", "").lower()
                        attendees = week_attendance.get("attendees", [])
                        
                        if att_status == "did_not_meet":
                            cell_status = "did_not_meet"
                            did_not_meet = True
                        elif att_status == "complete" or len(attendees) > 0:
                            cell_status = "complete"
                            did_not_meet = False
                        else:
                            cell_status = "incomplete"
                            did_not_meet = False
                    
                    if status and status != 'all' and status != cell_status:
                        continue
                    
                    captured_by_leader = week_attendance.get("captured_by_leader_at_12", False) if week_attendance else False
                    
                    if role == "admin" and not (personal or show_personal_cells) and captured_by_leader:
                        continue
                    
                    is_overdue = instance_date < today and cell_status == "incomplete"
                    
                    instance = {
                        "_id": f"{cell['_id']}_{exact_date_str}",
                        "UUID": cell.get("UUID", ""),
                        "eventName": cell.get("Event Name", ""),
                        "eventType": "Cells",
                        "eventLeaderName": cell.get("Leader", ""),
                        "eventLeaderEmail": cell.get("Email", ""),
                        "leader1": cell.get("leader1", ""),
                        "leader12": cell.get("Leader @12", ""),
                        "day": day_name.capitalize(),
                        "date": exact_date_str,
                        "display_date": instance_date.strftime("%d - %m - %Y"),
                        "location": cell.get("Location", ""),
                        "status": cell_status,
                        "attendees": attendees,
                        "persistent_attendees": cell.get("persistent_attendees", []),
                        "_is_overdue": is_overdue,
                        "original_event_id": str(cell["_id"]),
                        "is_recurring": True,
                        "attendance": week_attendance,
                        "did_not_meet": did_not_meet,
                    }
                     
                    if cell.get('time'):
                        instance['time'] = cell.get('time')
                    if cell.get('Time'):
                        instance['Time'] = cell.get('Time')
                    
                    cell_instances.append(instance)
                    
            except Exception as e:
                print(f"Error processing cell {cell.get('_id')}: {str(e)}")
                continue
        
        cell_instances.sort(key=lambda x: x['date'], reverse=True)
        
        unique_instances = {}
        for instance in cell_instances:
            key = f"{instance['original_event_id']}_{instance['date']}"
            if key not in unique_instances:
                unique_instances[key] = instance
        
        cell_instances = list(unique_instances.values())
        
        total = len(cell_instances)
        total_pages = (total + limit - 1) // limit if total > 0 else 1
        skip = (page - 1) * limit
        paginated = cell_instances[skip:skip + limit]
        
        return {
            "events": paginated,
            "total_events": total,
            "total_pages": total_pages,
            "current_page": page,
            "page_size": limit
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/submit-attendance/{event_id}")
async def submit_attendance(
    event_id: str = Path(...),
    submission: AttendanceSubmission = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        # Parse event ID and extract date
        actual_event_id = event_id
        extracted_date = None
        
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                if len(parts) >= 2:
                    try:
                        extracted_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
                    except Exception:
                        pass
        
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Get user info
        user_email = current_user.get("email", "")
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        role = current_user.get("role", "user").lower()
        
        # Set timezone
        timezone = pytz.timezone("Africa/Johannesburg")
        
        # Determine event date
        if extracted_date:
            event_date_local = timezone.localize(datetime.combine(extracted_date, datetime.min.time()))
        else:
            event_date = None
            for date_field in ["date", "Date Of Event", "eventDate"]:
                if date_field in event:
                    date_val = event[date_field]
                    if isinstance(date_val, datetime):
                        event_date = date_val.date()
                        break
                    elif isinstance(date_val, str):
                        try:
                            if "T" in date_val:
                                event_date = datetime.fromisoformat(date_val.replace("Z", "+00:00")).date()
                            else:
                                event_date = datetime.strptime(date_val, "%Y-%m-%d").date()
                            break
                        except:
                            continue
            
            if event_date:
                event_date_local = timezone.localize(datetime.combine(event_date, datetime.min.time()))
            else:
                event_date_local = datetime.now(timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        
        exact_date_str = event_date_local.date().isoformat()
        
        # Extract submission data
        attendees_data = submission.attendees or []
        persistent_attendees = getattr(submission, 'persistent_attendees', []) or []
        did_not_meet = submission.did_not_meet
        manual_headcount = getattr(submission, 'headcount', 0)
        is_ticketed = submission.isTicketed

        # ── FIX: Convert Pydantic models to plain dicts ──────────────────────
        def to_dict(obj):
            if isinstance(obj, dict):
                return obj
            if hasattr(obj, 'model_dump'):
                return obj.model_dump()
            if hasattr(obj, 'dict'):
                return obj.dict()
            return dict(obj)

        attendees_data = [to_dict(att) for att in attendees_data]
        persistent_attendees = [to_dict(att) for att in persistent_attendees]
        # ─────────────────────────────────────────────────────────────────────

        try:
            manual_headcount = int(manual_headcount) if manual_headcount else 0
        except:
            manual_headcount = 0
        
        # Debug: Print incoming data
        print(f"Received {len(attendees_data)} attendees")
        for att in attendees_data:
            print(f"Attendee: {att.get('fullName')} - price: {att.get('price')}, paid: {att.get('paid')}, paidAmount: {att.get('paidAmount')}")
        
        # Helper function to enrich attendee with financials
        def enrich_with_financials(attendee_dict):
            """Add paid, owing, change fields based on price and paid amount"""
            # Get price (default to 0 if not present)
            price = attendee_dict.get("price", 0)
            
            # Check multiple possible field names for paid amount
            paid = attendee_dict.get("paid", None)
            if paid is None:
                paid = attendee_dict.get("paidAmount", None)
            if paid is None:
                paid = attendee_dict.get("paid_amount", None)
            if paid is None:
                paid = 0
            
            # Ensure numeric values
            try:
                price = float(price) if price else 0
                paid = float(paid) if paid else 0
            except (ValueError, TypeError):
                price = 0
                paid = 0
            
            # Calculate financials
            if paid >= price:
                owing = 0
                change = paid - price
            elif paid > 0 and paid < price:
                owing = price - paid
                change = 0
            else:
                owing = price
                change = 0
            
            print(f"Financials - price: {price}, paid: {paid}, owing: {owing}, change: {change}")
            
            # Create enriched attendee with all fields
            enriched = {
                "id": attendee_dict.get("id", ""),
                "name": attendee_dict.get("name", attendee_dict.get("fullName", "")),
                "fullName": attendee_dict.get("fullName", attendee_dict.get("name", "")),
                "email": attendee_dict.get("email", ""),
                "phone": attendee_dict.get("phone", ""),
                "leader12": attendee_dict.get("leader12", ""),
                "leader144": attendee_dict.get("leader144", ""),
                "invitedBy": attendee_dict.get("invitedBy", ""),
                "decision": attendee_dict.get("decision", ""),
                "checked_in": attendee_dict.get("checked_in", True),
                "isPersistent": attendee_dict.get("isPersistent", True),
                "priceName": attendee_dict.get("priceName", ""),
                "price": price,
                "ageGroup": attendee_dict.get("ageGroup", ""),
                "paymentMethod": attendee_dict.get("paymentMethod", ""),
                "paid": paid,
                "owing": owing,
                "change": change,
                "check_in_date": datetime.now(timezone).isoformat() if not attendee_dict.get("check_in_date") else attendee_dict.get("check_in_date")
            }
            return enriched
        
        # Process persistent attendees
        persistent_attendees_dict = []
        for attendee in persistent_attendees:
            persistent_attendees_dict.append(enrich_with_financials(attendee))
        
        # Process checked-in attendees
        checked_in_attendees = []
        first_time_count = 0
        recommitment_count = 0
        
        for att in attendees_data:
            attendee_data = enrich_with_financials(att)
            
            # Handle decision tracking
            decision = att.get("decision", "")
            if decision:
                attendee_data["decision"] = decision
                decision_lower = decision.lower()
                if "first" in decision_lower:
                    first_time_count += 1
                elif "re-commitment" in decision_lower or "recommitment" in decision_lower:
                    recommitment_count += 1
            
            checked_in_attendees.append(attendee_data)
        
        # Calculate statistics
        total_associated = len(persistent_attendees_dict) or event.get("total_associated_count", 0)
        weekly_attendance = len(checked_in_attendees)
        total_decisions = first_time_count + recommitment_count
        
        # Determine status
        should_mark_as_did_not_meet = (did_not_meet and weekly_attendance == 0 and manual_headcount == 0)
        
        if should_mark_as_did_not_meet:
            date_status = "did_not_meet"
            has_attendance = False
        elif weekly_attendance == 0 and manual_headcount == 0:
            date_status = "incomplete"
            has_attendance = False
        else:
            date_status = "complete"
            has_attendance = True
        
        now = datetime.now(timezone)
        
        # Create weekly attendance entry
        weekly_attendance_entry = {
            "status": date_status,
            "attendees": checked_in_attendees if has_attendance else [],
            "submitted_at": now,
            "submitted_by": user_email,
            "submitted_by_name": user_name,
            "submitted_date": now.isoformat(),
            "event_date": event_date_local.isoformat(),
            "event_date_iso": exact_date_str,
            "event_date_exact": exact_date_str,
            "persistent_attendees": persistent_attendees_dict if has_attendance else [],
            "is_did_not_meet": (date_status == "did_not_meet"),
            "checked_in_count": weekly_attendance,
            "total_headcounts": manual_headcount,
            "is_ticketed": is_ticketed,
            "statistics": {
                "total_associated": total_associated,
                "weekly_attendance": weekly_attendance,
                "total_headcounts": manual_headcount,
                "decisions": {
                    "first_time": first_time_count,
                    "recommitment": recommitment_count,
                    "total": total_decisions
                }
            }
        }
        
        # Prepare update fields
        update_data = {
            "updated_at": now,
            "last_attendance_count": weekly_attendance,
            "last_headcount": manual_headcount,
            "last_attendance_date": exact_date_str,
            "last_status": date_status,
            "status": date_status,
            f"attendance.{exact_date_str}": weekly_attendance_entry
        }
        
        # Update persistent attendees if provided
        if persistent_attendees_dict:
            update_data["persistent_attendees"] = persistent_attendees_dict
            update_data["total_associated_count"] = len(persistent_attendees_dict)
        
        # For non-recurring events, update root-level attendees
        recurring_days = event.get("recurring_day", [])
        is_recurring = isinstance(recurring_days, list) and len(recurring_days) > 0
        
        if not is_recurring and date_status == "complete":
            update_data["attendees"] = checked_in_attendees
            update_data["total_attendance"] = weekly_attendance
        
        # Execute update
        result = await events_collection.update_one(
            {"_id": ObjectId(actual_event_id)},
            {"$set": update_data}
        )
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update event")
        
        return {
            "message": "Attendance submitted successfully",
            "event_id": actual_event_id,
            "status": date_status,
            "exact_date": exact_date_str,
            "checked_in_count": weekly_attendance,
            "total_headcounts": manual_headcount,
            "statistics": weekly_attendance_entry["statistics"],
            "success": True,
            "timestamp": now.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error submitting attendance: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.put("/events/{event_id}/persistent-attendees")
async def update_persistent_attendees(
    event_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update persistent attendees list for an event.
    Saves all attendee information including ticket and financial data.
    """
    try:
        print(f"PUT /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")
        
        # Parse event ID
        actual_event_id = event_id
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
        
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        # Fetch the event
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Get the updated persistent attendees from request
        persistent_attendees = update_data.get("persistent_attendees", [])
        
        # Enrich each attendee with proper fields and calculate financials
        enriched_attendees = []
        for attendee in persistent_attendees:
            if not isinstance(attendee, dict):
                continue
            
            # Get price and paid amount
            event_price = attendee.get("price", 0)
            paid_amount = attendee.get("paidAmount", attendee.get("paid", 0))
            
            # Calculate financials
            if paid_amount >= event_price:
                owing = 0
                change = paid_amount - event_price
            elif paid_amount > 0 and paid_amount < event_price:
                owing = event_price - paid_amount
                change = 0
            else:
                owing = event_price
                change = 0
            
            # Create enriched attendee object
            enriched_attendee = {
                "id": attendee.get("id", ""),
                "name": attendee.get("name", attendee.get("fullName", "")),
                "fullName": attendee.get("fullName", attendee.get("name", "")),
                "email": attendee.get("email", ""),
                "phone": attendee.get("phone", ""),
                "leader12": attendee.get("leader12", ""),
                "leader144": attendee.get("leader144", ""),
                "invitedBy": attendee.get("invitedBy", ""),
                "isPersistent": True,
                # Ticket information
                "priceName": attendee.get("priceName", ""),
                "price": event_price,
                "ageGroup": attendee.get("ageGroup", ""),
                "paymentMethod": attendee.get("paymentMethod", ""),
                # Financial information
                "paid": paid_amount,
                "paidAmount": paid_amount,
                "owing": owing,
                "change": change,
            }
            enriched_attendees.append(enriched_attendee)
        
        # Prepare update fields
        update_fields = {
            "persistent_attendees": enriched_attendees,
            "total_associated_count": len(enriched_attendees),
            "updated_at": datetime.utcnow(),
            "last_updated_by": {
                "email": current_user.get("email"),
                "name": f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip(),
                "role": current_user.get("role", "user"),
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        
        # If event has attendance data for specific dates, also update there
        target_date = None
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 2:
                try:
                    target_date = datetime.strptime(parts[1], "%Y-%m-%d").date().isoformat()
                except Exception:
                    pass
        
        if target_date and event.get("attendance", {}).get(target_date):
            # Also update the persistent attendees in the date-specific attendance record
            update_fields[f"attendance.{target_date}.persistent_attendees"] = enriched_attendees
            update_fields[f"attendance.{target_date}.statistics.total_associated"] = len(enriched_attendees)
            update_fields[f"attendance.{target_date}.updated_at"] = datetime.utcnow()
        
        # Execute the update
        result = await events_collection.update_one(
            {"_id": ObjectId(actual_event_id)},
            {"$set": update_fields}
        )
        
        if result.matched_count != 1:
            raise HTTPException(status_code=500, detail="Failed to update persistent attendees")
        
        # Return the updated attendees list
        return {
            "success": True,
            "message": f"Updated {len(enriched_attendees)} persistent attendees",
            "persistent_attendees": enriched_attendees,
            "total_associated": len(enriched_attendees),
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/events/{event_id}/persistent-attendees")
async def get_persistent_attendees(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get persistent attendees for an event with their ticket and financial information.
    Also returns attendance status for the specific date.
    """
    try:
        print(f"GET /events/{event_id}/persistent-attendees - User: {current_user.get('email')}")
        
        # Parse event ID to extract actual event ID and date
        actual_event_id = event_id
        target_date = None
        
        if "_" in event_id:
            parts = event_id.split("_")
            if len(parts) >= 1 and ObjectId.is_valid(parts[0]):
                actual_event_id = parts[0]
                if len(parts) >= 2:
                    try:
                        target_date = datetime.strptime(parts[1], "%Y-%m-%d").date()
                    except Exception:
                        pass
        
        if not ObjectId.is_valid(actual_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        # Fetch the event
        event = await events_collection.find_one({"_id": ObjectId(actual_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Determine the date we're looking for
        if not target_date:
            # Try to get date from event
            event_date = None
            for date_field in ["date", "Date Of Event", "eventDate", "startDate"]:
                if date_field in event:
                    date_val = event[date_field]
                    if isinstance(date_val, datetime):
                        event_date = date_val.date()
                        break
                    elif isinstance(date_val, str):
                        try:
                            if "T" in date_val:
                                event_date = datetime.fromisoformat(date_val.replace("Z", "+00:00")).date()
                            else:
                                event_date = datetime.strptime(date_val, "%Y-%m-%d").date()
                            break
                        except:
                            continue
            
            if event_date:
                target_date = event_date
            else:
                # Default to today
                target_date = datetime.now().date()
        
        exact_date_str = target_date.isoformat()
        
        # Get persistent attendees from the event
        persistent_attendees = event.get("persistent_attendees", [])
        
        # Check if there's attendance data for this specific date
        attendance_data = event.get("attendance", {})
        date_attendance = attendance_data.get(exact_date_str, {})
        
        # Determine attendance status
        attendance_status = "incomplete"
        checked_in_attendees = []
        total_headcounts = 0
        
        if date_attendance:
            attendance_status = date_attendance.get("status", "incomplete")
            checked_in_attendees = date_attendance.get("attendees", [])
            total_headcounts = date_attendance.get("total_headcounts", 0)
        else:
            # Check root-level status for non-recurring events
            root_status = event.get("status", "")
            if root_status in ["complete", "did_not_meet"]:
                attendance_status = root_status
                checked_in_attendees = event.get("attendees", [])
                total_headcounts = event.get("total_headcounts", 0)
        
        # Enrich persistent attendees with ticket and financial data
        enriched_attendees = []
        for attendee in persistent_attendees:
            if not isinstance(attendee, dict):
                continue
            
            # Find if this attendee has checked-in data for this date
            checked_in_data = None
            for checked in checked_in_attendees:
                if checked.get("id") == attendee.get("id"):
                    checked_in_data = checked
                    break
            
            # Create enriched attendee object with all fields
            enriched_attendee = {
                "id": attendee.get("id", ""),
                "name": attendee.get("name", ""),
                "fullName": attendee.get("fullName", attendee.get("name", "")),
                "email": attendee.get("email", ""),
                "phone": attendee.get("phone", ""),
                "leader12": attendee.get("leader12", ""),
                "leader144": attendee.get("leader144", ""),
                "invitedBy": attendee.get("invitedBy", ""),
                "isPersistent": True,
                # Ticket information
                "priceName": attendee.get("priceName", ""),
                "price": attendee.get("price", 0),
                "ageGroup": attendee.get("ageGroup", ""),
                "paymentMethod": attendee.get("paymentMethod", ""),
                # Financial information
                "paidAmount": attendee.get("paid", attendee.get("paidAmount", 0)),
                "paid": attendee.get("paid", attendee.get("paidAmount", 0)),
                "owing": attendee.get("owing", 0),
                "change": attendee.get("change", 0),
            }
            
            # Override with checked-in data if available
            if checked_in_data:
                enriched_attendee["checked_in"] = checked_in_data.get("checked_in", False)
                enriched_attendee["decision"] = checked_in_data.get("decision", "")
                enriched_attendee["check_in_date"] = checked_in_data.get("check_in_date", "")
                
                # Use checked-in ticket info if present (allows per-week overrides)
                if checked_in_data.get("priceName"):
                    enriched_attendee["priceName"] = checked_in_data.get("priceName")
                if checked_in_data.get("price") is not None:
                    enriched_attendee["price"] = checked_in_data.get("price")
                if checked_in_data.get("ageGroup"):
                    enriched_attendee["ageGroup"] = checked_in_data.get("ageGroup")
                if checked_in_data.get("paymentMethod"):
                    enriched_attendee["paymentMethod"] = checked_in_data.get("paymentMethod")
                if checked_in_data.get("paid") is not None:
                    enriched_attendee["paidAmount"] = checked_in_data.get("paid")
                    enriched_attendee["paid"] = checked_in_data.get("paid")
                if checked_in_data.get("owing") is not None:
                    enriched_attendee["owing"] = checked_in_data.get("owing")
                if checked_in_data.get("change") is not None:
                    enriched_attendee["change"] = checked_in_data.get("change")
            else:
                enriched_attendee["checked_in"] = False
                enriched_attendee["decision"] = ""
            
            enriched_attendees.append(enriched_attendee)
        
        # Build checked-in attendees list for response
        checked_in_list = []
        for att in checked_in_attendees:
            if not isinstance(att, dict):
                continue
            
            checked_in_item = {
                "id": att.get("id", ""),
                "name": att.get("name", ""),
                "fullName": att.get("fullName", att.get("name", "")),
                "email": att.get("email", ""),
                "phone": att.get("phone", ""),
                "leader12": att.get("leader12", ""),
                "leader144": att.get("leader144", ""),
                "checked_in": att.get("checked_in", True),
                "decision": att.get("decision", ""),
                "check_in_date": att.get("check_in_date", ""),
                "priceName": att.get("priceName", ""),
                "price": att.get("price", 0),
                "ageGroup": att.get("ageGroup", ""),
                "paymentMethod": att.get("paymentMethod", ""),
                "paid": att.get("paid", 0),
                "owing": att.get("owing", 0),
                "change": att.get("change", 0),
            }
            checked_in_list.append(checked_in_item)
        
        # Get total headcounts (for manual headcount tracking)
        total_headcounts_value = total_headcounts
        if not total_headcounts_value and attendance_status == "complete":
            total_headcounts_value = date_attendance.get("total_headcounts", 0)
        
        return {
            "persistent_attendees": enriched_attendees,
            "checked_in_attendees": checked_in_list,
            "attendance_status": attendance_status,
            "total_headcounts": total_headcounts_value,
            "event_date": exact_date_str,
            "is_ticketed": event.get("isTicketed", False),
            "total_associated": len(persistent_attendees)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting persistent attendees: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



@app.get("/events/{event_id}/last-attendance")
async def get_last_attendance(
    event_id: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        persistent = event.get("persistent_attendees", [])
        if persistent:
            return {
                "has_previous_attendance": True,
                "attendees": persistent,
                "statistics": {
                    "total_associated": len(persistent),
                    "last_attendance_count": event.get("last_attendance_count", 0),
                    "last_decisions_count": event.get("last_decisions_count", 0)
                }
            }

        attendance = event.get("attendance", {})
        if not attendance:
            return {
                "has_previous_attendance": False,
                "attendees": [],
                "statistics": {
                    "total_associated": 0,
                    "last_attendance_count": 0,
                    "last_decisions_count": 0
                }
            }

        weeks = sorted(attendance.keys(), reverse=True)
        if weeks:
            last_week_data = attendance[weeks[0]]
            return {
                "has_previous_attendance": True,
                "attendees": last_week_data.get("attendees", []),
                "statistics": {
                    "total_associated": event.get("total_associated_count", 0),
                    "last_attendance_count": event.get("last_attendance_count", 0),
                    "last_decisions_count": event.get("last_decisions_count", 0)
                }
            }

        return {
            "has_previous_attendance": False,
            "attendees": [],
            "statistics": {
                "total_associated": 0,
                "last_attendance_count": 0,
                "last_decisions_count": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))     

    
@app.get("/events/{event_id}/statistics")
async def get_event_statistics(
    event_id: str = Path(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        attendance = event.get("attendance", {})
        
        latest_week = None
        latest_stats = None
        
        if attendance:
            # Sort attendance dates
            weeks = sorted(attendance.keys(), reverse=True)
            if weeks:
                latest_week = weeks[0]
                latest_week_data = attendance[latest_week]
                
                # Get statistics from the stored data
                stats = latest_week_data.get("statistics", {})
                
                latest_stats = {
                    "week": latest_week,
                    "date": latest_week_data.get("event_date_iso", latest_week),
                    "attendance_count": latest_week_data.get("checked_in_count", 0),
                    "total_headcounts": latest_week_data.get("total_headcounts", 0),
                    "checked_in_attendees": len(latest_week_data.get("attendees", [])),
                    "did_not_meet": latest_week_data.get("is_did_not_meet", False),
                    "status": latest_week_data.get("status", ""),
                    "statistics": {
                        "total_associated": stats.get("total_associated", 0),
                        "weekly_attendance": stats.get("weekly_attendance", 0),
                        "total_headcounts": stats.get("total_headcounts", 0),
                        "decisions": stats.get("decisions", {
                            "first_time": 0, 
                            "recommitment": 0, 
                            "total": 0
                        })
                    }
                }
        
        # Also get the last attendance data from the event-level fields
        last_attendance_breakdown = event.get("last_attendance_breakdown", {})
        if not last_attendance_breakdown and latest_stats:
            last_attendance_breakdown = {
                "first_time": latest_stats["statistics"]["decisions"]["first_time"],
                "recommitment": latest_stats["statistics"]["decisions"]["recommitment"],
                "total": latest_stats["statistics"]["decisions"]["total"],
                "date": latest_stats["date"]
            }
        
        return {
            "event_id": str(event["_id"]),
            "event_name": event.get("Event Name", event.get("eventName", "Unknown")),
            "leader": event.get("Leader", event.get("eventLeader", event.get("eventLeaderName", ""))),
            "day": event.get("Day", event.get("day", "")),
            "time": event.get("Time", event.get("time", "")),
            "status": event.get("status", event.get("last_status", "")),
            "statistics": {
                "latest_week": latest_stats,
                "last_attendance_count": event.get("last_attendance_count", 0),
                "last_headcount": event.get("last_headcount", 0),
                "last_decisions_count": event.get("last_decisions_count", 0),
                "last_attendance_date": event.get("last_attendance_date", ""),
                "last_attendance_breakdown": last_attendance_breakdown
            },
            "has_attendance_data": len(attendance) > 0,
            "total_associated": event.get("total_associated_count", 0),
            "persistent_attendees_count": len(event.get("persistent_attendees", []))
        }
        
    except Exception as e:
        print(f"Error in get_event_statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/events/{event_id}")
async def delete_event(event_id: str = Path(...)):
    try:
        print(f" DELETE REQUEST - Event ID: {event_id}")
        print(f" ID length: {len(event_id)}")
        print(f" ID is valid ObjectId: {ObjectId.is_valid(event_id)}")
        
        if not ObjectId.is_valid(event_id):
            print(f" Invalid ObjectId format: {event_id}")
            raise HTTPException(status_code=400, detail="Invalid event ID format")
        
        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        
        if not existing_event:
            print(f" Event not found with ID: {event_id}")
            print(f" Checking if event exists with different casing or format...")
            
            similar_events = await events_collection.find({
                "eventName": {"$regex": ".*", "$options": "i"}
            }).limit(3).to_list(None)
            
            print(f" Sample events in DB:")
            for evt in similar_events:
                print(f"   - ID: {evt.get('_id')}, Name: {evt.get('eventName', 'N/A')}")
            
            raise HTTPException(status_code=404, detail=f"Event not found. ID: {event_id}")
        
        print(f"Found event to delete:")
        print(f"   - ID: {existing_event.get('_id')}")
        print(f"   - Name: {existing_event.get('eventName', 'N/A')}")
        print(f"   - Date: {existing_event.get('dateOfEvent', 'N/A')}")
        
        # Delete the event
        result = await events_collection.delete_one({"_id": ObjectId(event_id)})
        
        if result.deleted_count == 1:
            print(f" Successfully deleted event: {event_id}")
            return {"message": "Event deleted successfully"}
        else:
            print(f" Delete operation failed for: {event_id}")
            raise HTTPException(status_code=500, detail="Failed to delete event")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error deleting event {event_id}: {str(e)}")
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
        
        if event.get('time'):
            event['time'] = event['time']
        if event.get('Time'):
            event['Time'] = event['Time']
       
        #  ENSURE NEW FIELDS ARE RETURNED
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


@app.get("/ping")
async def ping():
    return JSONResponse(content={"message": "Server is alive "}, status_code=200)

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
#  END OF EVENTS-----------------------------------------------


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
    try:
        token_user_id = current_user.get("user_id") or current_user.get("_id")
        if not token_user_id:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")
        if str(token_user_id) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized to access this profile")
        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail=f"Invalid user ID format: {user_id}")

        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail=f"User not found with ID: {user_id}")

        user_email = user.get("email", "")

        # ── Always look up the people doc — it has the authoritative LeaderPath ──
        person = None
        if user_email:
            person = await people_collection.find_one(
                {"Email": {"$regex": f"^{re.escape(user_email)}$", "$options": "i"}}
            )

        # ── Resolve leader path: prefer people doc, fall back to users doc ──────
        raw_path = []
        if person and person.get("LeaderPath"):
            raw_path = person["LeaderPath"]
        elif user.get("LeaderPath"):
            raw_path = user["LeaderPath"]

        # Normalise to ObjectId list
        leader_path_oids = []
        for entry in raw_path:
            try:
                leader_path_oids.append(
                    entry if isinstance(entry, ObjectId) else ObjectId(str(entry))
                )
            except Exception:
                pass

        # ── Resolve the three leader levels from LeaderPath ─────────────────────
        # LeaderPath is root-first: [root(level1), level12, level144, ...]
        async def _resolve_leader(oid: ObjectId) -> Optional[dict]:
            if not oid:
                return None
            doc = await people_collection.find_one(
                {"_id": oid},
                {"_id": 1, "Name": 1, "Surname": 1, "Email": 1, "Number": 1}
            )
            if not doc:
                return None
            return {
                "id": str(doc["_id"]),
                "name": doc.get("Name", ""),
                "surname": doc.get("Surname", ""),
                "email": doc.get("Email", ""),
                "phone_number": doc.get("Number", "")
            }

        leader_at_1   = await _resolve_leader(leader_path_oids[0]) if len(leader_path_oids) > 0 else None
        leader_at_12  = await _resolve_leader(leader_path_oids[1]) if len(leader_path_oids) > 1 else None
        leader_at_144 = await _resolve_leader(leader_path_oids[2]) if len(leader_path_oids) > 2 else None

        # ── Resolve InvitedBy display name ──────────────────────────────────────
        # Use people doc's InvitedBy string, or derive from the direct leader
        invited_by = ""
        if person:
            invited_by = person.get("InvitedBy", "") or user.get("invited_by", "")
        else:
            invited_by = user.get("invited_by", "")

        # If invited_by is still empty but we have a direct leader, use their name
        if not invited_by and leader_at_1:
            invited_by = f"{leader_at_1['name']} {leader_at_1['surname']}".strip()

        organization = user.get("Organization") or user.get("organization", "")
        leader_path_strs = [str(o) for o in leader_path_oids]

        return {
            "id": str(user["_id"]),
            "name": user.get("name", ""),
            "surname": user.get("surname", ""),
            "date_of_birth": user.get("date_of_birth", ""),
            "home_address": user.get("home_address", ""),
            "invited_by": invited_by,
            "phone_number": user.get("phone_number", ""),
            "email": user.get("email", ""),
            "gender": user.get("gender", ""),
            "role": user.get("role", "user"),
            "profile_picture": user.get("profile_picture", ""),
            "organization": organization,
            "leader_path": leader_path_strs,
            "leaders": {
                "leaderAt1":   leader_at_1,
                "leaderAt12":  leader_at_12,
                "leaderAt144": leader_at_144,
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Profile fetch error: {str(e)}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch profile: {str(e)}")
    try:
        token_user_id = current_user.get("user_id") or current_user.get("_id")
        
        if not token_user_id:
            raise HTTPException(status_code=401, detail="Invalid user ID in token")
        
        if str(token_user_id) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized to access this profile")
        
        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail=f"Invalid user ID format: {user_id}")
        
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail=f"User not found with ID: {user_id}")
        
        person = await people_collection.find_one({"email": user.get("email")})
        
        leader_path = person.get("LeaderPath", []) if person else []
        
        leader_at_1 = None
        leader_at_12 = None
        leader_at_144 = None
        
        if len(leader_path) > 0 and ObjectId.is_valid(leader_path[0]):
            leader_doc = await people_collection.find_one({"_id": ObjectId(leader_path[0])})
            if leader_doc:
                leader_at_1 = {
                    "id": str(leader_doc["_id"]),
                    "name": leader_doc.get("name", ""),
                    "surname": leader_doc.get("surname", ""),
                    "email": leader_doc.get("email", ""),
                    "phone_number": leader_doc.get("phone_number", "")
                }
        
        if len(leader_path) > 1 and ObjectId.is_valid(leader_path[1]):
            leader_doc = await people_collection.find_one({"_id": ObjectId(leader_path[1])})
            if leader_doc:
                leader_at_12 = {
                    "id": str(leader_doc["_id"]),
                    "name": leader_doc.get("name", ""),
                    "surname": leader_doc.get("surname", ""),
                    "email": leader_doc.get("email", ""),
                    "phone_number": leader_doc.get("phone_number", "")
                }
        
        if len(leader_path) > 2 and ObjectId.is_valid(leader_path[2]):
            leader_doc = await people_collection.find_one({"_id": ObjectId(leader_path[2])})
            if leader_doc:
                leader_at_144 = {
                    "id": str(leader_doc["_id"]),
                    "name": leader_doc.get("name", ""),
                    "surname": leader_doc.get("surname", ""),
                    "email": leader_doc.get("email", ""),
                    "phone_number": leader_doc.get("phone_number", "")
                }
        
        response_data = {
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
            "organization": user.get("Organization", user.get("organization", "")),
            "leader_path": leader_path,
            "leaders": {
                "leaderAt1": leader_at_1,
                "leaderAt12": leader_at_12,
                "leaderAt144": leader_at_144
            }
        }
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Profile fetch error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch profile: {str(e)}")
@app.put("/profile/{user_id}")
async def update_profile(
    user_id: str,
    profile_update: UserProfileUpdate,
    current_user: dict = Depends(get_current_user)
):
    try:
        token_user_id = current_user.get("user_id") or current_user.get("_id")
        if not token_user_id or str(token_user_id) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        existing_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")

        update_payload = {}
        
        if profile_update.name is not None:
            update_payload["name"] = profile_update.name
        if profile_update.surname is not None:
            update_payload["surname"] = profile_update.surname
        if profile_update.email is not None:
            update_payload["email"] = profile_update.email
        if profile_update.date_of_birth is not None:
            update_payload["date_of_birth"] = profile_update.date_of_birth
        if profile_update.home_address is not None:
            update_payload["home_address"] = profile_update.home_address
        if profile_update.phone_number is not None:
            update_payload["phone_number"] = profile_update.phone_number
        if profile_update.invited_by is not None:
            update_payload["invited_by"] = profile_update.invited_by
        if profile_update.gender is not None:
            update_payload["gender"] = profile_update.gender.capitalize()
        if profile_update.organization is not None:
            update_payload["organization"] = profile_update.organization
            update_payload["Organization"] = profile_update.organization
            update_payload["org_id"] = profile_update.organization.lower().replace(" ", "-")
        
        update_payload["updated_at"] = datetime.utcnow().isoformat()

        if not update_payload:
            return {
                "message": "No changes to update",
                "user": {
                    "id": str(existing_user["_id"]),
                    "name": existing_user.get("name", ""),
                    "surname": existing_user.get("surname", ""),
                    "email": existing_user.get("email", ""),
                    "organization": existing_user.get("organization", existing_user.get("Organization", "")),
                }
            }

        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": update_payload}
        )
        
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
            "organization": updated_user.get("organization", updated_user.get("Organization", "")),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Profile update error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to update profile: {str(e)}") 
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
        "organization": user.get("organization", ""),
    }

@app.get("/users")
async def get_users_by_organization(
    organization: Optional[str] = Query(None),
):
    """Get users filtered by organization - used by signup Invited By dropdown"""
    try:
        query = {}
        if organization:
            query["$or"] = [
                {"Organization": {"$regex": f"^{re.escape(organization)}$", "$options": "i"}},
                {"organization": {"$regex": f"^{re.escape(organization)}$", "$options": "i"}},
            ]
        
        cursor = users_collection.find(
            query,
            {"_id": 1, "name": 1, "surname": 1, "email": 1}
        ).limit(200)
        
        users = await cursor.to_list(length=200)
        
        formatted = []
        for user in users:
            full_name = f"{user.get('name', '')} {user.get('surname', '')}".strip()
            if full_name:
                formatted.append({
                    "_id": str(user["_id"]),
                    "name": user.get("name", ""),
                    "surname": user.get("surname", ""),
                    "email": user.get("email", ""),
                    "label": full_name,
                })
        
        return {"users": formatted, "total": len(formatted)}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")    

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
    perPage: int = Query(50, ge=1, le=200),  # Changed default to 50, max 200
    name: Optional[str] = None,
    gender: Optional[str] = None,
    dob: Optional[str] = None,
    location: Optional[str] = None,
    leader: Optional[str] = None,
    stage: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    try:
        org_id = (
            current_user.get("org_id") or
            current_user.get("organization", "").lower().replace(" ", "-") or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        # Build organization conditions
        org_conditions = [
            {"org_id": org_id},
            {"Org_id": {"$regex": f"^{re.escape(org_id)}$", "$options": "i"}},
        ]

        if organization:
            org_conditions.append({"Organization": {"$regex": re.escape(organization), "$options": "i"}})
            org_conditions.append({"Organisation": {"$regex": re.escape(organization), "$options": "i"}})
            org_id_from_name = organization.lower().replace(" ", "-")
            if org_id_from_name != org_id:
                org_conditions.append({"org_id": org_id_from_name})

        org_name_from_id = org_id.replace("-", " ")
        if org_name_from_id != org_id:
            org_conditions.append({"Organization": {"$regex": re.escape(org_name_from_id), "$options": "i"}})
            org_conditions.append({"Organisation": {"$regex": re.escape(org_name_from_id), "$options": "i"}})
            org_conditions.append({"org_id": {"$regex": re.escape(org_name_from_id), "$options": "i"}})

        if org_id == "active-teams":
            org_conditions.append({"Organisation": {"$regex": "active church", "$options": "i"}})
            org_conditions.append({"Organization": {"$regex": "active church", "$options": "i"}})
            org_conditions.append({"org_id": {"$exists": False}})

        query = {"$or": org_conditions}

        # Build search filters
        if name:
            name_parts = name.strip().split()
            name_conditions = []
            for part in name_parts:
                name_conditions.append({"Name": {"$regex": re.escape(part), "$options": "i"}})
                name_conditions.append({"Surname": {"$regex": re.escape(part), "$options": "i"}})
            query["$and"] = query.get("$and", [])
            query["$and"].append({"$or": name_conditions})

        if gender:
            query["Gender"] = {"$regex": re.escape(gender), "$options": "i"}

        if dob:
            query["Birthday"] = dob

        if location:
            query["Address"] = {"$regex": re.escape(location), "$options": "i"}

        if leader:
            leader_conditions = [
                {"Leader @1": {"$regex": re.escape(leader), "$options": "i"}},
                {"Leader @12": {"$regex": re.escape(leader), "$options": "i"}},
                {"Leader @144": {"$regex": re.escape(leader), "$options": "i"}},
                {"Leader @1728": {"$regex": re.escape(leader), "$options": "i"}}
            ]
            query["$and"] = query.get("$and", [])
            query["$and"].append({"$or": leader_conditions})

        if stage:
            query["Stage"] = {"$regex": re.escape(stage), "$options": "i"}

        # Get total count first (using count_documents which is fast)
        total_count = await people_collection.count_documents(query)
        
        # Calculate pagination
        skip = (page - 1) * perPage
        
        # Use aggregation for better performance
        pipeline = [
            {"$match": query},
            {"$skip": skip},
            {"$limit": perPage},
            {"$project": {
                "_id": 1,
                "Name": 1,
                "Surname": 1,
                "Number": 1,
                "Email": 1,
                "Address": 1,
                "Gender": 1,
                "Birthday": 1,
                "InvitedBy": 1,
                "Stage": 1,
                "org_id": 1,
                "Organization": 1,
                "LeaderId": 1,
                "LeaderPath": 1,
                "DateCreated": 1,
                "UpdatedAt": 1
            }}
        ]
        
        cursor = people_collection.aggregate(pipeline)
        people_list = []
        async for person in cursor:
            people_list.append(person)
        
        # Get leader names efficiently with a single query
        all_leader_ids = set()
        for person in people_list:
            leader_path = person.get("LeaderPath", [])
            for lid in leader_path:
                if lid:
                    try:
                        if isinstance(lid, ObjectId):
                            all_leader_ids.add(lid)
                        else:
                            all_leader_ids.add(ObjectId(str(lid)))
                    except Exception:
                        pass
        
        name_map = {}
        if all_leader_ids:
            # Only fetch the leaders we need, with a timeout
            try:
                leader_cursor = people_collection.find(
                    {"_id": {"$in": list(all_leader_ids)}},
                    {"_id": 1, "Name": 1, "Surname": 1}
                )
                async for leader_doc in leader_cursor:
                    name_map[leader_doc["_id"]] = f"{leader_doc.get('Name', '')} {leader_doc.get('Surname', '')}".strip()
            except Exception as e:
                print(f"Error fetching leaders: {e}")
        
        def resolve_leader(lid):
            if not lid:
                return ""
            try:
                if isinstance(lid, ObjectId):
                    return name_map.get(lid, "")
                return name_map.get(ObjectId(str(lid)), "")
            except Exception:
                return ""
        
        # Build final response
        final_list = []
        for person in people_list:
            leader_path = person.get("LeaderPath", [])
            leader1 = resolve_leader(leader_path[0]) if len(leader_path) > 0 else ""
            leader12 = resolve_leader(leader_path[1]) if len(leader_path) > 1 else ""
            leader144 = resolve_leader(leader_path[2]) if len(leader_path) > 2 else ""
            leader1728 = resolve_leader(leader_path[3]) if len(leader_path) > 3 else ""
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            
            mapped = {
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Number": person.get("Number", ""),
                "Email": person.get("Email", ""),
                "Address": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "Birthday": person.get("Birthday", ""),
                "InvitedBy": person.get("InvitedBy", ""),
                "Stage": person.get("Stage", "Win"),
                "org_id": person.get("org_id") or person.get("Org_id", ""),
                "Organization": person.get("Organization") or person.get("Organisation", ""),
                "LeaderId": str(person["LeaderId"]) if person.get("LeaderId") else "",
                "LeaderPath": [str(lid) for lid in leader_path],
                "Date Created": person.get("DateCreated") or person.get("Date Created") or datetime.utcnow().isoformat(),
                "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
                "Leader @1": leader1,
                "Leader @12": leader12,
                "Leader @144": leader144,
                "Leader @1728": leader1728,
                "FullName": full_name
            }
            final_list.append(mapped)
        
        return {
            "page": page,
            "perPage": perPage,
            "total": total_count,
            "total_pages": (total_count + perPage - 1) // perPage,
            "results": final_list
        }
        
    except Exception as e:
        print(f"Error in get_people: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching people: {str(e)}")
    
@app.get("/people/search")
async def search_people(
    query: str = Query("", min_length=2),
    limit: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(get_current_user)
):
    try:
        org_id = (
            current_user.get("org_id") or
            current_user.get("organization", "").lower().replace(" ", "-") or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = (current_user.get("Organization") or current_user.get("organization", "")).lower()
        org_name_from_id = org_id.replace("-", " ").lower()

        if not people_cache["data"]:
            return {"success": False, "error": "Cache not ready", "results": []}

        search_term = query.lower().strip()
        results = []

        for person in people_cache["data"]:
            person_org_id = (person.get("org_id") or person.get("Org_id") or "").lower()
            person_org_name = (person.get("Organization") or person.get("Organisation") or "").lower()
            person_org_name_as_id = person_org_name.replace(" ", "-")

            org_match = (
                org_id in person_org_id or
                person_org_id in org_id or
                (organization and organization in person_org_name) or
                (organization and person_org_name in organization) or
                (org_name_from_id and org_name_from_id in person_org_name) or
                (org_name_from_id and person_org_name in org_name_from_id) or
                person_org_name_as_id == org_id or
                (org_id == "active-teams" and "active" in person_org_name)
            )

            if not org_match:
                continue

            if (
                search_term in person.get("FullName", "").lower() or
                search_term in person.get("Email", "").lower() or
                search_term in person.get("Number", "") or
                search_term in person.get("Address", "").lower() or
                search_term in person.get("Stage", "").lower()
            ):
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
        return {"success": False, "error": str(e), "results": []}

@app.post("/people")
async def create_person(
    person_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        org_id = current_user.get("org_id") or (
            current_user.get("Organization", "").lower().replace(" ", "-")
        ) or "active-teams"
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")

        # ── Resolve LeaderPath ──────────────────────────────────────────
        leader_path: list = []
        leader_id_obj     = None

        raw_leader = (
            person_data.get("leaderId") or
            person_data.get("leader_id") or
            person_data.get("invitedById") or
            None
        )

        if raw_leader:
            try:
                leader_id_obj = ObjectId(str(raw_leader))
            except Exception:
                leader_id_obj = None

        # Always re-fetch inviter from DB to get their FULL LeaderPath.
        # LeaderPath is root-first: [root_id, ..., direct_parent_id]
        # New person's path = inviter's ancestors + inviter (inviter is their direct leader)
        if leader_id_obj:
            try:
                inviter_doc = await people_collection.find_one(
                    {"_id": leader_id_obj},
                    {"_id": 1, "LeaderPath": 1}
                )
                if inviter_doc:
                    inv_own_path = [
                        ObjectId(str(x)) for x in inviter_doc.get("LeaderPath", []) if x
                    ]
                    # Root-first order: [root, ..., inviter's_parent, inviter]
                    leader_path = inv_own_path + [leader_id_obj]
                else:
                    leader_path = [leader_id_obj]
            except Exception as e:
                print(f"Warning: could not fetch inviter LeaderPath: {e}")
                leader_path = [leader_id_obj]
        else:
            # Fallback: resolve by name if no ObjectId supplied
            if person_data.get("invitedBy"):
                inviter_name = person_data["invitedBy"].strip()
                if inviter_name:
                    parts = inviter_name.split()
                    first = parts[0] if parts else ""
                    last  = " ".join(parts[1:]) if len(parts) > 1 else ""

                    inviter_query = {
                        "Name": {"$regex": f"^{re.escape(first)}$", "$options": "i"}
                    }
                    if last:
                        inviter_query["Surname"] = {
                            "$regex": f"^{re.escape(last)}$", "$options": "i"
                        }

                    inviter = await people_collection.find_one(
                        inviter_query,
                        {"_id": 1, "LeaderPath": 1}
                    )
                    if inviter:
                        inv_id       = inviter["_id"]
                        inv_own_path = [
                            ObjectId(str(x)) for x in inviter.get("LeaderPath", []) if x
                        ]
                        # Root-first: ancestors + inviter
                        leader_path   = inv_own_path + [inv_id]
                        leader_id_obj = inv_id

        # ── Validate required fields ────────────────────────────────────
        name    = (person_data.get("name")    or "").strip()
        surname = (person_data.get("surname") or "").strip()
        email   = (person_data.get("email")   or "").strip().lower()

        if not name or not surname:
            raise HTTPException(status_code=400, detail="name and surname are required")

        if email:
            existing = await people_collection.find_one(
                {"Email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"A person with email '{email}' already exists."
                )

        now = datetime.utcnow()

        person_doc = {
            "Name":         name.title(),
            "Surname":      surname.title(),
            "Email":        email,
            "Number":       (person_data.get("number") or person_data.get("phone") or "").strip(),
            "Address":      (person_data.get("address") or "").strip(),
            "Gender":       (person_data.get("gender")  or "").strip().capitalize(),
            "Birthday":     (person_data.get("dob")     or "").replace("-", "/"),
            "InvitedBy":    (person_data.get("invitedBy") or "").strip(),
            "Stage":        (person_data.get("stage")   or "Win"),
            "LeaderId":     leader_id_obj,
            "LeaderPath":   leader_path,
            "org_id":       org_id,
            "Organization": organization,
            "DateCreated":  now.isoformat(),
            "UpdatedAt":    now.isoformat(),
        }

        result      = await people_collection.insert_one(person_doc)
        inserted_id = result.inserted_id

        # ── Resolve leaders[] for response ──────────────────────────────
        path_strs  = [str(lid) for lid in leader_path]
        id_to_full: dict = {}
        if leader_path:
            docs = await people_collection.find(
                {"_id": {"$in": leader_path}},
                {"_id": 1, "Name": 1, "Surname": 1, "Email": 1, "Number": 1}
            ).to_list(length=None)
            for d in docs:
                pid = str(d["_id"])
                id_to_full[pid] = {
                    "id":    pid,
                    "name":  f"{d.get('Name','')} {d.get('Surname','')}".strip(),
                    "email": d.get("Email", "") or "",
                    "phone": d.get("Number", "") or "",
                }

        leaders_array = resolve_leaders(path_strs, id_to_full)

        # ── Sync Users doc if account exists ────────────────────────────
        if email:
            try:
                await users_collection.update_one(
                    {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}},
                    {"$set": {
                        "LeaderId":   leader_id_obj,
                        "LeaderPath": leader_path,
                        "people_id":  str(inserted_id),
                        "updated_at": now.isoformat(),
                    }}
                )
            except Exception as sync_err:
                print(f"Warning: user sync failed for {email}: {sync_err}")

        asyncio.create_task(
            invalidate_people_cache("create", {"person_id": str(inserted_id)})
        )

        person_response = {
            "_id":          str(inserted_id),
            "Name":         person_doc["Name"],
            "Surname":      person_doc["Surname"],
            "Email":        person_doc["Email"],
            "Number":       person_doc["Number"],
            "Gender":       person_doc["Gender"],
            "Birthday":     person_doc["Birthday"],
            "Address":      person_doc["Address"],
            "InvitedBy":    person_doc["InvitedBy"],
            "Stage":        person_doc["Stage"],
            "org_id":       person_doc["org_id"],
            "Organization": person_doc["Organization"],
            "LeaderId":     str(leader_id_obj) if leader_id_obj else None,
            "LeaderPath":   path_strs,
            "leaders":      leaders_array,
            "DateCreated":  person_doc["DateCreated"],
            "UpdatedAt":    person_doc["UpdatedAt"],
            "FullName":     f"{person_doc['Name']} {person_doc['Surname']}".strip(),
        }

        return {
            "success": True,
            "message": "Person created successfully",
            "person":  person_response,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating person: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creating person: {str(e)}")
    
    
@app.get("/people/search-fast")
async def search_people_fast(
    query: str = Query(..., min_length=2),
    limit: int = Query(25, le=50),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not query or len(query) < 2:
            return {"results": []}

        # ── If cache is ready, search it directly (guaranteed correct org filtering) ──
        if people_cache.get("data"):
            org_id = (
                current_user.get("org_id") or
                current_user.get("organization", "").lower().replace(" ", "-") or
                "active-teams"
            )
            org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
            organization = (current_user.get("Organization") or current_user.get("organization", "")).lower()
            org_name_from_id = org_id.replace("-", " ").lower()

            search_term = query.lower().strip()
            results = []

            for person in people_cache["data"]:
                # Org filter — same logic as /people/search which works
                person_org_id   = (person.get("org_id") or person.get("Org_id") or "").lower()
                person_org_name = (person.get("Organization") or person.get("Organisation") or "").lower()
                person_org_name_as_id = person_org_name.replace(" ", "-")

                org_match = (
                    org_id in person_org_id or
                    person_org_id in org_id or
                    (organization and organization in person_org_name) or
                    (organization and person_org_name in organization) or
                    (org_name_from_id and org_name_from_id in person_org_name) or
                    (org_name_from_id and person_org_name in org_name_from_id) or
                    person_org_name_as_id == org_id or
                    (org_id == "active-teams" and "active" in person_org_name)
                )
                if not org_match:
                    continue

                # Name/email/phone search
                full_name = (person.get("FullName") or f"{person.get('Name','')} {person.get('Surname','')}").lower()
                if (
                    search_term in full_name or
                    search_term in (person.get("Email") or "").lower() or
                    search_term in (person.get("Number") or "")
                ):
                    results.append({
                        "_id":      str(person.get("_id", "")),
                        "Name":     person.get("Name", ""),
                        "Surname":  person.get("Surname", ""),
                        "Email":    person.get("Email", ""),
                        "Number":   person.get("Number", ""),
                        "FullName": person.get("FullName") or full_name.title(),
                    })

                if len(results) >= limit:
                    break

            return {"results": results}

        # ── Cache not ready — fall back to DB with loose org filter ──
        search_regex = {"$regex": query.strip(), "$options": "i"}
        text_q = {"$or": [
            {"Name": search_regex},
            {"Surname": search_regex},
            {"Email": search_regex},
            {"Number": search_regex},
            {"$expr": {"$regexMatch": {
                "input": {"$concat": ["$Name", " ", "$Surname"]},
                "regex": query.strip(), "options": "i"
            }}}
        ]}

        # Use same loose org matching as /people/search instead of build_org_query
        org_id = (
            current_user.get("org_id") or
            current_user.get("organization", "").lower().replace(" ", "-") or
            "active-teams"
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)
        organization = current_user.get("Organization") or current_user.get("organization", "")
        org_name_from_id = org_id.replace("-", " ")

        org_conditions = [
            {"org_id": org_id},
            {"Org_id": {"$regex": f"^{org_id}$", "$options": "i"}},
        ]
        if organization:
            org_conditions.append({"Organization": {"$regex": organization, "$options": "i"}})
            org_conditions.append({"Organisation": {"$regex": organization, "$options": "i"}})
        if org_name_from_id != org_id:
            org_conditions.append({"Organization": {"$regex": org_name_from_id, "$options": "i"}})
            org_conditions.append({"Organisation": {"$regex": org_name_from_id, "$options": "i"}})
        if org_id == "active-teams":
            org_conditions.append({"Organization": {"$regex": "active church", "$options": "i"}})
            org_conditions.append({"Organisation": {"$regex": "active church", "$options": "i"}})

        org_q   = {"$or": org_conditions}
        final_q = {"$and": [text_q, org_q]}

        projection = {"_id": 1, "Name": 1, "Surname": 1, "Email": 1, "Number": 1}
        docs = await people_collection.find(final_q, projection).limit(limit).to_list(length=limit)

        return {"results": [{
            "_id":      str(doc["_id"]),
            "Name":     doc.get("Name", ""),
            "Surname":  doc.get("Surname", ""),
            "Email":    doc.get("Email", ""),
            "Number":   doc.get("Number", ""),
            "FullName": f"{doc.get('Name', '')} {doc.get('Surname', '')}".strip(),
        } for doc in docs]}

    except Exception as e:
        print(f"Error in search-fast: {str(e)}")
        return {"results": [], "error": str(e)}
    
@app.get("/people/{person_id}")
async def get_person(
    person_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Return a single person with full leaders[] array resolved from LeaderPath."""
    try:
        if not ObjectId.is_valid(person_id):
            raise HTTPException(status_code=400, detail="Invalid person ID")
 
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
 
        id_to_full = await _build_full_id_map_from_db()
        return transform_person_full(person, id_to_full=id_to_full)
 
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/people/{person_id}")
async def update_person(
    person_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(person_id):
            raise HTTPException(status_code=400, detail="Invalid person ID")

        existing = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not existing:
            raise HTTPException(status_code=404, detail="Person not found")

        now = datetime.utcnow()
        set_fields: dict = {"UpdatedAt": now.isoformat()}

        # ── Standard scalar fields ──────────────────────────────────────
        field_map = {
            "name":      ("Name",      lambda v: v.strip().title()),
            "surname":   ("Surname",   lambda v: v.strip().title()),
            "email":     ("Email",     lambda v: v.strip().lower()),
            "number":    ("Number",    lambda v: v.strip()),
            "phone":     ("Number",    lambda v: v.strip()),
            "address":   ("Address",   lambda v: v.strip()),
            "gender":    ("Gender",    lambda v: v.strip().capitalize()),
            "dob":       ("Birthday",  lambda v: v.replace("-", "/")),
            "invitedBy": ("InvitedBy", lambda v: v.strip()),
            "stage":     ("Stage",     lambda v: v),
            "Stage":     ("Stage",     lambda v: v),  # drag-drop sends capital S
        }
        for src_key, (dest_key, transform) in field_map.items():
            if src_key in update_data and update_data[src_key] is not None:
                set_fields[dest_key] = transform(str(update_data[src_key]))

        # ── LeaderPath / LeaderId ───────────────────────────────────────
        raw_leader = (
            update_data.get("leaderId")    or
            update_data.get("leader_id")   or
            update_data.get("invitedById") or
            None
        )

        new_leader_path = []
        new_leader_id   = None

        if raw_leader:
            try:
                new_leader_id = ObjectId(str(raw_leader))
            except Exception:
                pass

        if new_leader_id:
            try:
                inviter_doc = await people_collection.find_one(
                    {"_id": new_leader_id},
                    {"_id": 1, "LeaderPath": 1}
                )
                if inviter_doc:
                    inv_path = [
                        ObjectId(str(x)) for x in inviter_doc.get("LeaderPath", []) if x
                    ]
                    new_leader_path = inv_path + [new_leader_id]
                else:
                    new_leader_path = [new_leader_id]
            except Exception as e:
                print(f"Warning: could not fetch inviter LeaderPath on update: {e}")
                new_leader_path = [new_leader_id]

        elif "invitedBy" in update_data and update_data["invitedBy"]:
            inviter_name = update_data["invitedBy"].strip()
            parts = inviter_name.split()
            first = parts[0] if parts else ""
            last  = " ".join(parts[1:]) if len(parts) > 1 else ""
            inviter_query = {"Name": {"$regex": f"^{re.escape(first)}$", "$options": "i"}}
            if last:
                inviter_query["Surname"] = {
                    "$regex": f"^{re.escape(last)}$", "$options": "i"
                }
            inviter = await people_collection.find_one(
                inviter_query, {"_id": 1, "LeaderPath": 1}
            )
            if inviter:
                inv_id   = inviter["_id"]
                inv_path = [ObjectId(str(x)) for x in inviter.get("LeaderPath", []) if x]
                new_leader_path = inv_path + [inv_id]
                new_leader_id   = inv_id

        if new_leader_path:
            set_fields["LeaderPath"] = new_leader_path
        if new_leader_id:
            set_fields["LeaderId"] = new_leader_id

        # ── Write to DB ─────────────────────────────────────────────────
        await people_collection.update_one(
            {"_id": ObjectId(person_id)},
            {"$set": set_fields}
        )

        # ── Build response without a full DB scan ───────────────────────
        # Re-fetch only this one document instead of calling
        # _build_full_id_map_from_db() which scans the entire collection.
        updated = await people_collection.find_one({"_id": ObjectId(person_id)})

        # Resolve leader names for just the leaders in this person's path
        path_ids = [
            ObjectId(str(x)) for x in updated.get("LeaderPath", []) if x
        ]
        id_to_full: dict = {}
        if path_ids:
            async for ldoc in people_collection.find(
                {"_id": {"$in": path_ids}},
                {"_id": 1, "Name": 1, "Surname": 1, "Email": 1, "Number": 1}
            ):
                pid = str(ldoc["_id"])
                id_to_full[pid] = {
                    "id":    pid,
                    "name":  f"{ldoc.get('Name', '')} {ldoc.get('Surname', '')}".strip(),
                    "email": ldoc.get("Email", "") or "",
                    "phone": ldoc.get("Number", "") or "",
                }

        person_out = transform_person_full(updated, id_to_full=id_to_full)

        # ── Surgical in-memory cache update — no full refresh ───────────
        # Only touch the one record that changed. This is instant and
        # avoids triggering a full background reload on every PATCH.
        if people_cache.get("data"):
            for i, p in enumerate(people_cache["data"]):
                if str(p.get("_id")) == person_id:
                    # Apply every changed DB field directly onto the cached doc
                    for db_field, new_val in set_fields.items():
                        people_cache["data"][i][db_field] = new_val
                    # Keep the resolved leaders array in sync too
                    people_cache["data"][i]["leaders"] = person_out.get("leaders", [])
                    break

        return {
            "success": True,
            "message": "Person updated successfully",
            "person":  person_out,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating person {person_id}: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating person: {str(e)}")

@app.delete("/people/{person_id}")
async def delete_person(
    person_id: str,
    current_user: dict = Depends(get_current_user)
):
    try:
        if not ObjectId.is_valid(person_id):
            raise HTTPException(status_code=400, detail="Invalid person ID")
 
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
 
        asyncio.create_task(
            invalidate_people_cache("delete", {"person_id": person_id})
        )
        return {"success": True, "message": "Person deleted successfully"}
 
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

# ====================== POST /tasks ======================

from fastapi.encoders import jsonable_encoder

@app.post("/tasks")
async def create_task(task: TaskModel, current_user: dict = Depends(get_current_user)):
    try:
        organization = None
        for key in current_user.keys():
            if key.lower() == "organization":
                organization = current_user[key]
                break

        new_task_dict = task.dict()
        
        # Only set assignedfor if not already provided by frontend
        if new_task_dict.get("assignedfor"):
            new_task_dict["assignedfor"] = new_task_dict["assignedfor"].lower()
        else:
            new_task_dict["assignedfor"] = current_user["email"].lower()

        if not new_task_dict.get("assigned_to_email"):
            new_task_dict["assigned_to_email"] = new_task_dict["assignedfor"]

        # Always track creator
        new_task_dict["created_by_email"] = current_user["email"].lower()
        new_task_dict["created_by_name"] = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        new_task_dict["createdAt"] = datetime.utcnow()
        new_task_dict["Organization"] = organization

        result = await db["tasks"].insert_one(new_task_dict)
        new_task_dict["_id"] = str(result.inserted_id)
        return {"status": "success", "task": jsonable_encoder(new_task_dict)}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ====================== GET /tasks ======================

@app.get("/tasks")
async def get_user_tasks(
    email: str = Query(None),
    userId: str = Query(None),
    view_all: bool = Query(False),
    current_user: dict = Depends(get_current_user)
):
    try:
        # === ROBUST ORGANIZATION LOOKUP (ignores case - same as POST) ===
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        if not org_name:
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        is_super_admin = current_user.get("role") == "super_admin"
        is_leader = current_user.get("role") in ["admin", "leader", "manager", "org_admin"]
        if email:
            user_email = email.lower()
        elif userId:
            user = await users_collection.find_one({"_id": ObjectId(userId)})
            if user:
                user_email = user.get("email", "").lower()
        else:
            user_email = current_user.get("email", "").lower()

        if not user_email and not (is_leader and view_all):
            return {"error": "User email not found", "status": "failed"}
        
        if is_super_admin and view_all:
            query = {}                                     
        elif is_leader and view_all:
            query = {"Organization": org_name}       
        else:
            query = {
                "$or": [
                    {"assignedfor": user_email},
                    {"assigned_to_email": user_email},
                    {
                        "$and": [
                            {"leader_name": user_name},
                            {"is_consolidation_task": True}
                        ]
                    },
                    {
                        "$and": [
                            {"leader_assigned": user_name},
                            {"is_consolidation_task": True}
                        ]
                    }
                ]
            }

        # Build leader full name (kept from your original)
        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()
        timezone = pytz.timezone("Africa/Johannesburg")

        cursor = tasks_collection.find(query).sort("followup_date", -1).limit(500)
        all_tasks = []

        async for task in cursor:
            task_date_str = task.get("followup_date")
            task_datetime = None
            if task_date_str:
                if isinstance(task_date_str, datetime):
                    task_datetime = task_date_str.astimezone(timezone)
                else:
                    try:
                        task_datetime = datetime.fromisoformat(
                            str(task_date_str).replace("Z", "+00:00")
                        ).astimezone(timezone)
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
                "assigned_to_email": task.get("assigned_to_email", ""),
                "created_by_email": task.get("created_by_email", ""),
                "leader_name": task.get("leader_name", ""),
                "type": task.get("type", "call"),
                "contacted_person": task.get("contacted_person", {}),
                "isRecurring": bool(task.get("recurring_day")),
                "is_consolidation_task": bool(task.get("is_consolidation_task")),
                "consolidation_source": task.get("consolidation_source", "manual"),
                "source_display": task.get("source_display", "Manual")
            })

        # Sort newest first
        all_tasks.sort(key=lambda t: t["followup_date"] or "", reverse=True)

        return {
            "user_email": "all_users" if (is_leader and view_all) else current_user.get("email"),
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "status": "success",
            "is_leader_view": is_leader and view_all,
            "Organization": org_name
        }

    except Exception as e:
        logging.error(f"Error in get_user_tasks: {e}")
        return {"error": str(e), "status": "failed"}

# ====================== GET /tasktypes (NOW FETCHES BY ORGANIZATION) ======================

@app.get("/tasktypes", response_model=List[TaskTypeOut])
async def get_task_types(current_user: dict = Depends(get_current_user)):
    try:
        # === ROBUST ORGANIZATION LOOKUP (ignores case - same as POST) ===
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        is_super_admin = current_user.get("role") == "super_admin"

        # Multi-tenant filter - exactly like /tasks
        query = {} if is_super_admin else {"Organization": org_name}

        cursor = tasktypes_collection.find(query).sort("name", 1)
        types = []
        async for t in cursor:
            types.append(task_type_serializer(t))
        return types
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ====================== POST /tasktypes (CREATES WITH ORGANIZATION) ======================

@app.post("/tasktypes", response_model=TaskTypeOut)
async def create_task_type(task: TaskTypeIn, current_user: dict = Depends(get_current_user)):
    try:
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can create task types.")

        # === ROBUST ORGANIZATION LOOKUP (ignores case - same as POST) ===
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")
        existing = await tasktypes_collection.find_one({
            "name": task.name,
            "Organization": org_name
        })
        if existing:
            raise HTTPException(status_code=400, detail="Task type already exists in this organization.")
        new_task = {
            "name": task.name,
            "Organization": org_name
        }
        result = await tasktypes_collection.insert_one(new_task)
        created = await tasktypes_collection.find_one({"_id": result.inserted_id})

        return task_type_serializer(created)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# ====================== Helper (keep exactly as before) ======================
def serialize_doc(doc):
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

# ====================== PUT /tasktypes/{tasktype_id} ======================
@app.put("/tasktypes/{tasktype_id}")
async def update_task_type(
    tasktype_id: str,
    update_data: TaskTypeUpdate,         
    current_user: dict = Depends(get_current_user)
):
    try:
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can edit task types.")

        # === ROBUST ORGANIZATION LOOKUP (ignores case - same as POST) ===
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        try:
            oid = ObjectId(tasktype_id)
        except:
            raise HTTPException(status_code=400, detail="Invalid task type ID")

        # Check ownership
        existing = await tasktypes_collection.find_one({"_id": oid})
        if not existing:
            raise HTTPException(status_code=404, detail="Task type not found")

        # Cross-tenant protection
        if existing.get("Organization") != org_name and current_user.get("role") != "super_admin":
            raise HTTPException(
                status_code=403,
                detail="You don't have access to this church's data."
            )

        # Update
        updated = await tasktypes_collection.find_one_and_update(
            {"_id": oid},
            {"$set": {"name": update_data.name.strip()}},
            return_document=True
        )

        if not updated:
            raise HTTPException(status_code=404, detail="Task type not found")

        updated["_id"] = str(updated["_id"])
        return {"message": "Task type updated", "taskType": updated}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ====================== DELETE /tasktypes/{tasktype_id} ======================

@app.delete("/tasktypes/{tasktype_id}")
async def delete_task_type(
    tasktype_id: str,
    current_user: dict = Depends(get_current_user)
):
    try:
        # Admin-only check
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can delete task types.")

        # === ROBUST ORGANIZATION LOOKUP (ignores case - same as POST) ===
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        try:
            oid = ObjectId(tasktype_id)
        except:
            raise HTTPException(status_code=400, detail="Invalid task type ID")

        # Check ownership
        existing = await tasktypes_collection.find_one({"_id": oid})
        if not existing:
            raise HTTPException(status_code=404, detail="Task type not found")

        # Cross-tenant protection
        if existing.get("Organization") != org_name and current_user.get("role") != "super_admin":
            raise HTTPException(
                status_code=403,
                detail="You don't have access to this church's data."
            )

        deleted = await tasktypes_collection.find_one_and_delete({"_id": oid})
        if not deleted:
            raise HTTPException(status_code=404, detail="Task type not found")

        return {"message": "Task type deleted successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ====================== PUT /taskS ======================

# ====================== PUT /tasks ======================

@app.put("/tasks/{task_id}")
async def update_task(
    task_id: str,
    updated_task: dict,
    current_user: dict = Depends(get_current_user)
):
    try:
        # Extract organization name from current user
        org_name = None
        for key in current_user.keys():
            if key.lower() == "organization":
                org_name = current_user[key]
                break

        obj_id = ObjectId(task_id)
        task = await db["tasks"].find_one({"_id": obj_id})
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        # === CROSS-TENANT PROTECTION ===
        task_org = task.get("Organization")
        if (
            task_org
            and task_org.lower() != org_name.lower()
            and current_user.get("role") != "super_admin"
        ):
            raise HTTPException(
                status_code=403,
                detail="You don't have access to this church's data."
            )

        # Prepare update data
        update_data = {}

        if "name" in updated_task:
            update_data["name"] = updated_task["name"]

        if "taskType" in updated_task:
            update_data["taskType"] = updated_task["taskType"]

        if "contacted_person" in updated_task:
            update_data["contacted_person"] = updated_task["contacted_person"]

        if "followup_date" in updated_task:
            try:
                update_data["followup_date"] = updated_task["followup_date"]
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")

        if "status" in updated_task:
            # Always normalize status to lowercase
            normalized_status = updated_task["status"].lower()
            update_data["status"] = normalized_status

            if normalized_status in ["completed", "done", "closed", "finished"]:
                update_data["completedAt"] = datetime.utcnow()
            elif normalized_status in ["open", "pending", "incomplete"]:
                update_data["completedAt"] = None

        if "type" in updated_task:
            update_data["type"] = updated_task["type"]

        if "assignedfor" in updated_task:
            # Always normalize assignedfor to lowercase
            update_data["assignedfor"] = updated_task["assignedfor"].lower()

        if "assigned_to_email" in updated_task:
            update_data["assigned_to_email"] = updated_task["assigned_to_email"].lower()

        # Add updated timestamp
        update_data["updated_at"] = datetime.utcnow().isoformat()

        # Perform update
        result = await db["tasks"].update_one(
            {"_id": obj_id},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            if result.matched_count > 0:
                updated_task_in_db = await db["tasks"].find_one({"_id": obj_id})
                return {"updatedTask": serialize_doc(updated_task_in_db)}
            else:
                raise HTTPException(status_code=404, detail="Task not found")

        updated_task_in_db = await db["tasks"].find_one({"_id": obj_id})
        return {"updatedTask": serialize_doc(updated_task_in_db)}

    except Exception as e:
        print(f"Error updating task: {str(e)}")
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
        outstanding_cells = await events_collection.count_documents({
            "eventType": "Cell",
            "status": {"$nin": ["completed", "closed", "done"]}
        })
        outstanding_tasks = await tasks_collection.count_documents({
            "status": {"$nin": ["completed", "closed", "done"]}
        })
       
        total_people = await people_collection.count_documents({})
       
        period_events = await events_collection.find({
            "date": {"$gte": start_date, "$lt": end_date},
            "status": {"$in": ["completed", "closed"]},
            "eventType": {"$ne": "Cell"} 
        }).to_list(length=None)
       
        total_attendance = sum(event.get("total_attendance", 0) for event in period_events)
       
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
                    # aligning weekly breakdowns with date-based attendance keys (fixes mismatch bug).
                    week_start = event_date.date() - timedelta(days=event_date.weekday())  
                    key = week_start.strftime("%Y-%m-%d")  
               
                if key not in attendance_breakdown:
                    attendance_breakdown[key] = 0
                attendance_breakdown[key] += event.get("total_attendance", 0)
       
        return {
            "outstanding_cells": outstanding_cells,
            "outstanding_tasks": outstanding_tasks,  
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
       
        pipeline = [
            {
                "$match": {
                    "captured_by": {"$exists": True, "$ne": None} 
                }
            },
            {
                "$group": {
                    "_id": "$captured_by", 
                    "people_captured_count": {"$sum": 1},
                    "captured_people": {
                        "$push": {
                            "name": "$fullName",
                            "email": "$email",
                            "capture_date": "$created_date" 
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
                "$sort": {"people_captured_count": -1} 
            }
        ]
       
        results = list(db.people.aggregate(pipeline))  
       
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
    """Create a new user - Admin only (uses lowercase 'organization' for new users)"""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
    
    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        existing_user = await users_collection.find_one({"email": user_data.email})
        if existing_user:
            raise HTTPException(status_code=400, detail="User with this email already exists")
        
        # Validate role
        valid_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]
        if user_data.role not in valid_roles:
            raise HTTPException(status_code=400, detail="Invalid role")
        
        # Regular admins cannot create other admins
        if not is_supreme and user_data.role == "admin":
            raise HTTPException(status_code=403, detail="Cannot create admin users")
        
        from passlib.context import CryptContext
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        hashed_password = pwd_context.hash(user_data.password)
        
        # Use lowercase 'organization' for all new users (consistency)
        user_doc = {
            "name": user_data.name,
            "surname": user_data.surname,
            "email": user_data.email,
            "password": hashed_password,
            "phone_number": user_data.phone_number,
            "date_of_birth": user_data.date_of_birth.isoformat() if user_data.date_of_birth else None,
            "home_address": user_data.address,
            "gender": user_data.gender,
            "invited_by": user_data.invitedBy,
            "leader12": user_data.leader12,
            "leader144": user_data.leader144,
            "leader1728": user_data.leader1728,
            "stage": user_data.stage or "Win",
            "role": user_data.role,
            "Organization": current_user.get("Organization"),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = await users_collection.insert_one(user_doc)
        
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
SUPREME_ADMIN_EMAIL = "tkgenia1234@gmail.com"

ROLE_HIERARCHY = {
    "registrant":2,
    "user": 1,
    "leader": 3,
    "leaderAt12": 4,
    "admin": 5,
    "supreme_admin": 6
}

@app.get("/admin/users", response_model=UserList)
async def get_all_users(
    organization: Optional[str] = Query(None, description="Filter by organization"),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=5000),
    current_user: dict = Depends(get_current_user)
):
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL or current_user.get("is_supreme_admin", False)
        
        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        
        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            # Use Organization field with capital O to match your database
            org_filter = current_user.get("Organization")
        
        query = {}
        if org_filter:
            query["Organization"] = org_filter  # Capital O to match your document
        
        # Get total count
        total = await users_collection.count_documents(query)
        print(f"Total users in database for this org: {total}")
        
        # Get paginated results
        cursor = users_collection.find(
            query,
            {
                "_id": 1,
                "name": 1,
                "surname": 1,
                "email": 1,
                "role": 1,
                "phone_number": 1,
                "Organization": 1,  # Capital O
                "created_at": 1,
                "updated_at": 1,
                "date_of_birth": 1,
                "home_address": 1,  # Changed from address
                "gender": 1,
                "invited_by": 1,    # Changed from invitedBy
                "leader12": 1,
                "leader144": 1,
                "leader1728": 1,
                "stage": 1
            }
        ).skip(skip).limit(limit).sort("created_at", -1)
        
        users_raw = await cursor.to_list(length=limit)
        
        users = []
        for user in users_raw:
            users.append({
                "id": str(user["_id"]),
                "name": user.get("name", ""),
                "surname": user.get("surname", ""),
                "email": user.get("email", ""),
                "role": user.get("role", "user"),
                "phone_number": user.get("phone_number"),
                "organization": user.get("Organization") or "Unknown",  # Map to organization for frontend
                "created_at": user.get("created_at") or user.get("updated_at"),
                "date_of_birth": user.get("date_of_birth"),
                "address": user.get("home_address"),  # Map home_address to address for frontend
                "gender": user.get("gender"),
                "invitedBy": user.get("invited_by"),  # Map invited_by to invitedBy for frontend
                "leader12": user.get("leader12"),
                "leader144": user.get("leader144"),
                "leader1728": user.get("leader1728"),
                "stage": user.get("stage")
            })
        
        print(f"Returning {len(users)} users, total in DB: {total}")
        
        return {
            "users": users,
            "total": total,
            "skip": skip,
            "limit": limit
        }
        
    except Exception as e:
        print(f"ERROR in get_all_users: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/stats")
async def get_admin_stats(
    organization: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
        
        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        
        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            org_filter = current_user.get("Organization") or current_user.get("organization")
        
        query = {}
        if org_filter:
            query["Organization"] = org_filter
        
        pipeline = [
            {"$match": query},
            {"$group": {
                "_id": "$role",
                "count": {"$sum": 1}
            }}
        ]
        
        results = await users_collection.aggregate(pipeline).to_list(length=100)
        
        stats = {
            "total_users": 0,
            "administrators": 0,
            "leaders": 0,
            "leaders_at_12": 0,
            "registrants": 0,
            "regular_users": 0,
            "custom_roles": {}
        }
        
        for item in results:
            role = item["_id"] or "unknown"
            count = item["count"]
            stats["total_users"] += count
            
            if role == "admin":
                stats["administrators"] = count
            elif role == "leader":
                stats["leaders"] = count
            elif role == "leaderAt12":
                stats["leaders_at_12"] = count
            elif role == "registrant":
                stats["registrants"] = count
            elif role == "user":
                stats["regular_users"] = count
            else:
                stats["custom_roles"][role] = count
        
        distinct_roles = await users_collection.distinct("role", query)
        stats["all_roles"] = [r for r in distinct_roles if r]
        
        return stats
        
    except Exception as e:
        print(f"Error fetching stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

@app.get("/admin/roles/distinct")
async def get_distinct_roles(
    organization: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
        
        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            org_filter = current_user.get("Organization") or current_user.get("organization")
        else:
            raise HTTPException(status_code=400, detail="Organization required")
        
        ACTIVE_CHURCH_NAME = "Active Church"
        system_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]
        
        query = {}
        if org_filter:
            query["Organization"] = org_filter
        
        pipeline = [
            {"$match": query},
            {"$group": {
                "_id": "$role",
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results = await users_collection.aggregate(pipeline).to_list(length=100)
        
        roles_with_counts = []
        for item in results:
            role = item["_id"]
            if not role:
                continue
                
            count = item["count"]
            is_system = role in system_roles
            
            if org_filter == ACTIVE_CHURCH_NAME and not is_system:
                continue
                
            color = get_role_color(role)
            
            roles_with_counts.append({
                "name": role,
                "count": count,
                "is_system": is_system,
                "color": color,
                "can_create_custom": org_filter != ACTIVE_CHURCH_NAME
            })
        
        roles_with_counts.sort(key=lambda x: (not x["is_system"], x["name"]))
        
        return {
            "roles": roles_with_counts,
            "organization": org_filter,
            "can_create_custom_roles": org_filter != ACTIVE_CHURCH_NAME
        }
        
    except Exception as e:
        print(f"Error fetching distinct roles: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# Helper function for role colors
def get_role_color(role):
    role_colors = {
        "admin": "#f44336",
        "leader": "#2196f3",
        "leaderAt12": "#9c27b0", 
        "user": "#4caf50",
        "registrant": "#ff9800"
    }
    return role_colors.get(role, "#9c27b0") 

@app.put("/admin/users/{user_id}/role", response_model=MessageResponse)
async def update_user_role(
    user_id: str,
    role_update: RoleUpdate,
    current_user: dict = Depends(get_current_user)
):
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL or current_user.get("is_supreme_admin", False)
        
        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if not is_supreme:
            user_org = user.get("Organization")
            current_user_org = current_user.get("Organization")
            
            if user_org != current_user_org:
                raise HTTPException(
                    status_code=403,
                    detail=f"Cannot access users from other organizations. Your org: {current_user_org}, Target org: {user_org}"
                )
        
        old_role = user.get("role", "user")
        new_role = role_update.role
        
        user_org = user.get("Organization")
        ACTIVE_CHURCH_NAME = "Active Church"
        system_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]
        
        if user_org == ACTIVE_CHURCH_NAME:
            if new_role not in system_roles:
                raise HTTPException(
                    status_code=400,
                    detail=f"Active Church only supports standard roles: {', '.join(system_roles)}"
                )
            
            ROLE_HIERARCHY = {
                "registrant": 2,
                "user": 1,
                "leader": 3,
                "leaderAt12": 4,
                "admin": 5,
                "supreme_admin": 6
            }
            
            if not is_supreme:
                current_user_role_level = ROLE_HIERARCHY.get(current_user.get("role"), 0)
                target_user_level = ROLE_HIERARCHY.get(old_role, 0)
                new_role_level = ROLE_HIERARCHY.get(new_role, 0)
                
                if target_user_level >= current_user_role_level:
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot modify users with equal or higher role"
                    )
                
                if new_role_level >= current_user_role_level:
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot assign role equal to or higher than your own"
                    )
        else:
            if new_role == "admin" and not is_supreme:
                raise HTTPException(
                    status_code=403,
                    detail="Cannot assign admin role"
                )
            
            if new_role in system_roles and not is_supreme:
                ROLE_HIERARCHY = {
                    "registrant": 2,
                    "user": 1,
                    "leader": 3,
                    "leaderAt12": 4,
                    "admin": 5
                }
                current_user_role_level = ROLE_HIERARCHY.get(current_user.get("role"), 0)
                new_role_level = ROLE_HIERARCHY.get(new_role, 0)
                
                if new_role_level >= current_user_role_level:
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot assign system role equal to or higher than your own"
                    )
        
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {
                "$set": {
                    "role": new_role,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=400, detail="Failed to update user role")
        
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="ROLE_UPDATED",
            details=f"Updated {user.get('name')} {user.get('surname')}'s role from {old_role} to {new_role}"
        )
        
        return MessageResponse(message=f"User role updated to {new_role}")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating role: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating role: {str(e)}")

@app.delete("/admin/users/{user_id}", response_model=MessageResponse)
async def delete_user(
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a user - Admin only"""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
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


async def create_indexes():
    """Create database indexes for better performance"""
    try:
        # Index for Organization field (used in most queries) - matches your document structure
        await users_collection.create_index("Organization")
        print("✓ Index created on Organization field")
        
        # Compound index for Organization + role (used in role queries)
        await users_collection.create_index([("Organization", 1), ("role", 1)])
        print("✓ Index created on Organization + role fields")
        
        # Index for created_at for sorting
        await users_collection.create_index("created_at")
        print("✓ Index created on created_at field")
        
        # Index for email for login queries
        await users_collection.create_index("email", unique=True)
        print("✓ Index created on email field")
        
        # Index for refresh_token_id for token management
        await users_collection.create_index("refresh_token_id")
        print("✓ Index created on refresh_token_id field")
        
        # Index for _id (already exists by default, but including for completeness)
        print("✓ All indexes created successfully")

        await people_collection.create_index([("Name", 1)])
        await people_collection.create_index([("Surname", 1)])
        await people_collection.create_index([("Email", 1)])
        # Text index for full-text search (fastest for name searches)
        await people_collection.create_index([
            ("Name", "text"), 
            ("Surname", "text"),
            ("Email", "text")
        ], name="people_text_index")
        # Tasks indexes
        await db["tasks"].create_index([("assignedfor", 1)])
        await db["tasks"].create_index([("assigned_to_email", 1)])
        await db["tasks"].create_index([("Organization", 1)])
        await db["tasks"].create_index([("Organization", 1), ("assignedfor", 1)])
        
        # Task types index
        await tasktypes_collection.create_index([("Organization", 1)])

    except Exception as e:
        print(f"✗ Error creating indexes: {e}")

# Migration function to standardize field names (run once)
async def migrate_user_fields():
    """Migrate existing users to use consistent field names"""
    try:
        # Check if we have any users with lowercase 'organization' field
        lowercase_count = await users_collection.count_documents({"organization": {"$exists": True}})
        if lowercase_count > 0:
            # Rename lowercase organization to uppercase Organization
            result = await users_collection.update_many(
                {"organization": {"$exists": True}, "Organization": {"$exists": False}},
                {"$rename": {"organization": "Organization"}}
            )
            print(f"✓ Migrated {result.modified_count} users: organization -> Organization")
        
        # Check for address field to rename to home_address
        address_count = await users_collection.count_documents({"address": {"$exists": True}})
        if address_count > 0:
            result = await users_collection.update_many(
                {"address": {"$exists": True}, "home_address": {"$exists": False}},
                {"$rename": {"address": "home_address"}}
            )
            print(f"✓ Migrated {result.modified_count} users: address -> home_address")
        
        # Check for invitedBy field to rename to invited_by
        invited_count = await users_collection.count_documents({"invitedBy": {"$exists": True}})
        if invited_count > 0:
            result = await users_collection.update_many(
                {"invitedBy": {"$exists": True}, "invited_by": {"$exists": False}},
                {"$rename": {"invitedBy": "invited_by"}}
            )
            print(f"✓ Migrated {result.modified_count} users: invitedBy -> invited_by")
        
        # Remove duplicate organization field if both exist
        duplicate_count = await users_collection.count_documents({
            "organization": {"$exists": True}, 
            "Organization": {"$exists": True}
        })
        if duplicate_count > 0:
            result = await users_collection.update_many(
                {"organization": {"$exists": True}, "Organization": {"$exists": True}},
                {"$unset": {"organization": ""}}
            )
            print(f"✓ Removed duplicate organization field from {result.modified_count} users")
        
        print("✓ Field migration completed")
        
    except Exception as e:
        print(f"✗ Error during migration: {e}")

# Startup event
@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    print("=" * 50)
    print("Starting up application...")
    print(f"Database: {DB_NAME}")
    print("=" * 50)

    # First migrate existing data to consistent format (optional, can be removed after first run)
    await migrate_user_fields()
    
    # Then create indexes for performance
    await create_indexes()
    
    print("=" * 50)
    print("Application startup complete")
    print("=" * 50)
    """Run on application startup"""
    print("Starting up application...")
    
    print("Application startup complete")

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
   
# GETTING  ORGS FOR ADMIN USER 
@app.get("/admin/organizations", response_model=OrganizationList)
async def get_all_organizations(
    current_user: dict = Depends(get_current_user)
):
    """Get all organizations - Supreme Admin only"""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")
    
    try:
        organizations = []
        cursor = organizations_collection.find({})
        
        async for org in cursor:
            # Get user count for this organization
            user_count = await users_collection.count_documents({"organization": org["name"]})
            people_count = await people_collection.count_documents({"Organisation": org["name"]})
            
            organizations.append(OrganizationResponse(
                id=str(org["_id"]),
                name=org.get("name"),
                address=org.get("address"),
                phone=org.get("phone"),
                email=org.get("email"),
                user_count=user_count + people_count,
                created_at=org.get("created_at")
            ))
        
        return OrganizationList(organizations=organizations)
        
    except Exception as e:
        print(f"Error fetching organizations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching organizations: {str(e)}")

# ===== 6. NEW: POST /admin/organizations =====
@app.post("/admin/organizations", response_model=MessageResponse)
async def create_organization(
    org_data: OrganizationCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new organization - Supreme Admin only"""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")
    
    try:
        # Check if organization already exists
        existing_org = await organizations_collection.find_one({"name": org_data.name})
        if existing_org:
            raise HTTPException(status_code=400, detail="Organization already exists")
        
        org_doc = {
            "name": org_data.name,
            "address": org_data.address,
            "phone": org_data.phone,
            "email": org_data.email,
            "created_at": datetime.utcnow(),
            "created_by": str(current_user.get("_id"))
        }
        
        result = await organizations_collection.insert_one(org_doc)
        
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="ORGANIZATION_CREATED",
            details=f"Created new organization: {org_data.name}"
        )
        
        return MessageResponse(message=f"Organization {org_data.name} created successfully")
        
    except Exception as e:
        print(f"Error creating organization: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating organization: {str(e)}")

# ===== 7. NEW: PUT /admin/organizations/{org_id} =====
@app.put("/admin/organizations/{org_id}", response_model=MessageResponse)
async def update_organization(
    org_id: str,
    org_data: OrganizationUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update organization - Supreme Admin only"""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")
    
    try:
        org = await organizations_collection.find_one({"_id": ObjectId(org_id)})
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        
        update_data = {k: v for k, v in org_data.dict().items() if v is not None}
        update_data["updated_at"] = datetime.utcnow()
        
        result = await organizations_collection.update_one(
            {"_id": ObjectId(org_id)},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=400, detail="Failed to update organization")
        
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="ORGANIZATION_UPDATED",
            details=f"Updated organization: {org['name']}"
        )
        
        return MessageResponse(message="Organization updated successfully")
        
    except Exception as e:
        print(f"Error updating organization: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating organization: {str(e)}")

# ===== 8. NEW: DELETE /admin/organizations/{org_id} =====
@app.delete("/admin/organizations/{org_id}", response_model=MessageResponse)
async def delete_organization(
    org_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete organization - Supreme Admin only"""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")
    
    try:
        org = await organizations_collection.find_one({"_id": ObjectId(org_id)})
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        
        # Check if there are users in this organization
        user_count = await users_collection.count_documents({"organization": org["name"]})
        people_count = await people_collection.count_documents({"Organisation": org["name"]})
        
        if user_count + people_count > 0:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot delete organization with {user_count + people_count} members. Reassign them first."
            )
        
        result = await organizations_collection.delete_one({"_id": ObjectId(org_id)})
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=400, detail="Failed to delete organization")
        
        await log_activity(
            user_id=str(current_user.get("_id")),
            action="ORGANIZATION_DELETED",
            details=f"Deleted organization: {org['name']}"
        )
        
        return MessageResponse(message="Organization deleted successfully")
        
    except Exception as e:
        print(f"Error deleting organization: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting organization: {str(e)}")

# ===== 9. NEW: GET /admin/people with organization filtering =====
@app.get("/admin/people", response_model=PeopleList)
async def get_all_people(
    organization: Optional[str] = Query(None, description="Filter by organization - Supreme Admin only"),
    current_user: dict = Depends(get_current_user)
):
    """Get all people from People collection - Filtered by organization"""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
    
    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Build query
        query = {}
        if not is_supreme:
            query["Organisation"] = current_user.get("Organization")
        elif organization:
            query["Organisation"] = organization
        
        people = []
        cursor = people_collection.find(query)
        
        async for person in cursor:
            people.append(PeopleResponse(
                id=str(person["_id"]),
                name=person.get("Name", ""),
                surname=person.get("Surname", ""),
                email=person.get("Email", ""),
                phone=person.get("Number", ""),
                invitedBy=person.get("InvitedBy", ""),
                organisation=person.get("Organisation", ""),
                leaderId=person.get("LeaderId", ""),
                created_at=person.get("DateCreated")
            ))
        
        return PeopleList(people=people)
        
    except Exception as e:
        print(f"Error fetching people: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching people: {str(e)}")

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
    try:
        consolidation_id = str(ObjectId())
       
        print(f"Creating consolidation for: {consolidation.person_name} {consolidation.person_surname}")
        print(f"Assigned to leader: {consolidation.assigned_to} (email: {consolidation.assigned_to_email})")
        print(f"Source: {getattr(consolidation, 'source', 'manual')}")
       
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
            update_data = {
                "Stage": "Consolidate",
                "UpdatedAt": datetime.utcnow().isoformat(),
                "DecisionType": consolidation.decision_type.value,
                "DecisionDate": consolidation.decision_date,
            }
           
            existing_history = existing_person.get("DecisionHistory", [])
            if consolidation.decision_type == DecisionType.RECOMMITMENT:
                existing_history.append({
                    "type": "recommitment",
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id,
                    "source": getattr(consolidation, 'source', 'manual')
                })
                update_data["DecisionHistory"] = existing_history
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0) + 1
                update_data["LastDecisionDate"] = consolidation.decision_date
            else:
                existing_history.append({
                    "type": "first_time",
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id,
                    "source": getattr(consolidation, 'source', 'manual')
                })
                update_data["DecisionHistory"] = existing_history
                update_data["FirstDecisionDate"] = consolidation.decision_date
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0)
           
            await people_collection.update_one(
                {"_id": ObjectId(person_id)},
                {"$set": update_data}
            )
        else:
            person_doc = {
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Gender": "",
                "Address": "",
                "Birthday": "",
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
                "ConsolidationSource": getattr(consolidation, 'source', 'manual')
            }
           
            decision_history = [{
                "type": consolidation.decision_type.value,
                "date": consolidation.decision_date,
                "consolidation_id": consolidation_id,
                "source": getattr(consolidation, 'source', 'manual')
            }]
           
            person_doc["DecisionHistory"] = decision_history
            person_doc["TotalRecommitments"] = 1 if consolidation.decision_type == DecisionType.RECOMMITMENT else 0
           
            if consolidation.decision_type == DecisionType.FIRST_TIME:
                person_doc["FirstDecisionDate"] = consolidation.decision_date
            else:
                person_doc["LastDecisionDate"] = consolidation.decision_date
           
            result = await people_collection.insert_one(person_doc)
            person_id = str(result.inserted_id)
            print(f"Created new person: {person_id}")
           
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
                "FullName": f"{consolidation.person_name.strip()} {consolidation.person_surname.strip()}".strip(),
                "ConsolidationSource": getattr(consolidation, 'source', 'manual')
            }
            people_cache["data"].append(new_person_cache_entry)
            print(f"Added to cache: {new_person_cache_entry['FullName']}")

        # 2. Resolve leader email
        leader_email = consolidation.assigned_to_email
        leader_user_id = None
       
        if not leader_email:
            print(f"Searching for leader email: {consolidation.assigned_to}")
            
            leader_parts = consolidation.assigned_to.strip().split()
            first_name = leader_parts[0] if leader_parts else ""
            surname = " ".join(leader_parts[1:]) if len(leader_parts) > 1 else ""
            
            leader_person = await people_collection.find_one({
                "$or": [
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, consolidation.assigned_to]}},
                    {"Name": first_name, "Surname": surname},
                    {"$expr": {"$eq": [
                        {"$toLower": {"$concat": ["$Name", " ", "$Surname"]}},
                        consolidation.assigned_to.lower()
                    ]}}
                ]
            })
            # After the leader lookup attempt, add:
            print(f"Leader lookup for '{consolidation.assigned_to}': found={leader_person is not None}, email={leader_email}")
            if leader_person:
                leader_email = leader_person.get("Email")
                print(f"Found leader email from people: {leader_email}")
            
            if not leader_email and first_name:
                leader_user = await users_collection.find_one({
                    "$or": [
                        {"name": first_name, "surname": surname},
                        {"$expr": {"$eq": [
                            {"$toLower": {"$concat": ["$name", " ", "$surname"]}},
                            consolidation.assigned_to.lower()
                        ]}}
                    ]
                })
                if leader_user:
                    leader_email = leader_user.get("email")
                    print(f"Found leader email from users: {leader_email}")

        if leader_email:
            leader_user = await users_collection.find_one({"email": leader_email})
            if leader_user:
                leader_user_id = str(leader_user["_id"])
                print(f"Leader user account: {leader_email} (ID: {leader_user_id})")
            else:
                print(f"Leader has email {leader_email} but no user account")
        else:
            print(f"Could not find email for leader: {consolidation.assigned_to}")

        decision_display_name = "First Time Decision" if consolidation.decision_type == DecisionType.FIRST_TIME else "Recommitment"
        consolidation_source = getattr(consolidation, 'source', 'manual')
        source_display = "Service" if consolidation_source == "service_consolidation" else "Event" if consolidation_source == "event_consolidation" else "Manual"
        assigned_for = leader_email if leader_email else consolidation.assigned_to
       
        # 3. Create task
        task_doc = {
            "memberID": leader_user_id if leader_user_id else None,
            "name": f"Consolidation: {consolidation.person_name} {consolidation.person_surname} ({decision_display_name})",
            "taskType": "consolidation",
            "description": f"Follow up with {consolidation.person_name} {consolidation.person_surname} who made a {decision_display_name.lower()} on {consolidation.decision_date} ({source_display} Consolidation)",
            "followup_date": datetime.utcnow().isoformat(),
            "status": "Open",
            "assignedfor": assigned_for,
            "assigned_to_email": leader_email,
            "assigned_to_user_id": leader_user_id,
            "leader_assigned": consolidation.assigned_to,
            "leader_name": consolidation.assigned_to,
            "type": "followup",
            "priority": "high",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "decision_type": consolidation.decision_type.value,
            "decision_display_name": decision_display_name,
            "consolidation_source": consolidation_source,
            "source_display": source_display,
            "contacted_person": {
                "name": f"{consolidation.person_name} {consolidation.person_surname}",
                "email": person_email,
                "phone": consolidation.person_phone or ""
            },
            "created_at": datetime.utcnow().isoformat(),
            "created_by": current_user.get("email", ""),
            "is_consolidation_task": True
        }

        task_result = await tasks_collection.insert_one(task_doc)
        task_id = str(task_result.inserted_id)

        # 4. Build consolidation record
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
            "notes": consolidation.notes,
            "source": consolidation_source,
            "source_display": source_display,
            "task_id": task_id,
        }

        # 5. Add to event — strip date suffix before ObjectId lookup
        if consolidation.event_id:
            parts = consolidation.event_id.split("_")
            base_event_id = parts[0]
            instance_date = parts[1] if len(parts) > 1 else None

            if ObjectId.is_valid(base_event_id):
                event_for_cons = await events_collection.find_one({"_id": ObjectId(base_event_id)})
                is_recurring_event = bool(event_for_cons.get("recurring_day")) if event_for_cons else False

                if is_recurring_event:
                    if not instance_date:
                        timezone = pytz.timezone("Africa/Johannesburg")
                        instance_date = datetime.now(timezone).date().isoformat()

                    await events_collection.update_one(
                        {"_id": ObjectId(base_event_id)},
                        {
                            "$push": {f"attendance.{instance_date}.consolidations": consolidation_record},
                            "$set": {"updated_at": datetime.utcnow().isoformat()}
                        }
                    )
                    print(f"Added consolidation to recurring event attendance[{instance_date}]")
                else:
                    await events_collection.update_one(
                        {"_id": ObjectId(base_event_id)},
                        {
                            "$push": {"consolidations": consolidation_record},
                            "$set": {"updated_at": datetime.utcnow().isoformat()}
                        }
                    )
                    print(f"Added consolidation to non-recurring event root")

                # Verify write
                verification = await events_collection.find_one({"_id": ObjectId(base_event_id)})
                if is_recurring_event:
                    att = verification.get("attendance", {}).get(instance_date, {})
                    print(f"VERIFY: attendance[{instance_date}].consolidations = {len(att.get('consolidations', []))}")
                else:
                    print(f"VERIFY: root consolidations = {len(verification.get('consolidations', []))}")
            else:
                print(f"Invalid base event ID: {base_event_id}")

        # 6. Save to consolidations collection
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
            "task_id": task_id,
            "source": consolidation_source,
            "source_display": source_display
        }

        consolidations_collection = db["consolidations"]
        await consolidations_collection.insert_one(consolidation_doc)
        print(f"Created consolidation record: {consolidation_id}")

        total_people_count = await people_collection.count_documents({})

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
            exact_date_str = (datetime.utcnow().date() - timedelta(days=datetime.utcnow().date().weekday())).strftime("%Y-%m-%d")
            query = {"week": exact_date_str, "type": "weekly"} 
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
   
@app.get("/service-checkin/real-time-data")
async def get_service_checkin_real_time_data(
    event_id: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        parts = event_id.split("_")
        base_event_id = parts[0]
        instance_date = parts[1] if len(parts) > 1 else None

        if not ObjectId.is_valid(base_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        is_recurring = bool(event.get("recurring_day"))

        if is_recurring:
            if not instance_date:
                timezone = pytz.timezone("Africa/Johannesburg")
                instance_date = datetime.now(timezone).date().isoformat()

            attendance_data = event.get("attendance", {})
            date_data = attendance_data.get(instance_date, {}) if isinstance(attendance_data, dict) else {}

            attendees = date_data.get("attendees", [])
            new_people = date_data.get("new_people", [])
            consolidations = date_data.get("consolidations", [])

            print(f"Recurring [{instance_date}]: {len(attendees)} att, {len(new_people)} new, {len(consolidations)} cons")
        else:
            attendees = event.get("attendees", [])
            new_people = event.get("new_people", [])
            consolidations = event.get("consolidations", [])

        attendees = attendees if isinstance(attendees, list) else []
        new_people = new_people if isinstance(new_people, list) else []
        consolidations = consolidations if isinstance(consolidations, list) else []

        print(f"Real-time data returning: {len(attendees)} attendees, {len(new_people)} new, {len(consolidations)} consolidations")
        print(f"Instance date used: {instance_date}")
        print(f"Is recurring: {is_recurring}")
        
        return {
            "success": True,
            "event_id": event_id,
            "event_name": event.get("eventName", "Unknown Event"),
            "present_attendees": attendees,
            "new_people": new_people,
            "consolidations": consolidations,
            "present_count": len(attendees),
            "new_people_count": len(new_people),
            "consolidation_count": len(consolidations),
            "total_attendance": len(attendees),
            "refreshed_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting real-time data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {str(e)}")

@app.get("/service-checkin/validate-removal")
async def validate_removal(
    event_id: str = Query(..., description="Event ID"),
    consolidation_id: str = Query(None, description="Consolidation ID"),
    person_id: str = Query(None, description="Person ID"),
    current_user: dict = Depends(get_current_user)
):
    """
    Validate what will be affected by removal
    """
    try:
        if not consolidation_id and not person_id:
            raise HTTPException(status_code=400, detail="Either consolidation_id or person_id is required")
        
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        consolidation = None
        if consolidation_id:
            consolidations = event.get("consolidations", [])
            for cons in consolidations:
                if cons.get("id") == consolidation_id:
                    consolidation = cons
                    break
        
        warnings = []
        affected_tasks = []
        
        if consolidation:
            task_id = consolidation.get("task_id")
            if task_id and ObjectId.is_valid(task_id):
                # Get ONLY the specific task
                task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
                if task:
                    affected_tasks.append(task)
                    warnings.append(f"Task for {task.get('contacted_person', {}).get('name', 'Unknown')} will be deleted")
        
        return {
            "success": True,
            "validation": {
                "warnings": warnings,
                "affected_tasks": affected_tasks,
                "affected_tasks_count": len(affected_tasks)
            }
        }
        
    except Exception as e:
        logger.error(f"Validation error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@app.post("/service-checkin/checkin")
async def service_checkin_person(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        event_id = checkin_data.get("event_id")
        person_data = checkin_data.get("person_data", {})
        checkin_type = checkin_data.get("type", "attendee")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        is_recurring = bool(event.get("recurring_day"))
        now = datetime.utcnow().isoformat()

        # For recurring events, determine today's instance date
        instance_date = None
        if is_recurring:
            timezone = pytz.timezone("Africa/Johannesburg")
            instance_date = datetime.now(timezone).date().isoformat()

        if checkin_type == "attendee":
            person_id = person_data.get("id") or person_data.get("_id")
            if not person_id or not ObjectId.is_valid(person_id):
                raise HTTPException(status_code=400, detail="Valid person ID is required")

            existing = await people_collection.find_one({"_id": ObjectId(person_id)})
            if not existing:
                raise HTTPException(status_code=404, detail="Person does not exist")

            attendee_record = {
                "id": str(existing["_id"]),
                "name": existing.get("Name", ""),
                "surname": existing.get("Surname", ""),
                "email": existing.get("Email", ""),
                "phone": existing.get("Number", ""),
                "time": now,
                "checked_in": True,
                "type": "attendee"
            }

            if is_recurring:
                # Write to attendance[date].attendees, prevent duplicates
                result = await events_collection.update_one(
                    {
                        "_id": ObjectId(event_id),
                        f"attendance.{instance_date}.attendees.id": {"$ne": attendee_record["id"]}
                    },
                    {
                        "$push": {f"attendance.{instance_date}.attendees": attendee_record},
                        "$set": {
                            f"attendance.{instance_date}.updated_at": now,
                            "updated_at": now
                        }
                    }
                )
            else:
                result = await events_collection.update_one(
                    {
                        "_id": ObjectId(event_id),
                        "attendees.id": {"$ne": attendee_record["id"]}
                    },
                    {
                        "$push": {"attendees": attendee_record},
                        "$inc": {"total_attendance": 1},
                        "$set": {"updated_at": now}
                    }
                )

            if result.modified_count == 0:
                raise HTTPException(status_code=400, detail=f"{existing.get('Name')} is already checked in")

            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            if is_recurring:
                date_data = updated_event.get("attendance", {}).get(instance_date, {})
                present_count = len(date_data.get("attendees", []))
            else:
                present_count = len([a for a in updated_event.get("attendees", []) if a.get("checked_in")])

            return {
                "message": f"{existing.get('Name')} checked in",
                "type": "attendee",
                "attendee": attendee_record,
                "present_count": present_count,
                "success": True
            }

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
                "is_checked_in": True
            }

            if is_recurring:
                await events_collection.update_one(
                    {"_id": ObjectId(event_id)},
                    {
                        "$push": {f"attendance.{instance_date}.new_people": new_person_record},
                        "$set": {f"attendance.{instance_date}.updated_at": now, "updated_at": now}
                    }
                )
            else:
                await events_collection.update_one(
                    {"_id": ObjectId(event_id)},
                    {"$push": {"new_people": new_person_record}, "$set": {"updated_at": now}}
                )

            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            if is_recurring:
                date_data = updated_event.get("attendance", {}).get(instance_date, {})
                count = len(date_data.get("new_people", []))
            else:
                count = len(updated_event.get("new_people", []))

            return {
                "message": "Visitor added to event",
                "type": "new_person",
                "new_person": new_person_record,
                "new_people_count": count,
                "success": True
            }

        else:
            raise HTTPException(status_code=400, detail="Invalid type — must be attendee or new_person")

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
    try:
        event_id = removal_data.get("event_id")
        person_id = removal_data.get("person_id")
        data_type = removal_data.get("type")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        is_recurring = bool(event.get("recurring_day"))
        now = datetime.utcnow().isoformat()

        if is_recurring:
            timezone = pytz.timezone("Africa/Johannesburg")
            instance_date = datetime.now(timezone).date().isoformat()

            result = await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$pull": {f"attendance.{instance_date}.{data_type}": {"id": person_id}},
                    "$set": {f"attendance.{instance_date}.updated_at": now, "updated_at": now}
                }
            )
        else:
            update_query = {
                "$pull": {data_type: {"id": person_id}},
                "$set": {"updated_at": now}
            }
            if data_type == "attendees":
                update_query["$inc"] = {"total_attendance": -1}
            result = await events_collection.update_one({"_id": ObjectId(event_id)}, update_query)

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found in specified list")

        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})

        if is_recurring:
            date_data = updated_event.get("attendance", {}).get(instance_date, {})
            present_count = len(date_data.get("attendees", []))
            new_people_count = len(date_data.get("new_people", []))
            consolidation_count = len(date_data.get("consolidations", []))
        else:
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
        print(f"Error removing from service check-in: {str(e)}")
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
        data_type = update_data.get("type")  
        update_fields = update_data.get("update_fields", {})

        print(f"✏️ Updating service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        
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

        
        if "new_people" in event and "consolidations" in event:
            return {
                "success": True,
                "message": "Event already has the new structure",
                "already_initialized": True
            }

        
        update_data = {
            "attendees": event.get("attendees", []),
            "new_people": event.get("new_people", []),
            "consolidations": event.get("consolidations", []),
            "updated_at": datetime.utcnow().isoformat()
        }

        
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
   
# @app.post("/admin/migrate-all-events-structure")
# async def migrate_all_events_structure(current_user: dict = Depends(get_current_user)):
#     """
#     Migrate ALL events to the new three-type structure
#     Admin only
#     """
#     if current_user.get("role") != "admin":
#         raise HTTPException(status_code=403, detail="Admin access required")

#     try:
#         print("Starting migration of all events to new structure...")
       
        
#         all_events = await events_collection.find({}).to_list(length=None)
#         migrated_count = 0
#         results = []

#         for event in all_events:
#             try:
#                 event_id = event["_id"]
               
                
#                 if "new_people" in event and "consolidations" in event:
#                     continue

                
#                 old_attendees = event.get("attendees", [])
#                 new_attendees = []
#                 new_people = []
#                 consolidations = []

#                 for attendee in old_attendees:
#                     if isinstance(attendee, dict):
                        
#                         if attendee.get("decision") or attendee.get("is_consolidation"):
#                             consolidation_record = {
#                                 "id": attendee.get("id", f"consolidation_{secrets.token_urlsafe(8)}"),
#                                 "person_name": attendee.get("name", ""),
#                                 "person_surname": attendee.get("surname", ""),
#                                 "person_email": attendee.get("email", ""),
#                                 "person_phone": attendee.get("phone", ""),
#                                 "decision_type": attendee.get("decision", "first_time"),
#                                 "decision_display_name": attendee.get("decision_display",
#                                     "First Time Decision" if attendee.get("decision") == "first_time" else "Recommitment"),
#                                 "assigned_to": attendee.get("assigned_leader", ""),
#                                 "assigned_to_email": attendee.get("assigned_leader_email", ""),
#                                 "created_at": attendee.get("time", datetime.utcnow().isoformat()),
#                                 "type": "consolidation",
#                                 "status": "active"
#                             }
#                             consolidations.append(consolidation_record)
#                         else:
                            
#                             attendee_record = {
#                                 "id": attendee.get("id", f"attendee_{secrets.token_urlsafe(8)}"),
#                                 "name": attendee.get("name", ""),
#                                 "fullName": attendee.get("fullName", attendee.get("name", "")),
#                                 "email": attendee.get("email", ""),
#                                 "phone": attendee.get("phone", ""),
#                                 "leader12": attendee.get("leader12", ""),
#                                 "time": attendee.get("time", datetime.utcnow().isoformat()),
#                                 "checked_in": attendee.get("checked_in", True),
#                                 "type": "attendee"
#                             }
#                             new_attendees.append(attendee_record)

#                 update_data = {
#                     "attendees": new_attendees,
#                     "new_people": new_people,
#                     "consolidations": consolidations,
#                     "updated_at": datetime.utcnow().isoformat()
#                 }

#                 if "total_attendance" not in event:
#                     update_data["total_attendance"] = len(new_attendees)

#                 await events_collection.update_one(
#                     {"_id": event_id},
#                     {"$set": update_data}
#                 )

#                 migrated_count += 1
#                 results.append({
#                     "event_id": str(event_id),
#                     "event_name": event.get("eventName", "Unknown"),
#                     "attendees": len(new_attendees),
#                     "consolidations": len(consolidations)
#                 })

#                 print(f"Migrated: {event.get('eventName', 'Unknown')}")

#             except Exception as e:
#                 print(f"Error migrating event {event.get('eventName')}: {str(e)}")
#                 continue

#         print(f"Migration complete! Migrated {migrated_count} events")

#         return {
#             "success": True,
#             "message": f"Migrated {migrated_count} events to new structure",
#             "migrated_count": migrated_count,
#             "total_events": len(all_events),
#             "results": results
#         }

#     except Exception as e:
#         print(f"Error in bulk migration: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error migrating events: {str(e)}")

def get_period_range(period: str):
    """
    Accurate date range calculator matching frontend's DailyTasks filter:
    - today
    - thisWeek
    - thisMonth
    - previous7 (last 7 days)
    - previousWeek
    - previousMonth
    """
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    
    if period == "today":
        start = today
        end = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    
    
    if period == "thisWeek":
        start = today - timedelta(days=today.weekday())  
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end
    
    
    if period == "thisMonth":
        start = today.replace(day=1)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(today.year, today.month + 1, 1) - timedelta(microseconds=1)
        return start, end
    
    
    if period == "previous7":
        end = today - timedelta(days=1)  
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        start = end - timedelta(days=6)  
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end
    
    
    if period == "previousWeek":
        last_week = today - timedelta(weeks=1)
        start = last_week - timedelta(days=last_week.weekday())  
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end
    
    
    if period == "previousMonth":
        year = today.year
        month = today.month - 1
        if month == 0:
            month = 12
            year -= 1
        
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(year, month + 1, 1) - timedelta(microseconds=1)
        return start, end
    
    raise ValueError(f"Invalid period '{period}'")


EXCLUDED_TASK_TYPES_FROM_COMPLETED = ["no answer", "Awaiting Call"]

# ====================== COMPREHENSIVE DASHBOARD (MULTI-TENANT) ======================
@app.get("/stats/dashboard-comprehensive")
async def get_dashboard_comprehensive(
    period: str = Query(
        "today",
        pattern="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"
    ),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    try:
        org_name = current_user.get("Organization")
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        is_super_admin = current_user.get("role") == "super_admin"
        org_filter = {} if is_super_admin else {"Organization": org_name}

        print(f"[DASHBOARD] Comprehensive stats requested - Period: {period}, Org: {org_name}, SuperAdmin: {is_super_admin}")

        start, end = get_period_range(period)
        start_date_str = start.date().isoformat()
        end_date_str = end.date().isoformat()

        # Task types filtered by organization
        task_types_cursor = tasktypes_collection.find(org_filter, {"name": 1})
        task_types_list = await task_types_cursor.to_list(length=None)
        all_task_types = [tt.get("name") for tt in task_types_list if tt.get("name")]

        # Overdue cells pipeline
        overdue_cells_pipeline = [
            {
                "$match": {
                    **org_filter,
                    "$and": [
                        {
                            "$or": [
                                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
                            ]
                        },
                        {"date": {"$lte": end}},
                        {
                            "$or": [
                                {"status": "incomplete"},
                                {"status": {"$exists": False}},
                                {"status": None},
                                {"Status": "Incomplete"},
                                {"_is_overdue": True}
                            ]
                        }
                    ]
                }
            },
            {"$sort": {"date": -1}},
            {"$limit": 100},
            {
                "$project": {
                    "_id": 1,
                    "UUID": 1,
                    "eventName": {
                        "$ifNull": [
                            "$Event Name",
                            {"$ifNull": ["$eventName", {"$ifNull": ["$EventName", "Unnamed Event"]}]}
                        ]
                    },
                    "eventType": {
                        "$ifNull": [
                            "$Event Type",
                            {"$ifNull": ["$eventType", {"$ifNull": ["$eventTypeName", "Cells"]}]}
                        ]
                    },
                    "eventLeaderName": {
                        "$ifNull": [
                            "$Leader",
                            {"$ifNull": ["$eventLeaderName", {"$ifNull": ["$EventLeaderName", "Unknown Leader"]}]}
                        ]
                    },
                    "eventLeaderEmail": {
                        "$ifNull": [
                            "$Email",
                            {"$ifNull": ["$eventLeaderEmail", {"$ifNull": ["$EventLeaderEmail", ""]}]}
                        ]
                    },
                    "leader1": {
                        "$ifNull": [
                            "$leader1",
                            {"$ifNull": ["$Leader @1", ""]}
                        ]
                    },
                    "leader12": {
                        "$ifNull": [
                            "$Leader at 12",
                            {"$ifNull": ["$Leader @12", {"$ifNull": ["$leader12", {"$ifNull": ["$Leader12", ""]}]}]}
                        ]
                    },
                    "day": {
                        "$ifNull": [
                            "$Day",
                            {"$ifNull": ["$day", ""]}
                        ]
                    },
                    "date": 1,
                    "location": {
                        "$ifNull": [
                            "$Location",
                            {"$ifNull": ["$location", ""]}
                        ]
                    },
                    "attendees": {"$ifNull": ["$attendees", []]},
                    "persistent_attendees": {"$ifNull": ["$persistent_attendees", []]},
                    "hasPersonSteps": {"$ifNull": ["$hasPersonSteps", True]},
                    "status": {
                        "$ifNull": [
                            "$status",
                            {"$ifNull": ["$Status", "incomplete"]}
                        ]
                    },
                    "_is_overdue": {"$literal": True},
                    "is_recurring": {"$ifNull": ["$is_recurring", True]},
                    "week_identifier": 1,
                    "original_event_id": {"$toString": "$_id"}
                }
            }
        ]

        # Tasks pipeline
        tasks_pipeline = [
            {
                "$match": {
                    **org_filter,
                    "$or": [
                        {"followup_date": {"$gte": start, "$lte": end}},
                        {"completedAt": {"$gte": start, "$lte": end}},
                        {"createdAt": {"$gte": start, "$lte": end}}
                    ]
                }
            },
            {
                "$addFields": {
                    "task_type_label": {"$ifNull": ["$taskType", "Uncategorized"]},
                    "is_excluded_type": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$taskType", None]},
                                    {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    "is_completed": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": [
                                            {
                                                "$and": [
                                                    {"$ne": ["$taskType", None]},
                                                    {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    "completed_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$completedAt", None]},
                                    {"$gte": ["$completedAt", start]},
                                    {"$lte": ["$completedAt", end]},
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": [
                                            {
                                                "$and": [
                                                    {"$ne": ["$taskType", None]},
                                                    {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    "is_due_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$followup_date", None]},
                                    {"$gte": ["$followup_date", start]},
                                    {"$lte": ["$followup_date", end]}
                                ]
                            },
                            True,
                            False
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$assignedfor",
                    "tasks": {
                        "$push": {
                            "_id": "$_id",
                            "name": "$name",
                            "taskType": "$taskType",
                            "task_type_label": "$task_type_label",
                            "followup_date": "$followup_date",
                            "due_date": "$followup_date",
                            "completedAt": "$completedAt",
                            "createdAt": "$createdAt",
                            "status": "$status",
                            "assignedfor": "$assignedfor",
                            "type": "$type",
                            "contacted_person": "$contacted_person",
                            "isRecurring": {
                                "$cond": [
                                    {"$ifNull": ["$recurring_day", False]},
                                    True,
                                    False
                                ]
                            },
                            "priority": "$priority",
                            "is_completed": "$is_completed",
                            "is_due_in_period": "$is_due_in_period",
                            "completed_in_period": "$completed_in_period",
                            "is_excluded_type": "$is_excluded_type",
                            "description": "$description"
                        }
                    },
                    "total_tasks": {"$sum": 1},
                    "completed_tasks": {"$sum": {"$cond": ["$is_completed", 1, 0]}},
                    "completed_in_period": {"$sum": {"$cond": ["$completed_in_period", 1, 0]}},
                    "due_in_period": {"$sum": {"$cond": ["$is_due_in_period", 1, 0]}},
                    "task_type_counts": {
                        "$push": {
                            "task_type": "$task_type_label",
                            "is_completed": "$is_completed",
                            "completed_in_period": "$completed_in_period",
                            "is_due_in_period": "$is_due_in_period",
                            "is_excluded_type": "$is_excluded_type"
                        }
                    }
                }
            },
            {"$match": {"total_tasks": {"$gt": 0}}},
            {"$sort": {"_id": 1}}
        ]

        overdue_cells_cursor = events_collection.aggregate(overdue_cells_pipeline)
        tasks_cursor = tasks_collection.aggregate(tasks_pipeline)
        users_cursor = users_collection.find(
            org_filter,
            {"_id": 1, "email": 1, "name": 1, "surname": 1}
        ).limit(limit)

        overdue_cells, task_groups, users = await asyncio.gather(
            overdue_cells_cursor.to_list(100),
            tasks_cursor.to_list(None),
            users_cursor.to_list(limit)
        )

        formatted_overdue_cells = []
        for cell in overdue_cells:
            cell["_id"] = str(cell["_id"])
            if isinstance(cell.get("date"), datetime):
                cell["date"] = cell["date"].isoformat()
            formatted_overdue_cells.append(cell)

        # User map
        all_users_map = {}
        try:
            all_users_cursor = users_collection.find({}, {"_id": 1, "email": 1, "name": 1, "surname": 1})
            async for user in all_users_cursor:
                uid = str(user["_id"])
                email = user.get("email", "").lower()
                if not email:
                    continue

                person = await people_collection.find_one({
                    "$or": [
                        {"Email": {"$regex": f"^{email}$", "$options": "i"}},
                        {"user_id": uid}
                    ]
                })

                if person:
                    full_name = f"{person.get('Name', '').strip()} {person.get('Surname', '').strip()}".strip()
                else:
                    full_name = f"{user.get('name', '')} {user.get('surname', '')}".strip()

                if not full_name:
                    full_name = email.split("@")[0]

                all_users_map[email] = {
                    "_id": uid,
                    "email": email,
                    "fullName": full_name
                }
                all_users_map[uid] = all_users_map[email]

        except Exception as e:
            print(f"[DASHBOARD] Error building user map: {e}")

        # Process task groups
        grouped_tasks = []
        all_tasks_list = []
        global_total_tasks = 0
        global_completed_tasks = 0
        global_completed_in_period = 0
        global_due_in_period = 0
        global_incomplete_due = 0
        task_type_stats = {}

        for task_group in task_groups:
            email = task_group["_id"] or "unassigned@example.com"

            user_info = all_users_map.get(email.lower(), {
                "_id": f"unknown_{email}",
                "email": email,
                "fullName": email.split("@")[0]
            })

            tasks_list = task_group["tasks"]

            for task in tasks_list:
                task["_id"] = str(task["_id"])

                for date_field in ["followup_date", "due_date", "completedAt", "createdAt"]:
                    if isinstance(task.get(date_field), datetime):
                        task[date_field] = task[date_field].isoformat()

                task_type = task.get("taskType") or "Uncategorized"
                is_excluded = task.get("is_excluded_type", False)

                if task_type not in task_type_stats:
                    task_type_stats[task_type] = {
                        "total": 0,
                        "completed": 0,
                        "completed_in_period": 0,
                        "due_in_period": 0,
                        "incomplete_due": 0,
                        "is_excluded": is_excluded
                    }

                task_type_stats[task_type]["total"] += 1

                if task.get("is_completed"):
                    task_type_stats[task_type]["completed"] += 1

                if task.get("completed_in_period"):
                    task_type_stats[task_type]["completed_in_period"] += 1

                if task.get("is_due_in_period"):
                    task_type_stats[task_type]["due_in_period"] += 1

                if task.get("is_due_in_period") and not task.get("is_completed"):
                    task_type_stats[task_type]["incomplete_due"] += 1

            total_for_user = task_group["total_tasks"]
            completed_all = task_group["completed_tasks"]
            completed_in_period = task_group["completed_in_period"]
            due_in_period = task_group["due_in_period"]
            incomplete_due = sum(
                1 for t in tasks_list
                if t.get("is_due_in_period") and not t.get("is_completed")
            )
            incomplete_all = total_for_user - completed_all

            global_total_tasks += total_for_user
            global_completed_tasks += completed_all
            global_completed_in_period += completed_in_period
            global_due_in_period += due_in_period
            global_incomplete_due += incomplete_due

            grouped_tasks.append({
                "user": user_info,
                "tasks": tasks_list,
                "totalCount": total_for_user,
                "completedCount": completed_all,
                "incompleteCount": incomplete_all,
                "dueInPeriodCount": due_in_period,
                "completedInPeriodCount": completed_in_period,
                "incompleteDueInPeriodCount": incomplete_due,
                "taskTypes": list(set([t.get("taskType") or "Uncategorized" for t in tasks_list]))
            })

            all_tasks_list.extend(tasks_list)

        grouped_tasks.sort(key=lambda x: x["user"]["fullName"].lower())

        completion_rate_due = round(
            (global_completed_in_period / global_due_in_period * 100), 2
        ) if global_due_in_period > 0 else 0

        completion_rate_overall = round(
            (global_completed_tasks / global_total_tasks * 100), 2
        ) if global_total_tasks > 0 else 0

        unique_task_types_found = list(task_type_stats.keys())

        overview = {
            "total_attendance": sum(len(c.get("attendees", [])) for c in formatted_overdue_cells),
            "outstanding_cells": len(formatted_overdue_cells),
            "outstanding_tasks": global_incomplete_due,
            "tasks_due_in_period": global_due_in_period,
            "tasks_completed_in_period": global_completed_in_period,
            "total_tasks_in_period": global_total_tasks,
            "total_tasks_completed": global_completed_tasks,
            "total_tasks_incomplete": global_total_tasks - global_completed_tasks,
            "consolidation_tasks": task_type_stats.get("consolidation", {}).get("total", 0),
            "consolidation_completed": task_type_stats.get("consolidation", {}).get("completed", 0),
            "consolidation_completed_in_period": task_type_stats.get("consolidation", {}).get("completed_in_period", 0),
            "people_behind": len([g for g in grouped_tasks if g["incompleteDueInPeriodCount"] > 0]),
            "total_users": len(users),
            "completion_rate_due_tasks": completion_rate_due,
            "completion_rate_overall": completion_rate_overall,
            "consolidation_completion_rate": round(
                (
                    task_type_stats.get("consolidation", {}).get("completed", 0) /
                    task_type_stats.get("consolidation", {}).get("total", 1) * 100
                ),
                2
            ) if task_type_stats.get("consolidation", {}).get("total", 0) > 0 else 0,
            "task_type_breakdown": task_type_stats,
            "users_with_tasks": len(grouped_tasks),
            "users_without_tasks": len(users) - len(grouped_tasks),
            "available_task_types": all_task_types,
            "task_types_found": unique_task_types_found,
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "total_unique_task_types": len(unique_task_types_found),
            "note": "'no answer' and 'Awaiting Call' task types are excluded from completed counts"
        }

        # Deduplicate users
        seen_user_ids = set()
        all_users_list = []

        for user_info in all_users_map.values():
            if (
                isinstance(user_info, dict)
                and "_id" in user_info
                and not user_info["_id"].startswith("unknown_")
                and user_info["_id"] not in seen_user_ids
            ):
                seen_user_ids.add(user_info["_id"])
                full_name_parts = user_info["fullName"].split()

                all_users_list.append({
                    "_id": user_info["_id"],
                    "email": user_info["email"],
                    "name": full_name_parts[0] if full_name_parts else "",
                    "surname": " ".join(full_name_parts[1:]) if len(full_name_parts) > 1 else "",
                    "fullName": user_info["fullName"]
                })

        return {
            "overview": overview,
            "overdueCells": formatted_overdue_cells,
            "groupedTasks": grouped_tasks,
            "allTasks": all_tasks_list,
            "allUsers": all_users_list,
            "period": period,
            "date_range": {"start": start_date_str, "end": end_date_str},
            "task_type_stats": task_type_stats,
            "available_task_types": all_task_types,
            "task_types_found": unique_task_types_found,
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching comprehensive stats: {str(e)}")

# ====================== QUICK DASHBOARD (MULTI-TENANT) ======================
@app.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    current_user: dict = Depends(get_current_user)
):
    try:
        org_name = current_user.get("Organization")
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        is_super_admin = current_user.get("role") == "super_admin"
        org_filter = {} if is_super_admin else {"Organization": org_name}

        start, end = get_period_range(period)
        start_str = start.date().isoformat()
        end_str = end.date().isoformat()

        total_tasks_all = await tasks_collection.count_documents({
            **org_filter,
            "$or": [
                {"followup_date": {"$gte": start, "$lte": end}},
                {"completedAt": {"$gte": start, "$lte": end}},
                {"createdAt": {"$gte": start, "$lte": end}}
            ]
        })

        tasks_due_in_period = await tasks_collection.count_documents({
            **org_filter,
            "followup_date": {"$gte": start, "$lte": end},
            "status": {"$nin": ["completed", "done", "closed", "finished"]}
        })

        tasks_completed_in_period = await tasks_collection.count_documents({
            **org_filter,
            "completedAt": {"$gte": start, "$lte": end},
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
        })

        total_tasks_in_period = await tasks_collection.count_documents({
            **org_filter,
            "$or": [
                {"followup_date": {"$gte": start, "$lte": end}},
                {"completedAt": {"$gte": start, "$lte": end}},
                {"createdAt": {"$gte": start, "$lte": end}}
            ]
        })

        total_completed = await tasks_collection.count_documents({
            **org_filter,
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
        })

        consolidation_completed_in_period = await tasks_collection.count_documents({
            **org_filter,
            "completedAt": {"$gte": start, "$lte": end},
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": "consolidation"
        })

        total_consolidation_tasks = await tasks_collection.count_documents({
            **org_filter,
            "taskType": "consolidation"
        })

        total_consolidation_completed = await tasks_collection.count_documents({
            **org_filter,
            "taskType": "consolidation",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })

        no_answer_count = await tasks_collection.count_documents({
            **org_filter,
            "taskType": "no answer",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })

        awaiting_call_count = await tasks_collection.count_documents({
            **org_filter,
            "taskType": "Awaiting Call",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })

        pipeline = [
            {
                "$match": {
                    **org_filter,
                    "$or": [
                        {"followup_date": {"$gte": start, "$lte": end}},
                        {"completedAt": {"$gte": start, "$lte": end}},
                        {"createdAt": {"$gte": start, "$lte": end}}
                    ]
                }
            },
            {
                "$addFields": {
                    "task_type": {"$ifNull": ["$taskType", "Uncategorized"]},
                    "is_excluded": {
                        "$cond": [
                            {"$and": [{"$ne": ["$taskType", None]}, {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}]},
                            True, False
                        ]
                    },
                    "is_completed": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": [{"$toLower": {"$ifNull": ["$status", "pending"]}}, ["completed", "done", "closed", "finished"]]},
                                    {"$not": {"$cond": [{"$and": [{"$ne": ["$taskType", None]}, {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}]}, True, False]}}
                                ]
                            },
                            True, False
                        ]
                    },
                    "completed_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$completedAt", None]},
                                    {"$gte": ["$completedAt", start]},
                                    {"$lte": ["$completedAt", end]},
                                    {"$in": [{"$toLower": {"$ifNull": ["$status", "pending"]}}, ["completed", "done", "closed", "finished"]]},
                                    {"$not": {"$cond": [{"$and": [{"$ne": ["$taskType", None]}, {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}]}, True, False]}}
                                ]
                            },
                            True, False
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$task_type",
                    "total": {"$sum": 1},
                    "completed": {"$sum": {"$cond": ["$is_completed", 1, 0]}},
                    "completed_in_period": {"$sum": {"$cond": ["$completed_in_period", 1, 0]}},
                    "due_in_period": {"$sum": {"$cond": [{"$and": [{"$ne": ["$followup_date", None]}, {"$gte": ["$followup_date", start]}, {"$lte": ["$followup_date", end]}]}, 1, 0]}},
                    "is_excluded": {"$first": "$is_excluded"}
                }
            },
            {"$sort": {"total": -1}}
        ]

        task_type_cursor = tasks_collection.aggregate(pipeline)
        task_type_stats_raw = await task_type_cursor.to_list(None)

        task_type_stats = {}
        for stat in task_type_stats_raw:
            task_type = stat["_id"] or "Uncategorized"
            task_type_stats[task_type] = {
                "total": stat["total"],
                "completed": stat["completed"],
                "completed_in_period": stat["completed_in_period"],
                "due_in_period": stat["due_in_period"],
                "is_excluded": stat["is_excluded"],
                "completion_rate": round((stat["completed"] / stat["total"] * 100), 2) if stat["total"] > 0 else 0,
                "completion_rate_in_period": round((stat["completed_in_period"] / stat["due_in_period"] * 100), 2) if stat["due_in_period"] > 0 else 0
            }

        overdue_cells_count = await events_collection.count_documents({
            **org_filter,
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
            ],
            "date": {"$lte": end},
            "$or": [
                {"status": "incomplete"},
                {"status": {"$exists": False}},
                {"Status": "Incomplete"},
                {"_is_overdue": True}
            ]
        })

        return {
            "period": period,
            "date_range": {"start": start_str, "end": end_str},
            "taskCount": total_tasks_all,
            "tasksDueInPeriod": tasks_due_in_period,
            "tasksCompletedInPeriod": tasks_completed_in_period,
            "totalCompletedTasks": total_completed,
            "consolidationTasks": total_consolidation_tasks,
            "consolidationCompleted": total_consolidation_completed,
            "consolidationCompletedInPeriod": consolidation_completed_in_period,
            "consolidationCompletionRate": round((total_consolidation_completed / total_consolidation_tasks * 100), 2) if total_consolidation_tasks > 0 else 0,
            "overdueCells": overdue_cells_count,
            "completionRateDueTasks": round((tasks_completed_in_period / tasks_due_in_period * 100), 2) if tasks_due_in_period > 0 else 0,
            "overallCompletionRate": round((total_completed / total_tasks_all * 100), 2) if total_tasks_all > 0 else 0,
            "taskTypeBreakdown": task_type_stats,
            "totalTaskTypesFound": len(task_type_stats),
            "excludedTaskTypes": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp": datetime.utcnow().isoformat(),
            "note": "'no answer' and 'Awaiting Call' task types are excluded from completed counts"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error fetching quick stats: {str(e)}")
            
@app.patch("/events/{event_id}/toggle-status")
async def toggle_event_status(
    event_id: str,
    current_user: dict = Depends(get_current_user)
):
    try:
        parts = event_id.split("_")
        base_event_id = parts[0]
        instance_date = parts[1] if len(parts) > 1 else None

        print(f"Toggling event status: {base_event_id} (instance date: {instance_date})")

        if not ObjectId.is_valid(base_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        if instance_date:
            attendance_data = event.get("attendance", {})
            date_attendance = attendance_data.get(instance_date, {}) if isinstance(attendance_data, dict) else {}
            current_status = str(date_attendance.get("status", "")).lower() or event.get("status", "").lower()
        else:
            current_status = event.get("status", "").lower()

        # Reopening
        if current_status in ["complete", "closed"]:
            new_status = "incomplete"
            action_msg = "reopened"
            log_action = "EVENT_REOPENED"
            status_fields = {
                "reopened_by": current_user.get("email", ""),
                "reopened_at": datetime.utcnow().isoformat()
            }

        # Closing
        else:
            new_status = "complete"
            action_msg = "closed"
            log_action = "EVENT_CLOSED"
            status_fields = {
                "closed_by": current_user.get("email", ""),
                "closed_at": datetime.utcnow().isoformat()
            }

        update_data = {
            "updated_at": datetime.utcnow().isoformat(),
            **status_fields
        }

        if instance_date:
            update_data[f"attendance.{instance_date}.status"] = new_status
            update_data[f"attendance.{instance_date}.closed_by"] = current_user.get("email", "")
            update_data[f"attendance.{instance_date}.closed_at"] = datetime.utcnow().isoformat()
            update_data["status"] = new_status
            update_data["closed_by"] = current_user.get("email", "")
            update_data["closed_at"] = datetime.utcnow().isoformat()
        else:
            update_data["status"] = new_status

        result = await events_collection.update_one(
            {"_id": ObjectId(base_event_id)},
            {"$set": update_data}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update event status")

        await log_activity(
            user_id=current_user.get("_id"),
            action=log_action,
            details=f"{action_msg.capitalize()} event: {event.get('eventName', 'Unknown')} (ID: {base_event_id}, date: {instance_date})"
        )

        print(f"Event {event.get('eventName')} {action_msg} successfully")

        return {
            "success": True,
            "already_closed": False,
            "message": f"Event '{event.get('eventName', 'Unknown')}' {action_msg} successfully",
            "event_id": base_event_id,
            "event_name": event.get("eventName", "Unknown"),
            "previous_status": current_status,
            "new_status": new_status,
            "action": action_msg,
            "actioned_by": current_user.get("email", ""),
            "actioned_at": status_fields.get("closed_at") or status_fields.get("reopened_at")
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error toggling event status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error toggling event status: {str(e)}")
     
@app.post("/service-checkin/create-consolidation")
async def create_consolidation(
    consolidation_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        event_id = consolidation_data.get("event_id")
        person_data = consolidation_data.get("person_data", {})
        decision_type = consolidation_data.get("decision_type", "Commitment")
        assigned_to = consolidation_data.get("assigned_to", "")
        notes = consolidation_data.get("notes", "")

        if not event_id:
            raise HTTPException(status_code=400, detail="Event ID is required")

        # Handle recurring event ID (split base_id from date)
        parts = event_id.split("_")
        base_event_id = parts[0]
        instance_date = parts[1] if len(parts) > 1 else None

        if not ObjectId.is_valid(base_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID format")

        event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        is_recurring = bool(event.get("recurring_day"))

        # If recurring and no date provided, use today (Joburg time)
        if is_recurring and not instance_date:
            timezone = pytz.timezone("Africa/Johannesburg")
            instance_date = datetime.now(timezone).date().isoformat()

        person_name = person_data.get("name", "")
        person_surname = person_data.get("surname", "")
        person_email = person_data.get("email", "")
        person_phone = person_data.get("phone", "") or person_data.get("number", "")
        person_id = person_data.get("id", "")

        # Create task
        task_payload = {
            "memberID": current_user.get("user_id", current_user.get("email", "unknown")),
            "name": assigned_to or current_user.get("name", "Unknown"),
            "taskType": "consolidation",
            "contacted_person": {
                "name": f"{person_name} {person_surname}".strip(),
                "phone": person_phone,
                "email": person_email
            },
            "followup_date": datetime.utcnow().isoformat(),
            "status": "Open",
            "type": "consolidation",
            "assignedfor": consolidation_data.get("assigned_to_email") or current_user.get("email", "unknown"),
            "assigned_to_email": consolidation_data.get("assigned_to_email") or "",
            "is_consolidation_task": True,
            "leader_assigned": assigned_to,
            "leader_name": assigned_to,
            "consolidation_name": f"{person_name} {person_surname} - {consolidation_data.get('decision_type', 'Commitment')}",
            "decision_display_name": consolidation_data.get("decision_type", "Commitment"),
            "source_display": "Service",
            "consolidation_source": "Service",
            "person_name": person_name,
            "person_surname": person_surname,
            "person_email": person_email,
            "person_phone": person_phone,
            "person_id": person_id,
            "Organization": current_user.get("Organization") or current_user.get("organization", ""),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }

        task_result = await tasks_collection.insert_one(task_payload)
        task_id = str(task_result.inserted_id)

        consolidation_id = str(ObjectId())
        consolidation_record = {
            "id": consolidation_id,
            "task_id": task_id,
            "event_id": event_id,
            "person_id": person_id,
            "person_name": person_name,
            "person_surname": person_surname,
            "person_email": person_email,
            "person_phone": person_phone,
            "decision_type": decision_type,
            "assigned_to": assigned_to,
            "notes": notes,
            "created_by": current_user.get("email", "unknown"),
            "created_by_name": current_user.get("name", "Unknown"),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "status": "active",
            "source": "service_checkin"
        }

        # ── Write to the correct location based on recurring vs non-recurring ──
        if is_recurring:
            # Push into the nested attendance[date].consolidations array
            result = await events_collection.update_one(
                {"_id": ObjectId(base_event_id)},
                {
                    "$push": {f"attendance.{instance_date}.consolidations": consolidation_record},
                    "$set": {"updated_at": datetime.utcnow().isoformat()}
                }
            )
        else:
            # Push into the root consolidations array (original behaviour)
            result = await events_collection.update_one(
                {"_id": ObjectId(base_event_id)},
                {
                    "$push": {"consolidations": consolidation_record},
                    "$set": {"updated_at": datetime.utcnow().isoformat()}
                }
            )

        if result.modified_count == 0:
            await tasks_collection.delete_one({"_id": ObjectId(task_id)})
            raise HTTPException(status_code=500, detail="Failed to add consolidation to event")

        # Also save to consolidations collection
        try:
            consolidations_collection = db["consolidations"]
            await consolidations_collection.insert_one(
                {**consolidation_record, "_id": ObjectId(consolidation_id)}
            )
        except Exception as e:
            logger.warning(f"Could not add to consolidations collection: {e}")

        try:
            await log_activity(
                user_id=current_user.get("user_id", current_user.get("email", "unknown")),
                action="CONSOLIDATION_CREATED",
                details=f"Created consolidation for '{person_name} {person_surname}' in event '{event.get('eventName', 'Unknown')}'"
            )
        except Exception as e:
            logger.warning(f"Failed to log activity: {e}")

        updated_event = await events_collection.find_one({"_id": ObjectId(base_event_id)})

        # Get correct count based on recurring
        if is_recurring:
            attendance_data = updated_event.get("attendance", {})
            date_data = attendance_data.get(instance_date, {})
            cons_count = len(date_data.get("consolidations", []))
        else:
            cons_count = len(updated_event.get("consolidations", []))

        return {
            "success": True,
            "message": "Consolidation created successfully",
            "consolidation": consolidation_record,
            "task_id": task_id,
            "event_id": event_id,
            "event_name": event.get("eventName", "Unknown Event"),
            "updated_statistics": {
                "consolidations_count": cons_count,
            },
            "timestamp": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating consolidation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.delete("/service-checkin/remove-consolidation")
async def remove_consolidation(
    event_id: str = Query(...),
    consolidation_id: str = Query(...),
    keep_person_in_attendees: bool = Query(True),
    current_user: dict = Depends(get_current_user)
):
    try:
        # Strip date suffix to get base ObjectId
        parts = event_id.split("_")
        base_event_id = parts[0]
        instance_date = parts[1] if len(parts) > 1 else None

        if not ObjectId.is_valid(base_event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        is_recurring = bool(event.get("recurring_day"))
        now = datetime.utcnow().isoformat()

        # Determine instance date for recurring events
        if is_recurring and not instance_date:
            timezone = pytz.timezone("Africa/Johannesburg")
            instance_date = datetime.now(timezone).date().isoformat()

        # Find the consolidation in the right place
        if is_recurring:
            attendance_data = event.get("attendance", {})
            date_data = attendance_data.get(instance_date, {}) if isinstance(attendance_data, dict) else {}
            consolidations_list = date_data.get("consolidations", [])
        else:
            consolidations_list = event.get("consolidations", [])

        consolidation_to_remove = None
        updated_consolidations = []
        for c in consolidations_list:
            c_id = c.get("id") or c.get("_id")
            if str(c_id) == consolidation_id:
                consolidation_to_remove = c
            else:
                updated_consolidations.append(c)

        if not consolidation_to_remove:
            raise HTTPException(status_code=404, detail="Consolidation not found in event")

        person_name = consolidation_to_remove.get("person_name", "")
        person_surname = consolidation_to_remove.get("person_surname", "")

        # Write updated consolidations back to the right place
        if is_recurring:
            await events_collection.update_one(
                {"_id": ObjectId(base_event_id)},
                {
                    "$set": {
                        f"attendance.{instance_date}.consolidations": updated_consolidations,
                        f"attendance.{instance_date}.updated_at": now,
                        "updated_at": now
                    }
                }
            )
        else:
            await events_collection.update_one(
                {"_id": ObjectId(base_event_id)},
                {"$set": {"consolidations": updated_consolidations, "updated_at": now}}
            )

        # Delete from consolidations collection
        consolidations_col = db["consolidations"]
        if ObjectId.is_valid(consolidation_id):
            await consolidations_col.delete_one({"_id": ObjectId(consolidation_id)})
        await consolidations_col.delete_one({"id": consolidation_id})

        # Delete associated task
        task_deleted = False
        deleted_task_ids = []

        # Try by consolidation_id first, then by task_id stored in the record
        task = await tasks_collection.find_one({"consolidation_id": consolidation_id})
        if not task:
            task_id_from_record = consolidation_to_remove.get("task_id")
            if task_id_from_record and ObjectId.is_valid(task_id_from_record):
                task = await tasks_collection.find_one({"_id": ObjectId(task_id_from_record)})

        if task:
            await tasks_collection.delete_one({"_id": task["_id"]})
            task_deleted = True
            deleted_task_ids.append(str(task["_id"]))
            print(f"Deleted task: {task['_id']}")

        # Get updated stats
        updated_event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
        if is_recurring:
            date_data = updated_event.get("attendance", {}).get(instance_date, {})
            stats = {
                "consolidations_count": len(date_data.get("consolidations", [])),
                "new_people_count": len(date_data.get("new_people", [])),
                "total_attendance": len(date_data.get("attendees", []))
            }
        else:
            stats = {
                "consolidations_count": len(updated_event.get("consolidations", [])),
                "new_people_count": len(updated_event.get("new_people", [])),
                "total_attendance": updated_event.get("total_attendance", 0)
            }

        try:
            await log_activity(
                user_id=current_user.get("email"),
                action="CONSOLIDATION_REMOVED",
                details=f"Removed consolidation for {person_name} {person_surname}"
            )
        except Exception as log_error:
            print(f"Activity log failed: {log_error}")

        return {
            "success": True,
            "message": "Consolidation removed successfully",
            "task_deletion": {
                "deleted": task_deleted,
                "count": len(deleted_task_ids)
            },
            "updated_statistics": stats,
            "timestamp": now
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing consolidation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/service-checkin/migrate-consolidations")
async def migrate_consolidations(
    current_user: dict = Depends(get_current_user)
):
    """
    Migration endpoint to add task_id to existing consolidations
    Run this once to fix old consolidations
    """
    try:
        # Only allow admins to run migration
        if current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Only admins can run migrations")
        
        updates_made = 0
        
        # Get all events with consolidations
        events = await events_collection.find({"consolidations": {"$exists": True, "$ne": []}}).to_list(None)
        
        logger.info(f"Found {len(events)} events with consolidations to migrate")
        
        for event in events:
            consolidations = event.get("consolidations", [])
            updated_consolidations = []
            event_updated = False
            
            for consolidation in consolidations:
                if isinstance(consolidation, dict):
                    # If consolidation doesn't have task_id, try to find matching task
                    if "task_id" not in consolidation:
                        person_name = consolidation.get("person_name", "")
                        person_surname = consolidation.get("person_surname", "")
                        person_email = consolidation.get("person_email", "")
                        assigned_to = consolidation.get("assigned_to", "")
                        decision_type = consolidation.get("decision_type", "Commitment")
                        
                        # Try to find matching task
                        task_query = {
                            "is_consolidation_task": True,
                            "type": "consolidation"
                        }
                        
                        # Try multiple search criteria
                        if person_email:
                            task_query["contacted_person.email"] = person_email
                        elif person_name and person_surname:
                            full_name = f"{person_name} {person_surname}"
                            task_query["contacted_person.name"] = {"$regex": full_name, "$options": "i"}
                        
                        if assigned_to:
                            task_query["$or"] = [
                                {"leader_name": assigned_to},
                                {"leader_assigned": assigned_to},
                                {"name": assigned_to}
                            ]
                        
                        task = await tasks_collection.find_one(task_query)
                        
                        if not task and person_name:
                            # Try broader search
                            task = await tasks_collection.find_one({
                                "is_consolidation_task": True,
                                "$or": [
                                    {"consolidation_name": {"$regex": person_name, "$options": "i"}},
                                    {"person_name": {"$regex": person_name, "$options": "i"}},
                                    {"person_surname": {"$regex": person_surname, "$options": "i"}}
                                ]
                            })
                        
                        if task:
                            consolidation["task_id"] = str(task["_id"])
                            consolidation["_migrated"] = True
                            consolidation["_migrated_at"] = datetime.utcnow().isoformat()
                            updates_made += 1
                            event_updated = True
                            logger.info(f"Added task_id {task['_id']} to consolidation for {person_name} {person_surname}")
                        else:
                            logger.warning(f"No matching task found for consolidation: {person_name} {person_surname}")
                            consolidation["_migration_note"] = "No matching task found"
                    
                    updated_consolidations.append(consolidation)
            
            # Update the event if we made changes
            if event_updated:
                await events_collection.update_one(
                    {"_id": event["_id"]},
                    {"$set": {"consolidations": updated_consolidations}}
                )
        
        return {
            "success": True,
            "message": f"Migration complete. Updated {updates_made} consolidations with task_id.",
            "updates_made": updates_made,
            "events_processed": len(events)
        }
        
    except Exception as e:
        logger.error(f"Migration error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Migration failed: {str(e)}"
        )


# ==================== DEBUG: CHECK CONSOLIDATIONS ====================
@app.get("/service-checkin/debug-consolidations")
async def debug_consolidations(
    event_id: str = Query(None, description="Event ID (optional)"),
    current_user: dict = Depends(get_current_user)
):
    """
    Debug endpoint to check consolidation data structure
    """
    try:
        query = {}
        if event_id and ObjectId.is_valid(event_id):
            query["_id"] = ObjectId(event_id)
        elif event_id:
            query["eventName"] = {"$regex": event_id, "$options": "i"}
        
        events = await events_collection.find({"consolidations": {"$exists": True, "$ne": []}, **query}).to_list(None)
        
        debug_results = []
        
        for event in events:
            consolidations = event.get("consolidations", [])
            event_debug = {
                "event_id": str(event.get("_id")),
                "event_name": event.get("eventName", "Unknown"),
                "total_consolidations": len(consolidations),
                "consolidations": []
            }
            
            for i, cons in enumerate(consolidations):
                if isinstance(cons, dict):
                    cons_debug = {
                        "index": i,
                        "has_task_id": "task_id" in cons,
                        "task_id": cons.get("task_id"),
                        "person_name": cons.get("person_name", "Unknown"),
                        "person_email": cons.get("person_email", ""),
                        "assigned_to": cons.get("assigned_to", ""),
                        "decision_type": cons.get("decision_type", "")
                    }
                    
                    # Check if task exists
                    task_id = cons.get("task_id")
                    if task_id and ObjectId.is_valid(task_id):
                        task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
                        cons_debug["task_exists"] = task is not None
                        if task:
                            cons_debug["task_status"] = task.get("status")
                            cons_debug["task_name"] = task.get("name")
                    else:
                        cons_debug["task_exists"] = False
                    
                    event_debug["consolidations"].append(cons_debug)
            
            debug_results.append(event_debug)
        
        return {
            "success": True,
            "debug_results": debug_results,
            "total_events": len(debug_results)
        }
        
    except Exception as e:
        logger.error(f"Debug error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Debug failed: {str(e)}"
        )  
        
        
        
# ==================== CLEANUP ORPHANED TASKS
@app.delete("/tasks/cleanup-orphaned")
async def cleanup_orphaned_tasks(
    user_email: str = Query(None, description="User email to cleanup tasks for (optional)"),
    current_user: dict = Depends(get_current_user)
):
    """
    Clean up orphaned consolidation tasks that don't have corresponding consolidations
    """
    try:
        query = {
            "taskType": "consolidation",
            "status": {"$nin": ["completed", "cancelled", "deleted"]}
        }
        
        if user_email:
            query["assignedfor"] = user_email
        
        # Get all active consolidation tasks
        consolidation_tasks = await tasks_collection.find(query).to_list(None)
        
        deleted_count = 0
        deleted_ids = []
        
        for task in consolidation_tasks:
            task_id = str(task.get("_id"))
            consolidation_id = task.get("consolidation_id")
            person_email = task.get("contacted_person", {}).get("email")
            person_name = task.get("contacted_person", {}).get("name", "")
            
            consolidation_exists = False
            
            # Check if consolidation exists in events
            if consolidation_id:
                # Check in events collection
                event_with_consolidation = await events_collection.find_one({
                    "consolidations.id": consolidation_id,
                    "consolidations.status": {"$ne": "removed"}
                })
                
                if event_with_consolidation:
                    consolidation_exists = True
                else:
                    # Check in consolidations collection
                    consolidation = await consolidations_collection.find_one({
                        "$or": [
                            {"_id": ObjectId(consolidation_id) if ObjectId.is_valid(consolidation_id) else None},
                            {"id": consolidation_id}
                        ],
                        "status": {"$ne": "removed"}
                    })
                    if consolidation:
                        consolidation_exists = True
            
            # Also check by person name/email
            if not consolidation_exists and person_email:
                # Check if person exists in any active consolidation
                event_with_person = await events_collection.find_one({
                    "consolidations.person_email": person_email,
                    "consolidations.status": {"$ne": "removed"}
                })
                if event_with_person:
                    consolidation_exists = True
            
            # If consolidation doesn't exist, delete the task
            if not consolidation_exists:
                delete_result = await tasks_collection.delete_one({"_id": task["_id"]})
                if delete_result.deleted_count > 0:
                    deleted_count += 1
                    deleted_ids.append(task_id)
                    logger.info(f"Deleted orphaned task {task_id} for {person_name}")
        
        return {
            "success": True,
            "message": f"Cleaned up {deleted_count} orphaned consolidation tasks",
            "deleted_count": deleted_count,
            "deleted_ids": deleted_ids
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up orphaned tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Cleanup error: {str(e)}")     
        
        

# ─────────────────────────────────────────────────────────────
# ORG CONFIG ENDPOINTS
# ─────────────────────────────────────────────────────────────
@app.get("/org-config")
async def get_org_config(current_user: dict = Depends(get_current_user)):
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        print(f"ORG CONFIG REQUEST - email: {current_user.get('email')} | org_id in token: {current_user.get('org_id')} | derived org_id: {org_id}")

        config = await org_config_collection.find_one({"_id": org_id})

        if not config:
            sample_people = await people_collection.find(
                {"org_id": org_id}
            ).limit(10).to_list(10)

            standard_fields = {
                "_id", "Name", "Surname", "Email", "Number", "Phone", "Gender",
                "Address", "Birthday", "org_id", "Org_id", "Organisation",
                "DateCreated", "UpdatedAt", "Stage", "InvitedBy", "LeaderId",
                "LeaderPath", "FullName", "Date Created"
            }

            detected_fields = {}
            for person in sample_people:
                for key, value in person.items():
                    if key not in standard_fields and not key.startswith("_") and value:
                        detected_fields[key] = True

            detected_hierarchy = [
                {
                    "level": i + 1,
                    "field": key,
                    "label": key.replace("_", " ").title()
                }
                for i, key in enumerate(detected_fields.keys())
            ]

            print(f"AUTO-DETECTED HIERARCHY for {org_id}: {detected_hierarchy}")

            new_config = {
                "_id": org_id,
                "org_id": org_id,
                "org_name": current_user.get("organization") or current_user.get("org_tag") or org_id,
                "recurring_event_type": "Gatherings",
                "hierarchy": detected_hierarchy,
                "top_leaders": {"male": "", "female": ""},
                "permissions": {
                    "admin_create_event": True,
                    "admin_create_event_type": True,
                },
                "created_at": datetime.utcnow(),
            }
            await org_config_collection.insert_one(new_config)
            new_config.pop("_id", None)
            return new_config

        config["org_id"] = str(config["_id"])
        config.pop("_id", None)
        return config

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/org-config/detect-hierarchy")
async def detect_and_update_hierarchy(current_user: dict = Depends(get_current_user)):
    """
    Re-detect hierarchy from people data and update OrgConfig.
    Can be called by admin to refresh hierarchy after uploading new people data.
    """
    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )

        # Sample people from this org
        sample_people = await people_collection.find(
            {"org_id": org_id}
        ).limit(10).to_list(10)

        if not sample_people:
            raise HTTPException(
                status_code=404,
                detail=f"No people found for org '{org_id}'. Upload people data first."
            )

        # Only exclude truly universal system fields
        standard_fields = {
            "_id", "Name", "Surname", "Email", "Number", "Phone", "Gender",
            "Address", "Birthday", "org_id", "Org_id", "Organisation",
            "DateCreated", "UpdatedAt", "Stage", "InvitedBy", "LeaderId",
            "LeaderPath", "FullName", "Date Created"
        }

        # Collect all non-standard fields that have values
        detected_fields = {}
        for person in sample_people:
            for key, value in person.items():
                if key not in standard_fields and not key.startswith("_") and value:
                    detected_fields[key] = True

        # Build hierarchy from detected fields
        detected_hierarchy = [
            {
                "level": i + 1,
                "field": key,
                "label": key.replace("_", " ").title()
            }
            for i, key in enumerate(detected_fields.keys())
        ]

        print(f"RE-DETECTED HIERARCHY for {org_id}: {detected_hierarchy}")

        # Update OrgConfig
        await org_config_collection.update_one(
            {"_id": org_id},
            {"$set": {
                "hierarchy": detected_hierarchy,
                "updated_at": datetime.utcnow(),
                "updated_by": current_user.get("email"),
                "hierarchy_detected_at": datetime.utcnow(),
            }},
            upsert=True
        )

        return {
            "success": True,
            "org_id": org_id,
            "detected_hierarchy": detected_hierarchy,
            "message": f"Detected {len(detected_hierarchy)} hierarchy levels from people data"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/org-config")
async def update_org_config(
    config_data: dict,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        org_id = (
            current_user.get("org_id") or
            (current_user.get("organization", "").lower().replace(" ", "-")) or
            "active-teams"
        )
        allowed_fields = [
            "org_name", "recurring_event_type", "hierarchy",
            "top_leaders", "allows_create_event", "allows_create_event_type",
        ]
        update = {k: v for k, v in config_data.items() if k in allowed_fields}
        update["updated_at"] = datetime.utcnow()
        update["updated_by"] = current_user.get("email")

        await org_config_collection.update_one(
            {"_id": org_id},
            {"$set": update},
            upsert=True
        )
        return {"success": True, "message": "Config updated"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/detect-hierarchy")
async def detect_hierarchy_from_people(
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        sample_people = await people_collection.find({}).limit(10).to_list(10)

        if not sample_people:
            return {"detected_hierarchy": [], "message": "No people data found"}

        all_fields = set()
        for person in sample_people:
            all_fields.update(person.keys())

        hierarchy_keywords = [
            "leader", "pastor", "zone", "district",
            "region", "overseer", "bishop", "elder",
            "shepherd", "mentor", "coach"
        ]

        hierarchy_fields = []
        for field in all_fields:
            field_lower = field.lower()
            if field.startswith("_") or field in [
                "Name", "Surname", "Email", "Phone",
                "Gender", "created_at", "updated_at", "role"
            ]:
                continue
            if any(kw in field_lower for kw in hierarchy_keywords):
                num_match = re.search(r'\d+', field)
                level_num = int(num_match.group()) if num_match else 999
                hierarchy_fields.append({
                    "field": field,
                    "label": field,
                    "level_num": level_num
                })

        hierarchy_fields.sort(key=lambda x: x["level_num"])
        detected = [
            {"level": i + 1, "field": hf["field"], "label": hf["field"]}
            for i, hf in enumerate(hierarchy_fields)
        ]

        return {
            "detected_hierarchy": detected,
            "message": f"Detected {len(detected)} hierarchy levels",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_top_leader_dynamic(gender: str, org_id: str = "active-teams") -> str:
    try:
        config = await org_config_collection.find_one({"_id": org_id})
        gender_lower = gender.lower().strip()
        is_female = gender_lower in ["female", "f", "woman", "lady", "girl"]
        is_male   = gender_lower in ["male", "m", "man", "gentleman", "boy"]

        if config and config.get("top_leaders"):
            top = config["top_leaders"]
            if is_female: return top.get("female", "")
            if is_male:   return top.get("male", "")
            return ""

        # Fallback
        if is_female: return "Vicky Enslin"
        if is_male:   return "Gavin Enslin"
        return ""

    except Exception as e:
        print(f"Error in get_top_leader_dynamic: {e}")
        if "female" in gender.lower(): return "Vicky Enslin"
        if "male" in gender.lower():   return "Gavin Enslin"
        return ""
    
COLUMN_MAP: dict[str, str] = {
    # Name
    "personname":    "name",
    "full name":     "name",
    "fullname":      "name",
    "name":          "name",
    "firstname":     "name",
    "first name":    "name",
    "first_name":    "name",

    # Surname
    "familytag":     "surname",
    "surname":       "surname",
    "last name":     "surname",
    "lastname":      "surname",
    "family name":   "surname",
    "last_name":     "surname",

    # Phone
    "cellnumber":    "number",
    "phone":         "number",
    "number":        "number",
    "mobile":        "number",
    "cell":          "number",
    "phonenumber":   "number",
    "phone_number":  "number",
    "tel":           "number",

    # Email
    "sacredemail":   "email",
    "email":         "email",
    "emailaddress":  "email",
    "email address": "email",
    "email_address": "email",

    # Address
    "homebase":      "address",
    "address":       "address",
    "home address":  "address",
    "homeaddress":   "address",
    "location":      "address",
    "city":          "address",

    # Birthday
    "birthstar":     "birthday",
    "birthday":      "birthday",
    "dob":           "birthday",
    "date of birth": "birthday",
    "dateofbirth":   "birthday",
    "birth date":    "birthday",
    "birthdate":     "birthday",

    # Gender
    "gender":        "gender",
    "sex":           "gender",

    # Direct leader (used for LeaderPath resolution)
    "shepherd":      "invitedby",
    "invitedby":     "invitedby",
    "invited by":    "invitedby",
    "invited_by":    "invitedby",
    "direct leader": "invitedby",
    "directleader":  "invitedby",
    "leader":        "invitedby",
    "discipledby":   "invitedby",
    "disciple of":   "invitedby",

    # Stage / ministry track
    "stage":         "stage",
    "ministry":      "stage",
    "status":        "stage",

    # Organization
    "organization":  "organization",
    "organisation":  "organization",
    "church":        "organization",
    "org":           "organization",
    "churchname":    "organization",
    "church_name":   "organization",

    # Date joined
    "joinedscroll":  "date_created",
    "joined":        "date_created",
    "date joined":   "date_created",
    "datejoined":    "date_created",
    "created":       "date_created",
    "date created":  "date_created",

    # Top-level leader name — stored as a plain string, not used for path building
    "seniorpastor":  "senior_pastor",
    "senior pastor": "senior_pastor",

    # Silently ignored — computed/redundant columns
    "shepherdtrail":       "_ignore",
    "shepherd trail":      "_ignore",
    "lastheavenupdate":    "_ignore",
    "last heaven update":  "_ignore",
    "updatedat":           "_ignore",
    "updated at":          "_ignore",
    }


def _clean_col(col: str) -> str:
    return re.sub(r"\s+", " ", str(col).strip().lower())


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename, drop = {}, []
    for col in df.columns:
        key = _clean_col(col)
        mapped = COLUMN_MAP.get(key)
        if mapped is None or mapped == "_ignore":
            drop.append(col)
        else:
            rename[col] = mapped
    df = df.drop(columns=drop, errors="ignore")
    df = df.rename(columns=rename)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _safe_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _parse_birthday(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.strftime("%Y/%m/%d")
    return str(val).strip().replace("-", "/")


def _build_sheet_name_map(df: pd.DataFrame) -> dict[str, int]:
    """
    Maps every person's full name (lower) and first name (lower) to their
    row index. Built from the sheet itself — no DB involved.
    First-name-only is a fallback and will be overwritten if two people
    share a first name.
    """
    mapping: dict[str, int] = {}
    for idx, row in df.iterrows():
        name    = _safe_str(row.get("name",    "")).title()
        surname = _safe_str(row.get("surname", "")).title()
        full    = f"{name} {surname}".strip().lower()
        if full:
            mapping[full] = idx
        first = name.strip().lower()
        if first and first not in mapping:
            mapping[first] = idx
    return mapping


def _resolve_sheet_path(
    invitedby_name: str,
    sheet_name_map: dict[str, int],
    sheet_id_map:   dict[int, str],
    sheet_path_map: dict[int, list[str]],
) -> tuple[Optional[str], list[str]]:
    """
    Pure in-memory resolution — no DB calls.

    Returns (direct_leader_id_str, full_root_first_path_str_list).

    LeaderPath is root-first:
        [root, ..., inviter_parent, inviter]

    inviter's already-resolved path + inviter's own ID = this person's path.
    """
    clean = invitedby_name.strip().lower()
    if not clean:
        return None, []

    inviter_row = sheet_name_map.get(clean)
    if inviter_row is None:
        return None, []

    inviter_id = sheet_id_map.get(inviter_row)
    if not inviter_id:
        return None, []

    inviter_path = sheet_path_map.get(inviter_row, [])
    # root-first: inviter's ancestors + inviter
    return inviter_id, inviter_path + [inviter_id]


async def _import_rows(
    df: pd.DataFrame,
    default_organization: str,
    dry_run: bool,
    current_user: dict,
) -> dict:
    now = datetime.utcnow()
    results = {"inserted": 0, "skipped": 0, "errors": 0, "dry_run": dry_run, "rows": []}

    # ── PASS 1: pre-assign a stable ObjectId to every row ─────────────────
    # These IDs are used as references in LeaderPath *before* anything is
    # written to MongoDB, so cross-row references are always valid.
    sheet_id_map:   dict[int, str]       = {}
    sheet_path_map: dict[int, list[str]] = {}

    for idx in df.index:
        sheet_id_map[idx] = str(ObjectId())

    # ── PASS 2: build name → row-index lookup from the sheet ──────────────
    sheet_name_map = _build_sheet_name_map(df)

    # ── PASS 3: resolve every row's LeaderPath from sheet data only ───────
    # We loop until nothing changes. This handles any row ordering — a
    # disciple can appear before their shepherd and still get the correct
    # path once the shepherd's own path stabilises in a later pass.
    for _ in range(len(df) + 1):
        changed = False
        for idx, row in df.iterrows():
            invitedby = _safe_str(row.get("invitedby", ""))
            if not invitedby:
                new_path: list[str] = []
            else:
                _, new_path = _resolve_sheet_path(
                    invitedby, sheet_name_map, sheet_id_map, sheet_path_map
                )
            if new_path != sheet_path_map.get(idx):
                sheet_path_map[idx] = new_path
                changed = True
        if not changed:
            break

    # ── PASS 4: validate + insert ──────────────────────────────────────────
    for idx, row in df.iterrows():
        row_num = idx + 2   # 1-indexed + header row

        name         = _safe_str(row.get("name",         "")).title()
        surname      = _safe_str(row.get("surname",      "")).title()
        email        = _safe_str(row.get("email",        "")).lower()
        number       = _safe_str(row.get("number",       ""))
        address      = _safe_str(row.get("address",      ""))
        birthday     = _parse_birthday(row.get("birthday"))
        gender       = _safe_str(row.get("gender",       "")).capitalize()
        stage        = "Win"
        invitedby    = _safe_str(row.get("invitedby",    ""))
        senior_pastor = _safe_str(row.get("senior_pastor", ""))

        row_org = _safe_str(row.get("organization", ""))
        org = default_organization or row_org or (
            current_user.get("Organization") or
            current_user.get("organization") or ""
        )

        # date_created
        raw_dc = row.get("date_created")
        if raw_dc and not (isinstance(raw_dc, float) and pd.isna(raw_dc)):
            try:
                dc_str = (
                    pd.Timestamp(raw_dc).isoformat()
                    if not isinstance(raw_dc, str)
                    else str(raw_dc)
                )
            except Exception:
                dc_str = now.isoformat()
        else:
            dc_str = now.isoformat()

        # ── validation ──────────────────────────────────────────────────────
        if not name or not surname:
            results["skipped"] += 1
            results["rows"].append({
                "row": row_num, "status": "skipped",
                "reason": "Missing name or surname",
                "person": f"{name} {surname}".strip(),
            })
            continue

        if email:
            existing = await people_collection.find_one(
                {"Email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
            )
            if existing:
                results["skipped"] += 1
                results["rows"].append({
                    "row": row_num, "status": "skipped",
                    "reason": f"Email '{email}' already exists",
                    "person": f"{name} {surname}",
                })
                continue

        # ── resolve leader fields from pre-computed sheet maps ─────────────
        pre_assigned_id = sheet_id_map[idx]
        path_strs       = sheet_path_map.get(idx, [])

        # Convert string IDs → ObjectIds for MongoDB storage
        leader_id_obj: Optional[ObjectId] = None
        if path_strs:
            try:
                leader_id_obj = ObjectId(path_strs[-1])
            except Exception:
                pass

        leader_path: list[ObjectId] = []
        for s in path_strs:
            try:
                leader_path.append(ObjectId(s))
            except Exception:
                pass

        # ── build document ──────────────────────────────────────────────────
        person_doc = {
            "_id":          ObjectId(pre_assigned_id),  # stable pre-assigned ID
            "Name":         name,
            "Surname":      surname,
            "Email":        email,
            "Number":       number,
            "Address":      address,
            "Gender":       gender,
            "Birthday":     birthday,
            "InvitedBy":    invitedby,
            "SeniorPastor": senior_pastor,              # plain string, informational
            "Stage":        stage,
            "Organization": org,
            "LeaderId":     leader_id_obj,              # ObjectId | None
            "LeaderPath":   leader_path,                # [ObjectId, ...] root-first
            "DateCreated":  dc_str,
            "UpdatedAt":    now.isoformat(),
            "imported_by":  current_user.get("email", "unknown"),
        }

        if dry_run:
            results["inserted"] += 1
            results["rows"].append({
                "row":          row_num,
                "status":       "would_insert",
                "person":       f"{name} {surname}",
                "email":        email,
                "organization": org,
                "invitedby":    invitedby,
                "senior_pastor": senior_pastor,
                "leader_id":    str(leader_id_obj) if leader_id_obj else None,
                "leader_path":  path_strs,              # already strings
            })
        else:
            try:
                await people_collection.insert_one(person_doc)

                results["inserted"] += 1
                results["rows"].append({
                    "row":          row_num,
                    "status":       "inserted",
                    "person":       f"{name} {surname}",
                    "email":        email,
                    "organization": org,
                    "_id":          pre_assigned_id,
                    "invitedby":    invitedby,
                    "senior_pastor": senior_pastor,
                    "leader_id":    str(leader_id_obj) if leader_id_obj else None,
                    "leader_path":  path_strs,
                })

                # sync user account if one exists with this email
                if email:
                    await users_collection.update_one(
                        {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}},
                        {"$set": {
                            "LeaderId":   leader_id_obj,
                            "LeaderPath": leader_path,
                            "people_id":  pre_assigned_id,
                            "updated_at": now.isoformat(),
                        }}
                    )

            except Exception as exc:
                results["errors"] += 1
                results["rows"].append({
                    "row":    row_num,
                    "status": "error",
                    "person": f"{name} {surname}",
                    "error":  str(exc),
                })

    return results


@app.post("/people/import/spreadsheet")
async def import_people_from_spreadsheet(
    file:         UploadFile = File(...),
    organization: Optional[str] = Query(None),
    dry_run:      bool = Query(False),
    current_user: dict = Depends(get_current_user),
):
    filename  = file.filename or ""
    ext       = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    raw_bytes = await file.read()

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        if ext in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
        elif ext == "csv":
            df = pd.read_csv(
                io.StringIO(raw_bytes.decode("utf-8-sig", errors="replace")),
                dtype=str,
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type '.{ext}'. Use .xlsx, .xls, or .csv.",
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}")

    if df.empty:
        raise HTTPException(status_code=400, detail="The file contains no data rows.")

    original_columns = list(df.columns)
    df = _normalise_columns(df)
    df = df.where(pd.notna(df), None)

    results = await _import_rows(
        df=df,
        default_organization=organization or "",
        dry_run=dry_run,
        current_user=current_user,
    )
    
    if not dry_run:
        # After successful import, invalidate the cache
        await invalidate_people_cache("import", {
            "inserted": results["inserted"],
            "skipped": results["skipped"],
            "errors": results["errors"]
        })

    return {
        "success":          True,
        "dry_run":          dry_run,
        "file":             filename,
        "total_rows":       len(df),
        "inserted":         results["inserted"],
        "skipped":          results["skipped"],
        "errors":           results["errors"],
        "original_columns": original_columns,
        "mapped_columns":   list(df.columns),
        "rows":             results["rows"],
    }


@app.post("/people/import/preview-columns")
async def preview_spreadsheet_columns(
    file: UploadFile = File(...),
):
    filename  = file.filename or ""
    ext       = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    raw_bytes = await file.read()
    print(f"DEBUG preview: filename={filename!r}, ext={ext!r}, size={len(raw_bytes)}")

    try:
        if ext in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
        elif ext == "csv":
            df = pd.read_csv(
                io.StringIO(raw_bytes.decode("utf-8-sig", errors="replace")),
                dtype=str,
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type '.{ext}'.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}")

    original_columns = list(df.columns)
    column_mapping = []

    for col in original_columns:
        key    = _clean_col(col)
        mapped = COLUMN_MAP.get(key)
        if mapped is None or mapped == "_ignore":
            column_mapping.append({"original": col, "maps_to": None, "status": "ignored"})
        else:
            column_mapping.append({"original": col, "maps_to": mapped, "status": "mapped"})

    df_mapped = _normalise_columns(df)
    df_mapped = df_mapped.where(pd.notna(df_mapped), None)

    raw_sample = df_mapped.head(3).to_dict(orient="records")
    sample = []
    for row in raw_sample:
        sample.append({
            k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
            for k, v in row.items()
        })

    return {
        "success":        True,
        "file":           filename,
        "total_rows":     len(df),
        "column_mapping": column_mapping,
        "ignored_columns": [c["original"] for c in column_mapping if c["status"] == "ignored"],
        "sample_rows":    sample,
    }  