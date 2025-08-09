import os
import re
import secrets
from typing import Optional, List
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Depends, Body, Path
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, time as time_type, timedelta
from auth.models import (
    Event,
    CheckIn,
    UncaptureRequest,
    UserCreate,
    UserLogin,
    CellEventCreate,
    AddMembersRequest,
    RefreshTokenRequest
)
from auth.utils import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
    require_role,
)

# load env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
JWT_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))

# Weekday map used for recurring events
WEEKDAY_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

# FastAPI app
app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection using motor
client = AsyncIOMotorClient(MONGO_URI)
db = client["active-teams-db"]
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]


@app.on_event("startup")
async def startup_indexes_and_defaults():
    # create helpful indexes
    await people_collection.create_index([("Name", 1)])
    await users_collection.create_index([("email", 1)], unique=True)
    await events_collection.create_index([("type", 1)])

    # ensure existing events have total_attendance
    await events_collection.update_many({"total_attendance": {"$exists": False}}, {"$set": {"total_attendance": 0}})

# -------------------------
# Helpers
# -------------------------
def parse_time_string(t: Optional[str]) -> Optional[time_type]:
    if not t:
        return None
    try:
        hh, mm = t.split(":")
        return time_type(int(hh), int(mm))
    except Exception:
        return None


async def get_leader_cell_name_async(leader_id: str) -> str:
    try:
        doc = await users_collection.find_one({"_id": ObjectId(leader_id)})
    except Exception:
        doc = await users_collection.find_one({"user_id": leader_id})
    if doc:
        if "cell_name" in doc and doc["cell_name"]:
            return doc["cell_name"]
        name_parts = []
        if doc.get("name"):
            name_parts.append(doc["name"])
        if doc.get("surname"):
            name_parts.append(doc["surname"])
        if name_parts:
            return " ".join(name_parts) + "'s cell"
    return f"Cell of {leader_id}"


def get_next_occurrence_single(start_dt: datetime, recurring_day: str) -> datetime:
    if recurring_day is None:
        return start_dt

    target_weekday = WEEKDAY_MAP[recurring_day.lower()]
    today = datetime.utcnow().date()
    today_weekday = today.weekday()
    days_ahead = (target_weekday - today_weekday) % 7

    candidate_date = today + timedelta(days=days_ahead)

    start_date_only = start_dt.date()
    if candidate_date < start_date_only:
        candidate_date = candidate_date + timedelta(days=7)

    return datetime.combine(candidate_date, start_dt.time())

# -------------------------
# Auth: Signup / Login / Refresh / Logout
# -------------------------
# http://localhost:8000/signup
PASSWORD_REGEX = re.compile(r"^(?=.*[A-Za-z])(?=.*\d).{8,}$")

@app.post("/signup")
async def signup(user: UserCreate):
    existing = await users_collection.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    if not PASSWORD_REGEX.match(user.password):
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters and include letters and numbers",
        )

    role = getattr(user, "role", None) or "user"

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
        "role": role,
        "created_at": datetime.utcnow(),
        # refresh token fields reserved
        "refresh_token_id": None,
        "refresh_token_hash": None,
        "refresh_token_expires": None,
    }
    await users_collection.insert_one(user_dict)
    return {"message": "User created successfully"}

# http://localhost:8000/login
@app.post("/login")
async def login(user: UserLogin):
    existing = await users_collection.find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token_expires = timedelta(minutes=JWT_EXPIRE_MINUTES)
    token = create_access_token(
        {"user_id": str(existing["_id"]), "email": existing["email"], "role": existing.get("role", "registrant")},
        expires_delta=token_expires,
    )

    # generate refresh token id + token
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
        }},
    )

    return {
        "access_token": token,
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
async def logout(current=Depends(get_current_user)):
    user_id = current.get("user_id")
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

# -------------------------
# Events (admin only)
# -------------------------
# http://localhost:8000/events
@app.post("/events", dependencies=[Depends(require_role("admin"))])
async def create_event(event: Event, current=Depends(get_current_user)):
    try:
        event_data = event.dict()
        # ensure date stored as datetime
        try:
            if isinstance(event_data.get("date"), str):
                event_data["date"] = datetime.fromisoformat(event_data["date"])
        except Exception:
            event_data["date"] = datetime.utcnow()

        if "attendees" not in event_data or event_data["attendees"] is None:
            event_data["attendees"] = []
        event_data["created_by"] = current.get("user_id")
        event_data["created_at"] = datetime.utcnow()
        event_data.setdefault("total_attendance", 0)
        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Get a single event by id (admin only)
# http://localhost:8000/login/id
@app.get("/events/{event_id}", dependencies=[Depends(require_role("admin"))])
async def get_event(event_id: str = Path(...)):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        event["id"] = str(event["_id"])
        event.pop("_id", None)
        if isinstance(event.get("date"), datetime):
            event["date"] = event["date"].isoformat()
        return event
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Update an event (admin only)
# http://localhost:8000/events/{event_id}
@app.put("/events/{event_id}", dependencies=[Depends(require_role("admin"))])
async def update_event(event_id: str, updated_event: Event):
    try:
        event_data = updated_event.dict()
        # Convert date if string
        if isinstance(event_data.get("date"), str):
            try:
                event_data["date"] = datetime.fromisoformat(event_data["date"])
            except Exception:
                event_data["date"] = datetime.utcnow()

        update_result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": event_data},
        )
        if update_result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"message": "Event updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Delete an event (admin only)
# http://localhost:8000/events/{event_id}
@app.delete("/events/{event_id}", dependencies=[Depends(require_role("admin"))])
async def delete_event(event_id: str):
    try:
        delete_result = await events_collection.delete_one({"_id": ObjectId(event_id)})
        if delete_result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"message": "Event deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# -------------------------
# Search People
# -------------------------
# http://localhost:8000/people/search
@app.get("/people/search", dependencies=[Depends(require_role("admin", "registrant"))])
async def search_people(name: str = Query(..., min_length=1)):
    try:
        cursor = people_collection.find({"Name": {"$regex": name, "$options": "i"}}).limit(50)
        people = []
        async for p in cursor:
            people.append({"_id": str(p["_id"]), "Name": p["Name"]})
        return {"results": people}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Check-in (registrant or admin)
# -------------------------
# http://localhost:8000/checkin
@app.post("/checkin", dependencies=[Depends(require_role("registrant", "admin"))])
async def check_in_person(checkin: CheckIn, current=Depends(get_current_user)):
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
            "performed_by": current.get("user_id"),
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
# View Check-ins (registrant/admin required)
# -------------------------
@app.get("/checkins/{event_id}", dependencies=[Depends(require_role("registrant", "admin"))])
async def get_checkins(event_id: str, current=Depends(get_current_user)):
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

# -------------------------
# Uncapture (registrant or admin)
# -------------------------
# http://localhost:8000/uncapture
@app.post("/uncapture", dependencies=[Depends(require_role("registrant", "admin"))])
async def uncapture_person(data: UncaptureRequest, current=Depends(get_current_user)):
    try:
        # Pull the attendee and decrement
        update_result = await events_collection.update_one(
            {"_id": ObjectId(data.event_id)},
            {"$pull": {"attendees": {"name": data.name}}, "$inc": {"total_attendance": -1}},
        )
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found or already removed")

        # create an audit log entry (append to a separate field)
        audit_entry = {
            "action": "uncapture",
            "name": data.name,
            "time": datetime.utcnow(),
            "performed_by": current.get("user_id"),
        }
        await events_collection.update_one({"_id": ObjectId(data.event_id)}, {"$push": {"audit_log": audit_entry}})

        return {"message": f"{data.name} removed from check-ins."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Cell event creation and management
# -------------------------
# http://localhost:8000/events/cell
@app.post("/events/cell", dependencies=[Depends(require_role("admin"))])
async def create_cell_event(payload: CellEventCreate, current=Depends(get_current_user)):
    try:
        if payload.recurring and not payload.recurring_day:
            raise HTTPException(status_code=400, detail="recurring_day is required when recurring=True")

        start_dt = payload.start_date
        parsed_time = parse_time_string(payload.start_time)
        if parsed_time:
            start_dt = datetime.combine(start_dt.date(), parsed_time)

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
            "created_by": current.get("user_id"),
            "created_at": datetime.utcnow(),
            "total_attendance": 0,
        }

        result = await events_collection.insert_one(event_doc)
        return {"message": "Cell event created", "id": str(result.inserted_id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Capturing people into cells
# http://localhost:8000/events/cell/{event_id}/members
@app.post("/events/cell/{event_id}/members")
async def add_members_to_cell_event(event_id: str, payload: AddMembersRequest, current=Depends(get_current_user)):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
        if not event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        role = current.get("role")
        user_id = current.get("user_id")
        if role != "admin" and str(event.get("leader_id")) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized to modify this cell event")

        valid_member_ids: List[str] = []
        for mid in payload.member_ids:
            try:
                person = await people_collection.find_one({"_id": ObjectId(mid)})
            except Exception:
                person = await people_collection.find_one({"Name": {"$regex": f"^{mid}$", "$options": "i"}})
            if not person:
                continue
            valid_member_ids.append(str(person["_id"]))

        if not valid_member_ids:
            raise HTTPException(status_code=400, detail="No valid member ids provided")

        await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$addToSet": {"members": {"$each": valid_member_ids}}},
        )
        return {"message": f"Added {len(valid_member_ids)} members to the cell event"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/cell")
# http://localhost:8000/events/cell
async def list_cell_events(current=Depends(get_current_user)):
    try:
        role = current.get("role")
        user_id = current.get("user_id")
        if role == "admin":
            cursor = events_collection.find({"type": "cell"})
        else:
            cursor = events_collection.find({"type": "cell", "leader_id": user_id})

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

# Removing people from cell
# http://localhost:8000/events/cell/{event_id}/members/{member_id}
@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str, current=Depends(get_current_user)):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
        if not event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        role = current.get("role")
        user_id = current.get("user_id")
        if role != "admin" and str(event.get("leader_id")) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized")

        update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": member_id}})
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Member not found on event")
        return {"message": "Member removed"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
