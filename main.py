import os
from datetime import datetime, timedelta
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Depends, Path
from fastapi.middleware.cors import CORSMiddleware
from auth.models import Event, CheckIn, UncaptureRequest, UserCreate, UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest
from auth.utils import hash_password, verify_password, require_role, get_current_user, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token
import math
import secrets
from database import db, events_collection, people_collection, users_collection
from auth.email_utils import send_reset_password_email

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

# SIGNUP AND LOGIN ENDPOINTS
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


# --- FORGOT PASSWORD ---
@app.post("/forgot-password")
async def forgot_password(email: str = Body(...)):
    user = await users_collection.find_one({"email": email})
    if not user:
        # To avoid leaking info, respond success anyway
        return {"message": "If your email exists, you will receive a password reset email shortly."}

    # Generate a reset token (you can use a JWT or a secure random token)
    reset_token = create_access_token({"user_id": str(user["_id"])}, expires_delta=timedelta(hours=1))
    reset_link = f"https://yourfrontend.com/reset-password?token={reset_token}"

    status_code = send_reset_password_email(email, reset_link)
    if not status_code or status_code >= 400:
        raise HTTPException(status_code=500, detail="Failed to send reset email")

    return {"message": "If your email exists, you will receive a password reset email shortly."}


# --- RESET PASSWORD ---
# http://localhost:8000/reset-password
@app.post("/reset-password")
async def reset_password(data: ResetPasswordRequest):
    user = await users_collection.find_one({
        "reset_password_token": data.token,
        "reset_password_expires": {"$gt": datetime.utcnow()}
    })
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    hashed_pw = hash_password(data.new_password)
    await users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {"password": hashed_pw},
         "$unset": {"reset_password_token": "", "reset_password_expires": ""}}
    )

    return {"message": "Password has been reset successfully."}

# EVENT ENDPOINTS
# http://localhost:8000/event
@app.post("/event", dependencies=[Depends(require_role("admin"))])
async def create_event(event: Event):
    try:
        event_data = event.dict()
        event_data["date"] = datetime.fromisoformat(event_data["date"])
        if "attendees" not in event_data:
            event_data["attendees"] = []
        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}
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
# http://localhost:8000/events/{event_id}/checkin
@app.post("/events/{event_id}/checkin", dependencies=[Depends(require_role("registrant", "admin"))])
async def checkin_single_member_to_cell(event_id: str, data: AddMemberNamesRequest, current=Depends(get_current_user)):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    role = current.get("role")
    user_id = current.get("user_id")
    if role != "admin" and str(event.get("leader_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized")

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

    await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {
            "$push": {"members": member_obj},
            "$inc": {"total_attendance": 1}
        }
    )

    return {"message": f"{person['Name']} checked in successfully to the cell event."}

# http://localhost:8000/events/6897bcb2016b71e188b5119a/uncheckin
@app.post("/events/{event_id}/uncheckin", dependencies=[Depends(require_role("registrant", "admin"))])
async def uncheckin_single_member(event_id: str, data: RemoveMemberRequest, current=Depends(get_current_user)):
    event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
    if not event:
        raise HTTPException(status_code=404, detail="Cell event not found")

    role = current.get("role")
    user_id = current.get("user_id")
    if role != "admin" and str(event.get("leader_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized")

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

# CHECKIN AND UNCHECKIN ENDPOINTS
@app.post("/checkin", dependencies=[Depends(require_role("registrant", "admin"))])
async def check_in_person(checkin: CheckIn):
    try:
        event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        person = await people_collection.find_one({"Name": {"$regex": f"^{checkin.name}$", "$options": "i"}})
        if not person:
            raise HTTPException(status_code=400, detail="Person not found in people database")

        already_checked = any(a["name"].lower() == checkin.name.lower() for a in event.get("attendees", []))
        if already_checked:
            raise HTTPException(status_code=400, detail="Person already checked in")

        await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {
                "$push": {"attendees": {"name": checkin.name, "time": datetime.now()}},
                "$inc": {"total_attendance": 1}
            }
        )
        return {"message": f"{checkin.name} checked in successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/checkins/{event_id}
@app.get("/checkins/{event_id}", dependencies=[Depends(require_role("registrant", "admin"))])
async def get_checkins(event_id: str):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        event = sanitize_document(event)
        return {
            "event_id": event_id,
            "service_name": event.get("service_name"),
            "attendees": event.get("attendees", []),
            "total_attendance": event.get("total_attendance", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/uncapture
@app.post("/uncapture", dependencies=[Depends(require_role("registrant", "admin"))])
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

# PEOPLE ENDPOINTS
# http://localhost:8000/people?page=1&perPage=10
@app.get("/people", dependencies=[Depends(require_role("admin", "registrant"))])
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
            query["DateOfBirth"] = dob
        if location:
            query["Location"] = {"$regex": location, "$options": "i"}
        if leader:
            query["Leader"] = {"$regex": leader, "$options": "i"}
        if stage:
            query["Stage"] = {"$regex": stage, "$options": "i"}

        cursor = people_collection.find(query).skip(skip).limit(perPage)
        people_list = []
        async for person in cursor:
            person["_id"] = str(person["_id"])
            person = sanitize_document(person)
            people_list.append(person)

        total_count = await people_collection.count_documents(query)
        return {
            "page": page,
            "perPage": perPage,
            "total": total_count,
            "results": people_list
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/people/search", dependencies=[Depends(require_role("admin", "registrant"))])
async def search_people(name: str = Query(..., min_length=1)):
    try:
        cursor = people_collection.find({"Name": {"$regex": name, "$options": "i"}})
        people = []
        async for p in cursor:
            p["_id"] = str(p["_id"])
            p = sanitize_document(p)
            people.append({"_id": p["_id"], "Name": p["Name"]})
        return {"results": people}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/people/{person_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_person_by_id(person_id: str = Path(...)):
    try:
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        person["_id"] = str(person["_id"])
        person = sanitize_document(person)
        return person
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/people", dependencies=[Depends(require_role("registrant", "admin"))])
async def create_or_update_person(person_data: dict = Body(...)):
    try:
        if "_id" in person_data:  # Update existing person
            person_id = person_data["_id"]
            del person_data["_id"]
            result = await people_collection.update_one(
                {"_id": ObjectId(person_id)},
                {"$set": person_data}
            )
            if result.modified_count == 0:
                raise HTTPException(status_code=404, detail="Person not found or no changes made")
            return {"message": "Person updated successfully"}
        else:  # Create new person
            result = await people_collection.insert_one(person_data)
            return {"message": "Person created successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/people/{person_id}", dependencies=[Depends(require_role("registrant", "admin"))])
@app.delete("/person/{person_id}", dependencies=[Depends(require_role("registrant", "admin"))])
async def delete_person(person_id: str = Path(...)):
    try:
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
