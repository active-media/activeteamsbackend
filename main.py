
import os
from datetime import datetime, timedelta
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from auth.models import EventBase, CheckIn, UncaptureRequest, UserCreate, UserLogin, CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest, RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest, TaskModel, PersonCreate
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token
import math
import secrets
from database import db, events_collection, people_collection, users_collection
from auth.email_utils import send_reset_password_email
from typing import Optional, Literal, List

YOCO_SECRET_KEY = os.getenv("YOCO_SECRET_KEY")  # Secret key (backend only)
YOCO_PUBLIC_KEY = os.getenv("YOCO_PUBLIC_KEY")  # Publishable key (frontend)

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
# http://localhost:8000/event
@app.post("/event")
async def create_event(event: EventBase):
    try:
        event_data = event.dict()
        event_data["date"] = datetime.fromisoformat(event_data["date"])
        if "attendees" not in event_data:
            event_data["attendees"] = []
        result = await events_collection.insert_one(event_data)
        return {"message": "Event created", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/events
@app.get("/events")
async def get_all_events():
    try:
        events = []
        cursor = events_collection.find()
        async for event in cursor:
            event["_id"] = str(event["_id"])
            # Convert datetime objects to ISO strings for JSON serialization
            if "date" in event and isinstance(event["date"], datetime):
                event["date"] = event["date"].isoformat()
            if "start_date" in event and isinstance(event["start_date"], datetime):
                event["start_date"] = event["start_date"].isoformat()
            if "created_at" in event and isinstance(event["created_at"], datetime):
                event["created_at"] = event["created_at"].isoformat()
            
            event = sanitize_document(event)
            events.append(event)
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/events/type/{event_type}
@app.get("/events/type/{event_type}")
async def get_events_by_type(event_type: str = Path(...)):
    try:
        events = []
        # Use "type" field to match your existing cell events structure
        cursor = events_collection.find({"type": event_type})
        async for event in cursor:
            event["_id"] = str(event["_id"])
            # Convert datetime objects to ISO strings for JSON serialization
            if "date" in event and isinstance(event["date"], datetime):
                event["date"] = event["date"].isoformat()
            if "start_date" in event and isinstance(event["start_date"], datetime):
                event["start_date"] = event["start_date"].isoformat()
            if "created_at" in event and isinstance(event["created_at"], datetime):
                event["created_at"] = event["created_at"].isoformat()
            
            event = sanitize_document(event)
            events.append(event)
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/events/{event_id}
@app.put("/events/{event_id}")
async def update_event(event: EventBase, event_id: str = Path(...)):
    try:
        existing_event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not existing_event:
            raise HTTPException(status_code=404, detail="Event not found")
        
        update_data = event.dict()
        if "date" in update_data and isinstance(update_data["date"], str):
            update_data["date"] = datetime.fromisoformat(update_data["date"])
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Event not found or no changes made")
        
        return {"message": "Event updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/events/{event_id}
@app.delete("/events/{event_id}")
async def delete_event(event_id: str = Path(...)):
    try:
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
        raise HTTPException(status_code=500, detail=str(e))
    
# -------------------------
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

    await events_collection.update_one(
        {"_id": ObjectId(event_id)},
        {
            "$push": {"members": member_obj},
            "$inc": {"total_attendance": 1}
        }
    )

    return {"message": f"{person['Name']} checked in successfully to the cell event."}

# http://localhost:8000/events/{event_id}/uncheckin
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
        raise HTTPException(status_code=500, detail=str(e))

# Removing people from cell
# http://localhost:8000/events/cell/{event_id}/members/{member_id}
@app.delete("/events/cell/{event_id}/members/{member_id}")
async def remove_member_from_cell(event_id: str, member_id: str):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id), "type": "cell"})
        if not event:
            raise HTTPException(status_code=404, detail="Cell event not found")

        update_result = await events_collection.update_one({"_id": ObjectId(event_id)}, {"$pull": {"members": member_id}})
        if update_result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Member not found on event")
        return {"message": "Member removed"}
    except HTTPException:
        raise
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


# async def get_people(
#     page: int = Query(1, ge=1),
#     perPage: int = Query(100, ge=1, le=500),
#     name: Optional[str] = None,
#     gender: Optional[str] = None,
#     dob: Optional[str] = None,
#     location: Optional[str] = None,
#     leader: Optional[str] = None,
#     stage: Optional[str] = None
# ):
#     try:
#         # Compute the skip value for pagination
#         skip = (page - 1) * perPage

#         query = {}

#         # Construct the query based on provided parameters
#         if name:
#             query["Name"] = {"$regex": name, "$options": "i"}
#         if gender:
#             query["Gender"] = {"$regex": gender, "$options": "i"}
#         if dob:
#             query["DateOfBirth"] = dob
#         if location:
#             query["Location"] = {"$regex": location, "$options": "i"}
#         if leader:
#             query["$or"] = [
#                 {"Leader @12": {"$regex": leader, "$options": "i"}},
#                 {"Leader @144": {"$regex": leader, "$options": "i"}},
#                 {"Leader @ 1728": {"$regex": leader, "$options": "i"}}
#             ]
#         if stage:
#             query["Stage"] = {"$regex": stage, "$options": "i"}

#         # Fetch the people list with pagination and apply the query
#         cursor = people_collection.find(query).skip(skip).limit(perPage)

#         people_list = []
#         async for person in cursor:
#             person["_id"] = str(person["_id"])  # Convert the ObjectId to string
#             sanitized_person = sanitize_document(person)  # Sanitize the person document

#             # Collect leader fields (Leader @12, Leader @144, Leader @1728)
#             leader_data = {
#                 "Leader @12": person.get("Leader @12", ""),
#                 "Leader @144": person.get("Leader @144", ""),
#                 "Leader @ 1728": person.get("Leader @ 1728", "")
#             }

#             # Add leader information to the sanitized person data
#             sanitized_person.update(leader_data)
#             people_list.append(sanitized_person)

#         # Fetch total count for pagination metadata
#         total_count = await people_collection.count_documents(query)

#         # Return paginated response
#         return {
#             "page": page,
#             "perPage": perPage,
#             "total": total_count,
#             "results": people_list
#         }
    
#     except Exception as e:
#         # logging.error(f"Error occurred while fetching people: {e}")
#         raise HTTPException(status_code=500, detail="Internal Server Error")

# @app.get("/people/{person_id}")
# async def get_person_by_id(person_id: str = Path(...)):
#     try:
#         person = await people_collection.find_one({"_id": ObjectId(person_id)})
#         if not person:
#             raise HTTPException(status_code=404, detail="Person not found")
#         person["_id"] = str(person["_id"])
#         mapped = {
#             "_id": person["_id"],
#             "Name": person.get("Name", ""),
#             "Surname": person.get("Surname", ""),
#             "Phone": person.get("Number", ""),
#             "Email": person.get("Email", ""),
#             "Location": person.get("Address", ""),
#             "Gender": person.get("Gender", ""),
#             "DateOfBirth": person.get("Birthday", ""),
#             "Leader": (
#                 person.get("Leader @12")
#                 or person.get("Leader @144")
#                 or person.get("Leader @ 1728")
#                 or ""
#             ),
#             "Stage": person.get("Stage", "Win"),
#             "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat()
#         }
#         return mapped
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# def normalize_person_data(data: dict) -> dict:
#     return {
#         "name": data.get("Name") or data.get("name"),
#         "surname": data.get("Surname") or data.get("surname"),
#         "phone": data.get("Number") or data.get("phone"),
#         "email": data.get("Email") or data.get("email"),
#         "homeAddress": data.get("Address") or data.get("homeAddress"),
#         "dob": data.get("Birthday") or data.get("dob"),
#         "gender": data.get("Gender") or data.get("gender"),
#         "invitedBy": data.get("invitedBy"),
#         "leader12": data.get("Leader @12") or data.get("leader12"),
#         "leader144": data.get("Leader @144") or data.get("leader144"),
#         "leader1728": data.get("Leader @ 1728") or data.get("leader1728"),
#         # Add other fields if necessary
#     }

# @app.patch("/people/{person_id}")
# async def update_person(person_id: str = Path(...), update_data: dict = Body(...)):
#     try:
#         normalized_data = normalize_person_data(update_data)
#         normalized_data["updatedAt"] = datetime.utcnow().isoformat()

#         result = await people_collection.update_one(
#             {"_id": ObjectId(person_id)},
#             {"$set": normalized_data}
#         )
#         if result.matched_count == 0:
#             raise HTTPException(status_code=404, detail="Person not found")

#         # Fetch the updated person document
#         updated_person = await people_collection.find_one({"_id": ObjectId(person_id)})

#         if not updated_person:
#             raise HTTPException(status_code=404, detail="Person not found after update")

#         # Normalize the updated person before returning
#         normalized_person = normalize_person_data(updated_person)
#         normalized_person["_id"] = str(updated_person["_id"])

#         return normalized_person

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.post("/people")
# async def create_person(person_data: PersonCreate):
#     try:
#         # Check if email already exists
#         existing_person = await people_collection.find_one({"Email": person_data.email})
#         if existing_person:
#             raise HTTPException(
#                 status_code=400, 
#                 detail=f"A person with email '{person_data.email}' already exists"
#             )

#         # Prepare the document for insertion
#         person_doc = {
#             "Name": person_data.name.strip(),
#             "Surname": person_data.surname.strip() if person_data.surname else "",
#             "Email": person_data.email.lower().strip(),
#             "Number": person_data.phone.strip() if person_data.phone else "",
#             "Address": person_data.homeAddress.strip() if person_data.homeAddress else "",
#             "Gender": person_data.gender.strip() if person_data.gender else "",
#             "Birthday": person_data.dob.strip() if person_data.dob else "",
#             "InvitedBy": person_data.invitedBy.strip() if person_data.invitedBy else "",
#             "Leader @12": person_data.leader12.strip() if person_data.leader12 else "",
#             "Leader @144": person_data.leader144.strip() if person_data.leader144 else "",
#             "Leader @ 1728": person_data.leader1728.strip() if person_data.leader1728 else "",
#             "Stage": person_data.stage or "Win",
#             "Present": False,  # Default to not present
#             "CreatedAt": datetime.utcnow().isoformat(),
#             "UpdatedAt": datetime.utcnow().isoformat()
#         }

#         # Insert the person into the database
#         result = await people_collection.insert_one(person_doc)
        
#         # Prepare response data
#         created_person = {
#             "_id": str(result.inserted_id),
#             "Name": person_doc["Name"],
#             "Surname": person_doc["Surname"],
#             "Email": person_doc["Email"],
#             "Phone": person_doc["Number"],
#             "Location": person_doc["Address"],
#             "Gender": person_doc["Gender"],
#             "DateOfBirth": person_doc["Birthday"],
#             "InvitedBy": person_doc["InvitedBy"],
#             "Leader @12": person_doc["Leader @12"],
#             "Leader @144": person_doc["Leader @144"],
#             "Leader @ 1728": person_doc["Leader @ 1728"],
#             "Stage": person_doc["Stage"],
#             "Present": person_doc["Present"],
#             "CreatedAt": person_doc["CreatedAt"],
#             "UpdatedAt": person_doc["UpdatedAt"]
#         }

#         return {
#             "message": "Person created successfully",
#             "id": str(result.inserted_id),
#             "_id": str(result.inserted_id),
#             "person": created_person
#         }

#     except HTTPException:
#         # Re-raise HTTP exceptions (like duplicate email)
#         raise
#     except Exception as e:
#         # Log the error for debugging
#         print(f"Error creating person: {e}")
#         raise HTTPException(status_code=500, detail="Internal Server Error")


# @app.delete("/people/{person_id}")
# async def delete_person(person_id: str = Path(...)):
#     try:
#         result = await people_collection.delete_one({"_id": ObjectId(person_id)})
#         if result.deleted_count == 0:
#             raise HTTPException(status_code=404, detail="Person not found")
#         return {"message": "Person deleted successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

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
# -----------------------
# Yoco endpoints
# -----------------------

@app.get("/yoco/public-key")
async def get_yoco_public_key():
    """Return publishable Yoco key for frontend."""
    return {"publicKey": YOCO_PUBLIC_KEY}


@app.post("/yoco/charge")
async def charge_yoco(data: YocoPaymentRequest, token: str = Depends(oauth2_scheme)):
    """Process payment with Yoco and save to DB."""
    user = await get_current_user(token)

    # Call Yoco API
    response = requests.post(
        "https://online.yoco.com/v1/charges/",
        headers={
            "X-Auth-Secret-Key": YOCO_SECRET_KEY,
            "Content-Type": "application/json",
        },
        json={
            "token": data.token,
            "amountInCents": data.amount * 100,
            "currency": "ZAR",
        },
    )
    result = response.json()

    if "errorCode" in result:
        raise HTTPException(
            status_code=400,
            detail=result.get("displayMessage", "Payment failed"),
        )

    # Save global record
    payment_doc = {
        "user_id": str(user["_id"]),
        "yoco_charge_id": result.get("id"),
        "amount": data.amount,
        "currency": "ZAR",
        "email": data.email,
        "name": data.name,
        "status": result.get("status"),
        "created_at": datetime.utcnow(),
    }
    insert_result = await db["Payments"].insert_one(payment_doc)

    # Add donation entry to user document
    donation_entry = {
        "payment_id": str(insert_result.inserted_id),
        "yoco_charge_id": result.get("id"),
        "amount": data.amount,
        "currency": "ZAR",
        "status": result.get("status"),
        "created_at": datetime.utcnow(),
    }
    await db["Users"].update_one(
        {"_id": user["_id"]},
        {"$push": {"donations": donation_entry}}
    )

    return {
        "success": True,
        "chargeId": result.get("id"),
        "status": result.get("status"),
        "amount": data.amount,
    }


@app.get("/users/{user_id}/donations")
async def get_user_donations(user_id: str):
    """Fetch donations for a user (from Users.donations)."""
    user = await db["Users"].find_one({"_id": ObjectId(user_id)}, {"donations": 1})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {"donations": user.get("donations", [])}

# -----------------------
# Batch-Specific Contributions
# -----------------------


@app.get("/financial-reports/batches/{batch_id}/collections")
async def get_batch_collections(batch_id: str, user=Depends(verify_token)):
    batch = db.batches.find_one({"batchId": batch_id})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch.get("contributions", [])

@app.post("/financial-reports/batches/{batch_id}/collections")
async def add_collection(batch_id: str, data: Contribution, user=Depends(verify_token)):
    batch = db.batches.find_one({"batchId": batch_id})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    contrib_id = f"contrib_{uuid4()}"
    new_contrib = {
        "id": contrib_id,
        "batchId": batch_id,
        "name": data.name,
        "date": data.date,
        "amount": float(data.amount),
        "method": data.method,
        "account": data.account,
        "description": data.description or "",
        "createdBy": {"id": user["sub"], "name": user["name"]},
        "createdAt": _utcnow().isoformat(),
    }

    db.batches.update_one({"batchId": batch_id}, {"$push": {"contributions": new_contrib}})
    db.contributions.insert_one(new_contrib)
    return new_contrib

@app.put("/financial-reports/batches/{batch_id}/collections/{collection_id}")
async def update_collection(batch_id: str, collection_id: str, data: Contribution, user=Depends(verify_token)):
    batch = db.batches.find_one({"batchId": batch_id})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    updated = None
    contributions = []
    for c in batch.get("contributions", []):
        if c["id"] == collection_id:
            c.update(data.dict())
            updated = c
        contributions.append(c)

    if not updated:
        raise HTTPException(status_code=404, detail="Collection not found")

    db.batches.update_one({"batchId": batch_id}, {"$set": {"contributions": contributions}})
    db.contributions.update_one({"id": collection_id}, {"$set": updated})
    return updated

@app.delete("/financial-reports/batches/{batch_id}/collections/{collection_id}")
async def delete_collection(batch_id: str, collection_id: str, user=Depends(verify_token)):
    batch = db.batches.find_one({"batchId": batch_id})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    contributions = [c for c in batch.get("contributions", []) if c["id"] != collection_id]
    if len(contributions) == len(batch.get("contributions", [])):
        raise HTTPException(status_code=404, detail="Collection not found")

    db.batches.update_one({"batchId": batch_id}, {"$set": {"contributions": contributions}})
    db.contributions.delete_one({"id": collection_id})
    return {"status": "success", "deletedId": collection_id}

# -----------------------
# Global Contributions
# -----------------------
@app.get("/financial-reports/contributions")
async def get_global_contributions(user=Depends(verify_token)):
    return list(db.contributions.find({}, {"_id": 0}))

@app.post("/financial-reports/contributions")
async def add_global_contribution(data: Contribution, user=Depends(verify_token)):
    contrib_id = f"contrib_{uuid4()}"
    new_contrib = {
        "id": contrib_id,
        "batchId": None,
        "name": data.name,
        "date": data.date,
        "amount": float(data.amount),
        "method": data.method,
        "account": data.account,
        "description": data.description or "",
        "createdBy": {"id": user["sub"], "name": user["name"]},
        "createdAt": _utcnow().isoformat(),
    }
    db.contributions.insert_one(new_contrib)
    return new_contrib

@app.put("/financial-reports/contributions/{contrib_id}")
async def update_global_contribution(contrib_id: str, data: Contribution, user=Depends(verify_token)):
    updated = data.dict()
    result = db.contributions.update_one({"id": contrib_id}, {"$set": updated})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Contribution not found")
    return updated

@app.delete("/financial-reports/contributions/{contrib_id}")
async def delete_global_contribution(contrib_id: str, user=Depends(verify_token)):
    result = db.contributions.delete_one({"id": contrib_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Contribution not found")
    return {"status": "success", "deletedId": contrib_id}

# -----------------------
# Batch CRUD (Optional)
# -----------------------
@app.get("/financial-reports/batches")
async def get_batches(user=Depends(verify_token)):
    return list(db.batches.find({}, {"_id": 0}))

@app.post("/financial-reports/batches")
async def add_batch(data: Batch, user=Depends(verify_token)):
    if db.batches.find_one({"batchId": data.batchId}):
        raise HTTPException(status_code=400, detail="Batch ID already exists")
    db.batches.insert_one(data.dict())
    return data
        

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


# @app.post("/people")
# async def create_person(person_data: PersonCreate):
#     try:
#         # Check if email already exists
#         if person_data.email:
#             existing_person = await people_collection.find_one({"Email": person_data.email})
#             if existing_person:
#                 raise HTTPException(
#                     status_code=400, 
#                     detail=f"A person with email '{person_data.email}' already exists"
#                 )

#         # Prepare the document for insertion
#         person_doc = {
#             "Name": person_data.name.strip(),
#             "Surname": person_data.surname.strip() if person_data.surname else "",
#             "Email": person_data.email.lower().strip() if person_data.email else "",
#             "Number": person_data.number.strip() if person_data.number else "",
#             "HomeAddress": person_data.address.strip() if person_data.address else "",
#             "Gender": person_data.gender.strip() if person_data.gender else "",
#             "Birthday": person_data.dob.strip() if person_data.dob else "",
#             "InvitedBy": person_data.invitedBy.strip() if person_data.invitedBy else "",
#             "Leader @12": getattr(person_data, 'leader12', '') or "",
#             "Leader @144": getattr(person_data, 'leader144', '') or "",
#             "Leader @ 1728": getattr(person_data, 'leader1728', '') or "",
#             "Stage": person_data.stage or "Win",
#             "Present": False,  # Default to not present
#             "CreatedAt": datetime.utcnow().isoformat(),
#             "UpdatedAt": datetime.utcnow().isoformat()
#         }

#         # Insert the person into the database
#         result = await people_collection.insert_one(person_doc)
        
#         # Return the created person in consistent format
#         created_person = {
#             "_id": str(result.inserted_id),
#             "Name": person_doc["Name"],
#             "Surname": person_doc["Surname"],
#             "Email": person_doc["Email"],
#             "Phone": person_doc["Number"],
#             # "Location": person_doc["Address"],
#             "Gender": person_doc["Gender"],
#             "DateOfBirth": person_doc["Birthday"],
#             "HomeAddress": person_doc["HomeAddress"],
#             "InvitedBy": person_doc["InvitedBy"],
#             "Leader @12": person_doc["Leader @12"],
#             "Leader @144": person_doc["Leader @144"],
#             "Leader @ 1728": person_doc["Leader @ 1728"],
#             "Leader": (
#                 person_doc["Leader @12"] or 
#                 person_doc["Leader @144"] or 
#                 person_doc["Leader @ 1728"] or 
#                 ""
#             ),
#             "Stage": person_doc["Stage"],
#             "Present": person_doc["Present"],
#             "CreatedAt": person_doc["CreatedAt"],
#             "UpdatedAt": person_doc["UpdatedAt"]
#         }

#         return {
#             "message": "Person created successfully",
#             "id": str(result.inserted_id),
#             "_id": str(result.inserted_id),
#             "person": created_person
#         }

#     except HTTPException:
#         # Re-raise HTTP exceptions (like duplicate email)
#         raise
#     except Exception as e:
#         # Log the error for debugging
#         print(f"Error creating person: {e}")
#         raise HTTPException(status_code=500, detail="Internal Server Error")

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
