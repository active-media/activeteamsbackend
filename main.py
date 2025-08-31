import os
from datetime import datetime, timedelta
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Path, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from auth.models import (
    EventCreate, CheckIn, UncaptureRequest, UserCreate, UserLogin,
    CellEventCreate, AddMemberNamesRequest, RemoveMemberRequest,
    RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest,
    TaskModel, TaskUpdate, Person
)
from auth.utils import (
    hash_password, verify_password, require_role, get_current_user,
    get_next_occurrence_single, parse_time_string, get_leader_cell_name_async,
    create_access_token, decode_access_token, convert_datetime_to_iso,
    sanitize_document
)
import math
import secrets
from database import db, events_collection, people_collection, users_collection, tasks_collection
from auth.email_utils import send_reset_password_email
from typing import Optional, Literal, List
from pymongo import ReturnDocument

# -------------------------
# App Setup
# -------------------------
app = FastAPI()

origins = [
    "https://activeteams.netlify.app",  # your frontend
    "http://localhost:5173",             # local dev (Vite default)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        # can be ["*"] for testing, but not recommended in prod
    allow_credentials=True,
    allow_methods=["*"],          # GET, POST, PUT, DELETE, etc
    allow_headers=["*"],          # Accepts all headers
)

# -------------------------
# Validation Exception
# -------------------------
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    formatted = [
        {"field": ".".join(err["loc"][1:]), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"errors": formatted})

app.add_exception_handler(RequestValidationError, validation_exception_handler)

# -------------------------
# Auth Endpoints
# -------------------------
@app.post("/auth/signup")
async def signup(user: UserCreate):
    existing_user = await users_collection.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_dict = user.dict()
    user_dict["password"] = hash_password(user_dict["password"])
    result = await users_collection.insert_one(user_dict)
    return {"message": "User created", "user_id": str(result.inserted_id)}

@app.post("/auth/login")
async def login(user: UserLogin):
    db_user = await users_collection.find_one({"email": user.email})
    if not db_user or not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    return {"message": "Login successful", "user_id": str(db_user["_id"])}

@app.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return current_user

# -------------------------
# People Endpoints
# -------------------------
@app.post("/people")
async def create_person(person: Person):
    person_dict = person.dict()
    result = await people_collection.insert_one(person_dict)
    person_dict["_id"] = str(result.inserted_id)
    return {"message": "Person created successfully", "person": person_dict}

@app.get("/people/{person_id}")
async def get_person(person_id: str):
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    person["_id"] = str(person["_id"])
    return person

@app.get("/people")
async def list_people(name: Optional[str] = None, perPage: int = 100):
    query = {}
    if name:
        # case-insensitive search for Name or Surname
        query["$or"] = [
            {"Name": {"$regex": name, "$options": "i"}},
            {"Surname": {"$regex": name, "$options": "i"}}
        ]

    cursor = people_collection.find(query).limit(perPage)
    people = []
    async for person in cursor:
        person["_id"] = str(person["_id"])
        people.append(person)

    return {"people": people}


@app.patch("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def update_person(person_id: str, update: dict = Body(...)):
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    await people_collection.update_one({"_id": ObjectId(person_id)}, {"$set": update})
    return {"message": "Person updated successfully"}

@app.delete("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def delete_person(person_id: str):
    if not ObjectId.is_valid(person_id):
        raise HTTPException(status_code=400, detail="Invalid person ID format")
    result = await people_collection.delete_one({"_id": ObjectId(person_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"message": "Person deleted successfully"}

# -------------------------
# Task Endpoints
# -------------------------
@app.get("/tasks", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_all_tasks():
    tasks = []
    cursor = tasks_collection.find()
    async for t in cursor:
        t["_id"] = str(t["_id"])
        tasks.append(t)
    return {"tasks": tasks}

@app.get("/tasks/member/{member_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_tasks_by_member(member_id: str):
    tasks = []
    cursor = tasks_collection.find({"memberID": member_id})
    async for t in cursor:
        t["_id"] = str(t["_id"])
        tasks.append(t)
    return {"tasks": tasks}

@app.post("/tasks", dependencies=[Depends(require_role("admin"))])
async def create_task(task: TaskModel):
    task_dict = task.dict()
    result = await tasks_collection.insert_one(task_dict)
    task_dict["_id"] = str(result.inserted_id)
    return {"message": "Task created successfully", "task": task_dict}

@app.patch("/tasks/{task_id}", dependencies=[Depends(require_role("admin"))])
async def update_task(task_id: str, update: TaskUpdate):
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=400, detail="Invalid task ID format")
    task = await tasks_collection.find_one({"_id": ObjectId(task_id)})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    update_dict = {k: v for k, v in update.dict(exclude_unset=True).items()}
    await tasks_collection.update_one({"_id": ObjectId(task_id)}, {"$set": update_dict})
    return {"message": "Task updated successfully"}

@app.delete("/tasks/{task_id}", dependencies=[Depends(require_role("admin"))])
async def delete_task(task_id: str):
    if not ObjectId.is_valid(task_id):
        raise HTTPException(status_code=400, detail="Invalid task ID format")
    result = await tasks_collection.delete_one({"_id": ObjectId(task_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task deleted successfully"}

# -------------------------
# Event Endpoints
# -------------------------
# -------------------------
# Event Endpoints
# -------------------------
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

@app.patch("/allevents/{event_id}")
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
@app.route('/events')
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



@app.get("/cells/upcoming")
async def get_upcoming_cells():
    try:
        now_utc = datetime.utcnow()
        
        # Calculate "show from" datetime for each day
        # 11 PM previous day SAST -> 21:00 UTC
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

            # Only consider the next occurrence for recurring cells
            next_occurrence = start_date
            if event.get("recurring") and event.get("recurring_day") is not None:
                next_occurrence = get_next_occurrence_single(start_date, event.get("recurring_day"))

            # Only show if next occurrence is within the next week and after "show from"
            if next_occurrence <= one_week_later and next_occurrence >= show_from_utc:
                event["_id"] = str(event["_id"])
                event["next_occurrence"] = next_occurrence.isoformat()
                event = sanitize_document(convert_datetime_to_iso(event))
                cells.append(event)

        return {"cells": cells}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving upcoming cells: {str(e)}")

# -------------------------
# Check-in / Uncapture Endpoints
# -------------------------
@app.post("/checkin", dependencies=[Depends(require_role("admin", "registrant"))])
async def check_in_person(checkin: CheckIn):
    if not ObjectId.is_valid(checkin.event_id):
        raise HTTPException(status_code=400, detail="Invalid event ID format")
    event = await events_collection.find_one({"_id": ObjectId(checkin.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    if checkin.name not in [a.get("name") for a in attendees]:
        attendees.append({"name": checkin.name, "checked_in_at": datetime.utcnow()})
        await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {"$set": {"attendees": attendees, "total_attendance": len(attendees)}}
        )
    return {"message": "Check-in successful"}

@app.post("/uncapture", dependencies=[Depends(require_role("admin"))])
async def uncapture_person(uncapture: UncaptureRequest):
    if not ObjectId.is_valid(uncapture.event_id):
        raise HTTPException(status_code=400, detail="Invalid event ID format")
    event = await events_collection.find_one({"_id": ObjectId(uncapture.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    attendees = [a for a in attendees if a.get("name") != uncapture.name]
    await events_collection.update_one(
        {"_id": ObjectId(uncapture.event_id)},
        {"$set": {"attendees": attendees, "total_attendance": len(attendees)}}
    )
    return {"message": "Person uncaptured successfully"}

# -------------------------
# Password Reset
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
        "Number": data.get("Number") or data.get("phone", ""),  # Store as Number
        "Email": data.get("Email") or data.get("email", ""),
        "HomeAddress": data.get("HomeAddress") or data.get("homeAddress") or data.get("location", ""),
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
        # Check if email already exists
        if person_data.email:
            existing_person = await people_collection.find_one({"Email": person_data.email})
            if existing_person:
                raise HTTPException(
                    status_code=400, 
                    detail=f"A person with email '{person_data.email}' already exists"
                )

        # Prepare the document for insertion
        person_doc = {
            "Name": person_data.name.strip(),
            "Surname": person_data.surname.strip() if person_data.surname else "",
            "Email": person_data.email.lower().strip() if person_data.email else "",
            "Number": person_data.phone.strip() if person_data.phone else "",
            "HomeAddress": person_data.homeAddress.strip() if person_data.homeAddress else "",
            "Gender": person_data.gender.strip() if person_data.gender else "",
            "Birthday": person_data.dob.strip() if person_data.dob else "",
            "InvitedBy": person_data.invitedBy.strip() if person_data.invitedBy else "",
            "Leader @12": getattr(person_data, 'leader12', '') or "",
            "Leader @144": getattr(person_data, 'leader144', '') or "",
            "Leader @ 1728": getattr(person_data, 'leader1728', '') or "",
            "Stage": person_data.stage or "Win",
            "Present": False,  # Default to not present
            "CreatedAt": datetime.utcnow().isoformat(),
            "UpdatedAt": datetime.utcnow().isoformat()
        }

        # Insert the person into the database
        result = await people_collection.insert_one(person_doc)
        
        # Return the created person in consistent format
        created_person = {
            "_id": str(result.inserted_id),
            "Name": person_doc["Name"],
            "Surname": person_doc["Surname"],
            "Email": person_doc["Email"],
            "Phone": person_doc["Number"],
            # "Location": person_doc["Address"],
            "Gender": person_doc["Gender"],
            "DateOfBirth": person_doc["Birthday"],
            "HomeAddress": person_doc["homeAddress"],
            "InvitedBy": person_doc["InvitedBy"],
            "Leader @12": person_doc["Leader @12"],
            "Leader @144": person_doc["Leader @144"],
            "Leader @ 1728": person_doc["Leader @ 1728"],
            "Leader": (
                person_doc["Leader @12"] or 
                person_doc["Leader @144"] or 
                person_doc["Leader @ 1728"] or 
                ""
            ),
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
        # Re-raise HTTP exceptions (like duplicate email)
        raise
    except Exception as e:
        # Log the error for debugging
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