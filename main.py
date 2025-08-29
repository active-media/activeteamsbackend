# main.py
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Request, Depends, Body, Query ,Path
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from typing import Optional, List
from datetime import datetime  ,time as time_type, timedelta
from bson import ObjectId

# -------------------------
# Models
# -------------------------
from auth.models import (
    Person, TaskModel, TaskUpdate, UserCreate, UserLogin,
    CheckIn, UncaptureRequest, CellEventCreate,
    AddMemberNamesRequest, RemoveMemberRequest
)

# -------------------------
# Database
# -------------------------
from database import (
    db, users_collection, events_collection, tasks_collection,
    people_collection
)

# -------------------------
# Utils
# -------------------------
from utils import verify_password, hash_password, get_current_user, require_role

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
@app.post("/people", dependencies=[Depends(require_role("admin", "registrant"))])
async def create_person(person: Person):
    person_dict = person.dict()
    result = await people_collection.insert_one(person_dict)
    person_dict["_id"] = str(result.inserted_id)
    return {"message": "Person created successfully", "person": person_dict}

@app.get("/people/{person_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_person(person_id: str):
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    person["_id"] = str(person["_id"])
    return person

@app.patch("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def update_person(person_id: str, update: dict = Body(...)):
    person = await people_collection.find_one({"_id": ObjectId(person_id)})
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    await people_collection.update_one({"_id": ObjectId(person_id)}, {"$set": update})
    return {"message": "Person updated successfully"}

@app.delete("/people/{person_id}", dependencies=[Depends(require_role("admin"))])
async def delete_person(person_id: str):
    result = await people_collection.delete_one({"_id": ObjectId(person_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"message": "Person deleted successfully"}

# -------------------------
# Tasks Management
# -------------------------

# Create a new task

# POST /tasks

@app.post("/tasks")

async def create_task(task: TaskModel):

    print("Received task:", task)

    task_dict = task.dict()

    result = await tasks_collection.insert_one(task_dict)

    return {"message": "Task created", "id": str(result.inserted_id)}

# Retrieve all tasks

# GET /tasks

@app.get("/tasks", response_model=List[TaskModel])

async def get_tasks(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD"),
):

    query = {}
    if start_date or end_date:
        date_filter = {}
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                date_filter["$gte"] = start_dt

            except ValueError:

                raise HTTPException(status_code=400, detail="Invalid start_date format")

        if end_date:

            try:
                # Add one day to include entire end date
                end_dt = datetime.fromisoformat(end_date) + timedelta(days=1)
                date_filter["$lt"] = end_dt

            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format")
        query["followup_date"] = date_filter

    tasks = []

    cursor = db["Tasks"].find(query)
    async for task in cursor:
        task["_id"] = str(task["_id"])  # stringify ObjectId
        try:
            tasks.append(TaskModel(**task))  # validate + convert with Pydantic
        except Exception as e:
            print(f"Skipping invalid task: {e}, task={task}")

    return tasks 



@app.put("/tasks/{task_id}")

async def update_task(task_id: str = Path(...), task_data: TaskUpdate = None):
    if not ObjectId.is_valid(task_id):

        raise HTTPException(status_code=400, detail="Invalid task ID")

    updated_task = {k: v for k, v in task_data.dict(exclude_unset=True).items()}

    result = await tasks_collection.find_one_and_update(
        {"_id": ObjectId(task_id)},
        {"$set": updated_task},
        return_document=True  # from pymongo import ReturnDocument

    )

    if not result:

        raise HTTPException(status_code=404, detail="Task not found")
    # Convert ObjectId to str before returning

    result["_id"] = str(result["_id"])

    return result

# -------------------------
# Event Endpoints
# -------------------------
@app.get("/event/{event_id}", dependencies=[Depends(require_role("admin", "registrant"))])
async def get_event_by_id(event_id: str):
    event = await events_collection.find_one({"_id": ObjectId(event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    event["_id"] = str(event["_id"])
    return event

@app.get("/events/filter", dependencies=[Depends(require_role("admin", "registrant"))])
async def filter_events(
    event_type: Optional[str] = Query(None),
    leader: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    query = {}
    if event_type:
        query["eventType"] = event_type
    if leader:
        query["eventLeader"] = leader
    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter["$gte"] = datetime.fromisoformat(start_date)
        if end_date:
            date_filter["$lte"] = datetime.fromisoformat(end_date)
        query["date"] = date_filter
    events = []
    cursor = events_collection.find(query)
    async for e in cursor:
        e["_id"] = str(e["_id"])
        events.append(e)
    return {"events": events}

@app.post("/events", dependencies=[Depends(require_role("admin"))])
async def create_event(event: CellEventCreate):
    event_dict = event.dict()
    result = await events_collection.insert_one(event_dict)
    event_dict["_id"] = str(result.inserted_id)
    return {"message": "Event created successfully", "event": event_dict}

@app.patch("/events/{event_id}", dependencies=[Depends(require_role("admin"))])
async def update_event(event_id: str, update: dict = Body(...)):
    event = await events_collection.find_one({"_id": ObjectId(event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    await events_collection.update_one({"_id": ObjectId(event_id)}, {"$set": update})
    return {"message": "Event updated successfully"}

@app.delete("/events/{event_id}", dependencies=[Depends(require_role("admin"))])
async def delete_event(event_id: str):
    result = await events_collection.delete_one({"_id": ObjectId(event_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"message": "Event deleted successfully"}

# -------------------------
# Check-in / Uncapture Endpoints
# -------------------------
@app.post("/checkin", dependencies=[Depends(require_role("admin", "registrant"))])
async def check_in_person(checkin: CheckIn):
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
    return {"message": f"{checkin.name} checked in successfully"}

@app.post("/uncapture", dependencies=[Depends(require_role("admin"))])
async def uncapture_person(request: UncaptureRequest):
    event = await events_collection.find_one({"_id": ObjectId(request.event_id)})
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    attendees = event.get("attendees", [])
    new_attendees = [a for a in attendees if a.get("name") != request.name]
    await events_collection.update_one(
        {"_id": ObjectId(request.event_id)},
        {"$set": {"attendees": new_attendees, "total_attendance": len(new_attendees)}}
    )
    return {"message": f"{request.name} uncaptured successfully"}

# -------------------------
# Cell Event Management
# -------------------------
@app.post("/cells", dependencies=[Depends(require_role("admin"))])
async def create_cell_event(cell: CellEventCreate):
    cell_dict = cell.dict()
    result = await events_collection.insert_one(cell_dict)
    cell_dict["_id"] = str(result.inserted_id)
    return {"message": "Cell event created", "cell": cell_dict}

@app.post("/cells/{cell_id}/add_member", dependencies=[Depends(require_role("admin"))])
async def add_member(cell_id: str, member: AddMemberNamesRequest):
    cell = await events_collection.find_one({"_id": ObjectId(cell_id)})
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")
    members = cell.get("members", [])
    if member.name not in members:
        members.append(member.name)
        await events_collection.update_one({"_id": ObjectId(cell_id)}, {"$set": {"members": members}})
    return {"message": f"{member.name} added to cell"}

@app.post("/cells/{cell_id}/remove_member", dependencies=[Depends(require_role("admin"))])
async def remove_member(cell_id: str, member: RemoveMemberRequest):
    cell = await events_collection.find_one({"_id": ObjectId(cell_id)})
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")
    members = [m for m in cell.get("members", []) if m != member.name]
    await events_collection.update_one({"_id": ObjectId(cell_id)}, {"$set": {"members": members}})
    return {"message": f"{member.name} removed from cell"}
