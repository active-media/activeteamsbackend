import os
from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from auth.models import Event, CheckIn, UncaptureRequest, UserCreate, UserLogin
from auth.utils import hash_password, verify_password

# Load .env variables
load_dotenv()
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
MONGO_URI = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client["active-teams-db"]
events_collection = db["Events"]
people_collection = db["People"]

@app.get("/")
async def root():
    return {"message": "Server is running with MongoDB, Firebase, and AWS!"}


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
        "confirm_password": hashed
    }
    await db["Users"].insert_one(user_dict)
    return {"message": "User created successfully"}

@app.post("/login")
async def login(user: UserLogin):
    existing = await db["Users"].find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {"message": "Login successful"}


    # --- GET profile ---
@app.get("/profile/{user_id}")
async def get_profile(user_id: str):
    try:
        user = await db["Users"].find_one({"_id": ObjectId(user_id)}, {"password": 0, "confirm_password": 0})
    except:
        raise HTTPException(status_code=400, detail="Invalid user ID format")
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user["_id"] = str(user["_id"])  # Convert ObjectId to string
    return user


# --- PUT profile ---
@app.put("/profile/{user_id}")
async def update_profile(user_id: str, profile_data: dict = Body(...)):
    try:
        existing = await db["Users"].find_one({"_id": ObjectId(user_id)})
    except:
        raise HTTPException(status_code=400, detail="Invalid user ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent password from being updated here
    profile_data.pop("password", None)
    profile_data.pop("confirm_password", None)

    await db["Users"].update_one(
        {"_id": ObjectId(user_id)},
        {"$set": profile_data}
    )
    return {"message": "Profile updated successfully"}

# Create Event
@app.post("/events")
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

# EVENTS-----------------------


# Postman test-- POST http://localhost:8000/events/Encounter
        #  GET http://localhost:8000/events/Encounter
        # PUT http://localhost:8000/events/use id 
        # DELETE http://localhost:8000/events/ use id


@app.post("/events/{event_type}")
async def create_event(event_type: str, event: Event):
    try:
        event_data = event.dict()

        # Add event type from URL
        event_data["eventType"] = event_type

        # Convert date from string to datetime if possible
        if "date" in event_data and isinstance(event_data["date"], str):
            event_data["date"] = datetime.fromisoformat(event_data["date"])

        # Ensure attendees list exists
        if "attendees" not in event_data:
            event_data["attendees"] = []

        # Insert into MongoDB
        result = await events_collection.insert_one(event_data)

        return {
            "message": f"{event_type} event created successfully",
            "event_type": event_type,
            "id": str(result.inserted_id)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Get all events
@app.get("/events")
async def get_all_events():
    try:
        events = []
        cursor = events_collection.find()
        async for event in cursor:
            event["_id"] = str(event["_id"])
            events.append(event)
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Get events by type
@app.get("/events/{event_type}")
async def get_events_by_type(event_type: str):
    try:
        events = []
        cursor = events_collection.find({"eventType": event_type})
        async for event in cursor:
            event["_id"] = str(event["_id"])
            events.append(event)
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Update event
@app.put("/events/{event_id}")
async def update_event(event_id: str, event: Event):
    try:
        update_data = event.dict()
        if "date" in update_data and isinstance(update_data["date"], str):
            update_data["date"] = datetime.fromisoformat(update_data["date"])
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {"$set": update_data}
        )
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Event not found or no changes made")
        return {"message": "Event updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Delete event
@app.delete("/events/{event_id}")
async def delete_event(event_id: str):
    try:
        result = await events_collection.delete_one({"_id": ObjectId(event_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Event not found")
        return {"message": "Event deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Search People
@app.get("/people/search")
async def search_people(name: str = Query(..., min_length=1)):
    try:
        cursor = people_collection.find({"Name": {"$regex": name, "$options": "i"}})
        people = []
        async for p in cursor:
            people.append({"_id": str(p["_id"]), "Name": p["Name"]})
        return {"results": people}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Check-in
@app.post("/checkin")
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

        update_result = await events_collection.update_one(
            {"_id": ObjectId(checkin.event_id)},
            {
                "$push": {"attendees": {"name": checkin.name, "time": datetime.now()}},
                "$inc": {"total_attendance": 1}
            }
        )
        return {"message": f"{checkin.name} checked in successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# View Check-ins
@app.get("/checkins/{event_id}")
async def get_checkins(event_id: str):
    try:
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        return {
            "event_id": event_id,
            "service_name": event["service_name"],
            "attendees": event.get("attendees", []),
            "total_attendance": event.get("total_attendance", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Uncapture (Remove Check-in)
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
# Routes

@app.get("/")
async def root():
    return {"message": "Server is running with MongoDB, Firebase, and AWS!"}
