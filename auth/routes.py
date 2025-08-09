from fastapi import APIRouter, HTTPException
from auth.models import UserCreate, UserLogin , TaskModel
from auth.utils import hash_password, verify_password
from bson.objectid import ObjectId 
from db.mongo import db


router = APIRouter()

@router.post("/signup")
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

@router.post("/login")
async def login(user: UserLogin):
    existing = await db["Users"].find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {"message": "Login successful"}

@router.post("/tasks")
async def create_task(task: TaskModel):
    task_dict = task.dict()
    result = await db["Tasks"].find_one(task_dict)
    return {"message": "Task created", "id": str(result.inserted_id)}

@router.get("/tasks")
async def get_tasks():
    tasks = []
    cursor = db["Tasks"].find({})
    async for task in cursor:
        task["_id"] = str(task["_id"])
        tasks.append(task)
    return tasks
