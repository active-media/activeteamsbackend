import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "active-teams-db"

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
