import os 
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

load_dotenv() 

MONGO_URI = os.getenv("MONGO_URI", "None")
DB_NAME = os.getenv("DB_NAME", "active-teams-db")
print(f"--- CONNECTING TO DB: {DB_NAME} ---")
# DB_NAME = "test-data-active-teams"
client = AsyncIOMotorClient(MONGO_URI)

db = client[DB_NAME]
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
tasktypes_collection = db["TaskTypes"]
org_config_collection = db["OrgConfig"]
consolidations_collection=db["consolidations"]
organizations_collection = db["organizations"]

def get_database():
    return db