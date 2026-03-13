import os 
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

load_dotenv() 

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
DB_NAME = os.getenv("DB_NAME", "test-data-active-teams")
print(f"--- CONNECTING TO DB: {DB_NAME} ---")
client = AsyncIOMotorClient(MONGO_URI)

db = client[DB_NAME]
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
#  is used to test the whole data for events
# events_collection = db["cellst"]  
tasktypes_collection = db["TaskTypes"]
consolidations_collection=db["consolidations"]
organizations_collection = db["organizations"]

def get_database():
    return db