import os 
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

load_dotenv() 

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
DB_NAME = "active-teams-db"
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
# events_collection = db["cellst"]   is used to test the whole data for events
tasktypes_collection = db["TaskTypes"]

def get_database():
    return db