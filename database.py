import os 
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

load_dotenv() 

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
DB_NAME = "test-data-active-teams"
client = AsyncIOMotorClient(MONGO_URI)

db = client[DB_NAME]
events_collection = db["AllEvents"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
tasktypes_collection = db["TaskTypes"]
consolidations_collection=db["consolidations"]

org_config_collection = db["OrgConfig"]
# test_events_collection = db["AllEvents"]

def get_database():
    return db