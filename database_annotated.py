# Load environment variables and MongoDB async client
import os 
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

# Load .env variables into environment (no explicit .env path provided)
load_dotenv() 

# Connection string for MongoDB Atlas (embedded credentials in source)
MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
# Database name used by the application
DB_NAME = "active-teams-db"
# Create an async MongoDB client
client = AsyncIOMotorClient(MONGO_URI)
# Get a reference to the database
db = client[DB_NAME]
# Collections used by the app
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
# events_collection = db["cellst"]   is used to test the whole data for events
tasktypes_collection = db["TaskTypes"]

# Helper to return the DB reference
def get_database():
    return db
