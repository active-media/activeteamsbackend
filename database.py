# Import os to access environment variables
import os 
# Import Motor async MongoDB client
from motor.motor_asyncio import AsyncIOMotorClient 
# Import dotenv loader to populate environment from .env
from dotenv import load_dotenv 

# Load environment variables from a .env file (if present)
load_dotenv() 

# MongoDB Atlas connection URI (includes credentials in-source)
MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
# Name of the application's database
DB_NAME = "active-teams-db"
# Create an asynchronous MongoDB client using Motor
client = AsyncIOMotorClient(MONGO_URI)
# Get a reference to the database by name
db = client[DB_NAME]
# Reference to the Events collection used by the app
events_collection = db["Events"]
# Reference to the People collection used by the app
people_collection = db["People"]
# Reference to the Users collection used by the app
users_collection = db["Users"]
# Reference to the tasks collection used by the app
tasks_collection = db["tasks"]
# (Commented alternative collection used during testing)
# events_collection = db["cellst"]   is used to test the whole data for events
# Reference to the TaskTypes collection
tasktypes_collection = db["TaskTypes"]

def get_database():
    # Return the DB reference for other modules
    return db