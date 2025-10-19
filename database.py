import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
MAIN_DB_NAME = "active-teams-db"
PEOPLE_DB_NAME = "testing-data-active-teams"  # 👈 separate database for People

client = AsyncIOMotorClient(MONGO_URI)

# Main database (for most collections)
db = client[MAIN_DB_NAME]

# Separate database for People
people_db = client[PEOPLE_DB_NAME]

# Collections
events_collection = db["Events"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
# cells_collection = db["Cells"]

# 👇 Only this one fetches from the testing database
people_collection = people_db["People"]
