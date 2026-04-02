import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
# Make sure to set DB_NAME in your .env or it will default to active-teams-db
DB_NAME = os.getenv("DB_NAME", "active-teams-db")

async def seed_organizations():
    print(f"Connecting to MongoDB at {MONGO_URI}")
    print(f"Target Database: {DB_NAME}")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    org_collection = db["organizations"]
    
    test_orgs = [
        {"name": "Active Church", "tag": "Active Church"},
        {"name": "City Church", "tag": "City Church"},
        {"name": "Grace Chapel", "tag": "Grace Chapel"},
        {"name": "Victory Outreach", "tag": "Victory Outreach"},
        {"name": "New Life Fellowship", "tag": "New Life Fellowship"}
    ]
    
    print(f"Seeding {len(test_orgs)} organizations into '{DB_NAME}.organizations'...")
    
    for org in test_orgs:
        # Avoid duplicates by checking name
        existing = await org_collection.find_one({"name": org["name"]})
        if not existing:
            await org_collection.insert_one(org)
            print(f"  [+] Added: {org['name']}")
        else:
            print(f"  [.] Already exists: {org['name']}")
    
    print("Seeding complete!")
    client.close()

if __name__ == "__main__":
    asyncio.run(seed_organizations())
