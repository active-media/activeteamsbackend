# add_grace_user.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from passlib.context import CryptContext
from datetime import datetime

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)
db = client["test-data-active-teams"]
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def main():
    # Add grace-church config
    await db["OrgConfig"].update_one(
        {"_id": "grace-church"},
        {"$set": {
            "_id": "grace-church",
            "org_name": "Grace Community Church",
            "recurring_event_type": "Zones",
            "hierarchy": [
                {"level": 1, "field": "zonePastor",          "label": "Zone Pastor"},
                {"level": 2, "field": "districtLeader",      "label": "District Leader"},
                {"level": 3, "field": "regionalCoordinator", "label": "Regional Coordinator"},
            ],
            "top_leaders": {"male": "Pastor Samuel Dube", "female": "Pastor Ruth Dube"},
            "allows_create_event": True,
            "allows_create_event_type": True,
        }},
        upsert=True
    )
    print("Grace church config added!")

    # Add test user linked to grace-church
    await db["Users"].update_one(
        {"email": "grace@test.com"},
        {"$set": {
            "name": "Grace",
            "surname": "Test",
            "email": "grace@test.com",
            "password": pwd_context.hash("test1234"),
            "role": "admin",
            "org_id": "grace-church",
            "created_at": datetime.utcnow(),
        }},
        upsert=True
    )
    print("Grace test user added: grace@test.com / test1234")
    client.close()

asyncio.run(main())