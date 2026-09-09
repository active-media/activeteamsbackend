import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/")
DB_NAME = os.getenv("DB_NAME", "active-teams-db")

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
org_config_collection = db["OrgConfig"]

async def seed():
    print(f"Connecting to database: {DB_NAME}")
    print(f"Collection: OrgConfig")
    print("-" * 40)
    existing = await org_config_collection.find_one({"_id": "active-teams"})
    if existing:
        print("Config for 'active-teams' already exists, skipping.")
        return
    config = {
        "_id": "active-teams",
        "org_name": "Active Teams",
        "slug": "active-teams",
        "is_setup": True,
        "events_collection": "Events",
        "people_collection": "People",
        "recurring_event_type": "Cells",
        "hierarchy": [
            {"key": "leader1",   "level": 1,   "field": "leader1",   "label": "Leader @1"},
            {"key": "leader12",  "level": 12,  "field": "leader12",  "label": "Leader @12"},
            {"key": "leader144", "level": 144, "field": "leader144", "label": "Leader @144"},
            {"key": "leader1728","level": 1728,"field": "leader1728","label": "Leader @1728"}
        ],
        "roles": [
            {"key": "admin",      "label": "Admin",  "capabilities": ["admin"]},
            {"key": "leader",     "label": "Leader", "capabilities": ["view_people", "manage_people", "create_events", "close_events", "view_stats", "checkin"]},
            {"key": "user",       "label": "Member", "capabilities": ["checkin"]}
        ],
        "settings": {
            "recurring_event_type": "Cells",
            "top_leaders": {"male": "Gavin Enslin", "female": "Vicky Enslin"},
            "allows_create_event": True,
            "allows_create_event_type": True,
        },
        "top_leaders": {"male": "Gavin Enslin", "female": "Vicky Enslin"},
        "allows_create_event": True,
        "allows_create_event_type": True,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "created_by": "seed_script",
        "is_default": True
    }
    await org_config_collection.insert_one(config)
    print("Successfully seeded 'active-teams' config!")

async def tag_events():
    print("Tagging existing events with org_id...")
    result = await db["AllEvents"].update_many(
        {"org_id": {"$exists": False}},
        {"$set": {"org_id": "active-teams"}}
    )
    print(f"Tagged {result.modified_count} events with org_id: active-teams")

async def tag_event_types():
    print("Tagging existing event types with org_id...")
    result = await db["AllEvents"].update_many(
        {"isEventType": True, "org_id": {"$exists": False}},
        {"$set": {"org_id": "active-teams"}}
    )
    print(f"Tagged {result.modified_count} event types with org_id: active-teams")

async def main():
    try:
        await seed()
        await tag_events()
        await tag_event_types() 
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("Connection closed.")
if __name__ == "__main__":
    asyncio.run(main())