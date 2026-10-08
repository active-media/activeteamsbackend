import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

from database import resolve_mongo_uri, events_collection

MONGO_URI = resolve_mongo_uri()
# Must match the app: database.py resolves DB_NAME from the environment and the
# app reads/writes the "Events" collection. This script previously hardcoded a
# different database and wrote org_id into "AllEvents", a collection nothing in
# the app ever reads - so the backfill silently did nothing to live data.
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
        "events_collection": "Events",
        "people_collection": "People",
        "recurring_event_type": "Cells",
        "hierarchy": [
            {"level": 1, "field": "leader1",   "label": "Leader @1"},
            {"level": 2, "field": "leader12",  "label": "Leader @12"},
            {"level": 3, "field": "leader144", "label": "Leader @144"}
        ],
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
    """Backfill org_id onto pre-multi-tenant events.

    Only ever touches documents that have no org_id yet, and only ever sets a
    field - it cannot drop attendance or any other data. Take a mongodump first.
    """
    print(f"Tagging untagged events with org_id in {DB_NAME}.Events...")
    result = await events_collection.update_many(
        {"org_id": {"$exists": False}},
        {"$set": {"org_id": "active-teams"}}
    )
    print(f"Tagged {result.modified_count} events with org_id: active-teams")

    remaining = await events_collection.count_documents({"org_id": {"$exists": False}})
    print(f"Still untagged: {remaining}")

async def tag_event_types():
    print("Tagging existing event types with org_id...")
    result = await events_collection.update_many(
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