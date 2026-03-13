import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"

async def check_db(db_name):
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[db_name]
    count = await db['organizations'].count_documents({})
    colls = await db.list_collection_names()
    print(f"--- DB: {db_name} ---")
    print(f"Collections: {colls}")
    print(f"Organizations Count: {count}")
    if count > 0:
        cursor = db['organizations'].find({}, {"name": 1})
        orgs = await cursor.to_list(length=10)
        print(f"Sample Orgs: {[o['name'] for o in orgs]}")
    client.close()

async def main():
    await check_db("active-teams-db")
    await check_db("test-data-active-teams")

if __name__ == "__main__":
    asyncio.run(main())
