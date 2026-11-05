# reset_grace.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from auth.utils import hash_password

from database import resolve_mongo_uri

MONGO_URI = resolve_mongo_uri()
client = AsyncIOMotorClient(MONGO_URI)
db = client["test-data-active-teams"]

async def main():
    result = await db["Users"].update_one(
        {"email": "grace@test.com"},
        {"$set": {"password": hash_password("test1234")}}
    )
    print(f"Updated: {result.modified_count} user")
    client.close()

asyncio.run(main())