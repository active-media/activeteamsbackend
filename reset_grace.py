# reset_grace.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from passlib.context import CryptContext

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)
db = client["test-data-active-teams"]
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def main():
    result = await db["Users"].update_one(
        {"email": "grace@test.com"},
        {"$set": {"password": pwd_context.hash("test1234")}}
    )
    print(f"Updated: {result.modified_count} user")
    client.close()

asyncio.run(main())