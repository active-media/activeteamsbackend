# add_test_user.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from passlib.context import CryptContext
from datetime import datetime

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)
db = client["test-church-db"]

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def add_user():
    await db["Users"].insert_one({
        "name":       "Test",
        "surname":    "Admin",
        "email":      "tkgenia1234@gmail.com",
        "password":   pwd_context.hash("liana2022"),
        "role":       "admin",
        "org_id":     "grace-church",
        "created_at": datetime.utcnow(),
    })
    print("User added!")
    client.close()

asyncio.run(add_user())