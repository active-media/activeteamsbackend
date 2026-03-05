# copy_user.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"
client = AsyncIOMotorClient(MONGO_URI)

async def main():
    source = client["active-teams-db"]
    target = client["test-data-active-teams"]

    # Copy your user across
    user = await source["Users"].find_one({"email": "tkgenia1234@gmail.com"})
    
    if not user:
        print("User not found!")
        client.close()
        return

    user["org_id"] = "active-teams"

    await target["Users"].update_one(
        {"email": user["email"]},
        {"$set": user},
        upsert=True
    )
    print(f"Copied {user['email']} to test-data-active-teams")
    client.close()

asyncio.run(main())