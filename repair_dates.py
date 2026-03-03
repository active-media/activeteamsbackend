"""Simple one-off script to convert string-valued dates in the tasks
collection into real BSON Date objects so that the stats pipelines will
match them correctly.

Run this from the project root with the appropriate environment (same
settings used by the FastAPI app), e.g.:`python repair_dates.py`.

It is safe to run multiple times; already-correct documents are untouched.
"""

from pymongo import MongoClient
from datetime import datetime
import os

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "active-teams-db")

client = MongoClient(MONGO_URI)
db = client.get_database(DB_NAME)
tasks = db.tasks


def to_dt(v):
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None
    return v


if __name__ == "__main__":
    print("Scanning tasks for string dates...")
    cursor = tasks.find({
        "$or": [
            {"followup_date": {"$type": "string"}},
            {"completedAt": {"$type": "string"}},
            {"createdAt": {"$type": "string"}}
        ]
    })
    count = 0
    for t in cursor:
        upd = {}
        for field in ("followup_date", "completedAt", "createdAt"):
            if field in t and isinstance(t[field], str):
                dt = to_dt(t[field])
                if dt:
                    upd[field] = dt
        if upd:
            tasks.update_one({"_id": t["_id"]}, {"$set": upd})
            print("converted", t["_id"], upd)
            count += 1
    print(f"Done. Converted {count} documents.")