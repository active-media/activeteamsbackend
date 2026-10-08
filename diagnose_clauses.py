"""READ-ONLY. Attributes the ~51s eventsdata query to specific clauses.

Timed against production so the recommendation is evidence-based rather than a
guess. Each variant is run twice and the second (warm) result is reported.
"""

import asyncio
import os
import sys
import time

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri, redact_uri  # noqa: E402

NOR_CELLS = {
    "$nor": [
        {"Event Type": {"$regex": "Cells", "$options": "i"}},
        {"eventType": {"$regex": "Cells", "$options": "i"}},
        {"eventTypeName": {"$regex": "Cells", "$options": "i"}},
    ]
}
ORG_OR = {
    "$or": [
        {"org_id": "active-teams"},
        {"Organization": {"$regex": "Active\\ Church", "$options": "i"}},
        {"Organisation": {"$regex": "active church", "$options": "i"}},
        {"Organization": {"$regex": "active church", "$options": "i"}},
        {"org_id": {"$exists": False}},
    ]
}


async def warm_time(events, query, label):
    await events.count_documents(query)  # warm
    t0 = time.perf_counter()
    docs = await events.find(query).to_list(length=3000)
    print(f"  {label:<46} {len(docs):>4} docs  {time.perf_counter() - t0:>7.2f}s")
    return time.perf_counter() - t0


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    events = client[os.getenv("DB_NAME", "active-teams-db")]["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")
    print("=== attributing the query cost (warm timings) ===")

    await warm_time(events, {"$and": [ORG_OR, NOR_CELLS]}, "full eventsdata query")
    print()
    await warm_time(events, {}, "whole collection, no filter")
    await warm_time(events, {"org_id": "active-teams"}, "org_id equality only")
    await warm_time(events, {"Organization": {"$regex": "Active Church", "$options": "i"}},
                    "Organization regex only")
    print()
    await warm_time(events, NOR_CELLS, "$nor Cells regex only")
    await warm_time(events, {"Event Type": {"$regex": "Cells", "$options": "i"}},
                    "single Event Type regex")
    print()
    await warm_time(events, ORG_OR, "org $or block only")
    await warm_time(events, {"$and": [NOR_CELLS, ORG_OR]}, "nor + org (no $and nesting)")

    print("\n=== indexes on Events ===")
    for name, info in (await events.index_information()).items():
        print(f"  {name}: {info.get('key')}")


if __name__ == "__main__":
    asyncio.run(run())
