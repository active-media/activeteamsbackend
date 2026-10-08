"""READ-ONLY. Times the Events query with and without the added sort.

cProfile showed ~48s inside a single cursor._refresh, i.e. the initial find().
The deterministic sort on ("created_at", -1) is unserved by any index - most
documents have no created_at at all - so this checks whether adding the sort is
what made the query slow.
"""

import asyncio
import os
import re
import sys
import time

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri, redact_uri, MAX_EVENT_DOCUMENTS  # noqa: E402
from main import build_event_org_conditions  # noqa: E402

NOR_CELLS = {
    "$nor": [
        {"Event Type": {"$regex": "Cells", "$options": "i"}},
        {"eventType": {"$regex": "Cells", "$options": "i"}},
        {"eventTypeName": {"$regex": "Cells", "$options": "i"}},
    ]
}
QUERY = {"$and": [
    {"$or": build_event_org_conditions("active-teams", "Active Church")}, NOR_CELLS,
]}


async def timeit(label, factory):
    t0 = time.perf_counter()
    n = await factory()
    print(f"  {label:<52} {n:>4} docs  {time.perf_counter() - t0:>7.2f}s")


def counted(coro):
    """Wrap an awaitable-producing callable so timeit gets a coroutine factory."""
    async def wrapper():
        return len(await coro)
    return wrapper


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    events = client[os.getenv("DB_NAME", "active-teams-db")]["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")
    print("=== eventsdata query, repeated ===")

    for i in range(2):
        print(f" pass {i + 1}:")
        await timeit("find(query)  (no sort, no limit)  [original]",
                     counted(events.find(QUERY).to_list(length=None)))
        await timeit("find(query).sort(created_at,_id).limit(3000)  [added]",
                     counted(events.find(QUERY).sort([("created_at", -1), ("_id", -1)])
                             .to_list(length=MAX_EVENT_DOCUMENTS)))
        await timeit("find(query).sort(_id only).limit(3000)",
                     counted(events.find(QUERY).sort([("_id", -1)])
                             .to_list(length=MAX_EVENT_DOCUMENTS)))
        await timeit("count_documents(query)",
                     counted(events.count_documents(QUERY)))

    print("\n=== existing indexes on Events ===")
    for name, info in (await events.index_information()).items():
        print(f"  {name}: {info.get('key')}")


if __name__ == "__main__":
    asyncio.run(run())
