"""READ-ONLY. Measures how much data the eventsdata query actually ships.

explain() shows the server runs the filter in 2ms, so the ~50s is spent moving
and decoding results. This measures BSON payload size per document and per field,
to find what dominates.
"""

import asyncio
import os
import sys
import time

from bson import BSON
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri, redact_uri  # noqa: E402
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


def deep_size(obj):
    try:
        return len(BSON.encode({"v": obj}))
    except Exception:
        return 0


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[os.getenv("DB_NAME", "active-teams-db")]
    events = db["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")

    t0 = time.perf_counter()
    docs = await events.find(QUERY).to_list(length=3000)
    fetch_ms = (time.perf_counter() - t0) * 1000

    total = sum(deep_size(d) for d in docs)
    print(f"documents returned : {len(docs)}")
    print(f"total BSON payload : {total / 1e6:.2f} MB")
    print(f"wall clock to_list : {fetch_ms:.0f} ms")
    print(f"implied throughput : {total / 1e6 / (fetch_ms / 1000):.2f} MB/s\n")

    print("=== largest documents ===")
    sized = sorted(((deep_size(d), d) for d in docs), key=lambda x: -x[0])
    for size, d in sized[:8]:
        print(f"  {size/1e6:>7.2f} MB  {str(d.get('Event Name'))[:40]!r}")

    print("\n=== which fields dominate (summed across all docs) ===")
    field_totals = {}
    for _, d in sized:
        for k, v in d.items():
            field_totals[k] = field_totals.get(k, 0) + deep_size(v)
    for k, v in sorted(field_totals.items(), key=lambda kv: -kv[1])[:14]:
        pct = 100 * v / total if total else 0
        print(f"  {v/1e6:>7.2f} MB  {pct:>5.1f}%  {k}")

    print("\n=== how big is the whole Events collection? ===")
    stats = await db.command("dbStats")
    print(f"  dataSize: {stats.get('dataSize', 0)/1e6:.1f} MB across "
          f"{await events.count_documents({})} docs "
          f"(avg {stats.get('dataSize', 0)/max(1, await events.count_documents({}))/1e3:.1f} KB/doc)")

    print("\n=== timing: same query with a projection dropping the heavy fields ===")
    heavy = {"attendance": 0, "new_people": 0, "consolidations": 0,
             "attendees": 0, "attendance_data": 0, "consolidation_data": 0}
    proj = {f: 0 for f in heavy}
    t0 = time.perf_counter()
    lean = await events.find(QUERY, proj).to_list(length=3000)
    lean_ms = (time.perf_counter() - t0) * 1000
    lean_total = sum(deep_size(d) for d in lean)
    print(f"  full     : {len(docs)} docs, {total/1e6:>6.2f} MB, {fetch_ms:>7.0f} ms")
    print(f"  projected: {len(lean)} docs, {lean_total/1e6:>6.2f} MB, {lean_ms:>7.0f} ms")
    if fetch_ms:
        print(f"  speedup  : {fetch_ms / lean_ms:.1f}x faster, "
              f"{100 * (1 - lean_total / total):.0f}% less data")


if __name__ == "__main__":
    asyncio.run(run())
