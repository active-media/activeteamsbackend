"""READ-ONLY. Asks MongoDB why the eventsdata query takes ~50s.

Uses explain("executionStats") so the answer comes from the server planner rather
than from wall-clock guessing. Also lists every database and Events size, in case
the app on Render is pointed at a different - much larger - collection than the
one measured so far.
"""

import asyncio
import json
import os
import sys
import time

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


def summarize(expl, label):
    es = expl.get("executionStats", {})
    print(f"\n--- {label} ---")
    print(f"  winning plan : {json.dumps(expl.get('queryPlanner', {}).get('winningPlan', {}))[:200]}")
    print(f"  nReturned    : {es.get('nReturned')}")
    print(f"  totalKeysExamined  : {es.get('totalKeysExamined')}")
    print(f"  totalDocsExamined  : {es.get('totalDocsExamined')}")
    print(f"  executionTimeMillis: {es.get('executionTimeMillis')}")
    if es.get("executionStages"):
        print(f"  stage        : {json.dumps(es['executionStages'])[:300]}")


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    print(f"URI: {redact_uri(resolve_mongo_uri())}\n")

    print("=== all databases and their Events collection sizes ===")
    for db_name in await client.list_database_names():
        try:
            info = await client[db_name].command("dbStats")
            n = info.get("collections", [])
            ev = await client[db_name]["Events"].estimated_document_count()
            print(f"  {db_name:<32} Events≈{ev:<8} totalCollections={n} "
                  f"dataSize={info.get('dataSize', 0) / 1e6:.1f}MB")
        except Exception as e:
            print(f"  {db_name:<32} (no access: {type(e).__name__})")

    db = client[os.getenv("DB_NAME", "active-teams-db")]
    events = db["Events"]
    total = await events.count_documents({})
    print(f"\n=== working in {os.getenv('DB_NAME', 'active-teams-db')}: {total} Events ===")

    print("\n=== indexes ===")
    for name, inf in (await events.index_information()).items():
        print(f"  {name}: {inf.get('key')}")

    print("\n=== explain('executionStats') for the eventsdata query ===")
    try:
        expl = await db.command(
            "explain",
            {"find": "Events", "filter": QUERY},
            verbosity="executionStats",
        )
        summarize(expl, "eventsdata query")
    except Exception as e:
        print(f"  explain failed: {type(e).__name__}: {e}")

    print("\n=== explain the $nor regex clause alone ===")
    try:
        expl2 = await db.command(
            "explain", {"find": "Events", "filter": NOR_CELLS}, verbosity="executionStats"
        )
        summarize(expl2, "$nor Cells regex only")
    except Exception as e:
        print(f"  explain failed: {type(e).__name__}: {e}")

    print("\n=== wall-clock: trivial query vs the real one ===")
    for label, q in (("find({}) limit 1", {"$limit": 1}),
                     ("count_documents({})", None),
                     ("eventsdata query", QUERY)):
        t0 = time.perf_counter()
        if q is None:
            n = await events.count_documents({})
        else:
            n = len(await events.find(q).to_list(length=1))
        print(f"  {label:<26} {n:>6}  {time.perf_counter() - t0:>7.2f}s")


if __name__ == "__main__":
    asyncio.run(run())
