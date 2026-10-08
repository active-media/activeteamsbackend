"""READ-ONLY. Inspects the shape of the `attendance` field so a size reduction can
preserve precisely the keys the frontend reads.

resolveHeadcount() in attendanceLogic.js reads event.attendance[date]
  .total_headcounts  (a number) and nothing else, and the Events grid explicitly
excludes `attendance` from its columns. This shows what else lives in there.
"""

import asyncio
import os
import sys

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


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    events = client[os.getenv("DB_NAME", "active-teams-db")]["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")

    docs = await events.find(QUERY).to_list(length=3000)

    shapes = {}
    for d in docs:
        att = d.get("attendance")
        if not isinstance(att, dict):
            continue
        for date_key, val in att.items():
            if isinstance(val, dict):
                shapes.setdefault("dict", set()).update(val.keys())
            elif isinstance(val, list):
                shapes.setdefault("list", set()).add(
                    f"list[{len(val)}]"
                )
                if val and isinstance(val[0], dict):
                    shapes.setdefault("attendee_fields", set()).update(val[0].keys())
            else:
                shapes.setdefault("scalar", set()).add(type(val).__name__)

    for k, v in shapes.items():
        print(f"{k}: {sorted(v)[:20]}")

    print("\n=== a concrete sample entry ===")
    for d in docs:
        att = d.get("attendance")
        if isinstance(att, dict) and att:
            key = sorted(att.keys())[0]
            val = att[key]
            print(f"event {str(d.get('Event Name'))[:40]!r}")
            print(f"  attendance keys: {len(att)}  (first: {key!r}, last: {sorted(att.keys())[-1]!r})")
            if isinstance(val, dict):
                print(f"  value keys      : {list(val.keys())}")
                for kk, vv in val.items():
                    kind = f"list[{len(vv)}]" if isinstance(vv, list) else type(vv).__name__
                    print(f"     {kk}: {kind}")
            elif isinstance(val, list):
                print(f"  value is list[{len(val)}]")
                if val:
                    print(f"     attendee fields: {list(val[0].keys())}")
            break

    print("\n=== how many attendance date-keys per document (top 8 by key count) ===")
    counts = sorted(
        ((len(d["attendance"]), d) for d in docs
         if isinstance(d.get("attendance"), dict) and d["attendance"]),
        key=lambda x: -x[0],
    )
    for n, d in counts[:8]:
        print(f"  {n:>4} dates  {str(d.get('Event Name'))[:44]!r}")


if __name__ == "__main__":
    asyncio.run(run())
