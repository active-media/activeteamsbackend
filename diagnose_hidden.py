"""READ-ONLY. Enumerates the event documents that /events/eventsdata does NOT return.

Previous assumptions were wrong: the 3392 untagged docs are almost all Cells
(which eventsdata excludes anyway) and they carry an Organization field, so the
existing regex branch already matched them. This lists what is genuinely missing.
"""

import asyncio
import os
import re
import sys
from collections import Counter

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
ORG, NAME = "active-teams", "Active Church"
NEW_QUERY = {"$and": [{"$or": build_event_org_conditions(ORG, NAME)}, NOR_CELLS]}


def is_non_cells(doc):
    for f in ("Event Type", "eventType", "eventTypeName"):
        v = doc.get(f)
        if v and re.search("cells", str(v), re.I):
            return False
    return True


async def main():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[os.getenv("DB_NAME", "active-teams-db")]
    events = db["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")

    visible_ids = {
        d["_id"] for d in await events.find(NEW_QUERY, {"_id": 1}).to_list(length=20000)
    }
    print(f"eventsdata returns (doc level): {len(visible_ids)}")

    all_docs = await events.find({}).to_list(length=20000)
    non_cells = [d for d in all_docs if is_non_cells(d)]
    print(f"total docs                     : {len(all_docs)}")
    print(f"non-Cells docs                 : {len(non_cells)}")
    print(f"non-Cells VISIBLE              : {sum(1 for d in non_cells if d['_id'] in visible_ids)}")
    hidden = [d for d in non_cells if d["_id"] not in visible_ids]
    print(f"non-Cells HIDDEN               : {len(hidden)}\n")

    print("=== why the hidden docs are hidden (grouped) ===")
    groups = Counter()
    for d in hidden:
        org = d.get("org_id", "<missing>")
        has_org_field = "Organization" in d
        key = (org, f"has_Organization={has_org_field}")
        groups[key] += 1
    for (org, hf), n in groups.most_common():
        print(f"  {n:>4}  org_id={org!r:<28} {hf}")

    print("\n=== sample hidden non-Cells docs ===")
    for d in hidden[:12]:
        print(f"  - {str(d.get('Event Name'))[:46]!r} type={d.get('Event Type')!r} "
              f"org_id={d.get('org_id', '<missing>')!r} "
              f"Organization={(d.get('Organization') or '<missing>')!r}")

    print("\n=== does the date window hide anything? ===")
    print("  eventsdata start_date default is 2025-10-10 in the backend signature")
    dated = [d for d in non_cells if d.get("date")]
    print(f"  non-Cells docs carrying a 'date' field: {len(dated)}")
    if dated:
        vals = sorted(str(d["date"])[:10] for d in dated)
        print(f"  earliest date in data : {vals[0]}")
        print(f"  latest date in data   : {vals[-1]}")
        before = sum(1 for v in vals if v < "2025-10-10")
        print(f"  dated before 2025-10-10 (hidden by backend default): {before}")
    missing_date = [d for d in non_cells if not d.get("date")]
    print(f"  non-Cells docs with NO 'date' field: {len(missing_date)}")

    print("\n=== created_at presence (affects the deterministic sort+cap) ===")
    no_created = sum(1 for d in all_docs if "created_at" not in d)
    print(f"  docs missing created_at: {no_created} / {len(all_docs)}")
    print(f"  MAX_EVENT_DOCUMENTS cap: 3000")


if __name__ == "__main__":
    asyncio.run(main())
