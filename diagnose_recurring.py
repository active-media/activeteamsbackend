"""READ-ONLY. Quantifies the real root cause of disappearing events.

In /events/eventsdata the recurring branch loops `for week_back in range(0, 1)`,
so every recurring event contributes at most ONE instance: the current week's.
Past occurrences are never generated, and if this week's occurrence is still in
the future the event disappears from the list entirely.

This simulates both the current logic and a corrected walk-back, using the real
production documents, and reports the difference.
"""

import asyncio
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta

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
DAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def recurring_days_of(doc):
    for f in ("recurring_day", "recurringDay", "recurring_days"):
        v = doc.get(f)
        if isinstance(v, list) and v:
            return v
    if doc.get("recurring") is True:
        return []
    return None


async def main():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[os.getenv("DB_NAME", "active-teams-db")]
    docs = await db["Events"].find(QUERY).to_list(length=20000)
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")

    today = datetime.now().date()
    print(f"today = {today}\n")
    print(f"non-Cells docs in scope: {len(docs)}")

    recurring, non_recurring, undated = [], [], []
    for d in docs:
        rd = recurring_days_of(d)
        if rd is not None:
            recurring.append(d)
        elif d.get("date") or d.get("Date Of Event") or d.get("eventDate"):
            non_recurring.append(d)
        else:
            undated.append(d)

    print(f"  recurring (have recurring_day) : {len(recurring)}")
    print(f"  one-off (have a date)         : {len(non_recurring)}")
    print(f"  NEITHER date nor recurring_day : {len(undated)}  <-- silently dropped\n")

    # --- current logic: one instance, current week only ---
    current = 0
    current_visible = 0
    for d in recurring:
        rd = recurring_days_of(d) or []
        week_start = today - timedelta(days=today.weekday())
        for raw in rd:
            wd = DAY_MAP.get(str(raw).strip().lower())
            if wd is None:
                continue
            inst = week_start + timedelta(days=wd)
            if inst > today:
                continue
            current += 1
            current_visible += 1

    # --- corrected logic: walk back to the requested start date ---
    for label, start in (("backend default 2025-10-10", datetime(2025, 10, 10).date()),
                         ("frontend old default 2025-11-30", datetime(2025, 11, 30).date()),
                         ("frontend new default 2015-01-01", datetime(2015, 1, 1).date())):
        total = 0
        for d in recurring:
            rd = recurring_days_of(d) or []
            week_start = today - timedelta(days=today.weekday())
            weeks = (today - start).days // 7 + 1
            for raw in rd:
                wd = DAY_MAP.get(str(raw).strip().lower())
                if wd is None:
                    continue
                for back in range(weeks):
                    inst = (week_start + timedelta(days=wd)) - timedelta(weeks=back)
                    if inst > today or inst < start:
                        continue
                    total += 1
        print(f"  corrected, start={label:<34} -> {total} recurring instances")

    print(f"\n  CURRENT logic total recurring instances: {current_visible}")
    print(f"  (each recurring event contributes at most 1; past weeks are dropped)")

    print("\n=== why recurring events vanish today ===")
    future = 0
    for d in recurring:
        rd = recurring_days_of(d) or []
        week_start = today - timedelta(days=today.weekday())
        any_in_past = False
        for raw in rd:
            wd = DAY_MAP.get(str(raw).strip().lower())
            if wd is None:
                continue
            if (week_start + timedelta(days=wd)) <= today:
                any_in_past = True
        if not any_in_past:
            future += 1
    print(f"  recurring events whose only instance this week is in the FUTURE: "
          f"{future} / {len(recurring)}")
    print("  these render as zero rows in Events and Service Check-in history")

    print("\n=== the undated docs (dropped by `else: continue`) ===")
    for d in undated[:10]:
        print(f"  - {str(d.get('Event Name'))[:44]!r} type={d.get('Event Type')!r} "
              f"org_id={d.get('org_id', '<missing>')!r}")


if __name__ == "__main__":
    asyncio.run(main())
