"""READ-ONLY. Measures the two-phase read against production and proves the
counts it produces are identical to the counts the old full-document read
produced.

Correctness is the whole risk here. Phase 1 stops transferring attendee arrays
and hands the handler counts instead, so every status and every number on the
page is now derived from a count. If a count is wrong, an event silently reads
as incomplete and vanishes from the COMPLETED tab - the bug we are fixing. So
this compares, per document and per attendance date, the old way against the
new way, and reports any disagreement.

Run:
    venv/bin/python diagnose_two_phase.py
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
from main import (  # noqa: E402
    build_event_org_conditions,
    build_event_counts_pipeline,
    build_page_detail_projection,
    enrich_attendees_with_financials,
)

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

ARRAYS = ("attendees", "persistent_attendees", "new_people", "consolidations")


def bson_size(obj):
    try:
        return len(BSON.encode({"v": obj}))
    except Exception:
        return 0


async def timed(label, coro):
    t0 = time.perf_counter()
    docs = await coro
    elapsed = time.perf_counter() - t0
    print(f"  {label:<34} {len(docs):>4} docs  "
          f"{sum(bson_size(d) for d in docs)/1e6:>7.2f}MB  {elapsed:>7.2f}s")
    return docs, elapsed


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[os.getenv("DB_NAME", "active-teams-db")]
    events = db["Events"]
    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")

    print("=== phase 1 alone, cold then warm (cache is per-process) ===")
    import main as main_mod
    main_mod.clear_event_counts_cache()
    cold, cold_s = await timed("phase 1 (counts only)", events.aggregate(
        build_event_counts_pipeline(QUERY)).to_list(length=3000))
    main_mod.clear_event_counts_cache()
    warm, warm_s = await timed("phase 1 (counts only)", events.aggregate(
        build_event_counts_pipeline(QUERY)).to_list(length=3000))

    print("\n=== the old read, for comparison ===")
    old, old_s = await timed("old: find() with full documents",
                             events.find(QUERY).to_list(length=3000))

    old_mb = sum(bson_size(d) for d in old) / 1e6
    new_mb = sum(bson_size(d) for d in cold) / 1e6
    print(f"\n  payload  {old_mb:.2f}MB -> {new_mb:.2f}MB  "
          f"({100 * (1 - new_mb / old_mb):.0f}% less)")
    print(f"  time     {old_s:.2f}s -> {warm_s:.2f}s  ({old_s / max(warm_s, 0.01):.1f}x faster)")

    print("\n=== correctness: do the counts match the real arrays? ===")
    old_by_id = {str(d["_id"]): d for d in old}
    new_by_id = {str(d["_id"]): d for d in cold}

    if set(old_by_id) != set(new_by_id):
        print(f"  DOCUMENT SET DIFFERS: only-old={set(old_by_id) - set(new_by_id)} "
              f"only-new={set(new_by_id) - set(old_by_id)}")

    mismatches = []
    checked_dates = 0
    for key, new_doc in new_by_id.items():
        old_doc = old_by_id.get(key)
        if old_doc is None:
            continue

        for field, source in (
            ("attendees_count", "attendees"),
            ("new_people_count", "new_people"),
            ("consolidation_count", "consolidations"),
            ("persistent_attendees_count", "persistent_attendees"),
        ):
            real = old_doc.get(source)
            expected = len(real) if isinstance(real, list) else 0
            actual = new_doc.get(field)
            if actual != expected:
                mismatches.append(
                    f"{key} {field} (event level): expected {expected} from "
                    f"len({source!r}), got {actual}")

        old_att = old_doc.get("attendance") or {}
        new_att = new_doc.get("attendance") or {}
        if set(old_att) != set(new_att):
            mismatches.append(
                f"{key} attendance keys differ: only-old={set(old_att) - set(new_att)} "
                f"only-new={set(new_att) - set(old_att)}")
        for date, new_entry in new_att.items():
            old_entry = old_att.get(date) or {}
            checked_dates += 1
            for array, count in (("attendees", "attendees_count"),
                                 ("new_people", "new_people_count"),
                                 ("consolidations", "consolidation_count")):
                real = old_entry.get(array)
                expected = len(real) if isinstance(real, list) else 0
                if new_entry.get(count) != expected:
                    mismatches.append(
                        f"{key} {date} {count}: expected {expected} from "
                        f"len({array!r}), got {new_entry.get(count)}")
            # status / is_did_not_meet are supplied with "" / False defaults by the
            # pipeline when absent, so compare the *effective* value the handler
            # would see rather than None-vs-"" noise.
            for scalar, default in (("status", ""), ("is_did_not_meet", False)):
                expected = old_entry.get(scalar, default) or default
                if new_entry.get(scalar, default) != expected:
                    mismatches.append(
                        f"{key} {date} {scalar}: expected {expected!r}, "
                        f"got {new_entry.get(scalar)!r}")

    print(f"  documents compared : {len(new_by_id)}")
    print(f"  attendance dates   : {checked_dates}")
    if mismatches:
        print(f"  MISMATCHES         : {len(mismatches)}")
        for m in mismatches[:25]:
            print(f"      {m}")
    else:
        print("  MISMATCHES         : 0  - every count and status matches the old read")

    print("\n=== phase 2: people for one page of 25 rows ===")
    # Stand in for the page the handler would return: the 25 most recent dates.
    # Use the real _id values, not the stringified keys, and take the 25 newest
    # dates so the probe looks like an actual page of rows.
    real_ids = [d["_id"] for d in old]
    page_dates = sorted(
        {date for doc in old for date in (doc.get("attendance") or {})},
        reverse=True,
    )[:25]
    page_keys = {f"attendance.{date}" for date in page_dates}

    detail, detail_s = await timed(
        f"phase 2 ({len(page_keys)} date subkeys)",
        events.find({"_id": {"$in": real_ids}},
                    build_page_detail_projection(page_keys)).to_list(length=3000))

    people = sum(
        bson_size(e.get(a))
        for d in detail for e in (d.get("attendance") or {}).values()
        for a in ARRAYS if isinstance(e, dict)
    ) + sum(bson_size(d.get(a)) for d in detail for a in ARRAYS)
    print(f"  people bytes on that page: {people/1e6:.2f}MB")

    print("\n=== projected total for a page of 25 ===")
    total = warm_s + detail_s
    print(f"  phase 1 {warm_s:.2f}s + phase 2 {detail_s:.2f}s = {total:.2f}s")
    print(f"  was {old_s:.2f}s  ->  {old_s / max(total, 0.01):.1f}x faster")
    if warm_s > 0:
        print(f"  phase 2 is {100 * detail_s / total:.0f}% of the new total")


if __name__ == "__main__":
    asyncio.run(run())
