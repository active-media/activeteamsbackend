"""Reactivate events that were deactivated permanently.

Background
----------
`PUT /events/deactivate` can set `is_permanent_deact: True`, which makes a cell
invisible in the Cells list (the list only matches `is_active: True` or a missing
`is_active`). The nightly `auto_reactivate_expired_events` job deliberately skips
these documents:

    {"$or": [{"is_permanent_deact": {"$ne": True}}]}

so they never come back on their own. This script restores them.

Unlike that job, this script does NOT clear the deactivation audit fields. The
nightly job nulls `deactivation_reason` and `deactivation_start`, permanently
destroying the record of why each cell was taken offline; here they are kept.

SAFETY
------
Defaults to a dry run. Nothing is written unless you pass --apply. Take a
mongodump first.

Usage
-----
    python3 reactivate_events.py                 # dry run, prints what would change
    python3 reactivate_events.py --apply         # perform the update
    python3 reactivate_events.py --apply --org active-teams
    python3 reactivate_events.py --limit 50      # cap how many are touched
"""

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform the update. Without this, nothing is written.",
    )
    parser.add_argument(
        "--org",
        default="active-teams",
        help="Only reactivate events tagged with this org_id (default: active-teams).",
    )
    parser.add_argument(
        "--include-other-orgs",
        action="store_true",
        help="Also reactivate untagged/other-org events. Off by default so this "
             "cannot silently reach across organisations.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum documents to update (0 = no limit).",
    )
    args = parser.parse_args()

    db_name = os.getenv("DB_NAME", "active-teams-db")
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[db_name]
    events = db["Events"]

    query = {"is_permanent_deact": True}
    if not args.include_other_orgs:
        query["org_id"] = args.org

    total = await events.count_documents(query)
    all_perm = await events.count_documents({"is_permanent_deact": True})
    untagged_perm = await events.count_documents(
        {"is_permanent_deact": True, "org_id": {"$exists": False}}
    )

    print(f"Database:  {db_name}.Events")
    print(f"Matching:  {query}")
    print(f"Matches:   {total}")
    print(f"\nFor context, {all_perm} documents are permanently deactivated in total:")
    print(f"  tagged   org_id={args.org!r} : "
          f"{await events.count_documents({'is_permanent_deact': True, 'org_id': args.org})}")
    print(f"  tagged   some other org     : "
          f"{all_perm - untagged_perm - await events.count_documents({'is_permanent_deact': True, 'org_id': args.org})}")
    print(f"  UNTAGGED (no org_id field)  : {untagged_perm}")
    if untagged_perm and not args.include_other_orgs:
        print(
            f"\n  NOTE: {untagged_perm} of these have no org_id and will NOT be touched\n"
            f"        by this run. Add --include-other-orgs to reach them."
        )

    if total == 0:
        print("\nNothing to do.")
        return 0

    cursor = events.find(query)
    if args.limit:
        cursor = cursor.limit(args.limit)

    sample = []
    reasons = {}
    # Collect the ids actually being previewed so the write targets exactly the
    # same set. Previously --limit only capped this preview while the update
    # below re-ran the full query, so it would have modified every document.
    target_ids = []
    async for doc in cursor:
        target_ids.append(doc["_id"])
        reason = doc.get("deactivation_reason")
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1
        if len(sample) < 10:
            sample.append(
                "  - {!r} (is_active={!r}, org_id={!r}, reason={!r})".format(
                    doc.get("Event Name") or doc.get("eventName") or doc.get("_id"),
                    doc.get("is_active"),
                    doc.get("org_id"),
                    reason,
                )
            )

    print(f"\nDeactivation reasons found:")
    if reasons:
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"  {count:>5}  {reason}")
    else:
        print("  (none recorded)")

    print(f"\nFirst {len(sample)} to be reactivated:")
    for line in sample:
        print(line)
    if total > len(sample):
        print(f"  ... and {total - len(sample)} more")

    if not args.apply:
        print("\nDRY RUN - nothing was written. Re-run with --apply to perform this.")
        return 0

    # Set is_active True. deactivation_end is left as-is so the record of when
    # the deactivation was meant to lapse survives, and deactivation_reason /
    # deactivation_start are deliberately preserved (the nightly
    # auto_reactivate_expired_events job nulls them, destroying the audit trail).
    result = await events.update_many(
        {"_id": {"$in": target_ids}}, {"$set": {"is_active": True}}
    )

    print(f"\nAPPLIED - modified {result.modified_count} document(s).")
    still_hidden = await events.count_documents(query)
    print(f"Still permanently deactivated: {still_hidden}")
    if args.limit and still_hidden:
        print(
            f"NOTE: {still_hidden} remain. Re-run to continue, or raise --limit."
        )
    return 0


if __name__ == "__main__":
    asyncio.run(main())
