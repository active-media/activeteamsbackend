"""
leader_id_diagnostic.py
=======================
Run this script ONCE to:
  1. Inspect what values are actually stored in LeaderId across your people collection
  2. Try to match those values to real users in the users collection
  3. Print a clear report so you can decide the migration strategy
  4. Optionally run the migration to replace name/email LeaderId values
     with the actual MongoDB ObjectId of the matching user

HOW TO RUN:
  python leader_id_diagnostic.py --diagnose        # just inspect, no changes
  python leader_id_diagnostic.py --migrate         # inspect + fix matched records
  python leader_id_diagnostic.py --migrate --dry-run  # show what would change, no writes
"""

import asyncio
import argparse
import os
import re
from collections import Counter, defaultdict
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
DB_NAME   = os.getenv("DB_NAME", "test-data-active-teams")

if not MONGO_URI:
    raise SystemExit(
        "\n❌  MONGODB_URI is not set.\n"
        "    Make sure your .env file exists and contains:\n"
        "    MONGODB_URI=mongodb+srv://user:password@cluster.mongodb.net/dbname\n"
        "    Then re-run the script from the same directory as your .env file.\n"
    )

print(f"Connecting to: {MONGO_URI[:40]}...")  # only print first 40 chars for safety
client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db     = client[DB_NAME]

people_collection = db["people"]
users_collection  = db["users"]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def looks_like_object_id(val: str) -> bool:
    return bool(re.match(r'^[a-f0-9]{24}$', (val or '').strip().lower()))

def looks_like_email(val: str) -> bool:
    return bool(re.match(r'^[^@]+@[^@]+\.[^@]+$', (val or '').strip().lower()))

def classify_value(val: str) -> str:
    if not val or val.strip() == '':
        return 'empty'
    if looks_like_object_id(val):
        return 'objectid'
    if looks_like_email(val):
        return 'email'
    # Looks like a name if it has a space or is a single word with capitals
    return 'name_string'


# ─── Step 1: Diagnose ─────────────────────────────────────────────────────────

async def diagnose():
    print("\n" + "="*60)
    print("STEP 1: Inspecting LeaderId values in people collection")
    print("="*60)

    total = await people_collection.count_documents({})
    print(f"Total people: {total}")

    # Sample LeaderId values
    cursor = people_collection.find(
        {}, {"_id": 1, "Name": 1, "Surname": 1, "LeaderId": 1, "leader_id": 1}
    )
    docs = await cursor.to_list(length=None)

    type_counts = Counter()
    sample_by_type = defaultdict(list)
    all_leader_id_values = set()

    for doc in docs:
        raw = doc.get("LeaderId") or doc.get("leader_id") or ""
        val = str(raw).strip() if raw else ""
        t = classify_value(val)
        type_counts[t] += 1
        if len(sample_by_type[t]) < 5:
            sample_by_type[t].append({
                "person": f"{doc.get('Name','')} {doc.get('Surname','')}".strip(),
                "LeaderId": val
            })
        if val:
            all_leader_id_values.add(val)

    print(f"\nLeaderId value types found:")
    for t, count in type_counts.most_common():
        print(f"  {t:20s}: {count:6d} records")

    print(f"\nSample values per type:")
    for t, samples in sample_by_type.items():
        print(f"\n  [{t}]")
        for s in samples:
            print(f"    Person: {s['person']!r:30s}  LeaderId: {s['LeaderId']!r}")

    # ── Step 2: Check which leaders exist as users ───────────────────────────
    print("\n" + "="*60)
    print("STEP 2: Matching LeaderId values to users collection")
    print("="*60)

    # Build user lookup maps
    users_cursor = users_collection.find(
        {}, {"_id": 1, "name": 1, "surname": 1, "email": 1}
    )
    all_users = await users_cursor.to_list(length=None)

    user_by_id    = {str(u["_id"]): u for u in all_users}
    user_by_email = {(u.get("email") or "").lower().strip(): u for u in all_users}
    user_by_name  = {}
    user_name_collisions = set()  # names shared by >1 user — CANNOT match safely

    for u in all_users:
        full = f"{u.get('name','')} {u.get('surname','')}".strip().lower()
        if full in user_by_name:
            user_name_collisions.add(full)  # duplicate — unsafe to match
        else:
            user_by_name[full] = u

    print(f"\nUsers in system:          {len(all_users)}")
    print(f"Unique names (safe):      {len(user_by_name) - len(user_name_collisions)}")
    print(f"Duplicate names (UNSAFE): {len(user_name_collisions)}")
    if user_name_collisions:
        print("  Duplicates:", list(user_name_collisions)[:10])

    # Try matching each unique LeaderId value
    matched_to_user    = {}   # leader_id_value → user doc
    unmatched_values   = []
    collision_values   = []   # name matched but multiple users share it

    for val in all_leader_id_values:
        t = classify_value(val)
        if t == 'objectid':
            u = user_by_id.get(val)
            if u:
                matched_to_user[val] = u
            else:
                unmatched_values.append((val, t, "no user with this _id"))
        elif t == 'email':
            u = user_by_email.get(val.lower())
            if u:
                matched_to_user[val] = u
            else:
                unmatched_values.append((val, t, "no user with this email"))
        elif t == 'name_string':
            norm = val.lower().strip()
            if norm in user_name_collisions:
                collision_values.append(val)
            elif norm in user_by_name:
                matched_to_user[val] = user_by_name[norm]
            else:
                unmatched_values.append((val, t, "no user with this name"))

    print(f"\nLeaderId values that match a user:  {len(matched_to_user)}")
    print(f"LeaderId values with no user match: {len(unmatched_values)}")
    print(f"LeaderId values with name collision: {len(collision_values)} (UNSAFE — skipped)")

    if matched_to_user:
        print("\nSample matches (LeaderId value → user):")
        for val, u in list(matched_to_user.items())[:8]:
            print(f"  {val!r:35s} → {u.get('name','')} {u.get('surname','')} ({str(u['_id'])})")

    if unmatched_values:
        print(f"\nSample unmatched values:")
        for val, t, reason in unmatched_values[:8]:
            print(f"  {val!r:35s} ({t}) — {reason}")

    if collision_values:
        print(f"\nCollision values (skipped — cannot safely resolve):")
        for val in collision_values[:8]:
            print(f"  {val!r}")

    return matched_to_user, collision_values, unmatched_values, type_counts


# ─── Step 3: Migrate ─────────────────────────────────────────────────────────

async def migrate(dry_run: bool = False):
    matched_to_user, collision_values, unmatched_values, type_counts = await diagnose()

    # Skip migration if LeaderIds are already all ObjectIds
    already_objectid = type_counts.get('objectid', 0)
    total_non_empty  = sum(v for k, v in type_counts.items() if k != 'empty')
    if already_objectid == total_non_empty:
        print("\n✅ All non-empty LeaderId values are already ObjectIds. No migration needed.")
        return

    print("\n" + "="*60)
    print(f"STEP 3: {'DRY RUN — ' if dry_run else ''}Migrating matched LeaderId values to ObjectIds")
    print("="*60)

    # For each matched value, update all people records that have it
    updated_total  = 0
    skipped_total  = 0

    for old_val, user in matched_to_user.items():
        if looks_like_object_id(old_val):
            # Already an ObjectId string — check it points to a valid user
            continue

        new_val = str(user["_id"])
        filter_ = {
            "$or": [
                {"LeaderId": old_val},
                {"leader_id": old_val},
            ]
        }

        count = await people_collection.count_documents(filter_)
        if count == 0:
            continue

        print(f"\n  {old_val!r:35s} → {new_val}  ({count} people affected)")

        if not dry_run:
            result = await people_collection.update_many(
                filter_,
                {"$set": {"LeaderId": new_val, "leader_id": new_val}}
            )
            updated_total += result.modified_count
            print(f"    ✅ Updated {result.modified_count} records")
        else:
            print(f"    [DRY RUN] Would update {count} records")
            updated_total += count

    # Summary
    print(f"\n{'='*60}")
    if dry_run:
        print(f"DRY RUN complete. Would have updated ~{updated_total} people records.")
        print(f"Skipped {len(collision_values)} collision values and {len(unmatched_values)} unmatched values.")
        print("\nRun without --dry-run to apply changes.")
    else:
        print(f"Migration complete. Updated {updated_total} people records.")
        print(f"Skipped {len(collision_values)} collision values (duplicate names — manual fix needed).")
        print(f"Skipped {len(unmatched_values)} unmatched values (no user found).")

    if collision_values or unmatched_values:
        print("\n⚠️  Some records could not be automatically migrated.")
        print("   Run --diagnose again and manually assign correct ObjectIds for:")
        for val in collision_values[:5]:
            print(f"     [name collision] {val!r}")
        for val, t, reason in unmatched_values[:5]:
            print(f"     [unmatched] {val!r} — {reason}")


# ─── Entry point ─────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="LeaderId diagnostic and migration tool")
    parser.add_argument("--diagnose",  action="store_true", help="Inspect LeaderId values only")
    parser.add_argument("--migrate",   action="store_true", help="Migrate LeaderId values to ObjectIds")
    parser.add_argument("--dry-run",   action="store_true", help="Show what would change without writing")
    args = parser.parse_args()

    if args.migrate:
        await migrate(dry_run=args.dry_run)
    else:
        await diagnose()

    client.close()

if __name__ == "__main__":
    asyncio.run(main())