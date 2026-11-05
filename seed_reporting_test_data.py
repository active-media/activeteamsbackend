"""
seed_reporting_test_data.py

Populates Supabase with TEST data so the Reporting Dashboard has something to show:
3 campuses x (church service, cells, school cell, Life Class, School of Leaders, Plan 40),
weekly sessions + attendees for the previous month and this month, decisions,
new visitors, service targets and Twelve Tasks data.

Every row it creates is recorded in seed_manifest.json so it can be removed again.

    python seed_reporting_test_data.py                 # dry run: prints what WOULD be created
    python seed_reporting_test_data.py --apply         # writes to the database (asks to confirm)
    python seed_reporting_test_data.py --cleanup       # deletes everything listed in the manifest

Run it from the backend root (or from supabase_helpers/). Use a DEV / STAGING project -
the project it points at also holds your real People, Users and Tasks.

Markers on seeded rows (so they are easy to spot): event_name starts with "TESTSEED",
events.source_format = 'seed', People.imported_by = 'testseed', Tasks.created_by = 'testseed'.
Nothing existing is modified.
"""
import argparse
import json
import os
import random
import secrets
import sys
import uuid
import httpx
from datetime import date, datetime, timedelta, timezone

_here = os.path.dirname(os.path.abspath(__file__))
for _p in (_here, os.path.dirname(_here)):
    if os.path.isdir(os.path.join(_p, "supabase_helpers")) and _p not in sys.path:
        sys.path.insert(0, _p)
        os.chdir(_p)
        break

try:
    from supabase_helpers.supabase_connection import supabase_admin as db
except Exception as exc:
    print(f"Could not import supabase_admin: {exc}")
    sys.exit(1)

# ----------------------------------------------------------------------------
# CONFIG - edit here if the backend expects different names
# ----------------------------------------------------------------------------
TAG = "TESTSEED"
ORG_NAME = "Active Church"
ORG_ID = "69c63afc4c3e2fdfc5a4840d"
MANIFEST = "seed_manifest.json"

CAMPUSES = {  # names from the dashboard wireframe; campus_id is a uuid with no lookup table in the DB
    "Springfield": str(uuid.uuid5(uuid.NAMESPACE_DNS, "activeteams.testseed.springfield")),
    "Riverside": str(uuid.uuid5(uuid.NAMESPACE_DNS, "activeteams.testseed.riverside")),
    "Northside": str(uuid.uuid5(uuid.NAMESPACE_DNS, "activeteams.testseed.northside")),
}
CAMPUS_SIZE = {"Springfield": 1.0, "Riverside": 0.7, "Northside": 0.5}  # relative attendance

TYPE_SERVICE = "Church Service"
TYPE_CELLS = "Cells"
TYPE_LIFE_CLASS = "Life Class"
TYPE_SOL = "School Of Leaders"
TYPE_PLAN40 = "Plan 40"          # does not exist yet; created (and cleaned up) by this script

CELLS_PER_CAMPUS = 5
PEOPLE_PER_CAMPUS = 90
BATCH = 500

# primary key column per table (used for the manifest / cleanup)
PK = {
    "campuses": "campus_id", "People": "_id", "event_types": "event_type_id", "events": "event_id",
    "event_attendees": "attendee_id", "event_sessions": "session_id",
    "event_session_attendees": "id", "event_consolidations": "id",
    "event_new_people": "id", "service_targets": "id",
    "twelve_tasks_tracked_people": "person_id", "twelve_tasks_targets": "person_id",
    "Tasks": "_id",
}
INSERT_ORDER = ["campuses", "event_types", "People", "events", "event_attendees", "event_sessions",
                "event_session_attendees", "event_consolidations", "event_new_people",
                "service_targets", "twelve_tasks_tracked_people", "twelve_tasks_targets", "Tasks"]

FIRST_M = ["Sipho", "Thabo", "Lwazi", "Kagiso", "Tendai", "Joshua", "Daniel", "Mpho", "Themba", "Samuel"]
FIRST_F = ["Naledi", "Lerato", "Ayanda", "Grace", "Zanele", "Precious", "Thandi", "Esther", "Nomsa", "Hope"]
SURNAMES = ["Dlamini", "Nkosi", "Mokoena", "Smith", "Naidoo", "Botha", "Khumalo", "Mahlangu", "Pillay", "van Wyk"]


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def iso(d, hour=9):
    if isinstance(d, datetime):
        return d.astimezone(timezone.utc).isoformat()
    return datetime(d.year, d.month, d.day, hour, 0, tzinfo=timezone.utc).isoformat()


def week_id(d):
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def weekdays(start, end, weekday):  # Monday=0
    d = start + timedelta(days=(weekday - start.weekday()) % 7)
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def hex24():
    return secrets.token_hex(12)


def full_name(p):
    return f"{p['Name']} {p['Surname']}"


def table_columns(name):
    """Column names of a table, read from the PostgREST schema ([] if the table does not exist)."""
    headers = {"apikey": db.supabase_key, "Authorization": f"Bearer {db.supabase_key}"}
    r = httpx.get(f"{db.supabase_url}/rest/v1/", headers=headers, timeout=30)
    r.raise_for_status()
    spec = r.json()
    defs = spec.get("definitions") or spec.get("components", {}).get("schemas") or {}
    return list(defs.get(name, {}).get("properties", {}))


def build_campus_rows():
    """Rows for the campuses table, filled to match whatever columns the table really has."""
    cols = table_columns("campuses")
    if not cols:
        return None
    rows = []
    for name, cid in CAMPUSES.items():
        row = {"campus_id": cid}
        for c in ("name", "campus_name", "title"):
            if c in cols:
                row[c] = name
                break
        else:
            print(f"  [warn] campuses has no name-like column (columns: {cols}); rows will have only campus_id")
        for c, v in (("Organization", ORG_NAME), ("organization", ORG_NAME), ("org", ORG_NAME),
                     ("org_id", ORG_ID), ("is_active", True)):
            if c in cols:
                row[c] = v
        rows.append(row)
    return rows


# ----------------------------------------------------------------------------
# build the plan (nothing is written here)
# ----------------------------------------------------------------------------
def build_plan(today):
    start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)  # first day of previous month
    plan = {t: [] for t in INSERT_ORDER}
    campus_rows = build_campus_rows()
    if campus_rows is None:
        print("The 'campuses' table does not exist. Run create_campuses_table.sql in the Supabase SQL editor first.")
        sys.exit(1)
    plan["campuses"] = campus_rows

    # event types: reuse existing ones by name, create Plan 40 if missing
    existing = {r["name"]: r["event_type_id"] for r in
                db.table("event_types").select("event_type_id,name").eq("org_id", ORG_ID).execute().data}
    type_ids = dict(existing)
    for name in (TYPE_SERVICE, TYPE_CELLS, TYPE_LIFE_CLASS, TYPE_SOL, TYPE_PLAN40):
        if name not in type_ids:
            tid = str(uuid.uuid4())
            plan["event_types"].append({
                "event_type_id": tid, "name": name, "description": f"{TAG} event type",
                "is_ticketed": False, "is_global": False, "has_person_steps": False,
                "is_training": name in (TYPE_LIFE_CLASS, TYPE_SOL, TYPE_PLAN40), "org_id": ORG_ID,
            })
            type_ids[name] = tid

    # leaders at 12: reuse real Users (read only); otherwise synthetic names
    users = db.table("Users").select("_id,name,surname,email").eq("role", "leaderAt12") \
        .eq("Organization", ORG_NAME).limit(12).execute().data or []
    leaders = [{"id": u["_id"], "name": f"{u['name']} {u['surname']}".strip(), "email": u.get("email")}
               for u in users if u.get("email")]
    while len(leaders) < 4:
        n = len(leaders) + 1
        leaders.append({"id": hex24(), "name": f"{TAG} Leader {n}", "email": f"testseed.leader{n}@example.com"})

    # synthetic people, split per campus
    pools = {}
    for campus in CAMPUSES:
        pool = []
        for _ in range(PEOPLE_PER_CAMPUS):
            g = random.choice(["Male", "Female"])
            created = start + timedelta(days=random.randint(0, (today - start).days))
            pool.append({
                "_id": hex24(), "Name": random.choice(FIRST_M if g == "Male" else FIRST_F),
                "Surname": random.choice(SURNAMES), "Gender": g,
                "Stage": random.choice([None, "Win", "Consolidate", "Disciple"]),
                "Date Created": iso(created), "DateCreated": iso(created),
                "Organization": ORG_NAME, "org_id": ORG_ID, "imported_by": TAG.lower(),
            })
        pools[campus] = pool
        plan["People"].extend(pool)

    def make_event(campus, name, type_name, weekday, size, is_school_cell=False, is_global=False):
        eid = str(uuid.uuid4())
        leader = random.choice(leaders)
        plan["events"].append({
            "event_id": eid, "event_name": f"{TAG} {campus} {name}", "event_type_name": type_name,
            "event_type_id": type_ids[type_name], "location": f"{campus} venue",
            "description": f"{TAG} generated", "event_leader": leader["name"],
            "event_leader_email": leader["email"], "is_ticketed": False, "is_global": is_global,
            "is_active": True, "has_person_steps": False, "status": "open",
            "recurring_day": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][weekday],
            "is_recurring": True, "Organization": ORG_NAME, "org_id": ORG_ID,
            "event_date": iso(start + timedelta(days=(weekday - start.weekday()) % 7, hours=0), 18),
            "created_at": iso(start), "updated_at": iso(today), "source_format": "seed",
            "is_training": False, "is_permanent_deact": False, "is_school_cell": is_school_cell,
            "campus_id": CAMPUSES[campus], "total_attendance": 0,
        })
        event = plan["events"][-1]
        roster = random.sample(pools[campus], min(len(pools[campus]), max(8, int(size * 1.5))))
        for p in roster:  # registered / persistent roster
            plan["event_attendees"].append({
                "event_id": eid, "mongo_person_id": p["_id"], "full_name": full_name(p),
                "leader12": leader["name"], "leader144": "", "is_persistent": True,
            })
        total = 0
        for d in weekdays(start, today, weekday):
            did_not_meet = type_name == TYPE_CELLS and random.random() < 0.15
            present = [] if did_not_meet else random.sample(roster, max(1, int(len(roster) * random.uniform(0.55, 0.9))))
            first = random.randint(0, 3) if type_name == TYPE_SERVICE and present else 0
            recommit = random.randint(0, 5) if type_name == TYPE_SERVICE and present else 0
            sid = str(uuid.uuid4())
            plan["event_sessions"].append({
                "session_id": sid, "event_id": eid, "session_date": d.isoformat(),
                "week_identifier": week_id(d), "status": "did_not_meet" if did_not_meet else "complete",
                "is_did_not_meet": did_not_meet, "checked_in_count": len(present),
                "total_headcounts": len(present), "submitted_by": leader["email"],
                "submitted_by_name": leader["name"], "submitted_at": iso(d, 21),
                "decisions_first_time": first, "decisions_recommitment": recommit,
                "decisions_total": first + recommit, "total_associated": len(roster),
                "captured_by_leader_at_12": False,
            })
            total += len(present)
            for p in present:
                plan["event_session_attendees"].append({
                    "session_id": sid, "event_id": eid, "mongo_person_id": p["_id"],
                    "full_name": full_name(p), "is_checked_in": True, "check_in_date": iso(d, 10),
                    "is_persistent": True,
                })
            for i in range(first + recommit):
                p = random.choice(pools[campus])
                kind = "first_time" if i < first else "recommitment"
                plan["event_consolidations"].append({
                    "event_id": eid, "mongo_person_id": p["_id"], "person_name": p["Name"],
                    "person_surname": p["Surname"], "decision_type": kind,
                    "decision_display_name": "First Time Decision" if kind == "first_time" else "Recommitment",
                    "assigned_to": leader["name"], "assigned_to_email": leader["email"],
                    "status": "active", "created_at": iso(d, 12), "session_id": sid,
                })
            if type_name == TYPE_SERVICE and not did_not_meet:
                for _ in range(random.randint(2, 8)):  # new visitors
                    g = random.choice(["Male", "Female"])
                    plan["event_new_people"].append({
                        "event_id": eid, "mongo_id": hex24(),
                        "name": random.choice(FIRST_M if g == "Male" else FIRST_F),
                        "surname": random.choice(SURNAMES), "gender": g, "invited_by": random.choice(leaders)["name"],
                        "is_checked_in": True, "needs_db_entry": False, "added_at": iso(d, 11),
                    })
        event["total_attendance"] = total

    for campus, factor in CAMPUS_SIZE.items():
        make_event(campus, "Sunday Service", TYPE_SERVICE, 6, int(60 * factor), is_global=True)
        for n in range(1, CELLS_PER_CAMPUS + 1):
            make_event(campus, f"Cell {n} - Wednesday", TYPE_CELLS, 2, 8)
        make_event(campus, "School Cell - Thursday", TYPE_CELLS, 3, 10, is_school_cell=True)
        make_event(campus, "Life Class", TYPE_LIFE_CLASS, 1, int(25 * factor) + 6)
        make_event(campus, "School of Leaders", TYPE_SOL, 3, int(15 * factor) + 5)
        make_event(campus, "Plan 40", TYPE_PLAN40, 4, int(30 * factor) + 6)

    # service targets: current + previous week, per leader
    for d in (today, today - timedelta(days=7)):
        for l in leaders:
            plan["service_targets"].append({
                "leader_name": l["name"], "target_count": random.choice([100, 120, 150]),
                "email": l["email"], "organization": ORG_NAME, "week_identifier": week_id(d),
            })

    # twelve tasks: a few tracked people per leader, a target of 12, some completed tasks
    flat_people = [p for pool in pools.values() for p in pool]
    for l in leaders[:4]:
        for p in random.sample(flat_people, 3):
            plan["twelve_tasks_tracked_people"].append({
                "person_id": p["_id"], "leader12_id": l["id"], "org": ORG_NAME,
                "display_name": full_name(p), "active": True,
            })
            plan["twelve_tasks_targets"].append({"person_id": p["_id"], "target": 12, "updated_by": TAG.lower()})
            for _ in range(random.randint(0, 12)):
                when = today - timedelta(days=random.randint(0, 28))
                plan["Tasks"].append({
                    "_id": hex24(), "memberID": p["_id"], "name": f"{TAG} follow up: {full_name(p)}",
                    "taskType": "Follow Up", "followup_date": iso(when), "status": "completed",
                    "assignedfor": l["email"], "assigned_to_email": l["email"], "leader_assigned": l["name"],
                    "leader_name": l["name"], "type": "call", "person_id": p["_id"],
                    "person_name": p["Name"], "person_surname": p["Surname"],
                    "created_at": iso(when), "completedAt": iso(when, 15), "created_by": TAG.lower(),
                    "Organization": ORG_NAME, "org_id": ORG_ID,
                })
    return plan


# ----------------------------------------------------------------------------
# write / cleanup
# ----------------------------------------------------------------------------
def load_manifest():
    if os.path.exists(MANIFEST):
        with open(MANIFEST, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(m):
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump(m, f, indent=1)


def insert_all(plan, tables=INSERT_ORDER):
    manifest = load_manifest()
    for table in tables:
        rows = plan[table]
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            try:
                res = db.table(table).insert(chunk).execute()
            except Exception as exc:
                msg = str(exc)
                if table == "service_targets" and "id" in msg and "null" in msg.lower():
                    base = (db.table("service_targets").select("id").order("id", desc=True).limit(1).execute().data or [{"id": 0}])[0]["id"]
                    for n, r in enumerate(chunk, 1):
                        r["id"] = base + n
                    res = db.table(table).insert(chunk).execute()
                else:
                    print(f"\nFAILED inserting into {table}: {msg[:400]}")
                    print("Everything inserted so far is in seed_manifest.json - run with --cleanup to remove it.")
                    save_manifest(manifest)
                    sys.exit(1)
            manifest.setdefault(table, []).extend(r[PK[table]] for r in (res.data or chunk))
            save_manifest(manifest)
        print(f"  inserted {len(rows):>6} into {table}")


def cleanup():
    manifest = load_manifest()
    if not manifest:
        print("seed_manifest.json is missing or empty - nothing to clean up.")
        return
    for table in reversed(INSERT_ORDER):
        ids = manifest.get(table, [])
        for i in range(0, len(ids), 200):
            db.table(table).delete().in_(PK[table], ids[i:i + 200]).execute()
        if ids:
            print(f"  deleted {len(ids):>6} from {table}")
    os.remove(MANIFEST)
    print("Cleanup finished.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write to the database")
    ap.add_argument("--cleanup", action="store_true", help="delete everything recorded in the manifest")
    ap.add_argument("--campuses-only", action="store_true",
                    help="only insert the campuses rows (use after an earlier seed that lacked them)")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    random.seed(args.seed)
    host = str(db.supabase_url).split("//")[-1]

    if args.cleanup:
        print(f"Target project: {host}")
        cleanup()
        return
    if args.campuses_only:
        rows = build_campus_rows()
        if rows is None:
            print("The 'campuses' table does not exist. Run create_campuses_table.sql in the Supabase SQL editor first.")
            return
        have = set(load_manifest().get("campuses", []))
        plan = {t: [] for t in INSERT_ORDER}
        plan["campuses"] = [r for r in rows if r["campus_id"] not in have]
        print(f"Target project: {host}\nCampuses to insert: {[r.get('name', r['campus_id']) for r in plan['campuses']]}")
        if plan["campuses"]:
            insert_all(plan, ["campuses"])
        print("Done.")
        return
    if os.path.exists(MANIFEST):
        print(f"{MANIFEST} already exists - run --cleanup first so seeded data is not duplicated.")
        return

    plan = build_plan(date.today())
    print(f"Target project: {host}\nPlanned rows:")
    for t in INSERT_ORDER:
        print(f"  {len(plan[t]):>6}  {t}")
    if not args.apply:
        print("\nDry run only. Re-run with --apply to write these rows.")
        return
    if not args.yes and input(f"\nType 'seed' to write this to {host}: ").strip() != "seed":
        print("Aborted.")
        return
    insert_all(plan)
    print("\nDone. Reload the Reporting Dashboard (This Month / Previous Month).")


if __name__ == "__main__":
    main()