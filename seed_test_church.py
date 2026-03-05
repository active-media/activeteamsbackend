# seed_test_church.py
# Creates a complete second church dataset with different naming conventions
# Run: python seed_test_church.py

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import random
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = "mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/"


DB_NAME = "test-church-db"

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

people_collection     = db["People"]
events_collection     = db["Events"]
users_collection      = db["Users"]
org_config_collection = db["OrgConfig"]

# ─────────────────────────────────────────────────────────────
# TEST CHURCH DATA - Uses completely different naming
# Instead of: Leader @1, Leader @12, Leader @144
# They use:   Zone Pastor, District Leader, Regional Coordinator
# Instead of: Cells   →  they call it: Zones
# ─────────────────────────────────────────────────────────────

TOP_LEADERS = {
    "male":   "Pastor Samuel Dube",
    "female": "Pastor Ruth Dube"
}

# Zone Pastors (Level 1 - top)
ZONE_PASTORS = [
    {"name": "Samuel", "surname": "Dube",    "gender": "male",   "email": "samuel.dube@gracechurch.org"},
    {"name": "Ruth",   "surname": "Dube",    "gender": "female", "email": "ruth.dube@gracechurch.org"},
]

# District Leaders (Level 2)
DISTRICT_LEADERS = [
    {"name": "Thabo",   "surname": "Nkosi",   "gender": "male",   "email": "thabo.nkosi@gracechurch.org"},
    {"name": "Lerato",  "surname": "Mokoena", "gender": "female", "email": "lerato.mokoena@gracechurch.org"},
    {"name": "Sipho",   "surname": "Dlamini", "gender": "male",   "email": "sipho.dlamini@gracechurch.org"},
    {"name": "Nomsa",   "surname": "Zulu",    "gender": "female", "email": "nomsa.zulu@gracechurch.org"},
]

# Regional Coordinators (Level 3 - zone/home group leaders)
REGIONAL_COORDS = [
    {"name": "Bongani", "surname": "Cele",     "gender": "male",   "email": "bongani.cele@gracechurch.org"},
    {"name": "Zanele",  "surname": "Khoza",    "gender": "female", "email": "zanele.khoza@gracechurch.org"},
    {"name": "Mpho",    "surname": "Sithole",  "gender": "male",   "email": "mpho.sithole@gracechurch.org"},
    {"name": "Ntombi",  "surname": "Mthembu",  "gender": "female", "email": "ntombi.mthembu@gracechurch.org"},
    {"name": "Lucky",   "surname": "Shabalala", "gender": "male",  "email": "lucky.shabalala@gracechurch.org"},
    {"name": "Lindiwe", "surname": "Mahlangu", "gender": "female", "email": "lindiwe.mahlangu@gracechurch.org"},
]

# Regular members
FIRST_NAMES_M = ["James", "John", "Peter", "David", "Michael", "Daniel", "Joseph", "Andrew", "Matthew", "Thomas",
                  "Siyanda", "Lungelo", "Mthokozisi", "Sandile", "Nkosinathi", "Musa", "Lwazi", "Sifiso"]
FIRST_NAMES_F = ["Mary", "Sarah", "Grace", "Faith", "Hope", "Charity", "Elizabeth", "Rachel", "Rebecca", "Esther",
                  "Nokwanda", "Thandeka", "Busisiwe", "Nompumelelo", "Sithembile", "Nokukhanya", "Ayanda"]
SURNAMES      = ["Dlamini", "Nkosi", "Mthembu", "Zulu", "Ntuli", "Khumalo", "Ndlovu", "Mkhize",
                  "Shabalala", "Buthelezi", "Cele", "Ngcobo", "Gumede", "Maphumulo", "Mthethwa"]

LOCATIONS = [
    "Grace Community Hall, Soweto",
    "Diepkloof Community Centre",
    "Meadowlands Zone 6",
    "Orlando East Church Building",
    "Dobsonville Gardens",
    "Protea Glen Meeting Room",
    "Naledi Community Hall",
]


async def seed_org_config():
    print("Seeding OrgConfig...")
    existing = await org_config_collection.find_one({"_id": "grace-church"})
    if existing:
        print("  OrgConfig already exists, skipping")
        return

    config = {
        "_id": "grace-church",
        "org_name": "Grace Community Church",
        "events_collection": "Events",
        "people_collection": "People",

        # Key difference: this church calls their groups "Zones" not "Cells"
        "recurring_event_type": "Zones",

        # Key difference: completely different hierarchy field names and labels
        "hierarchy": [
            {"level": 1, "field": "zonePastor",           "label": "Zone Pastor"},
            {"level": 2, "field": "districtLeader",       "label": "District Leader"},
            {"level": 3, "field": "regionalCoordinator",  "label": "Regional Coordinator"},
        ],

        # Key difference: different top leaders
        "top_leaders": {
            "male":   "Pastor Samuel Dube",
            "female": "Pastor Ruth Dube"
        },

        "allows_create_event":      True,
        "allows_create_event_type": True,
        "created_at": datetime.utcnow(),
        "created_by": "seed_script",
    }

    await org_config_collection.insert_one(config)
    print("  OrgConfig seeded for Grace Community Church")


async def seed_people():
    print("Seeding People...")
    count = await people_collection.count_documents({})
    if count > 0:
        print(f"  People already exist ({count} records), skipping")
        return

    people = []

    # Add zone pastors
    for p in ZONE_PASTORS:
        people.append({
            "Name":    p["name"],
            "Surname": p["surname"],
            "Gender":  p["gender"],
            "Email":   p["email"],
            "Phone":   f"07{random.randint(10000000, 99999999)}",
            # Zone pastors have no leader above them in the hierarchy
            "zonePastor":          "",
            "districtLeader":      "",
            "regionalCoordinator": "",
            "role": "zone_pastor",
            "created_at": datetime.utcnow(),
        })

    # Add district leaders - assigned to a zone pastor
    for i, p in enumerate(DISTRICT_LEADERS):
        zone_pastor = ZONE_PASTORS[i % len(ZONE_PASTORS)]
        people.append({
            "Name":    p["name"],
            "Surname": p["surname"],
            "Gender":  p["gender"],
            "Email":   p["email"],
            "Phone":   f"07{random.randint(10000000, 99999999)}",
            "zonePastor":          f"{zone_pastor['name']} {zone_pastor['surname']}",
            "districtLeader":      "",
            "regionalCoordinator": "",
            "role": "district_leader",
            "created_at": datetime.utcnow(),
        })

    # Add regional coordinators - assigned to a district leader
    for i, p in enumerate(REGIONAL_COORDS):
        district = DISTRICT_LEADERS[i % len(DISTRICT_LEADERS)]
        zone_pastor = ZONE_PASTORS[i % len(ZONE_PASTORS)]
        people.append({
            "Name":    p["name"],
            "Surname": p["surname"],
            "Gender":  p["gender"],
            "Email":   p["email"],
            "Phone":   f"07{random.randint(10000000, 99999999)}",
            "zonePastor":          f"{zone_pastor['name']} {zone_pastor['surname']}",
            "districtLeader":      f"{district['name']} {district['surname']}",
            "regionalCoordinator": "",
            "role": "regional_coordinator",
            "created_at": datetime.utcnow(),
        })

    # Add 60 regular members
    for i in range(60):
        gender = random.choice(["male", "female"])
        first = random.choice(FIRST_NAMES_M if gender == "male" else FIRST_NAMES_F)
        surname = random.choice(SURNAMES)

        coord    = random.choice(REGIONAL_COORDS)
        district = random.choice(DISTRICT_LEADERS)
        pastor   = random.choice(ZONE_PASTORS)

        people.append({
            "Name":    first,
            "Surname": surname,
            "Gender":  gender,
            "Email":   f"{first.lower()}.{surname.lower()}{i}@gmail.com",
            "Phone":   f"07{random.randint(10000000, 99999999)}",
            # These field names match the hierarchy config above
            "zonePastor":          f"{pastor['name']} {pastor['surname']}",
            "districtLeader":      f"{district['name']} {district['surname']}",
            "regionalCoordinator": f"{coord['name']} {coord['surname']}",
            "role": "member",
            "created_at": datetime.utcnow(),
        })

    await people_collection.insert_many(people)
    print(f"  Seeded {len(people)} people")


async def seed_event_types():
    print("Seeding EventTypes...")
    # Use tasktypes or a dedicated collection
    event_types_col = db["TaskTypes"]
    count = await event_types_col.count_documents({})
    if count > 0:
        print(f"  EventTypes already exist ({count} records), skipping")
        return

    event_types = [
        {
            "name":           "Zones",          # Their version of "Cells"
            "displayName":    "Zones",
            "isGlobal":       False,
            "isTicketed":     False,
            "hasPersonSteps": True,             # Triggers hierarchy fields
            "isTraining":     False,
            "isEventType":    True,
            "created_at":     datetime.utcnow(),
        },
        {
            "name":           "Sunday Service",
            "displayName":    "Sunday Service",
            "isGlobal":       True,
            "isTicketed":     False,
            "hasPersonSteps": False,
            "isTraining":     False,
            "isEventType":    True,
            "created_at":     datetime.utcnow(),
        },
        {
            "name":           "Youth Camp",
            "displayName":    "Youth Camp",
            "isGlobal":       False,
            "isTicketed":     True,
            "hasPersonSteps": False,
            "isTraining":     False,
            "isEventType":    True,
            "created_at":     datetime.utcnow(),
        },
        {
            "name":           "Leadership Training",
            "displayName":    "Leadership Training",
            "isGlobal":       False,
            "isTicketed":     False,
            "hasPersonSteps": False,
            "isTraining":     True,
            "isEventType":    True,
            "created_at":     datetime.utcnow(),
        },
    ]

    await event_types_col.insert_many(event_types)
    print(f"  Seeded {len(event_types)} event types")


async def seed_events():
    print("Seeding Events...")
    count = await events_collection.count_documents({})
    if count > 0:
        print(f"  Events already exist ({count} records), skipping")
        return

    events = []
    today = datetime.utcnow()

    for i, coord in enumerate(REGIONAL_COORDS):
        district = DISTRICT_LEADERS[i % len(DISTRICT_LEADERS)]
        pastor   = ZONE_PASTORS[i % len(ZONE_PASTORS)]

        # Create 4 weekly zone instances per coordinator
        for week in range(4):
            event_date = today - timedelta(weeks=week)
            events.append({
                "UUID":             str(uuid.uuid4()),
                "eventTypeName":    "Zones",
                "eventName":        f"{coord['name']}'s Zone",
                "eventLeader":      f"{coord['name']} {coord['surname']}",
                "eventLeaderName":  f"{coord['name']} {coord['surname']}",
                "eventLeaderEmail": coord["email"],
                "date":             event_date,
                "location":         random.choice(LOCATIONS),
                "description":      f"Weekly zone meeting led by {coord['name']}",
                "isTicketed":       False,
                "isGlobal":         False,
                "hasPersonSteps":   True,
                "status":           "complete" if week > 0 else "open",
                "recurring_day":    ["Friday"],

                # Key difference: these field names match the hierarchy config
                "zonePastor":          f"{pastor['name']} {pastor['surname']}",
                "districtLeader":      f"{district['name']} {district['surname']}",
                "regionalCoordinator": f"{coord['name']} {coord['surname']}",

                "total_attendance": random.randint(5, 25) if week > 0 else 0,
                "attendees":        [],
                "priceTiers":       [],
                "created_at":       datetime.utcnow(),
                "updated_at":       datetime.utcnow(),
            })

    await events_collection.insert_many(events)
    print(f"  Seeded {len(events)} zone events")


async def seed_admin_user():
    print("Seeding admin user...")
    existing = await users_collection.find_one({"email": "admin@gracechurch.org"})
    if existing:
        print("  Admin user already exists, skipping")
        return

    await users_collection.insert_one({
        "name":       "Admin",
        "surname":    "Grace Church",
        "email":      "admin@gracechurch.org",
        "password":   "hashed_password_here",  # replace with actual hash
        "role":       "admin",
        "org_id":     "grace-church",           # links to OrgConfig
        "created_at": datetime.utcnow(),
    })
    print("  Admin user seeded: admin@gracechurch.org")


async def main():
    print("=" * 50)
    print(f"Seeding test church database: {DB_NAME}")
    print("=" * 50)

    try:
        await seed_org_config()
        await seed_people()
        await seed_event_types()
        await seed_events()
        await seed_admin_user()

        print("\n" + "=" * 50)
        print("DONE! Summary:")
        print(f"  Database:        {DB_NAME}")
        print(f"  Church name:     Grace Community Church")
        print(f"  Recurring type:  Zones  (not Cells)")
        print(f"  Level 1 label:   Zone Pastor  (not Leader @1)")
        print(f"  Level 2 label:   District Leader  (not Leader @12)")
        print(f"  Level 3 label:   Regional Coordinator  (not Leader @144)")
        print(f"  Top leaders:     Pastor Samuel & Ruth Dube")
        print(f"  Admin login:     admin@gracechurch.org")
        print("=" * 50)
        print("\nNext: point your backend DB_NAME to 'test-church-db'")
        print("and log in to see the frontend use the new labels.")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("Connection closed.")

if __name__ == "__main__":
    asyncio.run(main())