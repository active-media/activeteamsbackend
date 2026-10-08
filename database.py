import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient 
from dotenv import load_dotenv 

load_dotenv() 

# Values that mean "this was never really configured". A Render dashboard with a
# placeholder left in it, or a doc snippet that got pasted straight into the env
# var, used to reach the driver and surface as a cryptic DNS failure against a
# host literally called "none". Treat them as unset so the real cause is obvious.
_PLACEHOLDERS = {"", "none", "null", "undefined", "changeme", "your-mongodb-uri"}

DEFAULT_MONGO_URI = "mongodb://localhost:27017"

# Upper bound on event DOCUMENTS read per list request. This is not a result
# limit - pagination is applied to the synthesised instances afterwards, so this
# only bounds how much raw data one request may pull. It exists so a single
# request cannot exhaust memory on a large collection.
MAX_EVENT_DOCUMENTS = 3000

# Upper bound on how many weeks back a recurring event is expanded into
# instances. Without a bound, a wide start_date would synthesise one instance per
# recurring day per week across the whole window; this caps the work per request.
MAX_RECURRING_WEEKS_BACK = 520  # ~10 years


def _is_placeholder(value: str) -> bool:
    """True if the value is missing, or is a URI pointing at a placeholder host."""
    value = value.strip()
    if value.lower() in _PLACEHOLDERS:
        return True
    # Catch a well-formed URI whose *host* is the placeholder, e.g.
    # "mongodb://None:27017" - which is what reaches the driver as host "none".
    rest = value.partition("://")[2] or value
    host = rest.rpartition("@")[2].split("/")[0].split(":")[0]
    return host.lower() in _PLACEHOLDERS


def resolve_mongo_uri(default: str = DEFAULT_MONGO_URI) -> str:
    """Return the configured Mongo URI.

    MONGODB_URI is the canonical name, but MONGO_URI has been used throughout this
    project (and is what .env still ships), so accept either. Reading one variable
    in one place is what stops half the codebase talking to a different database
    than the other half.
    """
    present = False
    for name in ("MONGODB_URI", "MONGO_URI"):
        value = os.getenv(name)
        if value is None:
            continue
        present = True
        if not _is_placeholder(value):
            return value.strip()
        print(
            f"WARNING: {name} is set to the placeholder value {value.strip()!r}, "
            f"so it was ignored. Set {name} to a real MongoDB connection string.",
            file=sys.stderr,
        )

    if not present:
        print(
            f"WARNING: neither MONGODB_URI nor MONGO_URI is set. Falling back to "
            f"{default} - set MONGODB_URI (or MONGO_URI) in the environment.",
            file=sys.stderr,
        )

    return default


def redact_uri(uri: str) -> str:
    """Strip the password from a Mongo URI so it is safe to print to logs."""
    scheme, sep, rest = uri.partition("://")
    credentials, at, host = rest.rpartition("@")
    if not at:
        return uri
    user = credentials.split(":", 1)[0]
    return f"{scheme}{sep}{user}:***@{host}" if sep else f"{user}:***@{host}"


MONGO_URI = resolve_mongo_uri()
DB_NAME = os.getenv("DB_NAME", "active-teams-db")

print(f"--- CONNECTING TO DB: {DB_NAME} ---")
print(f"--- MONGO URI: {redact_uri(MONGO_URI)} ---")

client = AsyncIOMotorClient(MONGO_URI)

db = client[DB_NAME]
events_collection = db["Events"]
people_collection = db["People"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
tasktypes_collection = db["TaskTypes"]
org_config_collection = db["OrgConfig"]
consolidations_collection=db["consolidations"]
organizations_collection = db["organizations"]

def get_database():
    return db
