import os
import re
import secrets
from datetime import datetime, time as time_type, timedelta
from typing import Optional, Dict, Any, List
from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import users_collection, org_config_collection, organizations_collection
from bson import ObjectId
from datetime import datetime

# ==============================
# CONFIG
# ==============================
JWT_SECRET = os.getenv("JWT_SECRET", "replace_me_with_a_strong_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))
SUPREME_ADMIN_EMAIL = "tkgenia1234@gmail.com"
ORG_ID_MAP = {
    "active-church": "active-teams",
    "active church": "active-teams",
}

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer()

# ==============================
# PASSWORD UTILS
# ==============================
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

# ==============================
# TOKEN CREATION
# ==============================
def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    now = datetime.utcnow()
    expire = now + (expires_delta if expires_delta else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "iat": now})
    
    if "is_supreme_admin" not in to_encode:
        to_encode["is_supreme_admin"] = False
    
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)

def create_refresh_token() -> Dict[str, str]:
    refresh_token_id = secrets.token_urlsafe(16)
    refresh_plain = secrets.token_urlsafe(32)
    refresh_hash = hash_password(refresh_plain)
    refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    return {
        "id": refresh_token_id,
        "plain": refresh_plain,
        "hash": refresh_hash,
        "expires": refresh_expires
    }

def decode_access_token(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def convert_datetime_to_iso(doc: dict) -> dict:
    """
    Recursively converts all datetime values in a dict to ISO 8601 strings.
    """
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, dict):
            doc[key] = convert_datetime_to_iso(value)
        elif isinstance(value, list):
            doc[key] = [convert_datetime_to_iso(v) if isinstance(v, dict) else v for v in value]
    return doc


# ==============================
# REFRESH TOKEN HANDLING
# ==============================
async def refresh_access_token(refresh_token_id: str, refresh_token: str) -> Dict[str, Any]:
    user = await users_collection.find_one({"refresh_token_id": refresh_token_id})
    if (
        not user
        or not user.get("refresh_token_hash")
        or not verify_password(refresh_token, user["refresh_token_hash"])
        or not user.get("refresh_token_expires")
        or user["refresh_token_expires"] < datetime.utcnow()
    ):
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    email = user.get("email", "")
    is_supreme = email == SUPREME_ADMIN_EMAIL or user.get("is_supreme_admin", False)

    # Derive and normalize org_id
    organization = user.get("Organization") or user.get("organization", "")
    org_id = user.get("org_id") or organization.lower().replace(" ", "-") or "active-teams"
    org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

    new_access = create_access_token({
        "user_id": str(user["_id"]),
        "email": user["email"],
        "role": user.get("role", "user"),
        "is_supreme_admin": is_supreme,
        "org_id": org_id,
        "Organization": organization,
    })

    new_refresh = create_refresh_token()
    await users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "refresh_token_id": new_refresh["id"],
            "refresh_token_hash": new_refresh["hash"],
            "refresh_token_expires": new_refresh["expires"],
            "org_id": org_id,
        }}
    )

    return {
        "access_token": new_access,
        "refresh_token_id": new_refresh["id"],
        "refresh_token": new_refresh["plain"]
    }
# ==============================
# FORGOT / RESET PASSWORD
# ==============================
def create_password_reset_token(email: str, expires_delta: timedelta = timedelta(minutes=30)) -> str:
    expire = datetime.utcnow() + expires_delta
    payload = {"sub": email, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_password_reset_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None

# ==============================
# FASTAPI DEPENDENCIES
# ==============================
# In utils.py
async def get_current_user(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> Dict[str, Any]:
    try:
        # First, check if token is None or empty
        if not token or not token.credentials:
            raise HTTPException(status_code=401, detail="No token provided")
        
        payload = decode_access_token(token.credentials)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        
        # Try different possible user ID fields
        user_id = payload.get("user_id") or payload.get("sub") or payload.get("id")
        
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID not found in token")
        
        # Convert user_id to string if it's not already
        user_id = str(user_id)
        
        # Check if it's a valid ObjectId
        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=401, detail=f"Invalid user ID format: {user_id}")
        
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=401, detail=f"User not found with ID: {user_id}")
        
        # Convert ObjectId to string for JSON serialization
        user_id_str = str(user["_id"])
        
        email = user.get("email", "")
        is_supreme = email == SUPREME_ADMIN_EMAIL or user.get("is_supreme_admin", False)
        db_role = user.get("role", "user")
        
        # Handle both uppercase and lowercase Organization fields
        organization = user.get("Organization") or user.get("organization", "")
        
        # Get org_id, handle if missing
        org_id = user.get("org_id")
        if not org_id and organization:
            org_id = organization.lower().replace(" ", "-")
        if not org_id:
            org_id = "active-teams"
        
        # Apply ORG_ID_MAP if it exists
        if org_id.lower() in ORG_ID_MAP:
            org_id = ORG_ID_MAP[org_id.lower()]
        
        return {
            "user_id": user_id_str,
            "email": email,
            "role": db_role,
            "is_supreme_admin": is_supreme,
            "organization": organization,
            "Organization": organization,  # Include both for compatibility
            "org_id": org_id,
            "name": user.get("name", ""),
            "surname": user.get("surname", ""),
            "_id": user_id_str
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Authentication error: {str(e)}")  # Add logging
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")

def require_role(*allowed_roles: str):
    async def _checker(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
        payload = decode_access_token(token.credentials)
        role = payload.get("role")
        is_supreme = payload.get("is_supreme_admin", False)
        
        # Supreme admins have access to everything
        if is_supreme:
            return payload
        
        if not role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role not present in token")
        
        # Admin has access to everything
        if role == "admin":
            return payload
        
        # Check system roles
        SYSTEM_ROLES = ['admin', 'leader', 'leaderAt12', 'user', 'registrant']
        if role in SYSTEM_ROLES:
            if role in allowed_roles:
                return payload
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Insufficient permissions"
            )
        
        # Custom roles
        if 'user' in allowed_roles:
            return payload
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Insufficient permissions"
        )
        
    return _checker
def sanitize_document(doc: dict) -> dict:
    """
    Recursively convert ObjectId and other non-serializable fields.
    """
    from bson import ObjectId

    def sanitize(value):
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, dict):
            return sanitize_document(value)
        elif isinstance(value, list):
            return [sanitize(v) for v in value]
        return value

    return {k: sanitize(v) for k, v in doc.items()}

WEEKDAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}

def get_next_occurrence_single(start_dt: datetime, recurring_day: str) -> datetime:
    if recurring_day is None:
        return start_dt
    target_weekday = WEEKDAY_MAP[recurring_day.lower()]
    today = datetime.utcnow().date()
    today_weekday = today.weekday()
    days_ahead = (target_weekday - today_weekday) % 7
    candidate_date = today + timedelta(days=days_ahead)
    if candidate_date < start_dt.date():
        candidate_date += timedelta(days=7)
    return datetime.combine(candidate_date, start_dt.time())

async def get_leader_cell_name_async(leader_id: str) -> str:
    try:
        doc = await users_collection.find_one({"_id": ObjectId(leader_id)})
    except Exception:
        doc = await users_collection.find_one({"user_id": leader_id})
    if doc:
        if "cell_name" in doc and doc["cell_name"]:
            return doc["cell_name"]
        name_parts = [doc.get("name", ""), doc.get("surname", "")]
        name_parts = [p for p in name_parts if p]
        if name_parts:
            return " ".join(name_parts) + "'s cell"
    return f"Cell of {leader_id}"

def parse_time_string(t: Optional[str]) -> Optional[time_type]:
    if not t:
        return None
    try:
        hh, mm = t.split(":")
        return time_type(int(hh), int(mm))
    except Exception:
        return None

# --- Helper for ObjectId to string ---
def task_type_serializer(task_type) -> dict:
    return {
        "id": str(task_type["_id"]),
        "name": task_type["name"]
    }


# ==============================
# MULTI-ORG HELPERS
# ==============================
# Open-ended G12 default hierarchy (@1 is top-most; levels are the ×12 group sizes).
DEFAULT_HIERARCHY: List[dict] = [
    {"key": "leader1",   "label": "Leader @1",   "level": 1},
    {"key": "leader12",  "label": "Leader @12",  "level": 12},
    {"key": "leader144", "label": "Leader @144", "level": 144},
    {"key": "leader1728","label": "Leader @1728","level": 1728},
]

DEFAULT_ROLES: List[dict] = [
    {"key": "admin",      "label": "Admin",  "capabilities": ["admin"]},
    {"key": "leader",     "label": "Leader", "capabilities": ["view_people", "manage_people", "create_events", "close_events", "view_stats", "checkin"]},
    {"key": "user",       "label": "Member", "capabilities": ["checkin"]},
]

KEY_REGEX = re.compile(r"^[a-z][a-z0-9_]*$")


def normalize_org_id(org_id: str) -> str:
    """Normalize an org identifier into the canonical org_id (slug)."""
    raw = (org_id or "").strip()
    if not raw:
        return ""
    slug = raw.lower().replace(" ", "-")
    slug = ORG_ID_MAP.get(slug, slug)
    return slug


def get_org_id(user: dict) -> Optional[str]:
    """Return the user's org_id, or None if they have no org."""
    org_id = user.get("org_id")
    if org_id:
        return normalize_org_id(str(org_id))
    org = user.get("Organization") or user.get("organization") or user.get("Organisation")
    if org:
        return normalize_org_id(str(org))
    return None


def is_supreme(user: dict) -> bool:
    return bool(user.get("is_supreme_admin")) or user.get("role") in ("super_admin",)


async def require_org(user: dict) -> dict:
    """Fetch the user's org (Organization or dynamic OrgConfig). 404 if no org."""
    org_id = get_org_id(user)
    if not org_id:
        raise HTTPException(status_code=404, detail="User has no organisation")
    # Prefer the dynamic OrgConfig which holds is_setup/hierarchy/roles/settings.
    org = await org_config_collection.find_one({"_id": org_id})
    if org:
        org["_id"] = org_id
        return org
    # Fall back to the legacy Organization document.
    legacy = await organizations_collection.find_one({"_id": org_id})
    if legacy:
        return legacy
    # Fall back to an org record keyed by slug.
    legacy_by_slug = await organizations_collection.find_one({
        "$or": [
            {"name": {"$regex": f"^{re.escape(org_id)}$", "$options": "i"}},
            {"slug": org_id},
            {"tag": org_id},
        ]
    })
    if legacy_by_slug:
        return legacy_by_slug
    raise HTTPException(status_code=404, detail="Organisation not found")


async def require_admin(user: dict, org: Optional[dict] = None) -> None:
    """403 unless the user is a supreme admin, an org admin (capability), or legacy role admin."""
    if is_supreme(user):
        return
    if user.get("role") == "admin" or user.get("role") in ("org_admin", "super_admin"):
        return
    if org:
        roles = org.get("roles") or []
        for role in roles:
            caps = role.get("capabilities") or []
            if role.get("key") == user.get("role") and "admin" in caps:
                return
            # If the user's role key matches this role and it grants admin, allow.
            if caps and "admin" in caps and role.get("key") == user.get("role"):
                return
    raise HTTPException(status_code=403, detail="Admin access required")


def scoped(user: dict, doc: dict) -> None:
    """403 if the doc's org differs from the user's org, unless supreme admin."""
    if is_supreme(user):
        return
    user_org = get_org_id(user)
    if not user_org:
        raise HTTPException(status_code=403, detail="User has no organisation")
    doc_org = doc.get("org_id") or doc.get("Organization") or doc.get("organization") or ""
    if not doc_org:
        # Documents without an org tag are treated as belonging to the user's org.
        return
    if normalize_org_id(str(doc_org)) != user_org:
        raise HTTPException(status_code=403, detail="Not authorised for this organisation")


def get_org_hierarchy(org: Optional[dict]) -> List[dict]:
    """
    Return the org's configured hierarchy, or the default G12 hierarchy if the
    org has none configured / is not set up. Each entry: {key, field, label, level}.
    field === key so the frontend can migrate off `field` gracefully.
    """
    if not org:
        return [
            {**h, "field": h["key"]} for h in DEFAULT_HIERARCHY
        ]
    hierarchy = org.get("hierarchy") or []
    if not hierarchy or not org.get("is_setup"):
        return [
            {**h, "field": h["key"]} for h in DEFAULT_HIERARCHY
        ]
    result = []
    for h in hierarchy:
        entry = dict(h)
        entry["field"] = entry.get("key", "")
        entry["key"] = entry.get("key", "")
        result.append(entry)
    return result


async def fetch_org_hierarchy(user: dict) -> List[dict]:
    """Convenience: fetch the org hierarchy for a user in one call."""
    org = await require_org(user)
    return get_org_hierarchy(org)


# ── Capability model (spec §7) ────────────────────────────────────────────────
def resolve_capabilities(user: dict, org: Optional[dict] = None):
    """Resolve (role_key, capabilities) for a user from org roles or DEFAULT_ROLES.

    Returns a tuple: (resolved_role, capabilities_list).
    """
    if is_supreme(user):
        return ("admin", ["admin"])
    role = (str(user.get("role") or "user")).lower()
    if org:
        for r in org.get("roles") or []:
            if r.get("key") == role:
                caps = list(r.get("capabilities") or [])
                if caps:
                    return (role, caps)
    legacy_map = {
        "admin": "admin",
        "super_admin": "admin",
        "org_admin": "admin",
        "manager": "leader",
        "leaderat12": "leader",
        "leader_at_12": "leader",
        "leader": "leader",
        "registrant": "user",
        "registrar": "user",
        "user": "user",
    }
    mapped = legacy_map.get(role, "user")
    for d in DEFAULT_ROLES:
        if d["key"] == mapped:
            return (mapped, list(d["capabilities"]))
    return ("user", ["checkin"])


async def require_capability(user: dict, capability: str, org: Optional[dict] = None) -> None:
    """403 unless the user holds `capability` (admin capability grants all).

    Resolves org roles first; falls back to DEFAULT_ROLES so legacy/orgless
    users keep working while the frontend migrates (spec §7).
    """
    if not org:
        org_id = get_org_id(user)
        if org_id:
            try:
                org = await org_config_collection.find_one({"_id": org_id})
            except Exception:
                org = None
    _, caps = resolve_capabilities(user, org)
    if "admin" in caps or capability in caps:
        return
    raise HTTPException(status_code=403, detail=f"Insufficient permissions: {capability} required")