import os
import secrets
from datetime import datetime, time as time_type, timedelta
from typing import Optional, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase_helpers.supabase_connection import supabase as _supabase, supabase_admin as _supabase_admin
from database import users_collection
from bson import ObjectId
from datetime import datetime

# ==============================
# CONFIG
# ==============================
JWT_SECRET = os.getenv("JWT_SECRET", "replace_me_with_a_strong_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))
SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"
SUPREME_ADMIN_EMAIL = "chibuzorobi738@gmail.com"
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
# (kept for password-reset flow in main.py — do not remove)
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
        "expires": refresh_expires,
    }

def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Decodes tokens signed with the app's own JWT_SECRET (HS256).
    Used only for password-reset tokens — NOT for Supabase-issued JWTs.
    """
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def convert_datetime_to_iso(doc: dict) -> dict:
    """Recursively converts all datetime values in a dict to ISO 8601 strings."""
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
    result = (
        _supabase_admin.table("Users")
        .select(
            "_id, email, role, Organization, org_id, is_supreme_admin, "
            "refresh_token_id, refresh_token_hash, refresh_token_expires"
        )
        .eq("refresh_token_id", refresh_token_id)
        .single()
        .execute()
    )
    user = result.data

    if (
        not user
        or not user.get("refresh_token_hash")
        or not verify_password(refresh_token, user["refresh_token_hash"])
        or not user.get("refresh_token_expires")
        or datetime.fromisoformat(user["refresh_token_expires"]) < datetime.utcnow()
    ):
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    email = user.get("email", "")
    is_supreme = email == SUPREME_ADMIN_EMAIL or bool(user.get("is_supreme_admin"))
    organization = user.get("Organization") or user.get("organization", "")
    org_id = user.get("org_id") or organization.lower().replace(" ", "-") or "active-teams"
    org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

    new_access = create_access_token({
        "user_id": user["_id"],
        "email": email,
        "role": user.get("role", "user"),
        "is_supreme_admin": is_supreme,
        "org_id": org_id,
        "Organization": organization,
    })

    new_refresh = create_refresh_token()
    _supabase_admin.table("Users").update({
        "refresh_token_id": new_refresh["id"],
        "refresh_token_hash": new_refresh["hash"],
        "refresh_token_expires": new_refresh["expires"].isoformat(),
        "org_id": org_id,
    }).eq("_id", user["_id"]).execute()

    return {
        "access_token": new_access,
        "refresh_token_id": new_refresh["id"],
        "refresh_token": new_refresh["plain"],
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
async def get_current_user(
    token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> Dict[str, Any]:
    """
    1. Validates the Supabase JWT via supabase_admin.auth.get_user()
       (requires service role key — handles ES256 + expiry automatically).
    2. Extracts the user's email from the Supabase auth response.
    3. Looks up the Users row by email (service role bypasses RLS) to get
       role / org data. Uses email not UUID because Users._id is legacy Mongo format.
    """
    if not token or not token.credentials:
        raise HTTPException(status_code=401, detail="No token provided")

    raw_token = token.credentials

    # ------------------------------------------------------------------
    # 1. Validate token via Supabase Admin client (service role key)
    # ------------------------------------------------------------------
    try:
        auth_response = _supabase_admin.auth.get_user(raw_token)
        sb_user = auth_response.user
        if not sb_user:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token validation failed: {e}")

    sb_email = sb_user.email
    supabase_uuid = sb_user.id

    if not sb_email:
        raise HTTPException(status_code=401, detail="Token missing email claim")

    # ------------------------------------------------------------------
    # 2. Load the user row by EMAIL via service role (bypasses RLS).
    #    Users._id is a legacy Mongo ObjectId, not the Supabase Auth UUID,
    #    so we match on email. service role key means no RLS blocking.
    # ------------------------------------------------------------------
    try:
        result = (
            _supabase_admin.table("Users")
            .select(
                "_id, name, surname, email, role, "
                "Organization, org_id, is_supreme_admin"
            )
            .eq("email", sb_email)
            .order("created_at", desc=True)   # prefer the most recently created row if duplicates exist
            .limit(1)
            .execute()
        )
        rows = result.data
        db_user = rows[0] if rows else None
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"User lookup failed: {e}")

    if not db_user:
        raise HTTPException(status_code=401, detail=f"User not found in database: {sb_email}")

    # ------------------------------------------------------------------
    # 3. Build the current_user dict consumed by all endpoints
    # ------------------------------------------------------------------
    email = db_user.get("email", "")
    is_supreme = email == SUPREME_ADMIN_EMAIL or bool(db_user.get("is_supreme_admin"))
    organization = db_user.get("Organization") or db_user.get("organization") or ""

    org_id = db_user.get("org_id") or ""
    if not org_id and organization:
        org_id = organization.lower().replace(" ", "-")
    if not org_id:
        org_id = "active-teams"
    org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

    # Use the legacy Mongo _id for all downstream DB queries that reference it
    legacy_id = db_user.get("_id", supabase_uuid)

    return {
        "_id": legacy_id,
        "user_id": legacy_id,
        "id": legacy_id,
        "supabase_uuid": supabase_uuid,
        "email": email,
        "role": db_user.get("role", "user"),
        "is_supreme_admin": is_supreme,
        "Organization": organization,
        "organization": organization,
        "org_id": org_id,
        "name": db_user.get("name", ""),
        "surname": db_user.get("surname", ""),
    }


def require_role(*allowed_roles: str):
    """Dependency for role-based access. Supreme admins and admins bypass all checks."""
    async def _checker(
        token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    ):
        current_user = await get_current_user(token)
        role = current_user.get("role")
        is_supreme = current_user.get("is_supreme_admin", False)

        if is_supreme or role == "admin":
            return current_user
        if not role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role not present")

        SYSTEM_ROLES = {"admin", "leader", "leaderAt12", "user", "registrant"}
        if role in SYSTEM_ROLES:
            if role in allowed_roles:
                return current_user
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")

        if "user" in allowed_roles:
            return current_user

        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")

    return _checker
def sanitize_document(doc: dict) -> dict:
    """
    Recursively convert ObjectId and other non-serializable fields.
    """
    from bson import ObjectId


# ==============================
# MISC HELPERS
# ==============================
def sanitize_document(doc: dict) -> dict:
    """Recursively convert non-serializable fields."""
    def sanitize(value):
        if isinstance(value, dict):
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
        result = (
            _supabase_admin.table("Users")
            .select("name, surname")
            .eq("_id", leader_id)
            .single()
            .execute()
        )
        doc = result.data
    except Exception:
        doc = None
    if doc:
        name_parts = [p for p in [doc.get("name", ""), doc.get("surname", "")] if p]
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


def task_type_serializer(task_type) -> dict:
    return {
        "id": str(task_type.get("_id") or task_type.get("id", "")),
        "name": task_type["name"],
    }