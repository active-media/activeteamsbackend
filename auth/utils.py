import os
import secrets
import logging
import math
import re
import time
from datetime import datetime, time as time_type, timedelta
from typing import Optional, Dict, Any
import httpx
from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import users_collection
from bson import ObjectId

# ==============================
# CONFIG
# ==============================
JWT_SECRET = os.getenv("JWT_SECRET", "replace_me_with_a_strong_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_API_KEY = (
    os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
).strip()
SUPABASE_JWT_ISSUER = os.getenv(
    "SUPABASE_JWT_ISSUER", f"{SUPABASE_URL}/auth/v1"
).strip().rstrip("/")
SUPABASE_JWT_AUDIENCE = os.getenv("SUPABASE_JWT_AUDIENCE", "authenticated").strip()
SUPREME_ADMIN_EMAIL = "tkgenia1234@gmail.com"
logger = logging.getLogger(__name__)
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
def _validate_supabase_claims(token: str) -> Dict[str, Any]:
    try:
        claims = jwt.get_unverified_claims(token)
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid access token") from exc

    if claims.get("iss") != SUPABASE_JWT_ISSUER:
        raise HTTPException(status_code=401, detail="Invalid token issuer")

    audience = claims.get("aud")
    if not (
        audience == SUPABASE_JWT_AUDIENCE
        or (
            isinstance(audience, list)
            and SUPABASE_JWT_AUDIENCE in audience
        )
    ):
        raise HTTPException(status_code=401, detail="Invalid token audience")

    expiration = claims.get("exp")
    if (
        isinstance(expiration, bool)
        or not isinstance(expiration, (int, float))
        or (isinstance(expiration, float) and not math.isfinite(expiration))
        or expiration <= time.time()
    ):
        raise HTTPException(status_code=401, detail="Token has expired or is invalid")

    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise HTTPException(status_code=401, detail="Invalid token subject")

    return claims


async def _get_supabase_user(token: str) -> Dict[str, Any]:
    if (
        not SUPABASE_URL
        or not SUPABASE_API_KEY
        or not SUPABASE_JWT_ISSUER
        or not SUPABASE_JWT_AUDIENCE
    ):
        logger.error("Supabase authentication is not configured")
        raise HTTPException(
            status_code=503,
            detail="Authentication service is not configured",
        )

    claims = _validate_supabase_claims(token)
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{SUPABASE_URL}/auth/v1/user",
                headers={
                    "apikey": SUPABASE_API_KEY,
                    "Authorization": f"Bearer {token}",
                },
            )
    except httpx.RequestError as exc:
        logger.warning("Supabase Auth request failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Authentication service is unavailable",
        ) from exc

    if response.status_code in (400, 401, 403):
        raise HTTPException(status_code=401, detail="Invalid or expired access token")
    if response.status_code != 200:
        logger.error("Supabase Auth returned HTTP %s", response.status_code)
        raise HTTPException(
            status_code=503,
            detail="Authentication service is unavailable",
        )

    try:
        supabase_user = response.json()
    except ValueError as exc:
        logger.error("Supabase Auth returned an invalid user response")
        raise HTTPException(
            status_code=503,
            detail="Authentication service returned an invalid response",
        ) from exc

    if (
        not isinstance(supabase_user, dict)
        or supabase_user.get("id") != claims["sub"]
    ):
        raise HTTPException(status_code=401, detail="Invalid access token subject")

    return supabase_user


async def get_current_user(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> Dict[str, Any]:
    if not token or not token.credentials:
        raise HTTPException(status_code=401, detail="No token provided")

    supabase_user = await _get_supabase_user(token.credentials)
    supabase_user_id = supabase_user["id"]
    user = await users_collection.find_one({"supabase_user_id": supabase_user_id})

    email = supabase_user.get("email")
    if not user and isinstance(email, str) and email.strip():
        if not supabase_user.get("email_confirmed_at"):
            raise HTTPException(
                status_code=403,
                detail="A confirmed email is required to link this account",
            )
        user = await users_collection.find_one(
            {
                "email": {
                    "$regex": f"^{re.escape(email.strip())}$",
                    "$options": "i",
                }
            }
        )

    if not user:
        raise HTTPException(
            status_code=403,
            detail="Supabase user is not linked to an application account",
        )

    user_id = str(user["_id"])
    user_email = user.get("email") or ""
    if not isinstance(user_email, str):
        user_email = ""
    is_supreme = (
        user_email.lower() == SUPREME_ADMIN_EMAIL.lower()
        or user.get("is_supreme_admin", False)
    )
    db_role = user.get("role", "user")
    organization = user.get("Organization") or user.get("organization", "")

    org_id = user.get("org_id")
    if not org_id and organization:
        org_id = organization.lower().replace(" ", "-")
    if not org_id:
        org_id = "active-teams"
    if org_id.lower() in ORG_ID_MAP:
        org_id = ORG_ID_MAP[org_id.lower()]

    return {
        "user_id": user_id,
        "email": user_email,
        "role": db_role,
        "is_supreme_admin": is_supreme,
        "organization": organization,
        "Organization": organization,
        "org_id": org_id,
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "_id": user_id,
    }

def require_role(*allowed_roles: str):
    async def _checker(current_user: Dict[str, Any] = Depends(get_current_user)):
        role = current_user.get("role")
        is_supreme = current_user.get("is_supreme_admin", False)
        
        # Supreme admins have access to everything
        if is_supreme:
            return current_user
        
        if not role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role not present in token")
        
        # Admin has access to everything
        if role == "admin":
            return current_user
        
        # Check system roles
        SYSTEM_ROLES = ['admin', 'leader', 'leaderAt12', 'user', 'registrant']
        if role in SYSTEM_ROLES:
            if role in allowed_roles:
                return current_user
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Insufficient permissions"
            )
        
        # Custom roles
        if 'user' in allowed_roles:
            return current_user
        
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