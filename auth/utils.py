import os
import secrets
from datetime import datetime, time as time_type, timedelta
from typing import Optional, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import users_collection
from bson import ObjectId
from datetime import datetime

# ==============================
# CONFIG
# ==============================
JWT_SECRET = os.getenv("JWT_SECRET", "replace_me_with_a_strong_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

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

    new_access = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "registrant")}
    )

    new_refresh = create_refresh_token()
    await users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "refresh_token_id": new_refresh["id"],
            "refresh_token_hash": new_refresh["hash"],
            "refresh_token_expires": new_refresh["expires"]
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
async def get_current_user(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> Dict[str, Any]:
    payload = decode_access_token(token.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    
    user = await users_collection.find_one({"_id": ObjectId(payload["user_id"])})
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    payload["role"] = user.get("role", "user")
    payload["_id"] = user["_id"]

    # Always derive from DB
    org_name = (
        user.get("org_id") or
        user.get("organization") or
        "active-teams"
    )
    payload["org_id"] = org_name.lower().replace(" ", "-")
    
    print(f"get_current_user: {user.get('email')} -> org_id: {payload['org_id']}")
    
    return payload

def require_role(*allowed_roles: str):
    async def _checker(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
        payload = decode_access_token(token.credentials)
        role = payload.get("role")
        if not role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Role not present in token")
        if role == "admin":
            return payload
        if role not in allowed_roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return payload
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