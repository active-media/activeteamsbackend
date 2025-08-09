# auth/utils.py
# Requires: pip install python-jose[cryptography] passlib[bcrypt]
import os
from datetime import datetime, time as time_type, timedelta
from typing import Optional, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import users_collection
from bson import ObjectId


# Load from env if present, otherwise defaults
JWT_SECRET = os.getenv("JWT_SECRET", "replace_me_with_a_strong_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer()


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    now = datetime.utcnow()
    expire = now + (expires_delta if expires_delta is not None else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "iat": now})
    encoded = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return encoded


def decode_access_token(token: str) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def get_current_user(token: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> Dict[str, Any]:
    """
    Returns the decoded token payload.
    Expect token payload to include at least: {"user_id": "...", "email": "...", "role": "..."}
    """
    payload = decode_access_token(token.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
    return payload


def require_role(*allowed_roles: str):
    """
    Dependency factory. Use like: Depends(require_role("admin")) or Depends(require_role("registrant", "admin"))
    Admin bypass: role 'admin' will be allowed for any allowed_roles.
    """
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

# Weekday map used for recurring events
WEEKDAY_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

def get_next_occurrence_single(start_dt: datetime, recurring_day: str) -> datetime:
    if recurring_day is None:
        return start_dt

    target_weekday = WEEKDAY_MAP[recurring_day.lower()]
    today = datetime.utcnow().date()
    today_weekday = today.weekday()
    days_ahead = (target_weekday - today_weekday) % 7

    candidate_date = today + timedelta(days=days_ahead)

    start_date_only = start_dt.date()
    if candidate_date < start_date_only:
        candidate_date = candidate_date + timedelta(days=7)

    return datetime.combine(candidate_date, start_dt.time())


async def get_leader_cell_name_async(leader_id: str) -> str:
    try:
        doc = await users_collection.find_one({"_id": ObjectId(leader_id)})
    except Exception:
        doc = await users_collection.find_one({"user_id": leader_id})
    if doc:
        if "cell_name" in doc and doc["cell_name"]:
            return doc["cell_name"]
        name_parts = []
        if doc.get("name"):
            name_parts.append(doc["name"])
        if doc.get("surname"):
            name_parts.append(doc["surname"])
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
