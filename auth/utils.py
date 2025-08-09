# auth/utils.py
# Requires: pip install python-jose[cryptography] passlib[bcrypt]
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from passlib.context import CryptContext
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

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
