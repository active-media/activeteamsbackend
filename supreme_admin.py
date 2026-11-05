# supreme_admin.py
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from auth import get_current_user, SUPREME_ADMIN_EMAIL
from auth.utils import is_true
from supabase_helpers.supabase_connection import supabase_admin as supabase
router = APIRouter(prefix="/admin/supreme", tags=["supreme_admin"])

# ===== Models =====
class SupremeAdminCreate(BaseModel):
    email: EmailStr

class SupremeAdminResponse(BaseModel):
    id: str
    email: str
    name: str
    surname: str
    added_by: str
    added_at: datetime
    is_supreme_admin: bool

class SupremeAdminList(BaseModel):
    admins: List[SupremeAdminResponse]
    total: int

SUPREME_COLUMNS = (
    "_id, email, name, surname, role, is_supreme_admin, "
    "added_as_supreme_at, added_as_supreme_by, created_at"
)


def _find_user(email: str) -> Optional[dict]:
    """Return the most recent Users row for an email (case-insensitive)."""
    result = (
        supabase.table("Users")
        .select(SUPREME_COLUMNS)
        .ilike("email", email)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


# ===== Helper Functions =====
async def is_supreme_admin(email: str) -> bool:
    """Check if a user is a supreme admin"""
    if email == SUPREME_ADMIN_EMAIL:
        return True

    user = _find_user(email)
    return is_true(user.get("is_supreme_admin")) if user else False

# ===== Endpoints =====
@router.post("/add", response_model=dict)
async def add_supreme_admin(
    data: SupremeAdminCreate,
    current_user: dict = Depends(get_current_user)
):
    """Add a new supreme admin - Only existing supreme admins can do this"""

    # Check if current user is supreme admin
    current_email = current_user.get("email")
    if current_email != SUPREME_ADMIN_EMAIL and not current_user.get("is_supreme_admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only supreme admins can add other supreme admins"
        )

    # Check if user exists
    user = _find_user(str(data.email))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with email {data.email} not found"
        )

    # Update user to be supreme admin (flag is TEXT in Supabase)
    supabase.table("Users").update(
        {
            "is_supreme_admin": "true",
            "role": "admin",
            "added_as_supreme_by": current_email,
            "added_as_supreme_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
    ).eq("_id", user["_id"]).execute()

    return {
        "success": True,
        "message": f"{data.email} is now a supreme admin"
    }

@router.post("/remove", response_model=dict)
async def remove_supreme_admin(
    data: SupremeAdminCreate,
    current_user: dict = Depends(get_current_user)
):
    """Remove a supreme admin - Only original supreme admin can remove others"""

    current_email = current_user.get("email")

    # Only the original supreme admin can remove others
    if current_email != SUPREME_ADMIN_EMAIL:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only the original supreme admin can remove other supreme admins"
        )

    # Cannot remove yourself
    if data.email == SUPREME_ADMIN_EMAIL:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove the original supreme admin"
        )

    # Check if user exists
    user = _find_user(str(data.email))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with email {data.email} not found"
        )

    # Remove supreme admin status
    supabase.table("Users").update(
        {
            "is_supreme_admin": "false",
            "updated_at": datetime.utcnow().isoformat(),
        }
    ).eq("_id", user["_id"]).execute()

    return {
        "success": True,
        "message": f"{data.email} removed from supreme admins"
    }

@router.get("/list", response_model=SupremeAdminList)
async def list_supreme_admins(
    current_user: dict = Depends(get_current_user)
):
    """List all supreme admins - Only supreme admins can view this"""

    current_email = current_user.get("email")
    if current_email != SUPREME_ADMIN_EMAIL and not current_user.get("is_supreme_admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only supreme admins can view this list"
        )

    result = supabase.table("Users").select(SUPREME_COLUMNS).execute()
    rows = [
        row
        for row in (result.data or [])
        if is_true(row.get("is_supreme_admin"))
        or (row.get("email") or "").lower() == SUPREME_ADMIN_EMAIL.lower()
    ]
    rows.sort(key=lambda row: str(row.get("added_as_supreme_at") or ""), reverse=True)

    admins = []
    for doc in rows:
        added_at = doc.get("added_as_supreme_at") or doc.get("created_at") or datetime.utcnow()
        admins.append(SupremeAdminResponse(
            id=str(doc["_id"]),
            email=doc["email"],
            name=doc.get("name") or "",
            surname=doc.get("surname") or "",
            added_by=doc.get("added_as_supreme_by") or "system",
            added_at=added_at,
            is_supreme_admin=True
        ))

    return SupremeAdminList(
        admins=admins,
        total=len(admins)
    )

@router.get("/check/{email}", response_model=dict)
async def check_supreme_admin(email: str):
    """Check if a user is a supreme admin"""
    is_admin = await is_supreme_admin(email)
    return {
        "email": email,
        "is_supreme_admin": is_admin
    }
