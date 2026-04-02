# supreme_admin.py
from datetime import datetime
from typing import List, Optional
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from database import users_collection
from auth import get_current_user, SUPREME_ADMIN_EMAIL
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

# ===== Helper Functions =====
async def is_supreme_admin(email: str) -> bool:
    """Check if a user is a supreme admin"""
    if email == SUPREME_ADMIN_EMAIL:
        return True
    
    user = await users_collection.find_one({"email": email})
    return user.get("is_supreme_admin", False) if user else False

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
    user = await users_collection.find_one({"email": data.email})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with email {data.email} not found"
        )
    
    # Update user to be supreme admin
    await users_collection.update_one(
        {"email": data.email},
        {
            "$set": {
                "is_supreme_admin": True,
                "role": "admin",
                "added_as_supreme_by": current_email,
                "added_as_supreme_at": datetime.utcnow()
            }
        }
    )
    
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
    user = await users_collection.find_one({"email": data.email})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with email {data.email} not found"
        )
    
    # Remove supreme admin status
    await users_collection.update_one(
        {"email": data.email},
        {
            "$set": {
                "is_supreme_admin": False,
                "removed_as_supreme_by": current_email,
                "removed_as_supreme_at": datetime.utcnow()
            }
        }
    )
    
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
    
    # Find all supreme admins (including original)
    cursor = users_collection.find({
        "$or": [
            {"email": SUPREME_ADMIN_EMAIL},
            {"is_supreme_admin": True}
        ]
    }).sort("added_as_supreme_at", -1)
    
    admins = []
    async for doc in cursor:
        admins.append(SupremeAdminResponse(
            id=str(doc["_id"]),
            email=doc["email"],
            name=doc.get("name", ""),
            surname=doc.get("surname", ""),
            added_by=doc.get("added_as_supreme_by", "system"),
            added_at=doc.get("added_as_supreme_at", doc.get("created_at", datetime.utcnow())),
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