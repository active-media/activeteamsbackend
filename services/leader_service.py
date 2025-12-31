"""Leader service - handles leader-related logic"""

from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException
from activeteamsbackend.auth.models import LeaderStatusResponse
from activeteamsbackend.auth.utils import get_current_user
from database import people_collection, events_collection

app = FastAPI()


async def get_person_by_email(email: str) -> Optional[dict]:
    """Find person by email."""
    return await people_collection.find_one({"Email": {"$regex": f"^{email}$", "$options": "i"}})


async def get_person_by_name(name: str) -> Optional[dict]:
    """Find person by name."""
    return await people_collection.find_one({
        "$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, name.strip()]}
    })


async def get_leader_at_1_by_gender(name: str) -> str:
    """Get Leader @1 based on person's gender."""
    if not name:
        return ""
    
    person = await get_person_by_name(name)
    if not person:
        return ""
    
    gender = (person.get("Gender") or "").lower().strip()
    
    if gender in ["female", "f", "woman", "lady", "girl"]:
        return "Vicky Enslin"
    elif gender in ["male", "m", "man", "gentleman", "boy"]:
        return "Gavin Enslin"
    
    return ""


async def user_has_cell(email: str) -> bool:
    """Check if user has a cell."""
    person = await get_person_by_email(email)
    if not person:
        return False
    
    user_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
    
    cells = await events_collection.find({
        "Event Type": "Cells",
        "$or": [
            {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
            {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
            {"Leader at 144": {"$regex": f"^{user_name}$", "$options": "i"}}
        ]
    }).to_list(length=1)
    
    return len(cells) > 0


@app.get("/leaders")
async def get_all_leaders():
    """Get all unique leaders."""
    try:
        people = await people_collection.find({}).to_list(length=None)
        leaders = []
        
        for person in people:
            # Leader @12
            if person.get("Leader @12"):
                name = person["Leader @12"].strip()
                if name:
                    leaders.append({"name": name.title(), "position": 12})
            
            # Leader @144
            if person.get("Leader @144"):
                name = person["Leader @144"].strip()
                if name:
                    leaders.append({"name": name.title(), "position": 144})
        
        # Remove duplicates
        unique_leaders = [dict(t) for t in {tuple(d.items()) for d in leaders}]
        unique_leaders.sort(key=lambda x: (x["position"], x["name"]))
        
        return {"leaders": unique_leaders}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leaders/cells-for/{email}")
async def get_leader_cells(email: str):
    """Get cells visible to a leader."""
    try:
        # Find person
        person = await get_person_by_email(email)
        if not person:
            return {"error": "Person not found", "email": email}
        
        user_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
        
        # Find cells
        cells = await events_collection.find({
            "Event Type": "Cells",
            "$or": [
                {"Leader": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 12": {"$regex": f"^{user_name}$", "$options": "i"}},
                {"Leader at 144": {"$regex": f"^{user_name}$", "$options": "i"}}
            ]
        }).to_list(None)
        
        result = []
        for cell in cells:
            leader12_name = cell.get("Leader at 12", "")
            leader1_name = cell.get("Leader at 1", "")
            
            # Assign Leader @1 if missing
            if leader12_name and not leader1_name:
                leader1_name = await get_leader_at_1_by_gender(leader12_name)
            
            result.append({
                "event_name": cell.get("Event Name"),
                "leader": cell.get("Leader"),
                "leader_email": cell.get("Email"),
                "leader_at_12": leader12_name,
                "leader_at_144": cell.get("Leader at 144", ""),
                "leader_at_1": leader1_name,
                "day": cell.get("Day"),
                "time": cell.get("Time"),
            })
        
        return {
            "leader_email": email,
            "leader_name": user_name,
            "total_cells": len(result),
            "cells": result
        }
        
    except Exception as e:
        return {"error": str(e)}


@app.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status(current_user: dict = Depends(get_current_user)):
    """Check if user is a leader or has a cell."""
    try:
        user_email = current_user.get("email")
        user_role = current_user.get("role", "").lower()
        
        if not user_email:
            raise HTTPException(status_code=401, detail="User email not found")
        
        # Regular users need a cell
        if user_role == "user":
            has_cell = await user_has_cell(user_email)
            return {
                "isLeader": False,
                "hasCell": has_cell,
                "canAccessEvents": has_cell
            }
        
        # Admins and registrants always have access
        if user_role in ["admin", "registrant"]:
            return {
                "isLeader": True,
                "hasCell": True,
                "canAccessEvents": True
            }
        
        # Check if person is a leader
        person = await get_person_by_email(user_email)
        if person:
            is_leader = any(
                person.get(f"Leader @{level}")
                for level in [12, 144, 1728]
            )
            
            if is_leader:
                return {
                    "isLeader": True,
                    "hasCell": True,
                    "canAccessEvents": True
                }
        
        # Default: no access
        return {
            "isLeader": False,
            "hasCell": False,
            "canAccessEvents": False
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))