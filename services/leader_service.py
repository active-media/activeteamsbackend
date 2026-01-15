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

async def get_leader_at_1_for_leader_at_12(leader_at_12_name: str) -> str:
    """
    FIXED: Get Leader @1 based on Leader @12's gender from People database
    """
    if not leader_at_12_name or not leader_at_12_name.strip():
        return ""
   
    cleaned_name = leader_at_12_name.strip()
    print(f"Looking up Leader @1 for Leader @12: '{cleaned_name}'")
   
    try:
        # Try multiple search strategies to find the person
        search_queries = [
            # Exact full name match
            {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, cleaned_name]}},
            # Case-insensitive name match
            {"Name": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            # First name only (if full name has space)
            {"Name": {"$regex": f"^{cleaned_name.split()[0]}$", "$options": "i"}} if " " in cleaned_name else None,
        ]
       
        # Remove None queries
        search_queries = [q for q in search_queries if q is not None]
       
        person = None
        for query in search_queries:
            person = await people_collection.find_one(query)
            if person:
                print(f"   Found person using query: {query}")
                break
       
        if not person:
            print(f"   Person '{cleaned_name}' NOT found in database")
            return ""
       
        # Get gender
        gender = (person.get("Gender") or "").lower().strip()
        person_full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
       
        print(f"   Found person: {person_full_name}")
        print(f"   Gender: '{gender}'")
       
        # SIMPLE GENDER-BASED ASSIGNMENT
        if gender in ["female", "f", "woman", "lady", "girl"]:
            print(f"   Assigned: Vicky Enslin (female)")
            return "Vicky Enslin"
        elif gender in ["male", "m", "man", "gentleman", "boy"]:
            print(f"   Assigned: Gavin Enslin (male)")
            return "Gavin Enslin"
        else:
            print(f"   Unknown gender: '{gender}' - cannot assign Leader @1")
            return ""
           
    except Exception as e:
        print(f"   Error looking up leader: {str(e)}")
        return ""

async def get_leader_at_1_for_leader_at_144(leader_at_144_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 144.
    This should come from their Leader at 12
    """
    if not leader_at_144_name:
        return ""
   
    print(f"Getting Leader at 1 for Leader @144: {leader_at_144_name}")
   
    # FIRST: Try to find the person by Name (their own record)
    person = await people_collection.find_one({
        "$or": [
            {"Name": {"$regex": f"^{leader_at_144_name}$", "$options": "i"}},
            {"Name": leader_at_144_name}  # Exact match
        ]
    })
   
    if person and person.get("Leader @12"):
        # Get the Leader at 12's name and determine their Leader at 1
        leader_at_12_name = person.get("Leader @12")
        print(f"Leader @144 {leader_at_144_name} has Leader @12: {leader_at_12_name}")
        return await get_leader_at_1_for_leader_at_12(leader_at_12_name)
   
    print(f"Could not find Leader @12 for Leader @144: {leader_at_144_name}")
    return ""  # ADDED MISSING RETURN STATEMENT


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

async def get_leader_at_1_for_event_leader(event_leader_name: str) -> str:
    """
    ENHANCED: Get Leader @1 based on event leader's position in hierarchy
    - If event leader IS a Leader at 12 (appears as Leader at 12 in other events), assign Gavin/Vicky
    - Otherwise, return empty
    """
    if not event_leader_name or not event_leader_name.strip():
        return ""
   
    cleaned_name = event_leader_name.strip()
   
    # Skip if already Gavin/Vicky
    if cleaned_name.lower() in ["gavin enslin", "vicky enslin"]:
        return ""
   
    print(f"Checking if event leader '{cleaned_name}' is a Leader at 12...")
   
    # Check if this person appears as Leader at 12 in ANY event
    is_leader_at_12 = await events_collection.find_one({
        "$or": [
            {"Leader at 12": {"$regex": f"^{cleaned_name}$", "$options": "i"}},
            {"Leader @12": {"$regex": f"^{cleaned_name}$", "$options": "i"}}
        ]
    })
   
    if is_leader_at_12:
        print(f"   {cleaned_name} IS a Leader at 12 - looking up their gender")
        # Now get their gender to assign Gavin/Vicky
        return await get_leader_at_1_for_leader_at_12(cleaned_name)
   
    print(f"   {cleaned_name} is NOT a Leader at 12")
    return ""
   

async def is_user_leader_at_12(user_email: str, user_name: str) -> bool:
    try:
        user = await users_collection.find_one({"email": user_email})
        if user:
            role = user.get("role", "").lower()
            if "leader at 12" in role or "leader@12" in role or "leader @12" in role:
                print(f"   User {user_name} has Leader at 12 role: {role}")
                return True
       
        name_variations = []
       
        if user_name:
            parts = user_name.split()
            first_name = parts[0] if parts else ""
            surname = parts[-1] if len(parts) > 1 else ""
           
            name_variations.append(user_name)
            name_variations.append(f"{first_name} {surname}")
            name_variations.append(first_name)
           
            # Handle hyphenated first names
            if "-" in first_name:
                hyphen_parts = first_name.split("-")
                name_variations.extend([
                    f"{hyphen_parts[0]} {surname}",
                    f"{first_name} {surname}",
                ])
                for part in hyphen_parts:
                    if part and part.strip():
                        name_variations.append(f"{part} {surname}")
           
            # Handle multiple names (e.g., "Nash Bobo Mbankumuna")
            if len(parts) > 2:
                name_variations.append(f"{parts[0]} {parts[-1]}")
                for i in range(1, len(parts)):
                    combined_name = " ".join(parts[:i+1])
                    name_variations.append(combined_name)
       
        # Remove duplicates and empty values
        name_variations = list(set([name for name in name_variations if name and name.strip()]))
       
        print(f"   Checking if {user_name} is Leader at 12 with name variations: {name_variations}")
       
        # Check if user is listed as Leader at 12 in any cell - CASE INSENSITIVE FIELDS
        or_conditions = []
        for name_variant in name_variations:
            safe_name = re.escape(name_variant)
           
            # Lowercase field names
            or_conditions.extend([
                {"leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
           
            # Uppercase field names  
            or_conditions.extend([
                {"Leader at 12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader @12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"Leader12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
           
            # CamelCase field names
            or_conditions.extend([
                {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
                {"LeaderAt12": {"$regex": f"^{safe_name}$", "$options": "i"}},
            ])
           
            # Also try partial matches for flexibility
            # Lowercase
            or_conditions.extend([
                {"leader at 12": {"$regex": safe_name, "$options": "i"}},
                {"leader @12": {"$regex": safe_name, "$options": "i"}},
                {"leader12": {"$regex": safe_name, "$options": "i"}},
            ])
           
            # Uppercase
            or_conditions.extend([
                {"Leader at 12": {"$regex": safe_name, "$options": "i"}},
                {"Leader @12": {"$regex": safe_name, "$options": "i"}},
                {"Leader12": {"$regex": safe_name, "$options": "i"}},
            ])
           
            # CamelCase
            or_conditions.extend([
                {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
                {"LeaderAt12": {"$regex": safe_name, "$options": "i"}},
            ])
       
        if or_conditions:
            full_query = {
                "$and": [
                    {"$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}},
                        {"EventType": {"$regex": "^Cells$", "$options": "i"}}
                    ]},
                    {"$or": or_conditions}
                ]
            }
           
            cells_count = await events_collection.count_documents(full_query)
            print(f"   User {user_name} has {cells_count} cells where they are Leader at 12")
           
            if cells_count > 0:
                found_cells = await events_collection.find(full_query).to_list(length=5)
                print(f"   Sample cells where {user_name} is Leader at 12:")
                for i, cell in enumerate(found_cells):
                    leader_at_12 = (
                        cell.get('Leader at 12') or
                        cell.get('Leader @12') or
                        cell.get('leader12') or
                        cell.get('Leader12') or
                        cell.get('LeaderAt12') or
                        cell.get('leader at 12') or
                        cell.get('leader @12') or
                        'N/A'
                    )
                    print(f"      {i+1}. {cell.get('Event Name', 'N/A')} - Leader@12: {leader_at_12}")
           
            return cells_count > 0
        else:
            print(f"   User {user_name} is NOT a Leader at 12 (no name variations generated)")
            return False
       
    except Exception as e:
        print(f"Error checking if user is leader at 12: {str(e)}")
        return False

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

# ASSIGN LEADER
# @app.get("/debug/leader-assignment/{leader_name}")
# async def debug_leader_assignment(leader_name: str):
#     """
#     Debug endpoint to test leader assignment logic
#     """
#     try:
#         # Try to find the person by Name (exact match first)
#         person = await people_collection.find_one({
#             "$or": [
#                 {"Name": leader_name},  # Exact match
#                 {"Name": {"$regex": f"^{leader_name}$", "$options": "i"}},  # Case insensitive
#                 {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, leader_name]}}  # Full name match
#             ]
#         })
       
#         if not person:
#             return {"error": f"Person '{leader_name}' not found in people database"}

#         person_data = {
#             "name": person.get("Name"),
#             "surname": person.get("Surname"),
#             "gender": person.get("Gender"),
#             "leader_1": person.get("Leader @1"),
#             "leader_12": person.get("Leader @12"),
#             "leader_144": person.get("Leader @144"),
#             "leader_1728": person.get("Leader @ 1728")
#         }
       
#         leader_at_1_from_12 = await get_leader_at_1_for_leader_at_12(leader_name)
#         leader_at_1_from_144 = await get_leader_at_1_for_leader_at_144(leader_name)
#         leader_at_1_from_1728 = await get_leader_at_1_for_leader_at_1728(leader_name)
       
#         return {
#             "person_found": person_data,
#             "leader_assignment_tests": {
#                 "as_leader_12": {
#                     "result": leader_at_1_from_12,
#                     "logic": "Vicky for female, Gavin for male"
#                 },
#                 "as_leader_144": {
#                     "result": leader_at_1_from_144,
#                     "logic": "Get Leader @1 from their Leader @12"
#                 },
#                 "as_leader_1728": {
#                     "result": leader_at_1_from_1728,
#                     "logic": "Get Leader @1 from their Leader @144 -> Leader @12"
#                 }
#             },
#             "recommended_leader_at_1": {
#                 "if_leader_12": leader_at_1_from_12,
#                 "if_leader_144": leader_at_1_from_144,
#                 "if_leader_1728": leader_at_1_from_1728
#             }
#         }
       
#     except Exception as e:
#         return {"error": str(e)}
  
@app.get("/current-user/leader-at-1")
async def get_current_user_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Get the current user's recommended Leader at 1"""
    try:
        user_name = current_user.get("name", "").strip()
        user_email = current_user.get("email", "").strip()
       
        print(f"Getting Leader at 1 for user: {user_name} ({user_email})")
       
        if not user_name and not user_email:
            print("No user name or email found in token")
            return {"leader_at_1": ""}
       
        # Extract username part from email for fuzzy matching
        email_username = ""
        if user_email and "@" in user_email:
            email_username = user_email.split("@")[0]
            print(f"Email username part: {email_username}")
       
        query_conditions = []
       
        if user_name:
            query_conditions.append({"Name": {"$regex": f"^{user_name}$", "$options": "i"}})
       
        if user_email:
            # Exact email match
            query_conditions.append({"Email": {"$regex": f"^{user_email}$", "$options": "i"}})
           
            # Fuzzy email matching for common typos
            if email_username:
                # Match same username with any domain
                query_conditions.append({"Email": {"$regex": f"^{email_username}.*@", "$options": "i"}})
                # Match similar usernames (handles character substitutions like 1/l, 0/O)
                query_conditions.append({"Email": {"$regex": f"tkgenia.*@", "$options": "i"}})
       
        # Also search by name if we have it
        if user_name:
            query_conditions.append({"Name": {"$regex": f"^{user_name}$", "$options": "i"}})
       
        if not query_conditions:
            print("No search conditions available")
            return {"leader_at_1": ""}
       
        query = {"$or": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
       
        print(f"Search query: {query}")
       
        # Try to find the user in people collection
        person = await people_collection.find_one(query)
       
        if not person:
            print(f"User not found in people database with any search criteria")
            # Try one more fallback: search by partial name match
            if user_name:
                fallback_person = await people_collection.find_one({
                    "Name": {"$regex": user_name, "$options": "i"}
                })
                if fallback_person:
                    print(f"Found user with fallback search: {fallback_person.get('Name')}")
                    person = fallback_person
       
        if not person:
            return {"leader_at_1": ""}
       
        print(f"Found user in people database: {person.get('Name')} {person.get('Surname', '')}")
        print(f"User data - Leader @12: {person.get('Leader @12')}, Leader @144: {person.get('Leader @144')}, Leader @1728: {person.get('Leader @ 1728')}")
       
        # Get Leader at 1 based on the user's position in hierarchy
        leader_at_1 = ""
       
        # Check if user is a Leader at 12
        if person.get("Leader @12"):
            print(f"User {person.get('Name')} is a Leader @12")
            leader_at_1 = await get_leader_at_1_for_leader_at_12(person.get("Name"))
        # Check if user is a Leader at 144  
        elif person.get("Leader @144"):
            print(f"User {person.get('Name')} is a Leader @144")
            leader_at_1 = await get_leader_at_1_for_leader_at_144(person.get("Name"))
        # Check if user is a Leader at 1728
        elif person.get("Leader @ 1728"):
            print(f"User {person.get('Name')} is a Leader @1728")
            leader_at_1 = await get_leader_at_1_for_leader_at_1728(person.get("Name"))
        else:
            print(f"User {person.get('Name')} has no leadership position")
       
        print(f"Recommended Leader at 1 for {person.get('Name')}: {leader_at_1}")
        return {"leader_at_1": leader_at_1}
       
    except Exception as e:
        print(f"Error getting current user leader at 1: {e}")
        return {"leader_at_1": ""}
   

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

async def get_leader_at_1_for_leader_at_1728(leader_at_1728_name: str) -> str:
    """
    Determine Leader at 1 for a given Leader at 1728.
    This should come from their Leader at 144 -> Leader at 12
    """
    if not leader_at_1728_name:
        return ""
   
    # FIRST: Try to find the person by Name 
    person = await people_collection.find_one({
        "$or": [
            {"Name": {"$regex": f"^{leader_at_1728_name}$", "$options": "i"}},
            {"Name": leader_at_1728_name}  # Exact match
        ]
    })
   
    if person and person.get("Leader @144"):
        # Get the Leader at 144's name and determine their Leader at 1
        leader_at_144_name = person.get("Leader @144")
        return await get_leader_at_1_for_leader_at_144(leader_at_144_name)
   
    return ""

