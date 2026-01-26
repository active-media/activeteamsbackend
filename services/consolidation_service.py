"""Consolidation service - handles consolidation logic"""
# This is a placeholder - extract consolidation logic from main.py

from datetime import datetime
from typing import Optional, Dict, Any
from database import db, consolidations_collection, people_collection, tasks_collection
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks

# Placeholder functions - these need to be extracted from main.py

# async def create_consolidation(consolidation_data, current_user: dict):
#     """Create consolidation - extract from main.py line ~8450"""
#     # TODO: Extract logic from main.py
#     pass
@app.post("/consolidations")
async def create_consolidation(
    consolidation: ConsolidationCreate,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new consolidation record and associated task assigned to the leader
    """
    try:
        consolidation_id = str(ObjectId())
       
        print(f"Creating consolidation for: {consolidation.person_name} {consolidation.person_surname}")
        print(f"👥 Assigned to leader: {consolidation.assigned_to} (email: {consolidation.assigned_to_email})")
       
        # 1. Create or find the person
        person_email = consolidation.person_email
        if not person_email:
            person_email = f"{consolidation.person_name.lower()}.{consolidation.person_surname.lower()}@consolidation.temp"
       
        existing_person = await people_collection.find_one({
            "$or": [
                {"Email": person_email},
                {"Name": consolidation.person_name, "Surname": consolidation.person_surname}
            ]
        })
       
        person_id = None
        if existing_person:
            person_id = str(existing_person["_id"])
            print(f"Found existing person: {person_id}")
            # Update existing person
            update_data = {
                "Stage": "Consolidate",
                "UpdatedAt": datetime.utcnow().isoformat(),
                "DecisionType": consolidation.decision_type.value,
                "DecisionDate": consolidation.decision_date,
            }
           
            # Safely handle decision history
            existing_history = existing_person.get("DecisionHistory", [])
            if consolidation.decision_type == DecisionType.RECOMMITMENT:
                existing_history.append({
                    "type": "recommitment",
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id
                })
                update_data["DecisionHistory"] = existing_history
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0) + 1
                update_data["LastDecisionDate"] = consolidation.decision_date
            else:
                existing_history.append({
                    "type": "first_time",
                    "date": consolidation.decision_date,
                    "consolidation_id": consolidation_id
                })
                update_data["DecisionHistory"] = existing_history
                update_data["FirstDecisionDate"] = consolidation.decision_date
                update_data["TotalRecommitments"] = existing_person.get("TotalRecommitments", 0)
           
            await people_collection.update_one(
                {"_id": ObjectId(person_id)},
                {"$set": update_data}
            )
        else:
            #  FIX: Create new person with ALL required fields
            person_doc = {
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Gender": "",  # Add default gender
                "Address": "",  # Add default address
                "Birthday": "",  # Add default birthday
                "Stage": "Consolidate",
                "DecisionType": consolidation.decision_type.value,
                "DecisionDate": consolidation.decision_date,
                "Date Created": datetime.utcnow().isoformat(),
                "UpdatedAt": datetime.utcnow().isoformat(),
                "InvitedBy": current_user.get("email", ""),
                "Leader @1": consolidation.leaders[0] if len(consolidation.leaders) > 0 else "",
                "Leader @12": consolidation.leaders[1] if len(consolidation.leaders) > 1 else "",
                "Leader @144": consolidation.leaders[2] if len(consolidation.leaders) > 2 else "",
                "Leader @1728": consolidation.leaders[3] if len(consolidation.leaders) > 3 else "",
            }
           
            # Add consolidation-specific fields
            decision_history = [{
                "type": consolidation.decision_type.value,
                "date": consolidation.decision_date,
                "consolidation_id": consolidation_id
            }]
           
            person_doc["DecisionHistory"] = decision_history
            person_doc["TotalRecommitments"] = 1 if consolidation.decision_type == DecisionType.RECOMMITMENT else 0
           
            if consolidation.decision_type == DecisionType.FIRST_TIME:
                person_doc["FirstDecisionDate"] = consolidation.decision_date
            else:
                person_doc["LastDecisionDate"] = consolidation.decision_date
           
            result = await people_collection.insert_one(person_doc)
            person_id = str(result.inserted_id)
           
            #  CRITICAL: Add the new person to the background cache
            new_person_cache_entry = {
                "_id": person_id,
                "Name": consolidation.person_name.strip(),
                "Surname": consolidation.person_surname.strip(),
                "Email": person_email,
                "Number": consolidation.person_phone or "",
                "Gender": "",
                "Leader @1": consolidation.leaders[0] if len(consolidation.leaders) > 0 else "",
                "Leader @12": consolidation.leaders[1] if len(consolidation.leaders) > 1 else "",
                "Leader @144": consolidation.leaders[2] if len(consolidation.leaders) > 2 else "",
                "Leader @1728": consolidation.leaders[3] if len(consolidation.leaders) > 3 else "",
                "FullName": f"{consolidation.person_name.strip()} {consolidation.person_surname.strip()}".strip()
            }
            people_cache["data"].append(new_person_cache_entry)
            print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")

        # 2. FIND OR CREATE LEADER'S USER ACCOUNT
        leader_email = consolidation.assigned_to_email
        leader_user_id = None
       
        if not leader_email:
            # Try to find leader's email from people collection
            leader_person = await people_collection.find_one({
                "$or": [
                    {"Name": consolidation.assigned_to},
                    {"$expr": {"$eq": [{"$concat": ["$Name", " ", "$Surname"]}, consolidation.assigned_to]}}
                ]
            })
            if leader_person:
                leader_email = leader_person.get("Email")
                print(f"Found leader email from people collection: {leader_email}")
       
        if leader_email:
            # Find leader's user account
            leader_user = await users_collection.find_one({"email": leader_email})
            if leader_user:
                leader_user_id = str(leader_user["_id"])
                print(f"Found leader user account: {leader_email} (ID: {leader_user_id})")
            else:
                print(f"Leader {consolidation.assigned_to} has no user account with email: {leader_email}")
        else:
            print(f"Could not find email for leader: {consolidation.assigned_to}")

        # 3. Create task assigned to the leader
        decision_display_name = "First Time Decision" if consolidation.decision_type == DecisionType.FIRST_TIME else "Recommitment"
       
        # Use leader's email for assignment, fallback to leader's name
        assigned_for = leader_email if leader_email else consolidation.assigned_to
       
        task_doc = {
            "name": f"Consolidation: {consolidation.person_name} {consolidation.person_surname} ({decision_display_name})",
            "taskType": "consolidation",
            "description": f"Follow up with {consolidation.person_name} {consolidation.person_surname} who made a {decision_display_name.lower()} on {consolidation.decision_date}",
            "followup_date": datetime.utcnow().isoformat(),
            "status": "Open",
            "assignedfor": assigned_for,  # Assign to leader's email or name
            "assigned_to_email": leader_email,
            "assigned_to_user_id": leader_user_id,
            "type": "followup",
            "priority": "high",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "decision_type": consolidation.decision_type.value,
            "decision_display_name": decision_display_name,
            "contacted_person": {
                "name": f"{consolidation.person_name} {consolidation.person_surname}",
                "email": person_email,
                "phone": consolidation.person_phone or ""
            },
            "created_at": datetime.utcnow().isoformat(),
            "created_by": current_user.get("email", ""),
            "is_consolidation_task": True,
            "leader_assigned": consolidation.assigned_to
        }

        task_result = await tasks_collection.insert_one(task_doc)
        task_id = str(task_result.inserted_id)
        print(f"Created consolidation task: {task_id} assigned to {assigned_for}")

        # 4.  CRITICAL FIX: Add to event consolidations array ONLY, NOT attendees
        if consolidation.event_id and ObjectId.is_valid(consolidation.event_id):
            consolidation_record = {
                "id": consolidation_id,
                "person_id": person_id,
                "person_name": consolidation.person_name,
                "person_surname": consolidation.person_surname,
                "person_email": person_email,
                "person_phone": consolidation.person_phone or "",
                "decision_type": consolidation.decision_type.value,
                "decision_display_name": decision_display_name,
                "assigned_to": consolidation.assigned_to,
                "assigned_to_email": leader_email,
                "created_at": datetime.utcnow().isoformat(),
                "type": "consolidation",
                "status": "active",
                "notes": consolidation.notes
            }

            #  FIX: Only add to consolidations array, NOT attendees
            await events_collection.update_one(
                {"_id": ObjectId(consolidation.event_id)},
                {
                    "$push": {"consolidations": consolidation_record},
                    "$set": {"updated_at": datetime.utcnow().isoformat()}
                }
            )
            print(f"Added to event consolidations: {consolidation.event_id}")

        # 5. Create consolidation record
        consolidation_doc = {
            "_id": ObjectId(consolidation_id),
            "person_id": person_id,
            "person_name": consolidation.person_name,
            "person_surname": consolidation.person_surname,
            "person_email": person_email,
            "person_phone": consolidation.person_phone,
            "decision_type": consolidation.decision_type.value,
            "decision_display_name": decision_display_name,
            "decision_date": consolidation.decision_date,
            "assigned_to": consolidation.assigned_to,
            "assigned_to_email": leader_email,
            "assigned_to_user_id": leader_user_id,
            "event_id": consolidation.event_id,
            "notes": consolidation.notes,
            "created_by": current_user.get("email", ""),
            "created_at": datetime.utcnow().isoformat(),
            "status": "active",
            "task_id": task_id
        }

        consolidations_collection = db["consolidations"]
        await consolidations_collection.insert_one(consolidation_doc)
        print(f"Created consolidation record: {consolidation_id}")

        total_people_count = await people_collection.count_documents({})
        print(f"Updated total people count: {total_people_count}")

        return {
            "message": f"{decision_display_name} recorded successfully and assigned to {consolidation.assigned_to}",
            "consolidation_id": consolidation_id,
            "person_id": person_id,
            "task_id": task_id,
            "decision_type": consolidation.decision_type.value,
            "assigned_to": consolidation.assigned_to,
            "assigned_to_email": leader_email,
            "leader_user_id": leader_user_id,
            "people_count_updated": total_people_count,
            "success": True
        }

    except Exception as e:
        print(f"Error creating consolidation: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error creating consolidation: {str(e)}")


# === ADD THIS AT THE END OF main.py (no import needed) ===



# async def get_consolidations(assigned_to: Optional[str], status: Optional[str], page: int, perPage: int, current_user: dict):
#     """Get consolidations - extract from main.py line ~8878"""
#     # TODO: Extract logic from main.py
#     pass
@app.get("/consolidations")
async def get_consolidations(
    assigned_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    perPage: int = Query(50, ge=1),
    current_user: dict = Depends(get_current_user)
):
    """
    Get consolidation records with filtering
    """
    try:
        query = {}
       
        if assigned_to:
            query["assigned_to"] = assigned_to
        if status:
            query["status"] = status
       
        consolidations_collection = db["consolidations"]
        skip = (page - 1) * perPage
       
        cursor = consolidations_collection.find(query).skip(skip).limit(perPage)
        consolidations = []
       
        async for consolidation in cursor:
            consolidation["_id"] = str(consolidation["_id"])
            # Get person details
            person = await people_collection.find_one({"_id": ObjectId(consolidation["person_id"])})
            if person:
                consolidation["person_details"] = {
                    "name": person.get("Name", ""),
                    "surname": person.get("Surname", ""),
                    "email": person.get("Email", ""),
                    "phone": person.get("Number", ""),
                    "stage": person.get("Stage", "")
                }
            consolidations.append(consolidation)
       
        total = await consolidations_collection.count_documents(query)
       
        return {
            "consolidations": consolidations,
            "total": total,
            "page": page,
            "perPage": perPage
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# async def update_consolidation(consolidation_id: str, update_data: dict, current_user: dict):
#     """Update consolidation - extract from main.py line ~8929"""
#     # TODO: Extract logic from main.py
#     pass
@app.put("/consolidations/{consolidation_id}")
async def update_consolidation(
    consolidation_id: str,
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update consolidation status or details
    """
    try:
        if not ObjectId.is_valid(consolidation_id):
            raise HTTPException(status_code=400, detail="Invalid consolidation ID")
       
        consolidations_collection = db["consolidations"]
        consolidation = await consolidations_collection.find_one({"_id": ObjectId(consolidation_id)})
       
        if not consolidation:
            raise HTTPException(status_code=404, detail="Consolidation not found")
       
        # Update consolidation
        update_data["updated_at"] = datetime.utcnow().isoformat()
        await consolidations_collection.update_one(
            {"_id": ObjectId(consolidation_id)},
            {"$set": update_data}
        )
       
        # If status is completed, update person's stage
        if update_data.get("status") == "completed":
            await people_collection.update_one(
                {"_id": ObjectId(consolidation["person_id"])},
                {"$set": {"Stage": "Disciple", "UpdatedAt": datetime.utcnow().isoformat()}}
            )
           
            # Also update the associated task
            if consolidation.get("task_id"):
                await tasks_collection.update_one(
                    {"_id": ObjectId(consolidation["task_id"])},
                    {"$set": {"status": "completed"}}
                )
       
        return {"message": "Consolidation updated successfully", "success": True}
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# async def get_consolidation_stats(period: str, current_user: dict):
#     """Get consolidation stats - extract from main.py line ~8974"""
#     # TODO: Extract logic from main.py
#     pass
@app.get("/consolidations/stats")
async def get_consolidation_stats(
    period: str = Query("monthly", regex="^(daily|weekly|monthly|yearly)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get consolidation statistics"""
    try:
        stats_collection = db["consolidation_stats"]
       
        if period == "daily":
            date_key = datetime.utcnow().date().isoformat()
            query = {"date": date_key, "type": "daily"}
        elif period == "weekly":
            week_key = datetime.utcnow().strftime("%Y-W%U")
            query = {"week": week_key, "type": "weekly"}
        elif period == "monthly":
            month_key = datetime.utcnow().strftime("%Y-%m")
            query = {"month": month_key, "type": "monthly"}
        else:  # yearly
            year_key = datetime.utcnow().strftime("%Y")
            query = {"year": year_key, "type": "yearly"}
       
        stats = await stats_collection.find_one(query)
       
        if not stats:
            return {
                "period": period,
                "total_consolidations": 0,
                "first_time_count": 0,
                "recommitment_count": 0,
                "first_time_percentage": 0,
                "recommitment_percentage": 0
            }
       
        total = stats.get("total_consolidations", 0)
        first_time = stats.get("first_time_count", 0)
        recommitment = stats.get("recommitment_count", 0)
       
        return {
            "period": period,
            "total_consolidations": total,
            "first_time_count": first_time,
            "recommitment_count": recommitment,
            "first_time_percentage": round((first_time / total) * 100, 1) if total > 0 else 0,
            "recommitment_percentage": round((recommitment / total) * 100, 1) if total > 0 else 0
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




# async def get_person_consolidation_history(person_id: str, current_user: dict):
#     """Get person consolidation history - extract from main.py line ~9024"""
#     # TODO: Extract logic from main.py
#     pass
@app.get("/consolidations/person/{person_id}")
async def get_person_consolidation_history(
    person_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get consolidation history for a specific person"""
    try:
        if not ObjectId.is_valid(person_id):
            raise HTTPException(status_code=400, detail="Invalid person ID")
       
        # Get person details
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
       
        # Get all consolidations for this person
        consolidations_collection = db["consolidations"]
        consolidations = await consolidations_collection.find({
            "person_id": person_id
        }).sort("decision_date", -1).to_list(length=None)
       
        for consolidation in consolidations:
            consolidation["_id"] = str(consolidation["_id"])
       
        return {
            "person_details": {
                "name": person.get("Name", ""),
                "surname": person.get("Surname", ""),
                "email": person.get("Email", ""),
                "phone": person.get("Number", ""),
                "first_decision_date": person.get("FirstDecisionDate"),
                "last_decision_date": person.get("LastDecisionDate"),
                "total_recommitments": person.get("TotalRecommitments", 0),
                "current_stage": person.get("Stage", "")
            },
            "consolidation_history": consolidations,
            "total_consolidations": len(consolidations)
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
   


# async def get_event_consolidations(event_id: str):
#     """Get event consolidations - extract from main.py line ~9066"""
#     # TODO: Extract logic from main.py
#     pass
@app.get("/events/{event_id}/consolidations")
async def get_event_consolidations(event_id: str = Path(...)):
    """Get all consolidations for a specific event"""
    try:
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
       
        consolidations_collection = db["consolidations"]
        consolidations = await consolidations_collection.find({
            "event_id": event_id
        }).sort("created_at", -1).to_list(length=None)
       
        # Enhance with person details
        enhanced_consolidations = []
        for consolidation in consolidations:
            consolidation["_id"] = str(consolidation["_id"])
           
            # Get person details
            person = await people_collection.find_one({
                "_id": ObjectId(consolidation["person_id"])
            })
            if person:
                consolidation["person_details"] = {
                    "name": person.get("Name", ""),
                    "surname": person.get("Surname", ""),
                    "email": person.get("Email", ""),
                    "phone": person.get("Number", ""),
                    "stage": person.get("Stage", ""),
                    "first_decision_date": person.get("FirstDecisionDate"),
                    "total_recommitments": person.get("TotalRecommitments", 0)
                }
           
            # Get task status
            task = await tasks_collection.find_one({
                "_id": ObjectId(consolidation["task_id"])
            })
            if task:
                consolidation["task_status"] = task.get("status", "Unknown")
                consolidation["task_priority"] = task.get("priority", "medium")
           
            enhanced_consolidations.append(consolidation)
       
        return {
            "event_id": event_id,
            "consolidations": enhanced_consolidations,
            "total": len(enhanced_consolidations)
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


