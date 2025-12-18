"""Service checkin service - handles service checkin logic"""
# This is a placeholder - extract service checkin logic from main.py
from bson import ObjectId
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import HTTPException, Query, Depends
from auth.utils import get_current_user  
import secrets
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks
from database import events_collection, people_collection

async def get_service_checkin_real_time_data(
    event_id: str = Query(..., description="Event ID to get real-time data for"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get real-time data for service check-in with all three data types
    - FIXED: Returns ACTUAL counts from database
    """
    try:
        print(f"Getting real-time data for event: {event_id}")
       
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Get the event FRESH from database
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Extract the three data types from the event - COUNT PROPERLY
        attendees = event.get("attendees", [])
        new_people = event.get("new_people", [])
        consolidations = event.get("consolidations", [])

        # Counts for stats cards - COUNT ACTUAL CHECKED-IN PEOPLE
        present_count = len([a for a in attendees if a.get("checked_in", False) or a.get("is_checked_in", False)])
        new_people_count = len(new_people)
        consolidation_count = len(consolidations)

        print(f"Real-time stats - Present: {present_count}, New: {new_people_count}, Consolidations: {consolidation_count}")

        return {
            "success": True,
            "event_id": event_id,
            "event_name": event.get("eventName", "Unknown Event"),
            "present_attendees": attendees,
            "new_people": new_people,
            "consolidations": consolidations,
            "present_count": present_count,  # ACTUAL COUNT FROM DB
            "new_people_count": new_people_count,  # ACTUAL COUNT FROM DB
            "consolidation_count": consolidation_count,  # ACTUAL COUNT FROM DB
            "total_attendance": len(attendees),
            "refreshed_at": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting real-time data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {str(e)}")

async def service_checkin_person(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Service Check-in - FIXED: Returns ACTUAL counts after operation
    """
    try:
        event_id = checkin_data.get("event_id")
        person_data = checkin_data.get("person_data", {})
        checkin_type = checkin_data.get("type", "attendee")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Get event FRESH from database
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        now = datetime.utcnow().isoformat()


        if checkin_type == "attendee":
            person_id = person_data.get("id") or person_data.get("_id")
            if not person_id or not ObjectId.is_valid(person_id):
                raise HTTPException(
                    status_code=400,
                    detail="Valid person ID is required for attendee check-in"
                )

            # Find person
            existing = await people_collection.find_one({"_id": ObjectId(person_id)})
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail="Person does not exist — add them first using /people"
                )

            # Prevent duplicate check-in
            already_checked = await events_collection.find_one({
                "_id": ObjectId(event_id),
                "attendees.id": str(existing["_id"])
            })
            if already_checked:
                raise HTTPException(
                    status_code=400,
                    detail=f"{existing.get('Name')} is already checked in"
                )

            attendee_record = {
                "id": str(existing["_id"]),
                "name": existing.get("Name", ""),
                "surname": existing.get("Surname", ""),
                "email": existing.get("Email", ""),
                "phone": existing.get("Number", ""),
                "time": now,
                "checked_in": True,  # IMPORTANT: Mark as checked in
                "type": "attendee"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"attendees": attendee_record},
                    "$inc": {"total_attendance": 1},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            updated_attendees = updated_event.get("attendees", [])
            present_count = len([a for a in updated_attendees if a.get("checked_in", False)])

            return {
                "message": f"{existing.get('Name')} checked in",
                "type": "attendee",
                "attendee": attendee_record,
                "present_count": present_count,  # ACTUAL COUNT FROM DB
                "success": True
            }

        # ============================================================
        # 2️⃣ NEW PERSON — Visitors NOT in database
        # ============================================================
        elif checkin_type == "new_person":
            new_person_id = f"new_{secrets.token_urlsafe(8)}"

            new_person_record = {
                "id": new_person_id,
                "name": person_data.get("name", ""),
                "surname": person_data.get("surname", ""),
                "email": person_data.get("email", ""),
                "phone": person_data.get("phone", ""),
                "gender": person_data.get("gender", ""),
                "invitedBy": person_data.get("invitedBy", ""),
                "added_at": now,
                "type": "new_person",
                "needs_database_entry": True,
                "is_checked_in": True,
                "notes": "Visitor - add to database later if needed"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"new_people": new_person_record},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            new_people_count = len(updated_event.get("new_people", []))

            return {
                "message": "Visitor added to event",
                "type": "new_person",
                "new_person": new_person_record,
                "new_people_count": new_people_count,  # ACTUAL COUNT FROM DB
                "success": True
            }

        # ============================================================
        #  CONSOLIDATION — Follow-up decisions
        # ============================================================
        elif checkin_type == "consolidation":
            consolidation_id = f"con_{secrets.token_urlsafe(8)}"

            consolidation_record = {
                "id": consolidation_id,
                "person_name": person_data.get("person_name", ""),
                "person_surname": person_data.get("person_surname", ""),
                "person_email": person_data.get("person_email", ""),
                "person_phone": person_data.get("person_phone", ""),
                "decision_type": person_data.get("decision_type", "first_time"),
                "decision_display_name": person_data.get("decision_display_name", ""),
                "assigned_to": person_data.get("assigned_to", ""),
                "notes": person_data.get("notes", ""),
                "created_at": now,
                "type": "consolidation",
                "status": "active"
            }

            # Update the event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"consolidations": consolidation_record},
                    "$set": {"updated_at": now}
                }
            )

            # Get UPDATED event to return ACTUAL counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            consolidation_count = len(updated_event.get("consolidations", []))

            return {
                "message": "Decision recorded",
                "type": "consolidation",
                "consolidation": consolidation_record,
                "consolidation_count": consolidation_count,  
                "success": True
            }

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid type — must be attendee, new_person, or consolidation"
            )

    except HTTPException:
        raise
    except Exception as e:
        print("Error in check-in:", e)
        raise HTTPException(status_code=500, detail="Check-in failed")

async def remove_from_service_checkin(
    removal_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Remove a person from any of the three data types in an event
    - attendees, new_people, or consolidations
    """
    try:
        event_id = removal_data.get("event_id")
        person_id = removal_data.get("person_id")
        data_type = removal_data.get("type")  

        print(f" Removing from service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        # Build the update query
        update_query = {
            "$pull": {data_type: {"id": person_id}},
            "$set": {"updated_at": datetime.utcnow().isoformat()}
        }

        # If removing from attendees, also decrement total_attendance
        if data_type == "attendees":
            update_query["$inc"] = {"total_attendance": -1}

        result = await events_collection.update_one(
            {"_id": ObjectId(event_id)},
            update_query
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found in specified list")

        print(f" Successfully removed from {data_type}")

        # Get updated counts for real-time sync
        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
       
        # Calculate updated counts
        present_count = len([a for a in updated_event.get("attendees", []) if a.get("checked_in", False)])
        new_people_count = len(updated_event.get("new_people", []))
        consolidation_count = len(updated_event.get("consolidations", []))

        return {
            "success": True,
            "message": f"Person removed from {data_type} successfully",
            "updated_counts": {
                "present_count": present_count,
                "new_people_count": new_people_count,
                "consolidation_count": consolidation_count
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f" Error removing from service check-in: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error removing person: {str(e)}")
   
async def update_service_checkin_person(
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update a person in any of the three data types
    """
    try:
        event_id = update_data.get("event_id")
        person_id = update_data.get("person_id")
        data_type = update_data.get("type")  
        update_fields = update_data.get("update_fields", {})

        print(f" Updating service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        
        set_fields = {}
        for field, value in update_fields.items():
            set_fields[f"{data_type}.$.{field}"] = value

        set_fields["updated_at"] = datetime.utcnow().isoformat()

        result = await events_collection.update_one(
            {
                "_id": ObjectId(event_id),
                f"{data_type}.id": person_id
            },
            {
                "$set": set_fields
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found or no changes made")

        print(f"Successfully updated in {data_type}")

        return {
            "success": True,
            "message": f"Person updated in {data_type} successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating service check-in: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating person: {str(e)}")
   