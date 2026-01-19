"""Service checkin service - handles service checkin logic"""
# This file contains all service check-in logic, including attendee, new person, and consolidation handling.

from bson import ObjectId
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import HTTPException, Query, Depends, Body
from auth.utils import get_current_user  
import secrets
from database import events_collection, people_collection


# ----------------------------
# Get real-time service check-in data for an event
# ----------------------------
async def get_service_checkin_real_time_data(
    event_id: str = Query(..., description="Event ID to get real-time data for"),
    current_user: dict = Depends(get_current_user)
):
    """
    Returns actual counts of attendees, new people, and consolidations for a specific event.
    Counts are derived from the database to ensure real-time accuracy.
    """
    try:
        print(f"Getting real-time data for event: {event_id}")

        # Validate event ID
        if not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Fetch fresh event data
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        # Extract attendees, new people, and consolidations
        attendees = event.get("attendees", [])
        new_people = event.get("new_people", [])
        consolidations = event.get("consolidations", [])

        # Calculate actual checked-in counts
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
            "present_count": present_count,
            "new_people_count": new_people_count,
            "consolidation_count": consolidation_count,
            "total_attendance": len(attendees),
            "refreshed_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting real-time data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching real-time data: {str(e)}")


# ----------------------------
# Service check-in for a person
# ----------------------------
async def service_checkin_person(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Handles check-in logic for:
    1️⃣ Attendees (already in database)
    2️⃣ New persons/visitors
    3️⃣ Consolidation/follow-up decisions

    Returns updated counts and check-in details after operation.
    """
    try:
        event_id = checkin_data.get("event_id")
        person_data = checkin_data.get("person_data", {})
        checkin_type = checkin_data.get("type", "attendee")

        # Validate event ID
        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")

        # Fetch fresh event
        event = await events_collection.find_one({"_id": ObjectId(event_id)})
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")

        now = datetime.utcnow().isoformat()

        # ----------------------------
        # 1️⃣ Attendee check-in
        # ----------------------------
        if checkin_type == "attendee":
            person_id = person_data.get("id") or person_data.get("_id")
            if not person_id or not ObjectId.is_valid(person_id):
                raise HTTPException(status_code=400, detail="Valid person ID is required for attendee check-in")

            # Fetch person from database
            existing = await people_collection.find_one({"_id": ObjectId(person_id)})
            if not existing:
                raise HTTPException(status_code=404, detail="Person does not exist — add them first using /people")

            # Prevent duplicate check-in
            already_checked = await events_collection.find_one({
                "_id": ObjectId(event_id),
                "attendees.id": str(existing["_id"])
            })
            if already_checked:
                raise HTTPException(status_code=400, detail=f"{existing.get('Name')} is already checked in")

            # Build attendee record
            attendee_record = {
                "id": str(existing["_id"]),
                "name": existing.get("Name", ""),
                "surname": existing.get("Surname", ""),
                "email": existing.get("Email", ""),
                "phone": existing.get("Number", ""),
                "time": now,
                "checked_in": True,
                "type": "attendee"
            }

            # Update event in DB
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {
                    "$push": {"attendees": attendee_record},
                    "$inc": {"total_attendance": 1},
                    "$set": {"updated_at": now}
                }
            )

            # Get updated counts
            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            updated_attendees = updated_event.get("attendees", [])
            present_count = len([a for a in updated_attendees if a.get("checked_in", False)])

            return {
                "message": f"{existing.get('Name')} checked in",
                "type": "attendee",
                "attendee": attendee_record,
                "present_count": present_count,
                "success": True
            }

        # ----------------------------
        # 2️⃣ New person (visitor) check-in
        # ----------------------------
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

            # Add new person to event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {"$push": {"new_people": new_person_record}, "$set": {"updated_at": now}}
            )

            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            new_people_count = len(updated_event.get("new_people", []))

            return {
                "message": "Visitor added to event",
                "type": "new_person",
                "new_person": new_person_record,
                "new_people_count": new_people_count,
                "success": True
            }

        # ----------------------------
        # 3️⃣ Consolidation / follow-up
        # ----------------------------
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

            # Add consolidation record to event
            await events_collection.update_one(
                {"_id": ObjectId(event_id)},
                {"$push": {"consolidations": consolidation_record}, "$set": {"updated_at": now}}
            )

            updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
            consolidation_count = len(updated_event.get("consolidations", []))

            return {
                "message": "Decision recorded",
                "type": "consolidation",
                "consolidation": consolidation_record,
                "consolidation_count": consolidation_count,
                "success": True
            }

        # Invalid type
        else:
            raise HTTPException(status_code=400, detail="Invalid type — must be attendee, new_person, or consolidation")

    except HTTPException:
        raise
    except Exception as e:
        print("Error in check-in:", e)
        raise HTTPException(status_code=500, detail="Check-in failed")


# ----------------------------
# Remove a person from event
# ----------------------------
async def remove_from_service_checkin(
    removal_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Remove a person from attendees, new_people, or consolidations.
    Returns updated counts for real-time display.
    """
    try:
        event_id = removal_data.get("event_id")
        person_id = removal_data.get("person_id")
        data_type = removal_data.get("type")

        print(f"Removing from service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        # Validation
        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        # Build update query
        update_query = {
            "$pull": {data_type: {"id": person_id}},
            "$set": {"updated_at": datetime.utcnow().isoformat()}
        }

        # Decrement total_attendance if removing attendee
        if data_type == "attendees":
            update_query["$inc"] = {"total_attendance": -1}

        # Execute update
        result = await events_collection.update_one({"_id": ObjectId(event_id)}, update_query)

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Person not found in specified list")

        print(f"Successfully removed from {data_type}")

        # Return updated counts
        updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
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
        print(f"Error removing from service check-in: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error removing person: {str(e)}")


# ----------------------------
# Update a person in any list
# ----------------------------
async def update_service_checkin_person(
    update_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Update fields of a person in attendees, new_people, or consolidations.
    Returns success/failure message.
    """
    try:
        event_id = update_data.get("event_id")
        person_id = update_data.get("person_id")
        data_type = update_data.get("type")
        update_fields = update_data.get("update_fields", {})

        print(f"Updating service check-in - Event: {event_id}, Type: {data_type}, ID: {person_id}")

        # Validation
        if not event_id or not ObjectId.is_valid(event_id):
            raise HTTPException(status_code=400, detail="Invalid event ID")
        if not person_id or not data_type:
            raise HTTPException(status_code=400, detail="Person ID and type are required")

        valid_types = ["attendees", "new_people", "consolidations"]
        if data_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Type must be one of: {valid_types}")

        # Prepare update set query
        set_fields = {f"{data_type}.$.{field}": value for field, value in update_fields.items()}
        set_fields["updated_at"] = datetime.utcnow().isoformat()

        # Execute update
        result = await events_collection.update_one(
            {"_id": ObjectId(event_id), f"{data_type}.id": person_id},
            {"$set": set_fields}
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
