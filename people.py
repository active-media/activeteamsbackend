from datetime import datetime
from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends
from auth.models import PersonCreate
from auth.utils import get_current_user
from database import db, people_collection

router = APIRouter()

def normalize_person_data(data: dict) -> dict:
    """Normalize person data for database operations"""
    return {
        "Name": data.get("Name") or data.get("name", ""),
        "Surname": data.get("Surname") or data.get("surname", ""),
        "Number": data.get("Number") or data.get("number", ""),
        "Email": data.get("Email") or data.get("email", ""),
        "Address": data.get("Address") or data.get("address", ""),
        "Birthday": data.get("Birthday") or data.get("birthday") or data.get("dob", ""),
        "Gender": data.get("Gender") or data.get("gender", ""),
        "InvitedBy": data.get("InvitedBy") or data.get("invitedBy", ""),
        "Leader @1": data.get("Leader @1") or data.get("leader1", ""),
        "Leader @12": data.get("Leader @12") or data.get("leader12", ""),
        "Leader @144": data.get("Leader @144") or data.get("leader144", ""),
        "Leader @1728": data.get("Leader @1728") or data.get("leader1728", ""),
        "Stage": data.get("Stage") or data.get("stage", "Win"),
        "UpdatedAt": datetime.utcnow().isoformat()
    }

@router.get("/people")
async def get_people(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=0),  
    name: Optional[str] = None,
    gender: Optional[str] = None,
    dob: Optional[str] = None,
    location: Optional[str] = None,
    leader: Optional[str] = None,
    stage: Optional[str] = None,
    email: Optional[str] = None  
):
    """Get people with optional filtering and pagination"""
    try:
        query = {}

        if name:
            query["$or"] = [
                {"Name": {"$regex": name, "$options": "i"}},
                {"Surname": {"$regex": name, "$options": "i"}},
                {"Email": {"$regex": name, "$options": "i"}}
            ]
        if email:
            query["Email"] = {"$regex": email, "$options": "i"}
        if gender:
            query["Gender"] = {"$regex": gender, "$options": "i"}
        if dob:
            query["Birthday"] = dob
        if location:
            query["Address"] = {"$regex": location, "$options": "i"}
        if leader:
            query["$or"] = [
                {"Leader @1": {"$regex": leader, "$options": "i"}},
                {"Leader @12": {"$regex": leader, "$options": "i"}},
                {"Leader @144": {"$regex": leader, "$options": "i"}},
                {"Leader @1728": {"$regex": leader, "$options": "i"}}
            ]
        if stage:
            query["Stage"] = {"$regex": stage, "$options": "i"}

        # Handle pagination or fetch all
        if perPage == 0:
            cursor = people_collection.find(query)
        else:
            skip = (page - 1) * perPage
            cursor = people_collection.find(query).skip(skip).limit(perPage)

        people_list = []
        async for person in cursor:
            person["_id"] = str(person["_id"])
            
            # Map to consistent field names
            mapped = {
                "_id": person["_id"],
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Number": person.get("Number", ""),
                "Email": person.get("Email", ""),
                "Address": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "Birthday": person.get("Birthday", ""),
                "InvitedBy": person.get("InvitedBy", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", ""),
                "Stage": person.get("Stage", "Win"),
                "Date Created": person.get("Date Created") or datetime.utcnow().isoformat(),
                "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
            }
            people_list.append(mapped)

        # Get total count for pagination metadata
        total_count = await people_collection.count_documents(query)

        return {
            "page": page,
            "perPage": perPage,
            "total": total_count,
            "results": people_list
        }
        
    except Exception as e:
        print(f"Error fetching people: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
# ========== SEARCH & AUTOCOMPLETE ENDPOINTS - MUST COME BEFORE {person_id} ==========

@router.get("/people/search-fast")
async def search_people_fast(
    query: str = Query(..., min_length=2),
    limit: int = Query(25, le=50)
):
    """
    FAST search endpoint for autocomplete - optimized for signup form
    Uses simple regex matching and returns minimal fields
    """
    try:
        if not query or len(query) < 2:
            return {"results": []}
        
        # Simple regex search on name fields
        search_regex = {"$regex": query.strip(), "$options": "i"}
        
        # Only fetch essential fields for autocomplete
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1, 
            "Email": 1,
            "Number": 1,
            "Leader @1": 1,
            "Leader @12": 1,
            "Leader @144": 1,
            "Leader @1728": 1
        }
        
        cursor = people_collection.find({
            "$or": [
                {"Name": search_regex},
                {"Surname": search_regex},
                {"Email": search_regex},
                {"$expr": {
                    "$regexMatch": {
                        "input": {"$concat": ["$Name", " ", "$Surname"]},
                        "regex": query.strip(),
                        "options": "i"
                    }
                }}
            ]
        }, projection).limit(limit)
        
        results = []
        async for person in cursor:
            results.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Number": person.get("Number", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"results": results}
        
    except Exception as e:
        print(f"Error in fast search: {e}")
        return {"results": []}

@router.get("/people/all-minimal")
async def get_all_people_minimal():
    """
    Get all people with minimal fields for client-side caching
    Much faster than full document fetch
    """
    try:
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Number": 1
        }
        
        cursor = people_collection.find({}, projection).limit(1000)
        
        people = []
        async for person in cursor:
            people.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Number": person.get("Number", "")
            })
        
        return {"people": people}
        
    except Exception as e:
        print(f"Error fetching minimal people: {e}")
        return {"people": []}

@router.get("/people/leaders-only")
async def get_leaders_only():
    """
    Get only people who are leaders (have people under them)
    Optimized for signup form where we mostly need leaders
    """
    try:
        # Find people who appear as leaders
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"Leader @1": {"$exists": True, "$ne": ""}},
                        {"Leader @12": {"$exists": True, "$ne": ""}},
                        {"Leader @144": {"$exists": True, "$ne": ""}},
                        {"Leader @1728": {"$exists": True, "$ne": ""}}
                    ]
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "Name": 1,
                    "Surname": 1,
                    "Email": 1,
                    "Number": 1,
                    "Leader @1": 1,
                    "Leader @12": 1,
                    "Leader @144": 1,
                    "Leader @1728": 1
                }
            },
            {"$limit": 500}
        ]
        
        cursor = people_collection.aggregate(pipeline)
        leaders = []
        
        async for person in cursor:
            leaders.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Number": person.get("Number", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"leaders": leaders}
        
    except Exception as e:
        print(f"Error fetching leaders: {e}")
        return {"leaders": []}

# ========== SPECIFIC PERSON ROUTES - MUST COME AFTER SEARCH ROUTES ==========

@router.get("/people/{person_id}")
async def get_person_by_id(person_id: str = Path(...)):
    """Get a single person by ID"""
    try:
        person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        
        person["_id"] = str(person["_id"])
        mapped = {
            "_id": person["_id"],
            "Name": person.get("Name", ""),
            "Surname": person.get("Surname", ""),
            "Number": person.get("Number", ""),
            "Email": person.get("Email", ""),
            "Address": person.get("Address", ""),
            "Gender": person.get("Gender", ""),
            "Birthday": person.get("Birthday", ""),
            "InvitedBy": person.get("InvitedBy", ""),
            "Leader @1": person.get("Leader @1", ""),
            "Leader @12": person.get("Leader @12", ""),
            "Leader @144": person.get("Leader @144", ""),
            "Leader @1728": person.get("Leader @1728", ""),
            "Stage": person.get("Stage", "Win"),
            "Date Created": person.get("Date Created") or datetime.utcnow().isoformat(),
            "UpdatedAt": person.get("UpdatedAt") or datetime.utcnow().isoformat(),
        }
        return mapped
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching person by ID: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/people")
async def create_person(person_data: PersonCreate):
    """Create a new person"""
    try:
        # Normalize email
        email = person_data.email.lower().strip()

        # Check if email already exists
        if email:
            existing_person = await people_collection.find_one({"Email": email})
            if existing_person:
                raise HTTPException(
                    status_code=400,
                    detail=f"A person with email '{email}' already exists"
                )

        # Extract leader fields from the list
        leader1 = person_data.leaders[0] if len(person_data.leaders) > 0 else ""
        leader12 = person_data.leaders[1] if len(person_data.leaders) > 1 else ""
        leader144 = person_data.leaders[2] if len(person_data.leaders) > 2 else ""
        leader1728 = person_data.leaders[3] if len(person_data.leaders) > 3 else ""

        # Prepare the document
        person_doc = {
            "Name": person_data.name.strip(),
            "Surname": person_data.surname.strip(),
            "Email": email,
            "Number": person_data.number.strip(),
            "Address": person_data.address.strip(),
            "Gender": person_data.gender.strip(),
            "Birthday": person_data.dob.strip(),
            "InvitedBy": person_data.invitedBy.strip(),
            "Leader @1": leader1,
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @1728": leader1728,
            "Stage": person_data.stage or "Win",
            "Date Created": datetime.utcnow().isoformat(),
            "UpdatedAt": datetime.utcnow().isoformat()
        }

        # Insert into MongoDB
        result = await people_collection.insert_one(person_doc)

        # Return the created person object
        created_person = {
            "_id": str(result.inserted_id),
            "Name": person_doc["Name"],
            "Surname": person_doc["Surname"],
            "Email": person_doc["Email"],
            "Number": person_doc["Number"],
            "Gender": person_doc["Gender"],
            "Birthday": person_doc["Birthday"],
            "Address": person_doc["Address"],
            "InvitedBy": person_doc["InvitedBy"],
            "Leader @1": person_doc["Leader @1"],
            "Leader @12": person_doc["Leader @12"],
            "Leader @144": person_doc["Leader @144"],
            "Leader @1728": person_doc["Leader @1728"],
            "Stage": person_doc["Stage"],
            "Date Created": person_doc["Date Created"],
            "UpdatedAt": person_doc["UpdatedAt"]
        }

        return {
            "message": "Person created successfully",
            "id": str(result.inserted_id),
            "_id": str(result.inserted_id),
            "person": created_person
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error creating person: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@router.patch("/people/{person_id}")
async def update_person(person_id: str = Path(...), update_data: dict = Body(...)):
    """Update a person's information"""
    try:
        normalized_data = normalize_person_data(update_data)
        
        result = await people_collection.update_one(
            {"_id": ObjectId(person_id)},
            {"$set": normalized_data}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")

        # Fetch the updated person document
        updated_person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not updated_person:
            raise HTTPException(status_code=404, detail="Person not found after update")

        # Return the updated person in the same format as GET
        updated_person["_id"] = str(updated_person["_id"])
        mapped = {
            "_id": updated_person["_id"],
            "Name": updated_person.get("Name", ""),
            "Surname": updated_person.get("Surname", ""),
            "Number": updated_person.get("Number", ""),
            "Email": updated_person.get("Email", ""),
            "Address": updated_person.get("Address", ""),
            "Gender": updated_person.get("Gender", ""),
            "Birthday": updated_person.get("Birthday", ""),
            "InvitedBy": updated_person.get("InvitedBy", ""),
            "Leader @1": updated_person.get("Leader @1", ""),
            "Leader @12": updated_person.get("Leader @12", ""),
            "Leader @144": updated_person.get("Leader @144", ""),
            "Leader @1728": updated_person.get("Leader @1728", ""),
            "Stage": updated_person.get("Stage", "Win"),
            "UpdatedAt": updated_person.get("UpdatedAt"),
        }
        return mapped

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating person: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/people/{person_id}")
async def delete_person(person_id: str = Path(...)):
    """Delete a person"""
    try:
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting person: {e}")
        raise HTTPException(status_code=500, detail=str(e))