"""People service - handles all people-related business logic"""
from datetime import datetime, timedelta
from bson import ObjectId
from typing import Optional, List, Dict, Any
from fastapi import HTTPException
import asyncio
import time
from database import people_collection, db

# Enhanced cache storage with background loading
people_cache = {
    "data": [],
    "last_updated": None,
    "expires_at": None,
    "is_loading": False,
    "background_task": None,
    "load_progress": 0,
    "total_loaded": 0,
    "last_error": None,
    "total_in_database": 0
}

CACHE_DURATION_MINUTES = 1440
BACKGROUND_LOAD_DELAY = 2


async def background_load_all_people():
    """Background task to load ALL people from the database"""
    try:
        await asyncio.sleep(BACKGROUND_LOAD_DELAY)
        
        if people_cache["is_loading"]:
            return
            
        people_cache["is_loading"] = True
        people_cache["last_error"] = None
        start_time = time.time()
        
        print("BACKGROUND: Starting to load ALL people...")
        
        all_people_data = []
        total_count = await people_collection.count_documents({})
        people_cache["total_in_database"] = total_count
        print(f"BACKGROUND: Total people in database: {total_count}")
        
        batch_size = 5000
        page = 1
        total_loaded = 0
        
        while True:
            try:
                skip = (page - 1) * batch_size
                
                projection = {
                    "_id": 1,
                    "Name": 1,
                    "Surname": 1,
                    "Email": 1,
                    "Number": 1,
                    "Gender": 1,
                    "Leader @1": 1,
                    "Leader @12": 1,
                    "Leader @144": 1,
                    "Leader @1728": 1
                }
                
                cursor = people_collection.find({}, projection).skip(skip).limit(batch_size)
                batch_data = await cursor.to_list(length=batch_size)
                
                if not batch_data:
                    break
                
                transformed_batch = []
                for person in batch_data:
                    transformed_batch.append({
                        "_id": str(person["_id"]),
                        "Name": person.get("Name", ""),
                        "Surname": person.get("Surname", ""),
                        "Email": person.get("Email", ""),
                        "Number": person.get("Number", ""),
                        "Gender": person.get("Gender", ""),
                        "Leader @1": person.get("Leader @1", ""),
                        "Leader @12": person.get("Leader @12", ""),
                        "Leader @144": person.get("Leader @144", ""),
                        "Leader @1728": person.get("Leader @1728", ""),
                        "FullName": f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
                    })
                
                all_people_data.extend(transformed_batch)
                total_loaded += len(transformed_batch)
                
                progress = (total_loaded / total_count) * 100 if total_count > 0 else 100
                people_cache["load_progress"] = round(progress, 1)
                people_cache["total_loaded"] = total_loaded
                
                print(f"BACKGROUND: Batch {page} - {len(transformed_batch)} people (Total: {total_loaded}/{total_count}, Progress: {progress:.1f}%)")
                
                page += 1
                await asyncio.sleep(0.1)
                
            except Exception as batch_error:
                print(f"BACKGROUND: Error in batch {page}: {str(batch_error)}")
                break
        
        people_cache["data"] = all_people_data
        people_cache["last_updated"] = datetime.utcnow().isoformat()
        people_cache["expires_at"] = (datetime.utcnow() + timedelta(minutes=CACHE_DURATION_MINUTES)).isoformat()
        people_cache["is_loading"] = False
        people_cache["load_progress"] = 100
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"BACKGROUND: Successfully loaded ALL {len(all_people_data)} people in {duration:.2f} seconds")
        
    except Exception as e:
        people_cache["is_loading"] = False
        people_cache["last_error"] = str(e)
        print(f"BACKGROUND: Failed to load people: {str(e)}")


async def get_cached_people():
    """Get cached people data - returns whatever is available immediately"""
    try:
        current_time = datetime.utcnow()
        
        if (people_cache["data"] and
            people_cache["expires_at"] and
            current_time < datetime.fromisoformat(people_cache["expires_at"])):
            
            print(f"CACHE HIT: Returning {len(people_cache['data'])} people")
            return {
                "success": True,
                "cached_data": people_cache["data"],
                "cached_at": people_cache["last_updated"],
                "expires_at": people_cache["expires_at"],
                "source": "cache",
                "total_count": len(people_cache["data"]),
                "is_complete": True,
                "load_progress": 100
            }
        
        if people_cache["is_loading"]:
            return {
                "success": True,
                "cached_data": people_cache["data"],
                "cached_at": people_cache["last_updated"],
                "source": "loading",
                "total_count": len(people_cache["data"]),
                "is_complete": False,
                "load_progress": people_cache["load_progress"],
                "loaded_so_far": people_cache["total_loaded"],
                "total_in_database": people_cache["total_in_database"],
                "message": f"Loading in background... {people_cache['load_progress']}% complete"
            }
        
        if not people_cache["data"] and not people_cache["is_loading"]:
            print("Cache empty, triggering background load...")
            asyncio.create_task(background_load_all_people())
            
            return {
                "success": True,
                "cached_data": [],
                "cached_at": None,
                "source": "triggered_load",
                "total_count": 0,
                "is_complete": False,
                "message": "Background loading started...",
                "load_progress": 0
            }
            
        if people_cache["data"]:
            print("Cache expired, returning stale data while refreshing...")
            if not people_cache["is_loading"]:
                asyncio.create_task(background_load_all_people())
            
            return {
                "success": True,
                "cached_data": people_cache["data"],
                "cached_at": people_cache["last_updated"],
                "expires_at": people_cache["expires_at"],
                "source": "stale_cache",
                "total_count": len(people_cache["data"]),
                "is_complete": True,
                "message": "Using stale data (refresh in progress)"
            }
        
        return {
            "success": True,
            "cached_data": [],
            "cached_at": None,
            "source": "empty",
            "total_count": 0,
            "is_complete": False
        }
        
    except Exception as e:
        print(f"Error in cache endpoint: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "cached_data": [],
            "total_count": 0
        }


async def get_people_simple(page: int, per_page: int):
    """Simple people endpoint as fallback"""
    try:
        skip = (page - 1) * per_page
        
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Number": 1,
            "Gender": 1,
            "Leader @1": 1,
            "Leader @12": 1,
            "Leader @144": 1
        }
        
        cursor = people_collection.find({}, projection).skip(skip).limit(per_page)
        people_list = await cursor.to_list(length=per_page)
        
        formatted_people = []
        for person in people_list:
            formatted_people.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Gender": person.get("Gender", ""),
                "Number": person.get("Number", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "FullName": f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            })
        
        total_count = await people_collection.count_documents({})
        
        return {
            "success": True,
            "results": formatted_people,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_count": total_count,
                "has_more": (skip + len(formatted_people)) < total_count
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "results": []
        }


async def refresh_people_cache():
    """Manually refresh the people cache"""
    try:
        if not people_cache["is_loading"]:
            print("Manual cache refresh triggered")
            asyncio.create_task(background_load_all_people())
            
        return {
            "success": True,
            "message": "Cache refresh triggered",
            "is_loading": people_cache["is_loading"],
            "current_progress": people_cache["load_progress"]
        }
        
    except Exception as e:
        print(f"Error refreshing cache: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


async def get_cache_status():
    """Get detailed cache status and loading progress"""
    total_in_db = await people_collection.count_documents({})
    cache_size = len(people_cache["data"])
    
    status_info = {
        "cache": {
            "size": cache_size,
            "last_updated": people_cache["last_updated"],
            "expires_at": people_cache["expires_at"],
            "is_loading": people_cache["is_loading"],
            "load_progress": people_cache["load_progress"],
            "total_loaded": people_cache["total_loaded"],
            "last_error": people_cache["last_error"]
        },
        "database": {
            "total_people": total_in_db,
            "coverage_percentage": round((cache_size / total_in_db) * 100, 1) if total_in_db > 0 else 0
        },
        "is_complete": cache_size >= total_in_db if total_in_db > 0 else True
    }
    
    return status_info


async def search_people(query: str, limit: int):
    """Fast search through cached people data"""
    try:
        if not people_cache["data"]:
            return {
                "success": False,
                "error": "Cache not ready",
                "results": []
            }
        
        search_term = query.lower().strip()
        results = []
        
        for person in people_cache["data"]:
            if (search_term in person.get("FullName", "").lower() or
                search_term in person.get("Email", "").lower() or
                search_term in person.get("Number", "")):
                results.append(person)
                
            if len(results) >= limit:
                break
        
        return {
            "success": True,
            "results": results,
            "total_found": len(results),
            "search_term": query,
            "source": "cache"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "results": []
        }


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


async def get_people(
    page: int,
    perPage: int,
    name: Optional[str] = None,
    gender: Optional[str] = None,
    dob: Optional[str] = None,
    location: Optional[str] = None,
    leader: Optional[str] = None,
    stage: Optional[str] = None
):
    """Get people with filtering and pagination"""
    try:
        query = {}

        if name:
            query["Name"] = {"$regex": name, "$options": "i"}
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

        if perPage == 0:
            cursor = people_collection.find(query)
        else:
            skip = (page - 1) * perPage
            cursor = people_collection.find(query).skip(skip).limit(perPage)

        people_list = []
        async for person in cursor:
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
            people_list.append(mapped)

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


async def get_person_by_id(person_id: str):
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
    except Exception as e:
        print(f"Error fetching person by ID: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def update_person(person_id: str, update_data: dict):
    """Update a person"""
    try:
        normalized_data = normalize_person_data(update_data)
        
        result = await people_collection.update_one(
            {"_id": ObjectId(person_id)},
            {"$set": normalized_data}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")

        updated_person = await people_collection.find_one({"_id": ObjectId(person_id)})
        if not updated_person:
            raise HTTPException(status_code=404, detail="Person not found after update")

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

    except Exception as e:
        print(f"Error updating person: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def create_person(person_data):
    """Create a new person"""
    try:
        email = person_data.email.lower().strip()

        if email:
            existing_person = await people_collection.find_one({"Email": email})
            if existing_person:
                raise HTTPException(
                    status_code=400,
                    detail=f"A person with email '{email}' already exists"
                )

        leader1 = person_data.leaders[0] if len(person_data.leaders) > 0 else ""
        leader12 = person_data.leaders[1] if len(person_data.leaders) > 1 else ""
        leader144 = person_data.leaders[2] if len(person_data.leaders) > 2 else ""
        leader1728 = person_data.leaders[3] if len(person_data.leaders) > 3 else ""

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

        result = await people_collection.insert_one(person_doc)

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


async def delete_person(person_id: str):
    """Delete a person"""
    try:
        result = await people_collection.delete_one({"_id": ObjectId(person_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}
    except Exception as e:
        print(f"Error deleting person: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def search_people_fast(query: str, limit: int):
    """FAST search endpoint for autocomplete - optimized for signup form"""
    try:
        if not query or len(query) < 2:
            return {"results": []}
        
        search_regex = {"$regex": query.strip(), "$options": "i"}
        
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Phone": 1,
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
                "Phone": person.get("Phone", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"results": results}
        
    except Exception as e:
        print(f"Error in fast search: {e}")
        return {"results": []}


async def get_all_people_minimal():
    """Get all people with minimal fields for client-side caching"""
    try:
        projection = {
            "_id": 1,
            "Name": 1,
            "Surname": 1,
            "Email": 1,
            "Phone": 1
        }
        
        cursor = people_collection.find({}, projection).limit(1000)
        
        people = []
        async for person in cursor:
            people.append({
                "_id": str(person["_id"]),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Phone": person.get("Phone", "")
            })
        
        return {"people": people}
        
    except Exception as e:
        print(f"Error fetching minimal people: {e}")
        return {"people": []}


async def get_leaders_only():
    """Get only people who are leaders (have people under them)"""
    try:
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
                    "Phone": 1,
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
                "Phone": person.get("Phone", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", "")
            })
        
        return {"leaders": leaders}
        
    except Exception as e:
        print(f"Error fetching leaders: {e}")
        return {"leaders": []}


def get_people_cache():
    """Get the people cache object (for use in other services)"""
    return people_cache

