import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends
from auth.models import PersonCreate
from auth.utils import get_current_user
from database import supabase, PEOPLE_TABLE

router = APIRouter()

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_person_data(data: dict) -> dict:
    """Normalize person data for database operations.

    NOTE: mirrors the original Mongo behavior - any field not present
    (under either its capitalized or lowercase key) is written as an
    empty string, so this remains a full-field overwrite rather than a
    partial patch.
    """
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
        "UpdatedAt": utcnow_iso(),
    }

def map_person(person: dict) -> dict:
    """Map a raw Supabase row to the consistent response shape."""
    return {
        "_id": person.get("_id", ""),
        "Name": person.get("Name") or "",
        "Surname": person.get("Surname") or "",
        "Number": person.get("Number") or "",
        "Email": person.get("Email") or "",
        "Address": person.get("Address") or "",
        "Gender": person.get("Gender") or "",
        "Birthday": person.get("Birthday") or "",
        "InvitedBy": person.get("InvitedBy") or "",
        "Leader @1": person.get("Leader @1") or "",
        "Leader @12": person.get("Leader @12") or "",
        "Leader @144": person.get("Leader @144") or "",
        "Leader @1728": person.get("Leader @1728") or "",
        "Stage": person.get("Stage") or "Win",
        "Date Created": person.get("Date Created") or utcnow_iso(),
        "UpdatedAt": person.get("UpdatedAt") or utcnow_iso(),
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
    email: Optional[str] = None,
):
    """Get people with optional filtering and pagination"""
    try:
        query = supabase.table(PEOPLE_TABLE).select("*", count="exact")

        if name:
            escaped = name.replace(",", "")
            query = query.or_(
                f'Name.ilike.%{escaped}%,'
                f'Surname.ilike.%{escaped}%,'
                f'Email.ilike.%{escaped}%'
            )
        if email:
            query = query.ilike("Email", f"%{email}%")
        if gender:
            query = query.ilike("Gender", f"%{gender}%")
        if dob:
            query = query.eq("Birthday", dob)
        if location:
            query = query.ilike("Address", f"%{location}%")
        if leader:
            escaped = leader.replace(",", "")
            query = query.or_(
                f'"Leader @1".ilike.%{escaped}%,'
                f'"Leader @12".ilike.%{escaped}%,'
                f'"Leader @144".ilike.%{escaped}%,'
                f'"Leader @1728".ilike.%{escaped}%'
            )
        if stage:
            query = query.ilike("Stage", f"%{stage}%")

        # Handle pagination or fetch all
        if perPage != 0:
            start = (page - 1) * perPage
            end = start + perPage - 1
            query = query.range(start, end)

        response = query.execute()

        people_list = [map_person(person) for person in (response.data or [])]
        total_count = response.count or 0

        return {
            "page": page,
            "perPage": perPage,
            "total": total_count,
            "results": people_list,
        }

    except Exception as e:
        print(f"Error fetching people: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

# ========== SEARCH & AUTOCOMPLETE ENDPOINTS - MUST COME BEFORE {person_id} ==========

@router.get("/people/search-fast")
async def search_people_fast(
    query: str = Query(..., min_length=2),
    limit: int = Query(25, le=50),
):
    """
    FAST search endpoint for autocomplete - optimized for signup form
    Uses simple ilike matching and returns minimal fields
    """
    try:
        term = query.strip()
        if not term or len(term) < 2:
            return {"results": []}

        escaped = term.replace(",", "")

        or_conditions = [
            f'Name.ilike.%{escaped}%',
            f'Surname.ilike.%{escaped}%',
            f'Email.ilike.%{escaped}%',
        ]

        tokens = term.split()
        if len(tokens) > 1:
            first, last = tokens[0], tokens[-1]
            or_conditions.append(f'Name.ilike.%{first}%')
            or_conditions.append(f'Surname.ilike.%{last}%')

        response = (
            supabase.table(PEOPLE_TABLE)
            .select("_id, Name, Surname, Email, Number, \"Leader @1\", \"Leader @12\", \"Leader @144\", \"Leader @1728\"")
            .or_(",".join(or_conditions))
            .limit(limit)
            .execute()
        )

        results = []
        for person in response.data or []:
            results.append({
                "_id": person.get("_id", ""),
                "Name": person.get("Name") or "",
                "Surname": person.get("Surname") or "",
                "Email": person.get("Email") or "",
                "Number": person.get("Number") or "",
                "Leader @1": person.get("Leader @1") or "",
                "Leader @12": person.get("Leader @12") or "",
                "Leader @144": person.get("Leader @144") or "",
                "Leader @1728": person.get("Leader @1728") or "",
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
        response = (
            supabase.table(PEOPLE_TABLE)
            .select("_id, Name, Surname, Email, Number")
            .limit(1000)
            .execute()
        )

        people = []
        for person in response.data or []:
            people.append({
                "_id": person.get("_id", ""),
                "Name": person.get("Name") or "",
                "Surname": person.get("Surname") or "",
                "Email": person.get("Email") or "",
                "Number": person.get("Number") or "",
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
        response = (
            supabase.table(PEOPLE_TABLE)
            .select("_id, Name, Surname, Email, Number, \"Leader @1\", \"Leader @12\", \"Leader @144\", \"Leader @1728\"")
            .or_(
                '"Leader @1".neq.,'
                '"Leader @12".neq.,'
                '"Leader @144".neq.,'
                '"Leader @1728".neq.'
            )
            .limit(500)
            .execute()
        )

        leaders = []
        for person in response.data or []:
            leaders.append({
                "_id": person.get("_id", ""),
                "Name": person.get("Name") or "",
                "Surname": person.get("Surname") or "",
                "Email": person.get("Email") or "",
                "Number": person.get("Number") or "",
                "Leader @1": person.get("Leader @1") or "",
                "Leader @12": person.get("Leader @12") or "",
                "Leader @144": person.get("Leader @144") or "",
                "Leader @1728": person.get("Leader @1728") or "",
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
        response = (
            supabase.table(PEOPLE_TABLE)
            .select("*")
            .eq("_id", person_id)
            .execute()
        )

        rows = response.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Person not found")

        return map_person(rows[0])

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
            existing = (
                supabase.table(PEOPLE_TABLE)
                .select("_id")
                .eq("Email", email)
                .execute()
            )
            if existing.data:
                raise HTTPException(
                    status_code=400,
                    detail=f"A person with email '{email}' already exists",
                )

        # Extract leader fields from the list
        leader1 = person_data.leaders[0] if len(person_data.leaders) > 0 else ""
        leader12 = person_data.leaders[1] if len(person_data.leaders) > 1 else ""
        leader144 = person_data.leaders[2] if len(person_data.leaders) > 2 else ""
        leader1728 = person_data.leaders[3] if len(person_data.leaders) > 3 else ""

        new_id = str(uuid.uuid4())
        now = utcnow_iso()

        # Prepare the row
        person_doc = {
            "_id": new_id,
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
            "Date Created": now,
            "UpdatedAt": now,
        }

        # Insert into Supabase
        insert_response = supabase.table(PEOPLE_TABLE).insert(person_doc).execute()
        if not insert_response.data:
            raise HTTPException(status_code=500, detail="Failed to create person")

        created_person = map_person(insert_response.data[0])

        return {
            "message": "Person created successfully",
            "id": new_id,
            "_id": new_id,
            "person": created_person,
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

        update_response = (
            supabase.table(PEOPLE_TABLE)
            .update(normalized_data)
            .eq("_id", person_id)
            .execute()
        )

        if not update_response.data:
            raise HTTPException(status_code=404, detail="Person not found")

        return map_person(update_response.data[0])

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating person: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/people/{person_id}")
async def delete_person(person_id: str = Path(...)):
    """Delete a person"""
    try:
        delete_response = (
            supabase.table(PEOPLE_TABLE)
            .delete()
            .eq("_id", person_id)
            .execute()
        )

        if not delete_response.data:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"message": "Person deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting person: {e}")
        raise HTTPException(status_code=500, detail=str(e))