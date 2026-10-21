import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends, BackgroundTasks
from auth.models import PersonCreate
from auth.utils import get_current_user
from database import supabase, PEOPLE_TABLE
import traceback

router = APIRouter()

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _apply_people_org_filter(query, current_user: dict):
    """Restrict a People-table query to the current user's organization."""
    if current_user.get("is_supreme_admin") or current_user.get("role") == "super_admin":
        return query
    org_id = (current_user.get("org_id") or current_user.get("orgId") or "").strip()
    organization = (current_user.get("Organization") or current_user.get("organization") or "").strip()
    conds = []
    if org_id:
        conds.append(f'org_id.eq.{org_id}')
    if organization:
        safe = organization.replace(",", "").replace('"', "")
        conds.append(f'Organization.ilike.%{safe}%')
    if conds:
        query = query.or_(",".join(conds))
    return query


def _person_matches_org(person: dict, current_user: dict) -> bool:
    """Python-side org check used for cached people lists."""
    if current_user.get("is_supreme_admin") or current_user.get("role") == "super_admin":
        return True
    org_id = (current_user.get("org_id") or current_user.get("orgId") or "").strip()
    organization = (current_user.get("Organization") or current_user.get("organization") or "").strip().lower()
    p_org_id = (person.get("org_id") or person.get("Org_id") or person.get("orgId") or "").strip()
    p_org_name = (person.get("Organization") or person.get("organization") or "").strip().lower()
    if org_id and p_org_id == org_id:
        return True
    if organization and p_org_name == organization:
        return True
    if not org_id and not organization:
        return True
    return False

def normalize_person_data(data: dict) -> dict:
    """Normalize person data for database operations.

    NOTE: mirrors the original Mongo behavior - any field not present
    (under either its capitalized or lowercase key) is written as an
    empty string, so this remains a full-field overwrite rather than a
    partial patch.
    """
    return {
        "person_id": data.get("person_id") or data.get("personId") or data.get("Person_id") or "",
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
        "person_id": person.get("person_id") or person.get("Person_id") or person.get("personId"),
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
    current_user: dict = Depends(get_current_user),
):
    """Get people with optional filtering and pagination"""
    try:
        query = supabase.table(PEOPLE_TABLE).select("*, person_id", count="exact")
        query = _apply_people_org_filter(query, current_user)

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
    current_user: dict = Depends(get_current_user),
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
            _apply_people_org_filter(
                supabase.table(PEOPLE_TABLE).select(
                    "_id, person_id, Name, Surname, Email, Number, \"Leader @1\", \"Leader @12\", \"Leader @144\", \"Leader @1728\""
                ),
                current_user,
            )
            .or_(",".join(or_conditions))
            .limit(limit)
            .execute()
        )

        results = []
        for person in response.data or []:
            results.append({
                "_id": person.get("_id", ""),
                "person_id": person.get("person_id") or person.get("Person_id") or person.get("personId"),
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
async def get_all_people_minimal(current_user: dict = Depends(get_current_user)):
    """
    Get all people with minimal fields for client-side caching
    Much faster than full document fetch
    """
    try:
        response = (
            _apply_people_org_filter(
                supabase.table(PEOPLE_TABLE).select("_id, person_id, Name, Surname, Email, Number"),
                current_user,
            )
            .limit(1000)
            .execute()
        )

        people = []
        for person in response.data or []:
            people.append({
                "_id": person.get("_id", ""),
                "person_id": person.get("person_id") or person.get("Person_id") or person.get("personId"),
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
async def get_leaders_only(current_user: dict = Depends(get_current_user)):
    """
    Get only people who are leaders (have people under them)
    Optimized for signup form where we mostly need leaders
    """
    try:
        response = (
            _apply_people_org_filter(
                supabase.table(PEOPLE_TABLE).select(
                    "_id, person_id, Name, Surname, Email, Number, \"Leader @1\", \"Leader @12\", \"Leader @144\", \"Leader @1728\""
                ),
                current_user,
            )
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
                "person_id": person.get("person_id") or person.get("Person_id") or person.get("personId"),
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

@router.get("/people/paginated")
async def get_people_paginated(
    page: int = Query(1, ge=1),
    perPage: int = Query(50, ge=1, le=200),
    name: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """
    Lightweight, paginated people list for the attendance modal.
    Only fetches the fields the modal needs (instead of select("*") on
    thousands of rows) and supports server-side name search.
    """
    try:
        query = supabase.table(PEOPLE_TABLE).select(
            '_id, person_id, Name, Surname, Email, Number, InvitedBy, Stage, person_id,'
            '"Leader @1", "Leader @12", "Leader @144", "Leader @1728"',
            count="exact",
        )
        query = _apply_people_org_filter(query, current_user)

        if name and name.strip():
            escaped = name.strip().replace(",", "")
            query = query.or_(
                f'Name.ilike.%{escaped}%,'
                f'Surname.ilike.%{escaped}%,'
                f'Email.ilike.%{escaped}%'
            )

        start = (page - 1) * perPage
        end = start + perPage - 1
        response = query.range(start, end).execute()

        results = []
        for person in response.data or []:
            results.append({
                "_id": person.get("_id", ""),
                "person_id": person.get("person_id") or person.get("Person_id") or person.get("personId"),
                "Name": person.get("Name") or "",
                "Surname": person.get("Surname") or "",
                "Email": person.get("Email") or "",
                "Number": person.get("Number") or "",
                "InvitedBy": person.get("InvitedBy") or "",
                "Stage": person.get("Stage") or "",
                "Leader @1": person.get("Leader @1") or "",
                "Leader @12": person.get("Leader @12") or "",
                "Leader @144": person.get("Leader @144") or "",
                "Leader @1728": person.get("Leader @1728") or "",
            })

        total = response.count or 0
        return {
            "page": page,
            "perPage": perPage,
            "total": total,
            "hasMore": start + len(results) < total,
            "results": results,
        }

    except Exception as e:
        print(f"Error fetching paginated people: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

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



# import traceback
# from datetime import datetime, timezone, timedelta
# from typing import Dict, Any, Optional
# from fastapi import FastAPI, Depends, BackgroundTasks
# from supabase import create_client, Client

# app = FastAPI()

# ── Initialize Supabase Client ───────────────────────────────────────────────
# SUPABASE_URL = "YOUR_SUPABASE_URL"
# SUPABASE_KEY = "YOUR_SUPABASE_SERVICE_ROLE_KEY"
# supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global in-memory cache store
people_cache: Dict[str, Any] = {
    "data": [],
    "is_valid": False,
    "is_loading": False,
    "pending_refresh": False,
    "last_updated": None,
    "expires_at": None,
    "version": 1,
    "load_progress": 0,
    "total_loaded": 0,
    "total_in_database": 0,
    "refresh_queue": []
}


# ── Helper Functions ─────────────────────────────────────────────────────────

def serialize_doc(doc: dict) -> dict:
    """Ensures document fields are JSON serializable."""
    if not isinstance(doc, dict):
        return {}
    serialized = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            serialized[k] = v.isoformat()
        else:
            serialized[k] = v
    return serialized


def parse_expires(val: Any) -> Optional[datetime]:
    """Converts strings/datetimes into UTC-aware datetime objects."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ── Background Worker Function with Pagination ───────────────────────────────

async def background_refresh_people_cache():
    """Paginates through Supabase to fetch ALL 17k+ records and populates cache."""
    try:
        people_cache["is_loading"] = True
        people_cache["load_progress"] = 0
        print("Starting paginated background load from Supabase...")

        # 1. Get total record count first
        count_response = supabase.table("People").select("person_id", count="exact").limit(1).execute()
        total_in_db = count_response.count if count_response.count is not None else 0
        people_cache["total_in_database"] = total_in_db

        all_records = []
        page_size = 1000  # Maximum page size allowed by Supabase
        start = 0

        # 2. Paginate until all rows are retrieved
        while True:
            end = start + page_size - 1
            response = supabase.table("People").select("*").range(start, end).execute()
            chunk = response.data or []

            if not chunk:
                break

            all_records.extend(chunk)

            # Update progress dynamically for live endpoint reporting
            if total_in_db > 0:
                progress = min(100, int((len(all_records) / total_in_db) * 100))
                people_cache["load_progress"] = progress
                people_cache["total_loaded"] = len(all_records)

            print(f"Fetched rows {start} to {end} ({len(all_records)} / {total_in_db if total_in_db else 'unknown'})")

            if len(chunk) < page_size:
                break

            start += page_size

        # 3. Format and update cache state
        serialized_records = [serialize_doc(p) for p in all_records]

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=1)

        people_cache["data"] = serialized_records
        people_cache["is_valid"] = True
        people_cache["is_loading"] = False
        people_cache["pending_refresh"] = False
        people_cache["last_updated"] = now.isoformat()
        people_cache["expires_at"] = expires_at.isoformat()
        people_cache["load_progress"] = 100
        people_cache["total_loaded"] = len(serialized_records)
        people_cache["total_in_database"] = len(serialized_records)
        people_cache["version"] = people_cache.get("version", 1) + 1

        print(f"Background refresh complete! Successfully cached {len(serialized_records)} records.")

    except Exception as err:
        print(f"Background refresh failed: {err}")
        traceback.print_exc()
        people_cache["is_loading"] = False
        people_cache["pending_refresh"] = False


# ── Endpoint Handler ─────────────────────────────────────────────────────────

@router.get("/cache/people")
async def get_cached_people(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    try:
        current_time = datetime.now(timezone.utc)

        org_label = current_user.get("Organization") or current_user.get("org_id") or "ALL"

        if "refresh_queue" not in people_cache:
            people_cache["refresh_queue"] = []

        expires_at = parse_expires(people_cache.get("expires_at"))
        raw_data = people_cache.get("data") or []
        data = [p for p in raw_data if _person_matches_org(p, current_user)]
        has_data = bool(raw_data)
        is_valid = people_cache.get("is_valid", False)
        is_loading = people_cache.get("is_loading", False)

        # ── 1. Fresh valid cache ──────────────────────────────────────────────
        if has_data and is_valid and expires_at and current_time < expires_at:
            print(f"CACHE HIT: {len(data)} people")
            return {
                "success":       True,
                "cached_data":   data,
                "cached_at":     people_cache["last_updated"],
                "expires_at":    people_cache["expires_at"],
                "source":        "cache",
                "total_count":   len(data),
                "is_complete":   True,
                "load_progress": 100,
                "cache_version": people_cache["version"],
                "is_valid":      True,
                "organization":  org_label,
            }

        # ── 2. Cache currently loading ────────────────────────────────────────
        if is_loading:
            return {
                "success":           True,
                "cached_data":       data,
                "cached_at":         people_cache["last_updated"],
                "source":            "loading",
                "total_count":       len(data),
                "is_complete":       False,
                "load_progress":     people_cache.get("load_progress", 0),
                "loaded_so_far":     people_cache.get("total_loaded", 0),
                "total_in_database": people_cache.get("total_in_database", 0),
                "message":           f"Loading... {people_cache.get('load_progress', 0)}% complete",
                "cache_version":     people_cache["version"],
                "is_valid":          people_cache["is_valid"],
                "organization":      org_label,
            }

        # ── 3. Stale cache — serve stale, trigger background refresh ─────────
        if has_data and not is_valid:
            print("Cache stale — returning stale data while refreshing...")
            if not is_loading:
                people_cache["is_loading"] = True
                background_tasks.add_task(background_refresh_people_cache)
            return {
                "success":        True,
                "cached_data":    data,
                "cached_at":      people_cache["last_updated"],
                "expires_at":     people_cache["expires_at"],
                "source":         "stale_cache",
                "total_count":    len(data),
                "is_complete":    True,
                "message":        "Stale data (refresh in progress)",
                "cache_version":  people_cache["version"],
                "is_valid":       False,
                "refresh_queued": True,
                "organization":   org_label,
            }

        # ── 4. Empty cache — trigger background load ─────────────────────────
        if not has_data and not is_loading:
            print("Cache empty — triggering background load...")
            people_cache["is_loading"] = True
            background_tasks.add_task(background_refresh_people_cache)
            return {
                "success":       True,
                "cached_data":   [],
                "cached_at":     None,
                "source":        "triggered_load",
                "total_count":   0,
                "is_complete":   False,
                "message":       "Background loading started...",
                "load_progress": 0,
                "cache_version": people_cache["version"],
                "organization":  org_label,
            }

        # ── 5. Fallback ───────────────────────────────────────────────────────
        return {
            "success":       True,
            "cached_data":   data,
            "cached_at":     people_cache["last_updated"],
            "source":        "fallback",
            "total_count":   len(data),
            "is_complete":   bool(data),
            "cache_version": people_cache["version"],
            "organization":  org_label,
        }

    except Exception as e:
        print(f"Error in /cache/people: {str(e)}")
        traceback.print_exc()
        return {
            "success":     False,
            "error":       str(e),
            "cached_data": [],
            "total_count": 0,
        }