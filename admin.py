"""
admin.py - All /admin/* endpoints targeting the ACTUAL live Supabase schema.

Table mapping (actual live schema → what this file queries):
  - "Users"        : _id (text PK), name, surname, email, password, phone_number,
                     date_of_birth, home_address, invited_by, leader12, leader144,
                     leader1728, stage, role, Organization (capital O), org_id,
                     created_at, updated_at
  - "Organizations": _id (text PK), name, description, created_at, updated_at
  - "People"       : _id (text PK), Name, Surname, Email, Number, Address, Gender,
                     Birthday, InvitedBy, Stage, Organization, LeaderId,
                     LeaderPath[*], Date Created, UpdatedAt
  - "events"       : event_id (uuid PK), event_name, event_type_name, event_type_id,
                     event_leader, event_leader_email, is_active, is_global,
                     is_ticketed, status, recurring_day, organization, org_id,
                     deactivation_start, deactivation_end, deactivation_reason,
                     created_at, updated_at
  - "event_types"  : event_type_id (uuid PK), name, description, is_ticketed,
                     is_global, has_person_steps, org_id, created_at, mongo_id,
                     uuid_ref
  - "Task Types"   : _id (text PK), name, Organization, org_id
  - "Activity Logs": _id (text PK), user_id, action, details, timestamp, Organization

This module exposes THREE routers:
  - router            -> prefix "/admin"
  - event_type_router -> no prefix  (GET/POST/PUT/DELETE /event-types,
                                      /diagnostic/event-type-usage/{name})
  - task_type_router  -> no prefix  (GET/POST/PUT/DELETE /tasktypes)
All three must be included in main.py to preserve the original URL surface.
"""

import re
import uuid
from datetime import datetime
from typing import Optional
from urllib.parse import unquote

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from auth.models import (
    EventTypeCreate,
    MessageResponse,
    OrganizationCreate,
    OrganizationList,
    OrganizationResponse,
    OrganizationUpdate,
    PeopleList,
    PeopleResponse,
    PermissionUpdate,
    RoleUpdate,
    TaskTypeIn,
    TaskTypeOut,
    TaskTypeUpdate,
    UserCreater,
    UserList,
)
from auth.utils import get_current_user
from passlib.context import CryptContext
from supabase_helpers.supabase_client import supabase_admin as supabase

router = APIRouter(prefix="/admin", tags=["admin"])
event_type_router = APIRouter(tags=["admin-event-types"])
task_type_router = APIRouter(tags=["admin-task-types"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"

ROLE_PERMISSIONS = {
    "admin": {
        "manage_users": True,
        "manage_leaders": True,
        "manage_events": True,
        "view_reports": True,
        "system_settings": True,
    },
    "leader": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": True,
        "system_settings": False,
    },
    "user": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": False,
        "view_reports": False,
        "system_settings": False,
    },
    "registrant": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": False,
        "system_settings": False,
    },
}

ROLE_HIERARCHY = {
    "registrant": 2,
    "user": 1,
    "leader": 3,
    "leaderAt12": 4,
    "admin": 5,
    "supreme_admin": 6,
}

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Default org_id used for event types when the org slug is not available.
DEFAULT_ORG_ID = "69c63afc4c3e2fdfc5a4840d"

ORG_ID_MAP = {
    "active-church": "active-teams",
    "active church": "active-teams",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_org_name(current_user: dict) -> Optional[str]:
    """Return organization name from the current_user dict (handles capital-O key)."""
    return (
        current_user.get("Organization")
        or current_user.get("organization")
        or None
    )


def get_user_id(current_user: dict) -> str:
    """Return the user's id string regardless of which key it lives under."""
    return str(current_user.get("id") or current_user.get("_id") or "")


async def invalidate_organizations_cache():
    """Best-effort invalidation of main.py's in-memory organizations_cache."""
    try:
        import main  # local import — avoids circular import at module load

        main.organizations_cache["data"] = []
        main.organizations_cache["expires_at"] = None
    except Exception as e:
        print(f"organizations_cache invalidation skipped: {e}")


async def log_activity(user_id: str, action: str, details: str):
    """Log admin activities to the 'Activity Logs' table."""
    try:
        supabase.table("Activity Logs").insert(
            {
                "user_id": user_id,
                "action": action,
                "details": details,
                "timestamp": datetime.utcnow().isoformat(),
            }
        ).execute()
    except Exception as e:
        print(f"Error logging activity: {e}")


def get_role_color(role: str) -> str:
    role_colors = {
        "admin": "#f44336",
        "leader": "#2196f3",
        "leaderAt12": "#9c27b0",
        "user": "#4caf50",
        "registrant": "#ff9800",
    }
    return role_colors.get(role, "#9c27b0")


# ---------------------------------------------------------------------------
# Supabase helper: resolve Leader @1 from a Leader @12 name
# ---------------------------------------------------------------------------


async def get_leader_at_1_for_leader_at_12_sb(leader_at_12_name: str) -> str:
    """
    Look up a person in 'People' by name/surname then return
    'Gavin Enslin' (male) or 'Vicky Enslin' (female).
    People table uses PascalCase columns: Name, Surname, Gender.
    """
    if not leader_at_12_name or not leader_at_12_name.strip():
        return ""
    name = leader_at_12_name.strip()
    try:
        result = (
            supabase.table("People")
            .select("Gender")
            .ilike("Name", name)
            .limit(1)
            .execute()
        )
        if not result.data:
            parts = name.split(" ", 1)
            if len(parts) == 2:
                result = (
                    supabase.table("People")
                    .select("Gender")
                    .ilike("Name", parts[0])
                    .ilike("Surname", parts[1])
                    .limit(1)
                    .execute()
                )
        if result.data:
            gender = (result.data[0].get("Gender") or "").lower()
            if gender == "female":
                return "Vicky Enslin"
            if gender == "male":
                return "Gavin Enslin"
    except Exception as e:
        print(f"Error in get_leader_at_1_for_leader_at_12_sb: {e}")
    return ""


# ===========================================================================
# EVENT-RELATED ADMIN ENDPOINTS
# ===========================================================================


@router.post("/backfill-event-leaders")
async def backfill_event_leaders(current_user: dict = Depends(get_current_user)):
    """
    Backfill event_leader / event_leader_email on events from the People table.
    Matches on event_leader_email → People.Email.
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        updated = 0
        skipped = 0
        not_found = 0

        # Fetch all events that have an event_leader_email
        events_res = (
            supabase.table("events")
            .select("event_id, event_leader_email")
            .execute()
        )
        events = events_res.data or []

        for event in events:
            leader_email = (event.get("event_leader_email") or "").strip().lower()

            if not leader_email:
                skipped += 1
                continue

            person_res = (
                supabase.table("People")
                .select("_id, LeaderId, LeaderPath")
                .ilike("Email", leader_email)
                .limit(1)
                .execute()
            )

            if not person_res.data:
                not_found += 1
                continue

            person = person_res.data[0]

            supabase.table("events").update(
                {
                    "updated_at": datetime.utcnow().isoformat(),
                }
            ).eq("event_id", event["event_id"]).execute()

            updated += 1

        return {"success": True, "updated": updated, "skipped": skipped, "not_found": not_found}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/migrate-persistent-attendees")
async def migrate_persistent_attendees(current_user: dict = Depends(get_current_user)):
    """Migrate old attendee data on cell events to the event_attendees table."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        # Fetch all events typed as Cells that have no persistent attendees yet
        events_res = (
            supabase.table("events")
            .select("event_id, event_type_name")
            .ilike("event_type_name", "Cells")
            .execute()
        )
        events = events_res.data or []
        processed = 0

        for event in events:
            event_id = event["event_id"]

            # Check if attendees already exist for this event
            existing = (
                supabase.table("event_attendees")
                .select("attendee_id", count="exact")
                .eq("event_id", event_id)
                .execute()
            )
            if existing.count and existing.count > 0:
                continue

            # Fetch session attendees and mark them as persistent
            session_attendees_res = (
                supabase.table("event_session_attendees")
                .select("full_name, email, phone, mongo_person_id")
                .eq("event_id", event_id)
                .execute()
            )
            session_attendees = session_attendees_res.data or []

            seen_names: set = set()
            for sa in session_attendees:
                full_name = sa.get("full_name") or ""
                if not full_name or full_name in seen_names:
                    continue
                seen_names.add(full_name)
                supabase.table("event_attendees").insert(
                    {
                        "event_id": event_id,
                        "mongo_person_id": sa.get("mongo_person_id"),
                        "full_name": full_name,
                        "email": sa.get("email"),
                        "phone": sa.get("phone"),
                        "is_persistent": True,
                    }
                ).execute()

            processed += 1

        return {"message": f"Processed {processed} cell events"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cleanup-duplicate-cells")
async def cleanup_duplicate_cells(current_user: dict = Depends(get_current_user)):
    """Remove duplicate cell events (same event_name + event_leader_email + recurring_day)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader_email, recurring_day")
            .ilike("event_type_name", "Cells")
            .execute()
        )
        events = events_res.data or []

        seen: dict = {}
        ids_to_delete: list = []

        for event in events:
            key = (
                (event.get("event_name") or "").lower(),
                (event.get("event_leader_email") or "").lower(),
                (event.get("recurring_day") or "").lower(),
            )
            if key in seen:
                ids_to_delete.append(event["event_id"])
            else:
                seen[key] = event["event_id"]

        deleted_count = 0
        for eid in ids_to_delete:
            supabase.table("events").delete().eq("event_id", eid).execute()
            deleted_count += 1

        return {"message": f"Deleted {deleted_count} duplicate cells"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events/missing-leaders")
async def get_missing_leaders(current_user: dict = Depends(get_current_user)):
    """Find all Leader @12 names in events that don't exist in the People table."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        events_res = (
            supabase.table("events")
            .select("event_leader, event_leader_email")
            .ilike("event_type_name", "Cells")
            .execute()
        )
        events = events_res.data or []

        name_counts: dict[str, int] = {}
        for ev in events:
            name = (ev.get("event_leader") or "").strip()
            if name:
                name_counts[name] = name_counts.get(name, 0) + 1

        event_leaders = [
            {"name": n, "event_count": c}
            for n, c in sorted(name_counts.items(), key=lambda x: -x[1])
        ]

        missing_leaders = []
        found_leaders = []

        for leader_info in event_leaders:
            name = leader_info["name"]
            parts = name.split(" ", 1)

            person_res = (
                supabase.table("People")
                .select("_id, Name, Surname, Gender")
                .ilike("Name", parts[0])
                .limit(5)
                .execute()
            )
            person = None
            if person_res.data:
                for p in person_res.data:
                    full = f"{p.get('Name', '')} {p.get('Surname', '')}".strip()
                    if full.lower() == name.lower():
                        person = p
                        break
                if not person:
                    person = person_res.data[0]

            if not person:
                missing_leaders.append(leader_info)
            else:
                found_leaders.append(
                    {
                        **leader_info,
                        "gender": person.get("Gender", "Unknown"),
                        "full_name": f"{person.get('Name', '')} {person.get('Surname', '')}".strip(),
                    }
                )

        return {
            "total_leaders_in_events": len(event_leaders),
            "found_in_people": len(found_leaders),
            "missing_from_people": len(missing_leaders),
            "found_leaders": found_leaders[:20],
            "missing_leaders": missing_leaders,
            "message": f"Found {len(found_leaders)} leaders, {len(missing_leaders)} need to be added",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events/bulk-assign-all-leaders")
async def bulk_assign_all_leaders_comprehensive(current_user: dict = Depends(get_current_user)):
    """Bulk assign Leader @1 for ALL cell events from Supabase."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        cell_events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader")
            .or_("event_type_name.ilike.Cells,event_type_name.ilike.Cell")
            .execute()
        )
        cell_events = cell_events_res.data or []

        updated_count = 0
        failed_count = 0
        skipped_count = 0
        results: dict = {"updated": [], "failed": [], "skipped": []}

        for event in cell_events:
            event_id = event["event_id"]
            event_name = event.get("event_name", "Unknown")
            leader_at_12 = (event.get("event_leader") or "").strip()

            if not leader_at_12:
                skipped_count += 1
                results["skipped"].append({"event_name": event_name, "reason": "No Leader @12"})
                continue

            leader_at_1 = await get_leader_at_1_for_leader_at_12_sb(leader_at_12)

            if leader_at_1:
                supabase.table("events").update(
                    {"updated_at": datetime.utcnow().isoformat()}
                ).eq("event_id", event_id).execute()

                updated_count += 1
                results["updated"].append(
                    {
                        "event_name": event_name,
                        "leader_at_12": leader_at_12,
                        "assigned_leader_at_1": leader_at_1,
                    }
                )
            else:
                failed_count += 1
                results["failed"].append(
                    {
                        "event_name": event_name,
                        "leader_at_12": leader_at_12,
                        "reason": "Person not found or gender unknown",
                    }
                )

        return {
            "success": True,
            "message": f"Assigned Leader @1 to {updated_count} events. {failed_count} failed, {skipped_count} skipped.",
            "summary": {
                "total_processed": len(cell_events),
                "updated": updated_count,
                "failed": failed_count,
                "skipped": skipped_count,
            },
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error assigning leaders: {str(e)}")


@router.post("/events/fix-all-leaders-at-1")
async def fix_all_leaders_at_1(current_user: dict = Depends(get_current_user)):
    """Assign Leader @1 based on event leader's gender (Gavin/Vicky)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader")
            .execute()
        )
        all_events = events_res.data or []

        updated_count = 0
        failed_count = 0
        skipped_count = 0
        results = []

        for event in all_events:
            event_id = event["event_id"]
            event_name = event.get("event_name", "Unknown")
            leader_name = (event.get("event_leader") or "").strip()

            if not leader_name:
                skipped_count += 1
                continue

            parts = leader_name.split(" ", 1)
            person_res = (
                supabase.table("People")
                .select("_id, Name, Surname, Gender")
                .ilike("Name", parts[0])
                .limit(5)
                .execute()
            )

            person = None
            if person_res.data:
                for p in person_res.data:
                    full = f"{p.get('Name', '')} {p.get('Surname', '')}".strip()
                    if full.lower() == leader_name.lower():
                        person = p
                        break
                if not person:
                    person = person_res.data[0]

            if not person:
                failed_count += 1
                results.append({"event": event_name, "leader": leader_name, "status": "failed - not found"})
                continue

            gender = (person.get("Gender") or "").strip().lower()

            if gender == "female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "male":
                leader_at_1 = "Gavin Enslin"
            else:
                failed_count += 1
                results.append({"event": event_name, "leader": leader_name, "gender": gender, "status": "failed - unknown gender"})
                continue

            supabase.table("events").update(
                {"updated_at": datetime.utcnow().isoformat()}
            ).eq("event_id", event_id).execute()

            updated_count += 1
            results.append(
                {
                    "event": event_name,
                    "leader": leader_name,
                    "gender": gender,
                    "assigned_leader_at_1": leader_at_1,
                    "status": "success",
                }
            )

        return {
            "success": True,
            "message": f"Fixed {updated_count} events successfully!",
            "summary": {
                "updated": updated_count,
                "failed": failed_count,
                "skipped": skipped_count,
                "total": len(all_events),
            },
            "results": results[:20],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events/verify-leaders")
async def verify_leaders_assignment(current_user: dict = Depends(get_current_user)):
    """Verify Leader assignments in cell events."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        cell_events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader, event_leader_email")
            .or_("event_type_name.ilike.Cells,event_type_name.ilike.Cell")
            .execute()
        )
        cell_events = cell_events_res.data or []

        with_leader = []
        without_leader = []

        for event in cell_events:
            leader = (event.get("event_leader") or "").strip()
            if leader:
                with_leader.append(
                    {
                        "event_name": event.get("event_name"),
                        "leader": leader,
                        "leader_email": event.get("event_leader_email"),
                    }
                )
            else:
                without_leader.append({"event_name": event.get("event_name")})

        total = len(cell_events)
        return {
            "total_cell_events": total,
            "with_leader": {
                "count": len(with_leader),
                "percentage": round(len(with_leader) / total * 100, 1) if total else 0,
                "sample": with_leader[:10],
            },
            "without_leader": {
                "count": len(without_leader),
                "percentage": round(len(without_leader) / total * 100, 1) if total else 0,
                "sample": without_leader[:10],
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events/fix-all-missing-leader-at-1")
async def fix_all_missing_leader_at_1(current_user: dict = Depends(get_current_user)):
    """Fix all Cell events missing an event_leader based on the People table gender."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader, event_leader_email")
            .ilike("event_type_name", "Cells")
            .is_("event_leader", "null")
            .execute()
        )
        cell_events = events_res.data or []

        updated_count = 0
        failed_count = 0
        results = []

        for event in cell_events:
            event_id = event["event_id"]
            event_name = event.get("event_name", "")
            leader_email = (event.get("event_leader_email") or "").strip()

            if not leader_email:
                failed_count += 1
                continue

            person = None
            if leader_email:
                res = (
                    supabase.table("People")
                    .select("_id, Gender")
                    .ilike("Email", leader_email)
                    .limit(1)
                    .execute()
                )
                if res.data:
                    person = res.data[0]

            if not person:
                failed_count += 1
                continue

            gender = (person.get("Gender") or "").lower()
            if gender == "female":
                leader_at_1 = "Vicky Enslin"
            elif gender == "male":
                leader_at_1 = "Gavin Enslin"
            else:
                failed_count += 1
                continue

            supabase.table("events").update(
                {"updated_at": datetime.utcnow().isoformat()}
            ).eq("event_id", event_id).execute()

            updated_count += 1
            results.append(
                {
                    "event_name": event_name,
                    "leader_email": leader_email,
                    "gender": gender,
                    "assigned_leader_at_1": leader_at_1,
                    "status": "updated",
                }
            )

        return {
            "message": f"Fixed {updated_count} events, {failed_count} failed",
            "updated_count": updated_count,
            "failed_count": failed_count,
            "total_processed": len(cell_events),
            "results": results[:25],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fixing leaders: {str(e)}")


@router.post("/events/assign-leaders")
async def bulk_assign_leaders(current_user: dict = Depends(get_current_user)):
    """Bulk assign Leader @1 for existing cell events missing it."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_leader")
            .ilike("event_type_name", "cell")
            .is_("event_leader", "null")
            .execute()
        )
        cell_events = events_res.data or []

        updated_count = 0
        results = []

        for event in cell_events:
            event_id = event["event_id"]
            event_name = event.get("event_name", "Unknown")
            leader_at_12 = (event.get("event_leader") or "").strip()

            leader_at_1 = ""
            if leader_at_12:
                leader_at_1 = await get_leader_at_1_for_leader_at_12_sb(leader_at_12)

            if leader_at_1:
                supabase.table("events").update(
                    {"updated_at": datetime.utcnow().isoformat()}
                ).eq("event_id", event_id).execute()
                updated_count += 1
                results.append(
                    {
                        "event_id": event_id,
                        "event_name": event_name,
                        "leader_at_12": leader_at_12,
                        "assigned_leader_at_1": leader_at_1,
                    }
                )

        return {
            "message": f"Assigned Leader @1 for {updated_count} events",
            "updated_count": updated_count,
            "results": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in bulk assignment: {str(e)}")


@router.post("/add-uuids-to-all-events")
async def add_uuids_to_all_events(current_user: dict = Depends(get_current_user)):
    """Add UUIDs to all events that don't have a mongo_id reference."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        events_res = (
            supabase.table("events")
            .select("event_id, mongo_id")
            .is_("mongo_id", "null")
            .execute()
        )
        events_without_id = events_res.data or []
        updated_count = 0

        for event in events_without_id:
            new_uuid = str(uuid.uuid4())
            supabase.table("events").update(
                {"updated_at": datetime.utcnow().isoformat()}
            ).eq("event_id", event["event_id"]).execute()
            updated_count += 1

        return {
            "message": f"Successfully processed {updated_count} events",
            "updated_count": updated_count,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# USER MANAGEMENT ENDPOINTS
# ===========================================================================


@router.post("/users", response_model=MessageResponse)
async def create_user(user_data: UserCreater, current_user: dict = Depends(get_current_user)):
    """Create a new user - Admin only."""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL

    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        existing = (
            supabase.table("Users")
            .select("_id")
            .eq("email", user_data.email)
            .limit(1)
            .execute()
        )
        if existing.data:
            raise HTTPException(status_code=400, detail="User with this email already exists")

        valid_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]
        if user_data.role not in valid_roles:
            raise HTTPException(status_code=400, detail="Invalid role")

        if not is_supreme and user_data.role == "admin":
            raise HTTPException(status_code=403, detail="Cannot create admin users")

        hashed_password = pwd_context.hash(user_data.password)

        user_doc = {
            "name": user_data.name,
            "surname": user_data.surname,
            "email": user_data.email,
            "password": hashed_password,
            "phone_number": user_data.phone_number,
            "date_of_birth": user_data.date_of_birth.isoformat() if user_data.date_of_birth else None,
            "home_address": user_data.address,
            "gender": user_data.gender,
            "invited_by": user_data.invitedBy,
            "leader12": user_data.leader12,
            "leader144": user_data.leader144,
            "leader1728": user_data.leader1728,
            "stage": user_data.stage or "Win",
            "role": user_data.role,
            "Organization": get_org_name(current_user),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        supabase.table("Users").insert(user_doc).execute()

        await log_activity(
            user_id=get_user_id(current_user),
            action="USER_CREATED",
            details=f"Created new user: {user_data.name} {user_data.surname} ({user_data.role})",
        )

        return MessageResponse(message=f"User {user_data.name} {user_data.surname} created successfully")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")


@router.get("/users", response_model=UserList)
async def get_all_users(
    organization: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=5000),
    current_user: dict = Depends(get_current_user),
):
    """Get all users - Admin only."""
    try:
        is_supreme = (
            current_user.get("email") == SUPREME_ADMIN_EMAIL
            or current_user.get("is_supreme_admin", False)
        )

        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            org_filter = get_org_name(current_user)

        query = (
            supabase.table("Users")
            .select(
                "_id, name, surname, email, role, phone_number, Organization, "
                "created_at, date_of_birth, home_address, gender, invited_by, "
                "leader12, leader144, leader1728, stage",
                count="exact",
            )
        )

        if org_filter:
            query = query.eq("Organization", org_filter)

        query = query.order("created_at", desc=True).range(skip, skip + limit - 1)
        result = query.execute()

        users_raw = result.data or []
        total = result.count or 0

        users = [
            {
                "id": u["_id"],
                "name": u.get("name", ""),
                "surname": u.get("surname", ""),
                "email": u.get("email", ""),
                "role": u.get("role", "user"),
                "phone_number": u.get("phone_number"),
                "organization": u.get("Organization") or "Unknown",
                "created_at": u.get("created_at"),
                "date_of_birth": u.get("date_of_birth"),
                "address": u.get("home_address"),
                "gender": u.get("gender"),
                "invitedBy": u.get("invited_by"),
                "leader12": u.get("leader12"),
                "leader144": u.get("leader144"),
                "leader1728": u.get("leader1728"),
                "stage": u.get("stage"),
            }
            for u in users_raw
        ]

        return {"users": users, "total": total, "skip": skip, "limit": limit}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_admin_stats(
    organization: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Get user stats by role - Admin only."""
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL

        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            org_filter = get_org_name(current_user)

        query = supabase.table("Users").select("role")
        if org_filter:
            query = query.eq("Organization", org_filter)

        result = query.execute()
        users = result.data or []

        stats: dict = {
            "total_users": 0,
            "administrators": 0,
            "leaders": 0,
            "leaders_at_12": 0,
            "registrants": 0,
            "regular_users": 0,
            "custom_roles": {},
        }

        all_roles: set = set()
        for u in users:
            role = u.get("role") or "unknown"
            all_roles.add(role)
            stats["total_users"] += 1
            if role == "admin":
                stats["administrators"] += 1
            elif role == "leader":
                stats["leaders"] += 1
            elif role == "leaderAt12":
                stats["leaders_at_12"] += 1
            elif role == "registrant":
                stats["registrants"] += 1
            elif role == "user":
                stats["regular_users"] += 1
            else:
                stats["custom_roles"][role] = stats["custom_roles"].get(role, 0) + 1

        stats["all_roles"] = sorted(list(all_roles))
        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")


@router.get("/roles/distinct")
async def get_distinct_roles(
    organization: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Get distinct roles with counts - Admin only."""
    try:
        is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL

        org_filter = None
        if is_supreme and organization:
            org_filter = organization
        elif not is_supreme:
            org_filter = get_org_name(current_user)
        else:
            raise HTTPException(status_code=400, detail="Organization required")

        ACTIVE_CHURCH_NAME = "Active Church"
        system_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]

        query = supabase.table("Users").select("role")
        if org_filter:
            query = query.eq("Organization", org_filter)

        result = query.execute()
        users = result.data or []

        role_counts: dict[str, int] = {}
        for u in users:
            r = u.get("role")
            if r:
                role_counts[r] = role_counts.get(r, 0) + 1

        roles_with_counts = []
        for role, count in sorted(role_counts.items()):
            is_system = role in system_roles
            if org_filter == ACTIVE_CHURCH_NAME and not is_system:
                continue

            roles_with_counts.append(
                {
                    "name": role,
                    "count": count,
                    "is_system": is_system,
                    "color": get_role_color(role),
                    "can_create_custom": org_filter != ACTIVE_CHURCH_NAME,
                }
            )

        roles_with_counts.sort(key=lambda x: (not x["is_system"], x["name"]))

        return {
            "roles": roles_with_counts,
            "organization": org_filter,
            "can_create_custom_roles": org_filter != ACTIVE_CHURCH_NAME,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/users/{user_id}/role", response_model=MessageResponse)
async def update_user_role(
    user_id: str,
    role_update: RoleUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Update a user's role - Admin only."""
    try:
        is_supreme = (
            current_user.get("email") == SUPREME_ADMIN_EMAIL
            or current_user.get("is_supreme_admin", False)
        )

        if not is_supreme and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        user_res = (
            supabase.table("Users")
            .select("_id, name, surname, role, Organization")
            .eq("_id", user_id)
            .limit(1)
            .execute()
        )
        if not user_res.data:
            raise HTTPException(status_code=404, detail="User not found")

        user = user_res.data[0]

        if not is_supreme:
            if user.get("Organization") != get_org_name(current_user):
                raise HTTPException(status_code=403, detail="Cannot access users from other organizations")

        old_role = user.get("role", "user")
        new_role = role_update.role
        user_org = user.get("Organization", "")

        ACTIVE_CHURCH_NAME = "Active Church"
        system_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]

        if user_org == ACTIVE_CHURCH_NAME:
            if new_role not in system_roles:
                raise HTTPException(
                    status_code=400,
                    detail=f"Active Church only supports standard roles: {', '.join(system_roles)}",
                )
        else:
            if new_role == "admin" and not is_supreme:
                raise HTTPException(status_code=403, detail="Cannot assign admin role")

        supabase.table("Users").update(
            {"role": new_role, "updated_at": datetime.utcnow().isoformat()}
        ).eq("_id", user_id).execute()

        await log_activity(
            user_id=get_user_id(current_user),
            action="ROLE_UPDATED",
            details=f"Updated {user.get('name')} {user.get('surname')}'s role from {old_role} to {new_role}",
        )

        return MessageResponse(message=f"User role updated to {new_role}")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating role: {str(e)}")


@router.delete("/users/{user_id}", response_model=MessageResponse)
async def delete_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a user - Admin only."""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        user_res = (
            supabase.table("Users")
            .select("_id, name, surname")
            .eq("_id", user_id)
            .limit(1)
            .execute()
        )
        if not user_res.data:
            raise HTTPException(status_code=404, detail="User not found")

        user = user_res.data[0]

        if user["_id"] == get_user_id(current_user):
            raise HTTPException(status_code=400, detail="Cannot delete your own account")

        user_name = f"{user.get('name')} {user.get('surname')}"

        supabase.table("Users").delete().eq("_id", user_id).execute()

        await log_activity(
            user_id=get_user_id(current_user),
            action="USER_DELETED",
            details=f"Deleted user: {user_name}",
        )

        return MessageResponse(message=f"User {user_name} deleted successfully")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting user: {str(e)}")


# ===========================================================================
# ROLE PERMISSIONS  (in-memory store)
# ===========================================================================


@router.put("/roles/{role_name}/permissions", response_model=MessageResponse)
async def update_role_permissions(
    role_name: str,
    permission_update: PermissionUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Update role permissions - Admin only."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        if role_name not in ROLE_PERMISSIONS:
            raise HTTPException(status_code=400, detail="Invalid role")

        ROLE_PERMISSIONS[role_name][permission_update.permission] = permission_update.enabled

        await log_activity(
            user_id=get_user_id(current_user),
            action="PERMISSION_UPDATED",
            details=f"Updated {permission_update.permission} for {role_name} to {permission_update.enabled}",
        )

        return MessageResponse(
            message=f"Permission {permission_update.permission} updated for role {role_name}"
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating permissions: {str(e)}")


@router.get("/roles/{role_name}/permissions")
async def get_role_permissions(role_name: str, current_user: dict = Depends(get_current_user)):
    """Get role permissions - Admin only."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    if role_name not in ROLE_PERMISSIONS:
        raise HTTPException(status_code=400, detail="Invalid role")

    return {"role": role_name, "permissions": ROLE_PERMISSIONS[role_name]}


# ===========================================================================
# ACTIVITY LOGS
# ===========================================================================


@router.get("/activity-logs")
async def get_activity_logs(
    limit: int = 50, current_user: dict = Depends(get_current_user)
):
    """Get activity logs - Admin only."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        result = (
            supabase.table("Activity Logs")
            .select("_id, action, details, timestamp, user_id")
            .order("timestamp", desc=True)
            .limit(limit)
            .execute()
        )

        logs = [
            {
                "id": log["_id"],
                "action": log.get("action"),
                "details": log.get("details"),
                "timestamp": log.get("timestamp"),
                "user_id": log.get("user_id"),
            }
            for log in (result.data or [])
        ]

        return {"logs": logs}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")


# ===========================================================================
# ORGANIZATIONS
# ===========================================================================


@router.get("/organizations", response_model=OrganizationList)
async def get_all_organizations(current_user: dict = Depends(get_current_user)):
    """Get all organizations - Supreme Admin only."""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")

    try:
        orgs_res = supabase.table("Organizations").select("*").execute()
        organizations = []

        for org in orgs_res.data or []:
            user_count_res = (
                supabase.table("Users")
                .select("_id", count="exact")
                .eq("Organization", org["name"])
                .execute()
            )
            people_count_res = (
                supabase.table("People")
                .select("_id", count="exact")
                .eq("Organization", org["name"])
                .execute()
            )
            user_count = user_count_res.count or 0
            people_count = people_count_res.count or 0

            organizations.append(
                OrganizationResponse(
                    id=str(org["_id"]),
                    name=org.get("name", ""),
                    # Organizations table has 'description', not address/phone/email
                    address=org.get("description"),
                    phone=None,
                    email=None,
                    user_count=user_count + people_count,
                    created_at=org.get("created_at"),
                )
            )

        return OrganizationList(organizations=organizations)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching organizations: {str(e)}")


@router.post("/organizations", response_model=MessageResponse)
async def create_organization(
    org_data: OrganizationCreate, current_user: dict = Depends(get_current_user)
):
    """Create a new organization - Supreme Admin only."""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")

    try:
        existing = (
            supabase.table("Organizations")
            .select("_id")
            .eq("name", org_data.name)
            .limit(1)
            .execute()
        )
        if existing.data:
            raise HTTPException(status_code=400, detail="Organization already exists")

        # Organizations table has: name, description, created_at, updated_at
        org_doc = {
            "name": org_data.name,
            # Collapse address/phone/email into description since the live schema
            # only has a 'description' text column (not separate address/phone/email).
            "description": " | ".join(
                filter(None, [org_data.address, org_data.phone, org_data.email])
            ) or None,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        supabase.table("Organizations").insert(org_doc).execute()
        await invalidate_organizations_cache()

        await log_activity(
            user_id=get_user_id(current_user),
            action="ORGANIZATION_CREATED",
            details=f"Created new organization: {org_data.name}",
        )

        return MessageResponse(message=f"Organization {org_data.name} created successfully")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating organization: {str(e)}")


@router.put("/organizations/{org_id}", response_model=MessageResponse)
async def update_organization(
    org_id: str,
    org_data: OrganizationUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Update organization - Supreme Admin only."""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")

    try:
        org_res = (
            supabase.table("Organizations")
            .select("_id, name")
            .eq("_id", org_id)
            .limit(1)
            .execute()
        )
        if not org_res.data:
            raise HTTPException(status_code=404, detail="Organization not found")

        org = org_res.data[0]

        # Build update dict; collapse address/phone/email into description
        update_data: dict = {"updated_at": datetime.utcnow().isoformat()}
        if org_data.name is not None:
            update_data["name"] = org_data.name
        description_parts = list(filter(None, [org_data.address, org_data.phone, org_data.email]))
        if description_parts:
            update_data["description"] = " | ".join(description_parts)

        supabase.table("Organizations").update(update_data).eq("_id", org_id).execute()
        await invalidate_organizations_cache()

        await log_activity(
            user_id=get_user_id(current_user),
            action="ORGANIZATION_UPDATED",
            details=f"Updated organization: {org['name']}",
        )

        return MessageResponse(message="Organization updated successfully")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating organization: {str(e)}")


@router.delete("/organizations/{org_id}", response_model=MessageResponse)
async def delete_organization(org_id: str, current_user: dict = Depends(get_current_user)):
    """Delete organization - Supreme Admin only."""
    if current_user.get("email") != SUPREME_ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Supreme admin access required")

    try:
        org_res = (
            supabase.table("Organizations")
            .select("_id, name")
            .eq("_id", org_id)
            .limit(1)
            .execute()
        )
        if not org_res.data:
            raise HTTPException(status_code=404, detail="Organization not found")

        org = org_res.data[0]

        user_count_res = (
            supabase.table("Users")
            .select("_id", count="exact")
            .eq("Organization", org["name"])
            .execute()
        )
        people_count_res = (
            supabase.table("People")
            .select("_id", count="exact")
            .eq("Organization", org["name"])
            .execute()
        )
        total_members = (user_count_res.count or 0) + (people_count_res.count or 0)

        if total_members > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete organization with {total_members} members. Reassign them first.",
            )

        supabase.table("Organizations").delete().eq("_id", org_id).execute()
        await invalidate_organizations_cache()

        await log_activity(
            user_id=get_user_id(current_user),
            action="ORGANIZATION_DELETED",
            details=f"Deleted organization: {org['name']}",
        )

        return MessageResponse(message="Organization deleted successfully")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting organization: {str(e)}")


# ===========================================================================
# PEOPLE
# ===========================================================================


@router.get("/people", response_model=PeopleList)
async def get_all_people(
    organization: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Get all people - filtered by organization. Admin only."""
    is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL

    if not is_supreme and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        # People table uses PascalCase column names
        query = supabase.table("People").select(
            "_id, Name, Surname, Email, Number, InvitedBy, Organization, LeaderId, \"Date Created\""
        )

        if not is_supreme:
            query = query.eq("Organization", get_org_name(current_user))
        elif organization:
            query = query.eq("Organization", organization)

        result = query.execute()

        people = [
            PeopleResponse(
                id=str(p["_id"]),
                name=p.get("Name", ""),
                surname=p.get("Surname", ""),
                email=p.get("Email", ""),
                phone=p.get("Number", ""),
                invitedBy=p.get("InvitedBy", ""),
                organisation=p.get("Organization", ""),
                leaderId=p.get("LeaderId", ""),
                created_at=p.get("Date Created"),
            )
            for p in (result.data or [])
        ]

        return PeopleList(people=people)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching people: {str(e)}")


# ===========================================================================
# DETECT HIERARCHY
# ===========================================================================


@router.post("/detect-hierarchy")
async def detect_hierarchy_from_people(current_user: dict = Depends(get_current_user)):
    """Detect hierarchy fields from People table sample."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    try:
        sample_res = supabase.table("People").select("*").limit(10).execute()
        sample_people = sample_res.data or []

        if not sample_people:
            return {"detected_hierarchy": [], "message": "No people data found"}

        all_fields: set = set()
        for person in sample_people:
            all_fields.update(person.keys())

        hierarchy_keywords = [
            "leader", "pastor", "zone", "district",
            "region", "overseer", "bishop", "elder",
            "shepherd", "mentor", "coach",
        ]

        skip_fields = {
            "name", "surname", "email", "phone", "gender",
            "created_at", "updated_at", "role", "id", "_id",
            "Name", "Surname", "Email", "Number", "Gender",
        }

        hierarchy_fields = []
        for field in all_fields:
            if field.startswith("_") or field in skip_fields:
                continue
            field_lower = field.lower()
            if any(kw in field_lower for kw in hierarchy_keywords):
                num_match = re.search(r"\d+", field)
                level_num = int(num_match.group()) if num_match else 999
                hierarchy_fields.append({"field": field, "label": field, "level_num": level_num})

        hierarchy_fields.sort(key=lambda x: x["level_num"])
        detected = [
            {"level": i + 1, "field": hf["field"], "label": hf["field"]}
            for i, hf in enumerate(hierarchy_fields)
        ]

        return {
            "detected_hierarchy": detected,
            "message": f"Detected {len(detected)} hierarchy levels",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================================================
# EVENT TYPE MANAGEMENT
# (mounted with NO prefix via event_type_router)
# ===========================================================================

RESERVED_EVENT_TYPE_PATTERN = re.compile(r"\bcell[s]?\b", re.IGNORECASE)


@event_type_router.get("/event-types")
async def get_event_types(current_user: dict = Depends(get_current_user)):
    """
    List event types visible to the current user's organization.
    'CELLS' is a built-in synthetic type for the active-teams org.
    event_types table uses: event_type_id (PK), name, description,
    is_ticketed, is_global, has_person_steps, org_id, created_at.
    """
    try:
        org_id = (
            current_user.get("org_id")
            or (current_user.get("organization", "") or "").lower().replace(" ", "-")
            or DEFAULT_ORG_ID
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

        event_types = []
        if org_id in ("active-teams", DEFAULT_ORG_ID):
            event_types.append(
                {
                    "id": "CELLS_BUILT_IN",
                    "name": "CELLS",
                    "is_built_in": True,
                    "is_event_type": True,
                    "is_global": False,
                    "org_id": org_id,
                }
            )

        result = (
            supabase.table("event_types")
            .select("event_type_id, name, description, is_ticketed, is_global, has_person_steps, org_id, created_at, uuid_ref")
            .eq("org_id", org_id)
            .order("created_at")
            .execute()
        )
        for et in result.data or []:
            if (et.get("name") or "").upper() == "CELLS":
                continue
            # Normalise PK to "id" for frontend compatibility
            et["id"] = et.pop("event_type_id", et.get("id"))
            event_types.append(et)

        # Also surface event types stored under the org's display name (legacy)
        organization = get_org_name(current_user)
        if organization:
            org_result = (
                supabase.table("event_types")
                .select("event_type_id, name, description, is_ticketed, is_global, has_person_steps, org_id, created_at, uuid_ref")
                .ilike("org_id", organization.lower().replace(" ", "-"))
                .execute()
            )
            existing_ids = {et.get("id") for et in event_types}
            for et in org_result.data or []:
                et["id"] = et.pop("event_type_id", et.get("id"))
                if et.get("id") not in existing_ids and (et.get("name") or "").upper() != "CELLS":
                    event_types.append(et)

        return event_types

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@event_type_router.post("/event-types")
async def create_event_type(
    event_type: EventTypeCreate, current_user: dict = Depends(get_current_user)
):
    """Create a new event type for the current user's organization."""
    try:
        if not event_type.name or not event_type.description:
            raise HTTPException(status_code=400, detail="Name and description are required.")

        name = event_type.name.strip().title()
        name_lower = name.lower()

        if RESERVED_EVENT_TYPE_PATTERN.search(name_lower) or "cell" in name_lower:
            raise HTTPException(
                status_code=400,
                detail="Event types containing 'cell' or 'cells' are reserved and cannot be created.",
            )

        org_id = (
            current_user.get("org_id")
            or (current_user.get("organization", "") or "").lower().replace(" ", "-")
            or DEFAULT_ORG_ID
        )
        org_id = ORG_ID_MAP.get(org_id.lower(), org_id)

        existing = (
            supabase.table("event_types")
            .select("event_type_id")
            .ilike("name", name)
            .eq("org_id", org_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            raise HTTPException(status_code=400, detail=f"Event type '{name}' already exists")

        is_global = event_type.isGlobal if event_type.isGlobal is not None else ("global" in name_lower)
        has_person_steps = (
            event_type.hasPersonSteps
            if event_type.hasPersonSteps is not None
            else any(keyword in name_lower for keyword in ["person", "individual"])
        )

        event_type_doc = {
            "name": name,
            "description": event_type.description.strip(),
            "is_ticketed": bool(getattr(event_type, "isTicketed", False)),
            "is_global": bool(is_global),
            "has_person_steps": bool(has_person_steps),
            "org_id": org_id,
            "uuid_ref": str(uuid.uuid4()),
            "created_at": datetime.utcnow().isoformat(),
        }

        result = supabase.table("event_types").insert(event_type_doc).execute()
        inserted = result.data[0] if result.data else event_type_doc

        # Normalise PK for frontend
        if "event_type_id" in inserted:
            inserted["id"] = inserted.pop("event_type_id")

        await log_activity(
            user_id=get_user_id(current_user),
            action="EVENT_TYPE_CREATED",
            details=f"Created event type: {name} for org: {org_id}",
        )

        return inserted

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating event type: {str(e)}")


@event_type_router.put("/event-types/{event_type_name}")
async def update_event_type(event_type_name: str, updated_data: EventTypeCreate = Body(...)):
    """
    Update an event type by name (or event_type_id).
    Cascades name changes to events.event_type_name.
    """
    try:
        decoded_name = unquote(event_type_name)

        existing_res = (
            supabase.table("event_types")
            .select("*")
            .ilike("name", decoded_name)
            .limit(1)
            .execute()
        )
        existing_event_type = existing_res.data[0] if existing_res.data else None

        if not existing_event_type:
            by_id_res = (
                supabase.table("event_types")
                .select("*")
                .eq("event_type_id", decoded_name)
                .limit(1)
                .execute()
            )
            existing_event_type = by_id_res.data[0] if by_id_res.data else None

        if not existing_event_type:
            raise HTTPException(status_code=404, detail=f"Event type '{decoded_name}' not found")

        pk = existing_event_type["event_type_id"]
        new_name = updated_data.name.strip().title()
        current_name = existing_event_type["name"]
        name_changed = new_name.lower() != current_name.lower()

        current_is_global = existing_event_type.get("is_global", False)
        new_is_global = updated_data.isGlobal if updated_data.isGlobal is not None else False
        is_global_changed = current_is_global != new_is_global

        if name_changed:
            dup = (
                supabase.table("event_types")
                .select("event_type_id")
                .ilike("name", new_name)
                .neq("event_type_id", pk)
                .limit(1)
                .execute()
            )
            if dup.data:
                raise HTTPException(status_code=400, detail="Event type with this name already exists")

        # Cascade name/global change to events.event_type_name
        events_updated_count = 0
        if name_changed or is_global_changed:
            event_update_fields: dict = {"updated_at": datetime.utcnow().isoformat()}
            if name_changed:
                event_update_fields["event_type_name"] = new_name

            if is_global_changed:
                affected_res = (
                    supabase.table("events")
                    .select("event_id, is_global")
                    .ilike("event_type_name", current_name)
                    .execute()
                )
                affected_events = affected_res.data or []
                to_cascade = [
                    e for e in affected_events
                    if e.get("is_global") in (None, "", current_is_global)
                ]
                events_updated_count = len(to_cascade)
                if events_updated_count > 0:
                    event_update_fields["is_global"] = new_is_global

            if name_changed or (is_global_changed and events_updated_count > 0):
                supabase.table("events").update(event_update_fields).ilike(
                    "event_type_name", current_name
                ).execute()

        update_payload = {
            "name": new_name,
            "description": (updated_data.description or existing_event_type.get("description", "")).strip(),
            "is_ticketed": getattr(updated_data, "isTicketed", existing_event_type.get("is_ticketed", False)),
            "is_global": new_is_global,
            "has_person_steps": getattr(
                updated_data, "hasPersonSteps", existing_event_type.get("has_person_steps", False)
            ),
        }
        update_payload = {k: v for k, v in update_payload.items() if v is not None}

        supabase.table("event_types").update(update_payload).eq("event_type_id", pk).execute()

        refreshed = (
            supabase.table("event_types").select("*").eq("event_type_id", pk).limit(1).execute()
        )
        row = refreshed.data[0] if refreshed.data else existing_event_type
        if "event_type_id" in row:
            row["id"] = row.pop("event_type_id")
        return row

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating event type: {str(e)}")


@event_type_router.delete("/event-types/{event_type_name}")
async def delete_event_type(
    event_type_name: str,
    force: bool = Query(False, description="Force delete even if events exist"),
):
    """Delete an event type. Refuses if events still reference it unless force=true."""
    try:
        decoded_name = unquote(event_type_name)

        existing_res = (
            supabase.table("event_types")
            .select("*")
            .ilike("name", decoded_name)
            .limit(1)
            .execute()
        )
        existing_event_type = existing_res.data[0] if existing_res.data else None
        if not existing_event_type:
            raise HTTPException(status_code=404, detail=f"Event type '{decoded_name}' not found")

        actual_name = existing_event_type["name"]
        pk = existing_event_type["event_type_id"]

        if "cell" in actual_name.lower():
            raise HTTPException(
                status_code=400,
                detail=f"'{actual_name}' is a reserved built-in event type and cannot be modified or deleted.",
            )

        # Check events referencing this type via event_type_name or FK
        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_date, event_leader, status")
            .ilike("event_type_name", actual_name)
            .execute()
        )
        # Also check FK references
        events_fk_res = (
            supabase.table("events")
            .select("event_id, event_name, event_date, event_leader, status")
            .eq("event_type_id", pk)
            .execute()
        )

        all_event_ids = {e["event_id"] for e in (events_res.data or [])}
        all_event_ids.update(e["event_id"] for e in (events_fk_res.data or []))
        events_using_type = [
            e for e in (events_res.data or []) + (events_fk_res.data or [])
            if e["event_id"] in all_event_ids
        ]
        # Deduplicate
        seen_ids: set = set()
        unique_events = []
        for e in events_using_type:
            if e["event_id"] not in seen_ids:
                seen_ids.add(e["event_id"])
                unique_events.append(e)

        events_count = len(unique_events)

        if events_count > 0:
            event_samples = [
                {
                    "id": e["event_id"],
                    "name": e.get("event_name", "Unnamed"),
                    "date": str(e.get("event_date", "")),
                    "leader": e.get("event_leader", ""),
                    "status": e.get("status", "unknown"),
                }
                for e in unique_events[:20]
            ]
            if not force:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": f"Cannot delete event type '{actual_name}': {events_count} event(s) are using it.",
                        "events_count": events_count,
                        "event_samples": event_samples,
                        "suggestion": "Please delete these events first, or use force=true to delete everything",
                    },
                )
            for e in unique_events:
                supabase.table("events").delete().eq("event_id", e["event_id"]).execute()

        supabase.table("event_types").delete().eq("event_type_id", pk).execute()

        return {
            "success": True,
            "message": f"Event type '{actual_name}' deleted successfully",
            "events_deleted": events_count if force else 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting event type: {str(e)}")


@event_type_router.get("/diagnostic/event-type-usage/{event_type_name}")
async def check_event_type_usage(
    event_type_name: str, current_user: dict = Depends(get_current_user)
):
    """Diagnostic: list every event currently tagged with a given event type. Admin only."""
    if (current_user.get("role") or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        decoded_name = unquote(event_type_name)

        event_type_res = (
            supabase.table("event_types")
            .select("event_type_id, name, description, org_id")
            .ilike("name", decoded_name)
            .limit(1)
            .execute()
        )
        if not event_type_res.data:
            return {
                "event_type_exists": False,
                "message": f"Event type '{decoded_name}' not found",
                "events_using_it": [],
            }

        event_type_doc = event_type_res.data[0]
        actual_name = event_type_doc["name"]
        pk = event_type_doc["event_type_id"]

        events_res = (
            supabase.table("events")
            .select("event_id, event_name, event_date, event_leader, event_leader_email, status, total_attendance")
            .ilike("event_type_name", actual_name)
            .execute()
        )
        events_fk_res = (
            supabase.table("events")
            .select("event_id, event_name, event_date, event_leader, event_leader_email, status, total_attendance")
            .eq("event_type_id", pk)
            .execute()
        )

        seen_ids: set = set()
        all_events = []
        for e in (events_res.data or []) + (events_fk_res.data or []):
            if e["event_id"] not in seen_ids:
                seen_ids.add(e["event_id"])
                all_events.append(e)

        event_details = [
            {
                "id": e["event_id"],
                "event_name": e.get("event_name"),
                "event_type": actual_name,
                "date": str(e.get("event_date", "")),
                "leader_name": e.get("event_leader"),
                "leader_email": e.get("event_leader_email"),
                "status": e.get("status"),
                "attendees_count": e.get("total_attendance", 0),
            }
            for e in all_events
        ]

        return {
            "event_type_exists": True,
            "event_type_name": actual_name,
            "event_type_id": pk,
            "events_count": len(all_events),
            "events": event_details,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Diagnostic error: {str(e)}")


# ===========================================================================
# TASK TYPE MANAGEMENT
# (mounted with NO prefix via task_type_router)
# Table name: "Task Types"  |  PK: _id  |  columns: name, Organization, org_id
# ===========================================================================


@task_type_router.get("/tasktypes", response_model=None)
async def get_task_types(current_user: dict = Depends(get_current_user)):
    """List task types for the current user's organization (or all, for super_admin)."""
    try:
        org_name = get_org_name(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        is_super_admin = current_user.get("role") == "super_admin"

        query = supabase.table("Task Types").select("_id, name, Organization, org_id").order("name")
        if not is_super_admin:
            query = query.eq("Organization", org_name)

        result = query.execute()

        # Normalise _id → id for frontend
        rows = []
        for r in (result.data or []):
            rows.append(
                {
                    "id": r["_id"],
                    "name": r.get("name"),
                    "organization": r.get("Organization"),
                    "org_id": r.get("org_id"),
                }
            )
        return rows

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@task_type_router.post("/tasktypes", response_model=None)
async def create_task_type(task: TaskTypeIn, current_user: dict = Depends(get_current_user)):
    """Create a task type scoped to the current user's organization. Admin only."""
    try:
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can create task types.")

        org_name = get_org_name(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        existing = (
            supabase.table("Task Types")
            .select("_id")
            .eq("name", task.name)
            .eq("Organization", org_name)
            .limit(1)
            .execute()
        )
        if existing.data:
            raise HTTPException(status_code=400, detail="Task type already exists in this organization.")

        new_task = {
            "name": task.name,
            "Organization": org_name,
            "org_id": (
                current_user.get("org_id")
                or org_name.lower().replace(" ", "-")
            ),
        }
        result = supabase.table("Task Types").insert(new_task).execute()

        row = result.data[0] if result.data else new_task
        if "_id" in row:
            row["id"] = row.pop("_id")
        if "Organization" in row:
            row["organization"] = row.pop("Organization")
        return row

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@task_type_router.put("/tasktypes/{tasktype_id}")
async def update_task_type(
    tasktype_id: str,
    update_data: TaskTypeUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Rename a task type. Admin only; cross-organization edits are blocked."""
    try:
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can edit task types.")

        org_name = get_org_name(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        existing_res = (
            supabase.table("Task Types")
            .select("*")
            .eq("_id", tasktype_id)
            .limit(1)
            .execute()
        )
        if not existing_res.data:
            raise HTTPException(status_code=404, detail="Task type not found")
        existing = existing_res.data[0]

        if existing.get("Organization") != org_name and current_user.get("role") != "super_admin":
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        supabase.table("Task Types").update({"name": update_data.name.strip()}).eq(
            "_id", tasktype_id
        ).execute()

        updated_res = (
            supabase.table("Task Types").select("*").eq("_id", tasktype_id).limit(1).execute()
        )
        if not updated_res.data:
            raise HTTPException(status_code=404, detail="Task type not found after update")

        row = updated_res.data[0]
        row["id"] = row.pop("_id", tasktype_id)
        row["organization"] = row.pop("Organization", org_name)

        return {"message": "Task type updated", "taskType": row}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@task_type_router.delete("/tasktypes/{tasktype_id}")
async def delete_task_type(tasktype_id: str, current_user: dict = Depends(get_current_user)):
    """Delete a task type. Admin only; cross-organization deletes are blocked."""
    try:
        if current_user.get("role") not in ["super_admin", "org_admin", "admin"]:
            raise HTTPException(status_code=403, detail="Only admins can delete task types.")

        org_name = get_org_name(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        existing_res = (
            supabase.table("Task Types")
            .select("_id, Organization")
            .eq("_id", tasktype_id)
            .limit(1)
            .execute()
        )
        if not existing_res.data:
            raise HTTPException(status_code=404, detail="Task type not found")
        existing = existing_res.data[0]

        if existing.get("Organization") != org_name and current_user.get("role") != "super_admin":
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        supabase.table("Task Types").delete().eq("_id", tasktype_id).execute()
        return {"message": "Task type deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ===========================================================================
# BACKGROUND JOBS
# ===========================================================================


async def auto_reactivate_expired_events():
    """
    Daily cron: reactivate events whose temporary deactivation window has lapsed.
    events table uses: event_id (PK), is_active, deactivation_end.
    """
    try:
        current_time = datetime.utcnow().isoformat()

        expired_res = (
            supabase.table("events")
            .select("event_id")
            .eq("is_active", False)
            .lte("deactivation_end", current_time)
            .not_.is_("deactivation_end", "null")
            .execute()
        )
        expired_events = expired_res.data or []

        updates = {
            "is_active": True,
            "deactivation_end": None,
            "deactivation_start": None,
            "deactivation_reason": None,
            "updated_at": datetime.utcnow().isoformat(),
        }

        reactivated = 0
        for event in expired_events:
            supabase.table("events").update(updates).eq("event_id", event["event_id"]).execute()
            reactivated += 1

        if reactivated > 0:
            print(f"Auto-reactivated {reactivated} events")

    except Exception as e:
        print(f"Auto-reactivation error: {e}")