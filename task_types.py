import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException
from auth.models import TaskTypeIn, TaskTypeUpdate
from auth.utils import get_current_user
from supabase_helpers.supabase_client import supabase

logger = logging.getLogger("task_types")

router = APIRouter(tags=["Task Types"])

ADMIN_ROLES = {"super_admin", "org_admin", "admin"}

# Helpers
def _org_name_from_user(current_user: dict) -> str:
    for key in current_user.keys():
        if key.lower() == "organization":
            return current_user[key] or ""
    return ""


def _org_id_from_user(current_user: dict) -> str:
    return current_user.get("org_id") or current_user.get("orgId") or ""


def _new_id() -> str:
    return str(uuid.uuid4())


def _format_row(row: dict) -> dict:
    return {
        "_id":          row.get("_id", ""),
        "id":           row.get("_id", ""),        # alias for any frontend that uses .id
        "name":         row.get("name", ""),
        "Organization": row.get("Organization", ""),
        "org_id":       row.get("org_id", ""),
    }


# GET /tasktypes
@router.get("/tasktypes")
async def get_task_types(current_user: dict = Depends(get_current_user)):
    try:
        org_name       = _org_name_from_user(current_user)
        is_super_admin = current_user.get("role") == "super_admin"

        if not org_name and not is_super_admin:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        query = supabase.table("Task Types").select("*").order("name")
        if not is_super_admin:
            query = query.eq('"Organization"', org_name)

        result = query.execute()
        return [_format_row(r) for r in (result.data or [])]

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_task_types error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# POST /tasktypes
@router.post("/tasktypes", status_code=201)
async def create_task_type(
    task:         TaskTypeIn,
    current_user: dict = Depends(get_current_user),
):
    try:
        if current_user.get("role") not in ADMIN_ROLES:
            raise HTTPException(status_code=403, detail="Only admins can create task types.")

        org_name = _org_name_from_user(current_user)
        org_id   = _org_id_from_user(current_user)

        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        # Duplicate check
        existing = (
            supabase.table("Task Types")
            .select("_id")
            .eq("name", task.name.strip())
            .eq('"Organization"', org_name)
            .execute()
        )
        if existing.data:
            raise HTTPException(status_code=400, detail="Task type already exists in this organization.")

        row = {
            "_id":          _new_id(),
            "name":         task.name.strip(),
            "Organization": org_name,
            "org_id":       org_id,
        }

        result = supabase.table("Task Types").insert(row).execute()
        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create task type")

        return _format_row(result.data[0])

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"create_task_type error: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))


# PUT /tasktypes/{tasktype_id}
@router.put("/tasktypes/{tasktype_id}")
async def update_task_type(
    tasktype_id:  str,
    update_data:  TaskTypeUpdate,
    current_user: dict = Depends(get_current_user),
):
    try:
        if current_user.get("role") not in ADMIN_ROLES:
            raise HTTPException(status_code=403, detail="Only admins can edit task types.")

        org_name = _org_name_from_user(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        existing = (
            supabase.table("Task Types")
            .select("*")
            .eq("_id", tasktype_id)
            .execute()
        )
        if not existing.data:
            raise HTTPException(status_code=404, detail="Task type not found")

        row = existing.data[0]

        # Cross-tenant guard
        if (
            row.get("Organization") != org_name
            and current_user.get("role") != "super_admin"
        ):
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        result = (
            supabase.table("Task Types")
            .update({"name": update_data.name.strip()})
            .eq("_id", tasktype_id)
            .execute()
        )
        if not result.data:
            raise HTTPException(status_code=500, detail="Update failed")

        return {
            "message":  "Task type updated",
            "taskType": _format_row(result.data[0]),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"update_task_type error: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))


# DELETE /tasktypes/{tasktype_id}
@router.delete("/tasktypes/{tasktype_id}")
async def delete_task_type(
    tasktype_id:  str,
    current_user: dict = Depends(get_current_user),
):
    try:
        if current_user.get("role") not in ADMIN_ROLES:
            raise HTTPException(status_code=403, detail="Only admins can delete task types.")

        org_name = _org_name_from_user(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        existing = (
            supabase.table("Task Types")
            .select("*")
            .eq("_id", tasktype_id)
            .execute()
        )
        if not existing.data:
            raise HTTPException(status_code=404, detail="Task type not found")

        row = existing.data[0]

        if (
            row.get("Organization") != org_name
            and current_user.get("role") != "super_admin"
        ):
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        supabase.table("Task Types").delete().eq("_id", tasktype_id).execute()
        return {"message": "Task type deleted successfully", "_id": tasktype_id}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"delete_task_type error: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(exc))