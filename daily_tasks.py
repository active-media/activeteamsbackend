import logging
import uuid
from datetime import datetime
from typing import Optional, List

import pytz
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from auth.models import TaskModel
from auth.utils import get_current_user
from database import users_collection          # still needed for userId lookups
from supabase_helpers.supabase_client import supabase

logger = logging.getLogger("daily_tasks")

router = APIRouter(tags=["Daily Tasks"])

# Constants
SAST = pytz.timezone("Africa/Johannesburg")
EXCLUDED_TASK_TYPES_FROM_COMPLETED = ["no answer", "Awaiting Call"]

TABLE = 'Tasks' 

# Helper functions
def _org_name_from_user(current_user: dict) -> str:
    for key in current_user.keys():
        if key.lower() == "organization":
            return current_user[key] or ""
    return ""


def _org_id_from_user(current_user: dict) -> str:
    return current_user.get("org_id") or current_user.get("orgId") or ""


def _parse_followup_date(raw) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.isoformat()
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
        except ValueError:
            return None
    if isinstance(raw, dict) and "$date" in raw:
        try:
            return datetime.fromisoformat(
                str(raw["$date"]).replace("Z", "+00:00")
            ).isoformat()
        except ValueError:
            return None
    return None


def _localize_iso(iso_str: Optional[str]) -> Optional[str]:
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        return dt.astimezone(SAST).isoformat()
    except Exception:
        return iso_str


def _format_task_row(row: dict) -> dict:
    """Map a Tasks row to the shape the frontend expects."""
    task_type       = row.get("taskType") or ""
    task_type_lower = task_type.lower()

    is_consolidation = (
        bool(row.get("is_consolidation_task"))
        or task_type_lower == "consolidation"
    )
    is_new_person = task_type_lower in ("service follow up", "new_person", "new person")

    # Reconstruct contacted_person dict from flat columns
    contacted_person = {
        "name":  row.get("contacted_person_name") or "",
        "email": row.get("contacted_person_email") or "",
        "phone": row.get("contacted_person_phone") or "",
    }

    return {
        "_id":                   row.get("_id", ""),
        "memberID":              row.get("memberID", ""),
        "name":                  row.get("name", "Unnamed Task"),
        "taskType":              task_type,
        "description":           row.get("description", ""),
        "followup_date":         _localize_iso(row.get("followup_date")),
        "status":                row.get("status", "Open"),
        "assignedfor":           row.get("assignedfor", ""),
        "assigned_to_email":     row.get("assigned_to_email", ""),
        "assigned_to_user_id":   row.get("assigned_to_user_id", ""),
        "leader_name":           row.get("leader_name", ""),
        "leader_assigned":       row.get("leader_assigned", ""),
        "type":                  row.get("type", "call"),
        "priority":              row.get("priority", ""),
        "consolidation_id":      row.get("consolidation_id", ""),
        "person_id":             row.get("person_id", ""),
        "person_name":           row.get("person_name", ""),
        "person_surname":        row.get("person_surname", ""),
        "decision_type":         row.get("decision_type", ""),
        "decision_display_name": row.get("decision_display_name", ""),
        "consolidation_source":  row.get("consolidation_source", "manual"),
        "source_display":        row.get("source_display", "Manual"),
        "contacted_person":      contacted_person,
        "is_consolidation_task": is_consolidation,
        "is_new_person_task":    is_new_person,
        "completedAt":           row.get("completedAt") or "",
        "created_at":            row.get("created_at") or "",
        "createdAt":             row.get("created_at") or "",
        "created_by":            row.get("created_by") or "",
        "Organization":          row.get("Organization") or "",
        "org_id":                row.get("org_id") or "",
    }


def _new_id() -> str:
    return str(uuid.uuid4())

@router.get("/test-supabase")
async def test_supabase():
    try:
        result = (
            supabase.table("Tasks")
            .select("_id")
            .limit(1)
            .execute()
        )

        return {
            "success": True,
            "rows_returned": len(result.data or []),
            "data": result.data,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# POST /tasks
@router.post("/tasks")
async def create_task(
    task: TaskModel,
    current_user: dict = Depends(get_current_user),
):
    try:
        org_name = _org_name_from_user(current_user)
        org_id   = _org_id_from_user(current_user)
        d        = task.dict()

        assignedfor       = (d.get("assignedfor") or current_user["email"]).lower()
        assigned_to_email = (d.get("assigned_to_email") or assignedfor).lower()

        # Flatten contacted_person
        cp = d.get("contacted_person") or {}
        if isinstance(cp, str):
            cp = {}

        row = {
            "_id":                   _new_id(),
            "memberID":              d.get("memberID") or d.get("member_id") or "",
            "name":                  d.get("name", ""),
            "taskType":              d.get("taskType", ""),
            "description":           d.get("description", ""),
            "followup_date":         _parse_followup_date(d.get("followup_date")),
            "status":                d.get("status", "Open"),
            "assignedfor":           assignedfor,
            "assigned_to_email":     assigned_to_email,
            "assigned_to_user_id":   d.get("assigned_to_user_id", ""),
            "leader_name":           d.get("leader_name", ""),
            "leader_assigned":       d.get("leader_assigned", ""),
            "type":                  d.get("type", "call"),
            "priority":              d.get("priority", ""),
            "consolidation_id":      d.get("consolidation_id", ""),
            "person_id":             d.get("person_id", ""),
            "person_name":           d.get("person_name", ""),
            "person_surname":        d.get("person_surname", ""),
            "decision_type":         d.get("decision_type") or d.get("decision_date", ""),
            "decision_display_name": d.get("decision_display_name", ""),
            "consolidation_source":  d.get("consolidation_source", "manual"),
            "source_display":        d.get("source_display", "Manual"),
            "contacted_person_name": cp.get("name", ""),
            "contacted_person_email": cp.get("email", ""),
            "contacted_person_phone": cp.get("phone", ""),
            "is_consolidation_task": bool(d.get("is_consolidation_task")),
            "created_at":            datetime.utcnow().isoformat(),
            "created_by":            current_user.get("email", "").lower(),
            "Organization":          org_name,
            "org_id":                org_id,
        }

        result = supabase.table("Tasks").insert(row).execute()
        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create task")

        return {
            "status": "success",
            "task":   jsonable_encoder(_format_task_row(result.data[0])),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"create_task error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# GET /tasks/my-special-tasks
@router.get("/tasks/my-special-tasks")
async def get_my_special_tasks(current_user: dict = Depends(get_current_user)):
    try:
        org_name   = _org_name_from_user(current_user)
        user_email = current_user.get("email", "").strip().lower()
        user_name  = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()

        if not org_name:
            raise HTTPException(status_code=403, detail="No organization found")

        result = (
            supabase.table("Tasks")
            .select("*")
            .eq('"Organization"', org_name)
            .or_(
                'is_consolidation_task.eq.true,'
                '"taskType".ilike.consolidation,'
                '"taskType".ilike.service follow up,'
                '"taskType".ilike.cell consolidation,'
                'consolidation_source.eq.cell_consolidation,'
                'consolidation_source.eq.service_consolidation'
            )
            .order("followup_date", desc=True)
            .limit(200)
            .execute()
        )

        rows = result.data or []

        def is_mine(row: dict) -> bool:
            return (
                (row.get("assignedfor") or "").lower() == user_email
                or (row.get("assigned_to_email") or "").lower() == user_email
                or (row.get("created_by") or "").lower() == user_email
                or row.get("leader_name") == user_name
                or row.get("leader_assigned") == user_name
            )

        tasks = [_format_task_row(r) for r in rows if is_mine(r)]
        return {"tasks": tasks, "total": len(tasks), "status": "success"}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_my_special_tasks error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# GET /tasks
@router.get("/tasks")
async def get_user_tasks(
    email:             Optional[str] = Query(None),
    assigned_to_email: Optional[str] = Query(None),
    assignedfor:       Optional[str] = Query(None),
    userId:            Optional[str] = Query(None),
    view_all:          bool          = Query(False),
    current_user:      dict          = Depends(get_current_user),
):
    try:
        org_name = _org_name_from_user(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        is_super_admin = current_user.get("role") == "super_admin"
        is_leader      = current_user.get("role") in ["admin", "leader", "manager", "org_admin"]

        # Resolve target email
        if email:
            user_email = email.strip().lower()
        elif assigned_to_email:
            user_email = assigned_to_email.strip().lower()
        elif assignedfor:
            user_email = assignedfor.strip().lower()
        elif userId:
            user_doc   = await users_collection.find_one({"_id": ObjectId(userId)})
            user_email = (user_doc.get("email", "") if user_doc else "").lower()
        else:
            user_email = current_user.get("email", "").lower()

        if not user_email and not (is_leader and view_all):
            return {"error": "User email not found", "status": "failed"}

        user_name = f"{current_user.get('name', '')} {current_user.get('surname', '')}".strip()

        query = supabase.table("Tasks").select("*").order("followup_date", desc=True).limit(500)

        if is_super_admin and view_all:
            pass
        else:
            query = query.eq('"Organization"', org_name)

        rows = (query.execute().data) or []

        if not (is_super_admin and view_all) and not (is_leader and view_all):
            def owned(row: dict) -> bool:
                af  = (row.get("assignedfor") or "").lower()
                ate = (row.get("assigned_to_email") or "").lower()
                cbe = (row.get("created_by") or "").lower()
                ln  = row.get("leader_name", "")
                la  = row.get("leader_assigned", "")
                return (
                    af == user_email
                    or ate == user_email
                    or cbe == user_email
                    or (ln == user_name and row.get("is_consolidation_task"))
                    or (la == user_name and row.get("is_consolidation_task"))
                )
            rows = [r for r in rows if owned(r)]

        tasks = sorted(
            [_format_task_row(r) for r in rows],
            key=lambda t: t["followup_date"] or "",
            reverse=True,
        )

        return {
            "user_email":     "all_users" if (is_leader and view_all) else current_user.get("email"),
            "total_tasks":    len(tasks),
            "tasks":          tasks,
            "status":         "success",
            "is_leader_view": is_leader and view_all,
            "Organization":   org_name,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_user_tasks error: {exc}", exc_info=True)
        return {"error": str(exc), "status": "failed"}


# GET /tasks/all
@router.get("/tasks/all")
async def get_all_tasks(current_user: dict = Depends(get_current_user)):
    try:
        role = current_user.get("role", "").lower()
        if role not in ["admin", "leader", "manager", "super_admin"]:
            raise HTTPException(status_code=403, detail="Access denied.")

        result = (
            supabase.table("Tasks")
            .select("*")
            .order("followup_date", desc=True)
            .execute()
        )
        tasks = [_format_task_row(r) for r in (result.data or [])]
        tasks.sort(key=lambda t: t["followup_date"] or "9999-12-31", reverse=True)

        return {
            "total_tasks": len(tasks),
            "tasks":       tasks,
            "status":      "success",
            "fetched_by":  current_user.get("email"),
            "timestamp":   datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_all_tasks error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# GET /tasks/leader/{leader_email}
@router.get("/tasks/leader/{leader_email}")
async def get_leader_tasks(
    leader_email: str,
    current_user: dict = Depends(get_current_user),
):
    try:
        result = (
            supabase.table("Tasks")
            .select("*")
            .eq("is_consolidation_task", True)
            .or_(
                f"assigned_to_email.ilike.{leader_email},"
                f"assignedfor.ilike.{leader_email},"
                f"leader_assigned.ilike.{leader_email}"
            )
            .execute()
        )
        tasks = [_format_task_row(r) for r in (result.data or [])]
        return {"leader_email": leader_email, "total_tasks": len(tasks), "tasks": tasks}

    except Exception as exc:
        logger.error(f"get_leader_tasks error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# PUT /tasks/{task_id}
@router.put("/tasks/{task_id}")
async def update_task(
    task_id:      str,
    updated_task: dict,
    current_user: dict = Depends(get_current_user),
):
    try:
        org_name = _org_name_from_user(current_user)

        existing = supabase.table("Tasks").select("*").eq("_id", task_id).execute()
        if not existing.data:
            raise HTTPException(status_code=404, detail="Task not found")

        row      = existing.data[0]
        task_org = row.get("Organization")

        if (
            task_org
            and task_org.lower() != org_name.lower()
            and current_user.get("role") != "super_admin"
        ):
            raise HTTPException(status_code=403, detail="You don't have access to this church's data.")

        payload: dict = {}

        if "name"         in updated_task: payload["name"]         = updated_task["name"]
        if "taskType"     in updated_task: payload["taskType"]     = updated_task["taskType"]
        if "description"  in updated_task: payload["description"]  = updated_task["description"]
        if "type"         in updated_task: payload["type"]         = updated_task["type"]
        if "priority"     in updated_task: payload["priority"]     = updated_task["priority"]
        if "person_name"  in updated_task: payload["person_name"]  = updated_task["person_name"]
        if "person_surname" in updated_task: payload["person_surname"] = updated_task["person_surname"]
        if "consolidation_id" in updated_task: payload["consolidation_id"] = updated_task["consolidation_id"]

        if "followup_date" in updated_task:
            payload["followup_date"] = _parse_followup_date(updated_task["followup_date"])

        if "status" in updated_task:
            normalized = updated_task["status"].lower()
            payload["status"] = normalized
            if normalized in ("completed", "done", "closed", "finished"):
                payload["completedAt"] = datetime.utcnow().isoformat()
            elif normalized in ("open", "pending", "incomplete"):
                payload["completedAt"] = None

        if "assignedfor" in updated_task:
            payload["assignedfor"] = updated_task["assignedfor"].lower()

        if "assigned_to_email" in updated_task:
            payload["assigned_to_email"] = updated_task["assigned_to_email"].lower()

        # Allow updating flattened contacted_person fields
        if "contacted_person" in updated_task:
            cp = updated_task["contacted_person"] or {}
            if isinstance(cp, dict):
                if "name"  in cp: payload["contacted_person_name"]  = cp["name"]
                if "email" in cp: payload["contacted_person_email"] = cp["email"]
                if "phone" in cp: payload["contacted_person_phone"] = cp["phone"]

        payload["updated_at"] = datetime.utcnow().isoformat()

        result = supabase.table("Tasks").update(payload).eq("_id", task_id).execute()
        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to update task")

        return {"updatedTask": _format_task_row(result.data[0])}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"update_task error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(exc)}")


# DELETE /tasks/cleanup-orphaned
@router.delete("/tasks/cleanup-orphaned")
async def cleanup_orphaned_tasks(
    user_email:   Optional[str] = Query(None),
    current_user: dict          = Depends(get_current_user),
):
    """Remove consolidation tasks that no longer have a corresponding consolidation record."""
    try:
        query = (
            supabase.table("Tasks")
            .select("*")
            .eq('"taskType"', "consolidation")
            .not_.in_("status", ["completed", "cancelled", "deleted"])
        )
        if user_email:
            query = query.eq("assignedfor", user_email.lower())

        rows          = (query.execute().data) or []
        deleted       = 0
        deleted_ids: List[str] = []

        for row in rows:
            task_id          = row.get("_id", "")
            consolidation_id = row.get("consolidation_id")
            person_email     = row.get("contacted_person_email")

            consolidation_exists = False

            if consolidation_id:
                check = (
                    supabase.table("consolidations")
                    .select("_id")
                    .eq("_id", consolidation_id)
                    .neq("status", "removed")
                    .execute()
                )
                consolidation_exists = bool(check.data)

            if not consolidation_exists and person_email:
                check = (
                    supabase.table("consolidations")
                    .select("_id")
                    .eq("person_email", person_email)
                    .neq("status", "removed")
                    .execute()
                )
                consolidation_exists = bool(check.data)

            if not consolidation_exists:
                supabase.table("Tasks").delete().eq("_id", task_id).execute()
                deleted += 1
                deleted_ids.append(task_id)

        return {
            "success":       True,
            "message":       f"Cleaned up {deleted} orphaned consolidation tasks",
            "deleted_count": deleted,
            "deleted_ids":   deleted_ids,
        }

    except Exception as exc:
        logger.error(f"cleanup_orphaned_tasks error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Cleanup error: {str(exc)}")


# Dashboard stats
def _get_period_range(period: str):
    from datetime import timedelta
    from calendar import monthrange

    now   = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "today":
        return today.isoformat(), today.replace(hour=23, minute=59, second=59).isoformat()
    if period == "thisWeek":
        start = today - timedelta(days=today.weekday())
        return start.isoformat(), (start + timedelta(days=6, hours=23, minutes=59, seconds=59)).isoformat()
    if period == "thisMonth":
        start = today.replace(day=1)
        _, last_day = monthrange(today.year, today.month)
        return start.isoformat(), today.replace(day=last_day, hour=23, minute=59, second=59).isoformat()
    if period == "previous7":
        end   = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        start = (end - timedelta(days=6)).replace(hour=0, minute=0, second=0)
        return start.isoformat(), end.isoformat()
    if period == "previousWeek":
        last_monday = today - timedelta(days=today.weekday() + 7)
        end         = (last_monday + timedelta(days=6)).replace(hour=23, minute=59, second=59)
        return last_monday.isoformat(), end.isoformat()
    if period == "previousMonth":
        year, month = today.year, today.month - 1
        if month == 0:
            month, year = 12, year - 1
        _, last_day = monthrange(year, month)
        return datetime(year, month, 1).isoformat(), datetime(year, month, last_day, 23, 59, 59).isoformat()

    raise ValueError(f"Invalid period '{period}'")


@router.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats(
    period:       str  = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    current_user: dict = Depends(get_current_user),
):
    try:
        org_name = _org_name_from_user(current_user)
        if not org_name:
            raise HTTPException(status_code=403, detail="Organization not associated with user")

        is_super_admin = current_user.get("role") == "super_admin"
        start_str, end_str = _get_period_range(period)

        def base_query():
            q = supabase.table("Tasks").select("*")
            if not is_super_admin:
                q = q.eq('"Organization"', org_name)
            return q

        period_rows    = (base_query().gte("followup_date", start_str).lte("followup_date", end_str).execute().data) or []
        completed_rows = (base_query().gte('"completedAt"', start_str).lte('"completedAt"', end_str).execute().data) or []
        all_rows       = (supabase.table("Tasks").select('_id,status,"taskType","completedAt"')
                         .eq('"Organization"', org_name).execute().data) or [] if not is_super_admin else \
                         (supabase.table("Tasks").select('_id,status,"taskType","completedAt"').execute().data) or []

        def is_done(status: str) -> bool:
            return (status or "").lower() in ("completed", "done", "closed", "finished")

        def not_excluded(task_type: str) -> bool:
            return (task_type or "") not in EXCLUDED_TASK_TYPES_FROM_COMPLETED

        tasks_due_in_period       = sum(1 for r in period_rows if not is_done(r.get("status", "")))
        tasks_completed_in_period = sum(1 for r in completed_rows if is_done(r.get("status", "")) and not_excluded(r.get("taskType", "")))
        total_completed           = sum(1 for r in all_rows if is_done(r.get("status", "")) and not_excluded(r.get("taskType", "")))
        total_tasks_all           = len(all_rows)

        cons_all  = [r for r in all_rows if (r.get("taskType") or "").lower() == "consolidation"]
        cons_done = sum(1 for r in cons_all if is_done(r.get("status", "")))
        cons_done_in_period = sum(1 for r in completed_rows if (r.get("taskType") or "").lower() == "consolidation" and is_done(r.get("status", "")))

        type_stats: dict = {}
        for r in all_rows:
            tt = r.get("taskType") or "Uncategorized"
            if tt not in type_stats:
                type_stats[tt] = {"total": 0, "completed": 0, "completed_in_period": 0, "due_in_period": 0, "is_excluded": tt in EXCLUDED_TASK_TYPES_FROM_COMPLETED}
            type_stats[tt]["total"] += 1
            if is_done(r.get("status", "")):
                type_stats[tt]["completed"] += 1

        for r in period_rows:
            tt = r.get("taskType") or "Uncategorized"
            if tt in type_stats:
                type_stats[tt]["due_in_period"] += 1

        for r in completed_rows:
            tt = r.get("taskType") or "Uncategorized"
            if tt in type_stats and is_done(r.get("status", "")):
                type_stats[tt]["completed_in_period"] += 1

        for s in type_stats.values():
            s["completion_rate"]           = round(s["completed"] / s["total"] * 100, 2) if s["total"] else 0
            s["completion_rate_in_period"] = round(s["completed_in_period"] / s["due_in_period"] * 100, 2) if s["due_in_period"] else 0

        return {
            "period":                          period,
            "date_range":                      {"start": start_str, "end": end_str},
            "taskCount":                       total_tasks_all,
            "tasksDueInPeriod":                tasks_due_in_period,
            "tasksCompletedInPeriod":          tasks_completed_in_period,
            "totalCompletedTasks":             total_completed,
            "consolidationTasks":              len(cons_all),
            "consolidationCompleted":          cons_done,
            "consolidationCompletedInPeriod":  cons_done_in_period,
            "consolidationCompletionRate":     round(cons_done / len(cons_all) * 100, 2) if cons_all else 0,
            "completionRateDueTasks":          round(tasks_completed_in_period / tasks_due_in_period * 100, 2) if tasks_due_in_period else 0,
            "overallCompletionRate":           round(total_completed / total_tasks_all * 100, 2) if total_tasks_all else 0,
            "taskTypeBreakdown":               type_stats,
            "totalTaskTypesFound":             len(type_stats),
            "excludedTaskTypes":               EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp":                       datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_dashboard_quick_stats error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching quick stats: {str(exc)}")