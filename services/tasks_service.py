"""Tasks service - handles all task-related business logic"""
from datetime import datetime
from bson import ObjectId
from typing import Optional, List, Dict, Any
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
import logging
import pytz
from database import tasks_collection, tasktypes_collection, users_collection
from auth.utils import task_type_serializer


async def create_task(task_data: dict, current_user: dict):
    """Create a new task"""
    try:
        new_task_dict = task_data.copy()
        new_task_dict["assignedfor"] = current_user["email"]

        result = await tasks_collection.insert_one(new_task_dict)
        new_task_dict["_id"] = str(result.inserted_id)

        return {"status": "success", "task": jsonable_encoder(new_task_dict)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}


async def get_user_tasks(
    email: Optional[str],
    userId: Optional[str],
    view_all: bool,
    current_user: dict
):
    """Get tasks for a user"""
    try:
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        
        user_email = None
        
        if email:
            user_email = email
        elif userId:
            user = await users_collection.find_one({"_id": ObjectId(userId)})
            if user:
                user_email = user.get("email")
        else:
            user_email = current_user.get("email")
        
        if not user_email:
            return {"error": "User email not found", "status": "failed"}
        
        timezone = pytz.timezone("Africa/Johannesburg")
        
        if is_leader and view_all:
            query = {}
        else:
            query = {"assignedfor": user_email}
        
        cursor = tasks_collection.find(query)
        all_tasks = []
        
        async for task in cursor:
            task_date_str = task.get("followup_date")
            task_datetime = None
            
            if task_date_str:
                if isinstance(task_date_str, datetime):
                    task_datetime = task_date_str
                else:
                    try:
                        task_datetime = datetime.fromisoformat(task_date_str)
                        task_datetime = task_datetime.astimezone(timezone)
                    except ValueError:
                        logging.warning(f"Invalid date format: {task_date_str}")
                        continue
            
            all_tasks.append({
                "_id": str(task["_id"]),
                "name": task.get("name", "Unnamed Task"),
                "taskType": task.get("taskType", ""),
                "followup_date": task_datetime.isoformat() if task_datetime else None,
                "status": task.get("status", "Open"),
                "assignedfor": task.get("assignedfor", ""),
                "type": task.get("type", "call"),
                "contacted_person": task.get("contacted_person", {}),
                "isRecurring": bool(task.get("recurring_day")),
            })
        
        all_tasks.sort(key=lambda t: t["followup_date"] or "", reverse=True)
        
        return {
            "user_email": user_email if not view_all else "all_users",
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "status": "success",
            "is_leader_view": is_leader and view_all
        }
        
    except Exception as e:
        logging.error(f"Error in get_user_tasks: {e}")
        return {"error": str(e), "status": "failed"}


async def get_task_types():
    """Get all task types"""
    try:
        cursor = tasktypes_collection.find().sort("name", 1)
        types = []
        async for t in cursor:
            types.append(task_type_serializer(t))
        return types
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def create_task_type(task_data):
    """Create a new task type"""
    try:
        existing = await tasktypes_collection.find_one({"name": task_data.name})
        if existing:
            raise HTTPException(status_code=400, detail="Task type already exists.")

        new_task = {"name": task_data.name}
        result = await tasktypes_collection.insert_one(new_task)
        created = await tasktypes_collection.find_one({"_id": result.inserted_id})
        return task_type_serializer(created)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def serialize_doc(doc):
    """Helper to convert ObjectId to string"""
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def update_task(task_id: str, updated_task: dict):
    """Update a task"""
    try:
        obj_id = ObjectId(task_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid task ID")

    result = await tasks_collection.update_one(
        {"_id": obj_id},
        {"$set": updated_task}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")

    updated = await tasks_collection.find_one({"_id": obj_id})
    return serialize_doc(updated)


async def get_all_tasks(current_user: dict):
    """Get all tasks (admin/leader only)"""
    try:
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        if not is_leader:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        cursor = tasks_collection.find({})
        tasks = []
        async for task in cursor:
            task["_id"] = str(task["_id"])
            tasks.append(task)
        
        return {"tasks": tasks, "total": len(tasks)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_leader_tasks(leader_email: str, current_user: dict):
    """Get tasks for a specific leader"""
    try:
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        if not is_leader:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        tasks = await tasks_collection.find({
            "$or": [
                {"assigned_to_email": leader_email},
                {"assignedfor": leader_email},
                {"assignedfor": {"$regex": f"^{leader_email}$", "$options": "i"}},
                {"leader_assigned": {"$regex": f"^{leader_email}$", "$options": "i"}}
            ]
        }).to_list(length=None)
        
        formatted_tasks = []
        for task in tasks:
            task["_id"] = str(task["_id"])
            formatted_tasks.append(task)
        
        return {
            "leader_email": leader_email,
            "total_tasks": len(formatted_tasks),
            "tasks": formatted_tasks
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

