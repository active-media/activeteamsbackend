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
from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks


a@app.post("/tasks")
async def create_task(task: TaskModel, current_user: dict = Depends(get_current_user)):
    try:
        # Convert Pydantic model to dict
        new_task_dict = task.dict()
        # Attach the creator's email for backward compatibility
        new_task_dict["assignedfor"] = current_user["email"]

        # Insert into MongoDB
        result = await db["tasks"].insert_one(new_task_dict)

        # Add the MongoDB _id as a string for the response
        new_task_dict["_id"] = str(result.inserted_id)

        # Encode safely for JSON response
        return {"status": "success", "task": jsonable_encoder(new_task_dict)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}
    
@app.get("/tasks")
async def get_user_tasks(
    email: str = Query(None),
    userId: str = Query(None),
    view_all: bool = Query(False),  # Add explicit parameter for viewing all tasks
    current_user: dict = Depends(get_current_user)
):
    try:
        # Check if current user is a leader
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
       
        # Determine user email based on parameters or current user
        user_email = None
       
        if email:
            user_email = email
        elif userId:
            user = await users_collection.find_one({"_id": ObjectId(userId)})
            if user:
                user_email = user.get("email")
        else:
            # No parameters provided - use current user's email
            user_email = current_user.get("email")
       
        if not user_email:
            return {"error": "User email not found", "status": "failed"}
       
        timezone = pytz.timezone("Africa/Johannesburg")
       
        # Build query based on permissions
        # Only show all tasks if user is a leader AND explicitly requests it with view_all=true
        if is_leader and view_all:
            query = {}
        else:
            # Always filter by specific user email (current user or specified user)
            query = {"assignedfor": user_email}
       
        # Fetch tasks
        cursor = tasks_collection.find(query)
        all_tasks = []
       
        async for task in cursor:
            task_date_str = task.get("followup_date")
            task_datetime = None
           
            # Parse followup_date
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
       
        # Sort by date (newest first)
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

a@app.get("/tasktypes", response_model=List[TaskTypeOut])
async def get_task_types():
    try:
        cursor = tasktypes_collection.find().sort("name", 1)
        types = []
        async for t in cursor:
            types.append(task_type_serializer(t))
        return types
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tasks")
async def create_task(task: TaskModel, current_user: dict = Depends(get_current_user)):
    try:
        # Convert Pydantic model to dict
        new_task_dict = task.dict()
        # Attach the creator's email for backward compatibility
        new_task_dict["assignedfor"] = current_user["email"]

        # Insert into MongoDB
        result = await db["tasks"].insert_one(new_task_dict)

        # Add the MongoDB _id as a string for the response
        new_task_dict["_id"] = str(result.inserted_id)

        # Encode safely for JSON response
        return {"status": "success", "task": jsonable_encoder(new_task_dict)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}


@app.get("/tasks/all")
async def get_all_tasks(
        current_user: dict = Depends(get_current_user)
    ):
        """
        Dedicated endpoint: Get ALL tasks for every user
        Only accessible to leaders, admins, and managers
        Used by StatsDashboard & Admin panels
        """
        try:
            # Permission check — only leaders can see all tasks
            role = current_user.get("role", "").lower()
            if role not in ["admin", "leader", "manager"]:
                return {
                    "error": "Access denied. You must be a leader or admin to view all tasks.",
                    "status": "failed"
                }, 403

            timezone = pytz.timezone("Africa/Johannesburg")
            cursor = tasks_collection.find({})  # No filter → ALL tasks
            all_tasks = []

            async for task in cursor:
                # Safely parse followup_date
                followup_raw = task.get("followup_date")
                followup_dt = None
                if followup_raw:
                    if isinstance(followup_raw, datetime):
                        followup_dt = followup_raw
                    else:
                        try:
                            dt_str = str(followup_raw).replace("Z", "+00:00")
                            followup_dt = datetime.fromisoformat(dt_str)
                        except:
                            try:
                                followup_dt = datetime.fromisoformat(str(followup_raw))
                            except:
                                logging.warning(f"Invalid date format in task {task['_id']}: {followup_raw}")

                    if followup_dt:
                        if followup_dt.tzinfo is None:
                            followup_dt = pytz.utc.localize(followup_dt)
                        followup_dt = followup_dt.astimezone(timezone)

                # Resolve full user info for legacy assignedfor (email string)
                assigned_to = None
                if task.get("assignedTo") and isinstance(task["assignedTo"], dict):
                    assigned_to = task["assignedTo"]
                elif task.get("assignedfor"):
                    user = await users_collection.find_one(
                        {"email": {"$regex": f"^{task['assignedfor'].strip()}$", "$options": "i"}},
                        {"name": 1, "surname": 1, "email": 1, "phone": 1}
                    )
                    if user:
                        assigned_to = {
                            "_id": str(user["_id"]),
                            "name": user.get("name", ""),
                            "surname": user.get("surname", ""),
                            "email": user.get("email", ""),
                            "phone": user.get("phone", "")
                        }

                all_tasks.append({
                    "_id": str(task["_id"]),
                    "name": task.get("name", "Unnamed Task"),
                    "taskType": task.get("taskType", ""),
                    "followup_date": followup_dt.isoformat() if followup_dt else None,
                    "status": task.get("status", "Open"),
                    "assignedfor": task.get("assignedfor", ""),
                    "assignedTo": assigned_to,  # Fully resolved user
                    "type": task.get("type", "call"),
                    "contacted_person": task.get("contacted_person", {}),
                    "isRecurring": bool(task.get("recurring_day")),
                    "createdAt": task.get("createdAt", datetime.utcnow()).isoformat() if task.get("createdAt") else None,
                })

            # Sort newest first
            all_tasks.sort(key=lambda x: x["followup_date"] or "9999-12-31", reverse=True)

            return {
                "total_tasks": len(all_tasks),
                "tasks": all_tasks,
                "status": "success",
                "fetched_by": current_user.get("email"),
                "role": current_user.get("role"),
                "timestamp": datetime.now(timezone).isoformat(),
                "message": "All tasks loaded successfully"
            }

        except Exception as e:
            logging.error(f"Error in /tasks/all: {e}", exc_info=True)
            return {
                "error": "Failed to fetch all tasks",
                "details": str(e),
                "status": "failed"
            }, 500        

@app.get("/tasks/leader/{leader_email}")
async def get_leader_tasks(
    leader_email: str,
    current_user: dict = Depends(get_current_user)
):
    """Get all consolidation tasks assigned to a specific leader"""
    try:
        # Find consolidation tasks assigned to this leader
        tasks = await tasks_collection.find({
            "is_consolidation_task": True,
            "$or": [
                {"assigned_to_email": leader_email},
                {"assignedfor": leader_email},
                {"assignedfor": {"$regex": f"^{leader_email}$", "$options": "i"}},
                {"leader_assigned": {"$regex": f"^{leader_email}$", "$options": "i"}}
            ]
        }).to_list(length=None)
       
        # Format response
        formatted_tasks = []
        for task in tasks:
            task["_id"] = str(task["_id"])
            formatted_tasks.append(task)
       
        return {
            "leader_email": leader_email,
            "total_tasks": len(formatted_tasks),
            "tasks": formatted_tasks
        }
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))