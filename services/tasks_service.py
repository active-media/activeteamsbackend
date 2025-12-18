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
    """
    Create a new task in the database.
    
    This asynchronous function takes task data and the current user's information,
    assigns the task to the current user by adding their email, inserts the task
    into the MongoDB collection, converts the inserted ID to a string, and returns
    a success response with the task details. If an error occurs during insertion,
    it catches the exception and returns a failure response with the error message.
    
    Args:
        task_data (dict): A dictionary containing the task details to be created.
        current_user (dict): A dictionary containing information about the current user,
                             including their email.
    
    Returns:
        dict: A dictionary with 'status' indicating success or failure, and either
              the created 'task' details or an 'error' message.
    """
    try:
        # Create a copy of the input task data to avoid modifying the original
        new_task_dict = task_data.copy()
        
        # Assign the task to the current user by adding their email
        new_task_dict["assignedfor"] = current_user["email"]
        
        # Insert the new task document into the MongoDB tasks collection asynchronously
        result = await tasks_collection.insert_one(new_task_dict)
        
        # Convert the MongoDB ObjectId to a string for JSON compatibility
        new_task_dict["_id"] = str(result.inserted_id)
        
        # Return success response with the encoded task data
        return {"status": "success", "task": jsonable_encoder(new_task_dict)}
    
    except Exception as e:
        # Catch any exceptions during the process and return failure response
        return {"status": "failed", "error": str(e)}
    
    
async def get_user_tasks(
    email: Optional[str],
    userId: Optional[str],
    view_all: bool,
    current_user: dict
):
    """
    Retrieve tasks for a specific user or all users (if permitted).
    
    This function determines which user's tasks to fetch based on the provided
    email, userId, or falls back to the current user. Leaders/admins can view
    all tasks when view_all is True. Tasks are sorted by followup_date (most
    recent first), and dates are normalized to the Africa/Johannesburg timezone.
    
    Args:
        email (Optional[str]): Direct email of the user whose tasks to fetch.
        userId (Optional[str]): ObjectId string of the user to look up their email.
        view_all (bool): If True and caller is a leader/admin, return all tasks.
        current_user (dict): Information about the authenticated user (must contain 'role' and 'email').
    
    Returns:
        dict: Contains status, user_email (or 'all_users'), total_tasks, tasks list,
              and is_leader_view flag. On error, returns status 'failed' with error message.
    """
    try:
        # Check if the current user has leader/admin privileges
        is_leader = current_user.get("role") in ["admin", "leader"]
        
        # Determine the target user's email
        user_email = None
        if email:
            # Priority 1: explicitly provided email
            user_email = email
        elif userId:
            # Priority 2: look up email by userId (ObjectId)
            user = await users_collection.find_one({"_id": ObjectId(userId)})
            if user:
                user_email = user.get("email")
        else:
            # Fallback: use current user's email
            user_email = current_user.get("email")
        
        # If no valid email found, return error
        if not user_email:
            return {"error": "User email not found", "status": "failed"}
        
        # Define timezone for date normalization
        timezone = pytz.timezone("Africa/Johannesburg")
        
        # Build query: leaders can view all tasks if requested
        if is_leader and view_all:
            query = {}  # No filter – retrieve all tasks
        else:
            query = {"assignedfor": user_email}  # Only tasks assigned to the target user
        
        # Fetch tasks asynchronously from the collection
        cursor = tasks_collection.find(query)
        all_tasks = []
        
        # Process each task document
        async for task in cursor:
            # Extract and parse the followup_date
            task_date_str = task.get("followup_date")
            task_datetime = None
            
            if task_date_str:
                if isinstance(task_date_str, datetime):
                    # Already a datetime object
                    task_datetime = task_date_str
                else:
                    try:
                        # Parse ISO format string and convert to specified timezone
                        task_datetime = datetime.fromisoformat(task_date_str)
                        task_datetime = task_datetime.astimezone(timezone)
                    except ValueError:
                        # Log invalid date formats and skip this task
                        logging.warning(f"Invalid date format: {task_date_str}")
                        continue
            
            # Build a clean, JSON-serializable task dictionary
            all_tasks.append({
                "_id": str(task["_id"]),  # Convert ObjectId to string
                "name": task.get("name", "Unnamed Task"),
                "taskType": task.get("taskType", ""),
                "followup_date": task_datetime.isoformat() if task_datetime else None,
                "status": task.get("status", "Open"),
                "assignedfor": task.get("assignedfor", ""),
                "type": task.get("type", "call"),
                "contacted_person": task.get("contacted_person", {}),
                "isRecurring": bool(task.get("recurring_day")),  # True if recurring_day exists
            })
        
        # Sort tasks by followup_date descending (most recent first)
        # Tasks without a date are placed at the end (empty string sorts last)
        all_tasks.sort(key=lambda t: t["followup_date"] or "", reverse=True)
        
        # Successful response
        return {
            "user_email": user_email if not view_all else "all_users",
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "status": "success",
            "is_leader_view": is_leader and view_all
        }
    
    except Exception as e:
        # Log the error for debugging and return a clean failure response
        logging.error(f"Error in get_user_tasks: {e}")
        return {"error": str(e), "status": "failed"}

async def get_task_types():
    """
    Retrieve all task types from the database.
    
    This asynchronous function fetches all documents from the tasktypes_collection,
    sorts them alphabetically by the 'name' field in ascending order, serializes
    each document using the task_type_serializer function, and returns the list
    of serialized task types.
    
    Returns:
        list: A list of serialized task type objects (typically dictionaries suitable for JSON response).
    
    Raises:
        HTTPException: With status code 500 if any error occurs during database retrieval or processing.
    """
    try:
        # Query all task types and sort them by name in ascending order (1 = ascending)
        cursor = tasktypes_collection.find().sort("name", 1)
        
        # List to hold the serialized task types
        types = []
        
        # Asynchronously iterate over the cursor to avoid loading everything into memory at once
        async for t in cursor:
            # Serialize each task type document (e.g., convert ObjectId to string, filter fields, etc.)
            types.append(task_type_serializer(t))
        
        # Return the list of serialized task types
        return types
    
    except Exception as e:
        # Log the error for debugging (optional but recommended in production)
        # logging.error(f"Error fetching task types: {e}")
        
        # Raise an HTTPException to be handled by FastAPI (returns 500 Internal Server Error)
        raise HTTPException(status_code=500, detail=str(e))

async def create_task_type(task_data):
    """
    Create a new task type in the database.
    
    This function validates that a task type with the given name does not already exist,
    inserts a new document containing only the 'name' field, retrieves the freshly created
    document, serializes it using task_type_serializer, and returns it.
    
    Args:
        task_data: A Pydantic model or dataclass/object containing at least a 'name' attribute
                   (typically validated by FastAPI dependency injection).
    
    Returns:
        The serialized newly created task type (usually a dict suitable for JSON response).
    
    Raises:
        HTTPException: 
            - 400 if a task type with the same name already exists.
            - 400 for any other error during insertion or retrieval (with error message in detail).
            - In production, you may want to distinguish validation errors (400) from server errors (500).
    """
    try:
        # Check for duplicate task type by name (case-sensitive)
        existing = await tasktypes_collection.find_one({"name": task_data.name})
        if existing:
            # Raise 400 Bad Request if the name is already taken
            raise HTTPException(status_code=400, detail="Task type already exists.")
        
        # Prepare minimal document – only store the name (other fields can be added later if needed)
        new_task = {"name": task_data.name}
        
        # Insert the new task type document into the collection
        result = await tasktypes_collection.insert_one(new_task)
        
        # Fetch the newly inserted document (to include the generated _id and any defaults)
        created = await tasktypes_collection.find_one({"_id": result.inserted_id})
        
        # Serialize the document (e.g., convert ObjectId to string, select/whitelist fields)
        return task_type_serializer(created)
    
    except HTTPException:
        # Re-raise HTTPExceptions as-is (e.g., the duplicate check above)
        raise
    except Exception as e:
        # Convert any other unexpected error into a 400 response
        # Note: In many APIs, unexpected server errors are better as 500
        raise HTTPException(status_code=400, detail=str(e))

def serialize_doc(doc):
    """Helper to convert ObjectId to string"""
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def update_task(task_id: str, updated_task: dict):
    """
    Update an existing task by its ID.
    
    This function validates the provided task_id as a valid MongoDB ObjectId,
    applies the updates from updated_task using $set, checks if a document was matched,
    and returns the freshly fetched updated task document serialized for JSON response.
    
    Args:
        task_id (str): The string representation of the task's MongoDB ObjectId.
        updated_task (dict): A dictionary containing the fields to update (will be applied via $set).
    
    Returns:
        The serialized updated task document (typically a dict with string _id).
    
    Raises:
        HTTPException:
            - 400 if the task_id is not a valid ObjectId.
            - 404 if no task with the given ID exists.
            - 500 for unexpected server errors (not caught here – handled by outer exception block if present).
    """
    try:
        # Convert the string task_id to a MongoDB ObjectId
        obj_id = ObjectId(task_id)
    except Exception:
        # Invalid ObjectId format
        raise HTTPException(status_code=400, detail="Invalid task ID")
    
    # Perform the update – only sets the fields provided in updated_task
    result = await tasks_collection.update_one(
        {"_id": obj_id},
        {"$set": updated_task}
    )
    
    # If no document matched the _id, the task doesn't exist
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Fetch the updated document to return the latest state
    updated = await tasks_collection.find_one({"_id": obj_id})
    
    # Serialize the document (e.g., convert ObjectId to string, filter fields, etc.)
    return serialize_doc(updated)


async def get_all_tasks(current_user: dict):
    """
    Retrieve all tasks in the system (restricted to admin, leader, or manager roles).
    
    This endpoint allows privileged users to fetch every task document.
    The _id field is converted to string for JSON compatibility.
    
    Args:
        current_user (dict): The authenticated user's data, must contain a 'role' field.
    
    Returns:
        dict: Contains 'tasks' (list of task documents) and 'total' (count).
    
    Raises:
        HTTPException:
            - 403 if the user does not have sufficient privileges.
            - 500 for unexpected errors during database retrieval.
    """
    try:
        # Check if the current user has an authorized role
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        if not is_leader:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        # Query all tasks without any filter
        cursor = tasks_collection.find({})
        
        tasks = []
        
        # Asynchronously iterate through all tasks
        async for task in cursor:
            # Convert ObjectId to string for safe JSON serialization
            task["_id"] = str(task["_id"])
            tasks.append(task)
        
        # Return the list of tasks along with the total count
        return {"tasks": tasks, "total": len(tasks)}
    
    except HTTPException:
        # Re-raise known HTTP exceptions (e.g., 403 from authorization check)
        raise
    except Exception as e:
        # Log the error in production (recommended)
        # logging.error(f"Error in get_all_tasks: {e}")
        
        # Unexpected server error
        raise HTTPException(status_code=500, detail=str(e))

async def get_leader_tasks(leader_email: str, current_user: dict):
    """
    Retrieve all tasks associated with a specific leader (by email).
    
    This endpoint is restricted to users with admin, leader, or manager roles.
    It searches for tasks where the provided leader_email matches any of several
    possible fields (case-insensitive where appropriate):
      - assigned_to_email
      - assignedfor (exact and regex case-insensitive)
      - leader_assigned (regex case-insensitive)
    
    All matching tasks are returned with their _id converted to string for JSON compatibility.
    
    Args:
        leader_email (str): The email address of the leader whose tasks to retrieve.
        current_user (dict): The authenticated user's data, must contain a 'role' field.
    
    Returns:
        dict: Contains:
            - leader_email: the queried email
            - total_tasks: number of tasks found
            - tasks: list of task documents (with string _id)
    
    Raises:
        HTTPException:
            - 403 if the current user is not authorized (not admin/leader/manager)
            - 500 for unexpected server errors during query execution
    """
    try:
        # Verify that the current user has sufficient privileges
        is_leader = current_user.get("role") in ["admin", "leader", "manager"]
        if not is_leader:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        # Build a flexible query to match the leader_email across multiple possible fields
        query = {
            "$or": [
                {"assigned_to_email": leader_email},                          # Exact match on assigned_to_email
                {"assignedfor": leader_email},                                # Exact match on assignedfor
                {"assignedfor": {"$regex": f"^{leader_email}$", "$options": "i"}},  # Case-insensitive exact match
                {"leader_assigned": {"$regex": f"^{leader_email}$", "$options": "i"}}   # Case-insensitive on leader_assigned
            ]
        }
        
        # Execute the query and retrieve all matching tasks in one go
        # to_list(None) fetches all documents (no limit)
        tasks = await tasks_collection.find(query).to_list(length=None)
        
        # Format tasks for JSON serialization (convert ObjectId to string)
        formatted_tasks = []
        for task in tasks:
            task["_id"] = str(task["_id"])
            formatted_tasks.append(task)
        
        # Return the results with metadata
        return {
            "leader_email": leader_email,
            "total_tasks": len(formatted_tasks),
            "tasks": formatted_tasks
        }
    
    except HTTPException:
        # Re-raise authorization or known client errors as-is
        raise
    except Exception as e:
        # Log the error in production for debugging (recommended)
        # logging.error(f"Error in get_leader_tasks for {leader_email}: {e}")
        
        # Unexpected server error
        raise HTTPException(status_code=500, detail=str(e))