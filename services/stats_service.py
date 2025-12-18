"""Stats service - handles statistics and dashboard logic"""
# This is a placeholder - extract stats logic from main.py
# Stats service will contain functions for:
# - Overview stats
# - Dashboard comprehensive stats
# - Dashboard quick stats
# - Outstanding items
# - People with tasks stats
# etc.

from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
import logging
from fastapi import Depends, Query, HTTPException
import asyncio
from database import db , users_collection ,events_collection,tasks_collection,tasktypes_collection
from auth.utils import get_current_user
# Placeholder functions - these need to be extracted from main.py

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auth")

def get_period_range(period: str):
    """
    Accurate date range calculator matching frontend's DailyTasks filter:
    - today
    - thisWeek
    - thisMonth
    - previous7 (last 7 days)
    - previousWeek
    - previousMonth
    """
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    
    if period == "today":
        start = today
        end = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    
    
    if period == "thisWeek":
        start = today - timedelta(days=today.weekday())  
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end
    
    
    if period == "thisMonth":
        start = today.replace(day=1)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(today.year, today.month + 1, 1) - timedelta(microseconds=1)
        return start, end
    
    
    if period == "previous7":
        end = today - timedelta(days=1)  
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        start = end - timedelta(days=6)  
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end
    
    
    if period == "previousWeek":
        last_week = today - timedelta(weeks=1)
        start = last_week - timedelta(days=last_week.weekday())  
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end
    
    
    if period == "previousMonth":
        year = today.year
        month = today.month - 1
        if month == 0:
            month = 12
            year -= 1
        
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(year, month + 1, 1) - timedelta(microseconds=1)
        return start, end
    
    raise ValueError(f"Invalid period '{period}'")


EXCLUDED_TASK_TYPES_FROM_COMPLETED = ["no answer", "Awaiting Call"]

async def get_dashboard_comprehensive(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    """
    Get comprehensive dashboard statistics for a given time period.
    
    Excludes task types from EXCLUDED_TASK_TYPES_FROM_COMPLETED when counting completed tasks.
    
    Returns:
        - Overview statistics (tasks, cells, completion rates)
        - Overdue cells list
        - Tasks grouped by user
        - All tasks list
        - All users list
    """
    try:
        # Log the request details
        print(f"[DASHBOARD] Period: {period}, User: {current_user.get('email')}")
        print(f"[DASHBOARD] Excluding types: {EXCLUDED_TASK_TYPES_FROM_COMPLETED}")
        
        # Calculate date range and fetch all data in parallel
        start, end = get_period_range(period)
        
        # Fetch all data with individual error handling in each function
        overdue_cells, task_groups, users, all_task_types = await asyncio.gather(
            _fetch_overdue_cells(end),
            _fetch_tasks_by_user(start, end),
            _fetch_users(limit),
            _fetch_task_types(),
            return_exceptions=True  # Don't fail entire request if one fetch fails
        )
        
        # Check for errors in parallel fetch operations
        if isinstance(overdue_cells, Exception):
            print(f"[ERROR] Failed to fetch overdue cells: {overdue_cells}")
            overdue_cells = []
        if isinstance(task_groups, Exception):
            print(f"[ERROR] Failed to fetch task groups: {task_groups}")
            task_groups = []
        if isinstance(users, Exception):
            print(f"[ERROR] Failed to fetch users: {users}")
            users = []
        if isinstance(all_task_types, Exception):
            print(f"[ERROR] Failed to fetch task types: {all_task_types}")
            all_task_types = []
        
        # Process and format the collected data
        formatted_cells = _format_cells(overdue_cells)
        user_map = _create_user_map(users)
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            task_groups, user_map
        )
        
        # Build the overview statistics
        overview = _build_overview(
            formatted_cells=formatted_cells,
            global_stats=global_stats,
            task_type_stats=task_type_stats,
            grouped_tasks=grouped_tasks,
            users=users,
            all_task_types=all_task_types
        )
        
        # Compile and return the complete response
        return {
            "overview": overview,
            "overdueCells": formatted_cells,
            "groupedTasks": grouped_tasks,
            "allTasks": [task for group in grouped_tasks for task in group["tasks"]],
            "allUsers": _format_users(users),
            "period": period,
            "date_range": {
                "start": start.date().isoformat(),
                "end": end.date().isoformat()
            },
            "task_type_stats": task_type_stats,
            "available_task_types": all_task_types,
            "task_types_found": list(task_type_stats.keys()),
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[CRITICAL ERROR] Dashboard comprehensive failed: {str(e)}")
        raise HTTPException(500, f"Error fetching comprehensive stats: {str(e)}")


# ============================================================================
# DATA FETCHING FUNCTIONS (with error handling)
# ============================================================================

async def _fetch_overdue_cells(end_date: datetime):
    """
    Fetch all overdue or incomplete cells up to the end date.
    Handles multiple field name variations (Event Type, eventType, eventTypeName).
    
    Returns empty list if fetch fails.
    """
    try:
        pipeline = [
            {
                "$match": {
                    # Match cells using various field name patterns
                    "$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
                    ],
                    "date": {"$lte": end_date},
                    # Match incomplete or overdue cells
                    "$or": [
                        {"status": "incomplete"},
                        {"status": {"$exists": False}},
                        {"status": None},
                        {"Status": "Incomplete"},
                        {"_is_overdue": True}
                    ]
                }
            },
            {"$sort": {"date": -1}},
            {"$limit": 100},
            {
                "$project": {
                    "_id": 1,
                    "UUID": 1,
                    # Normalize field names using $ifNull to handle variations
                    "eventName": {"$ifNull": ["$Event Name", "$eventName", "$EventName", "Unnamed Event"]},
                    "eventType": {"$ifNull": ["$Event Type", "$eventType", "$eventTypeName", "Cells"]},
                    "eventLeaderName": {"$ifNull": ["$Leader", "$eventLeaderName", "$EventLeaderName", "Unknown Leader"]},
                    "eventLeaderEmail": {"$ifNull": ["$Email", "$eventLeaderEmail", "$EventLeaderEmail", ""]},
                    "leader1": {"$ifNull": ["$leader1", "$Leader @1", ""]},
                    "leader12": {"$ifNull": ["$Leader at 12", "$Leader @12", "$leader12", "$Leader12", ""]},
                    "day": {"$ifNull": ["$Day", "$day", ""]},
                    "date": 1,
                    "location": {"$ifNull": ["$Location", "$location", ""]},
                    "attendees": {"$ifNull": ["$attendees", []]},
                    "persistent_attendees": {"$ifNull": ["$persistent_attendees", []]},
                    "hasPersonSteps": {"$ifNull": ["$hasPersonSteps", True]},
                    "status": {"$ifNull": ["$status", "$Status", "incomplete"]},
                    "_is_overdue": {"$literal": True},
                    "is_recurring": {"$ifNull": ["$is_recurring", True]},
                    "week_identifier": 1,
                    "original_event_id": {"$toString": "$_id"}
                }
            }
        ]
        
        cursor = events_collection.aggregate(pipeline)
        result = await cursor.to_list(100)
        print(f"[DASHBOARD] Successfully fetched {len(result)} overdue cells")
        return result
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch overdue cells: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty list instead of raising - allows dashboard to load with partial data
        return []


async def _fetch_tasks_by_user(start_date: datetime, end_date: datetime):
    """
    Fetch and aggregate tasks by assigned user for the given period.
    
    Groups tasks by user and calculates:
    - Total tasks
    - Completed tasks (excluding certain task types)
    - Tasks due in period
    - Tasks completed in period
    
    Returns empty list if fetch fails.
    """
    try:
        pipeline = [
            {
                "$match": {
                    # Match tasks with dates in the period
                    "$or": [
                        {"followup_date": {"$gte": start_date, "$lte": end_date}},
                        {"completedAt": {"$gte": start_date, "$lte": end_date}},
                        {"createdAt": {"$gte": start_date, "$lte": end_date}}
                    ]
                }
            },
            {
                "$addFields": {
                    # Normalize task type
                    "task_type_label": {"$ifNull": ["$taskType", "Uncategorized"]},
                    
                    # Check if this task type should be excluded from completed counts
                    "is_excluded_type": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$taskType", None]},
                                    {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    
                    # Check if task is completed (and not an excluded type)
                    "is_completed": {
                        "$and": [
                            {
                                "$in": [
                                    {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                    ["completed", "done", "closed", "finished"]
                                ]
                            },
                            {
                                "$not": {
                                    "$and": [
                                        {"$ne": ["$taskType", None]},
                                        {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                    ]
                                }
                            }
                        ]
                    },
                    
                    # Check if task was completed in this period
                    "completed_in_period": {
                        "$and": [
                            {"$ne": ["$completedAt", None]},
                            {"$gte": ["$completedAt", start_date]},
                            {"$lte": ["$completedAt", end_date]},
                            {
                                "$in": [
                                    {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                    ["completed", "done", "closed", "finished"]
                                ]
                            },
                            {
                                "$not": {
                                    "$and": [
                                        {"$ne": ["$taskType", None]},
                                        {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                    ]
                                }
                            }
                        ]
                    },
                    
                    # Check if task is due in this period
                    "is_due_in_period": {
                        "$and": [
                            {"$ne": ["$followup_date", None]},
                            {"$gte": ["$followup_date", start_date]},
                            {"$lte": ["$followup_date", end_date]}
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$assignedfor",
                    # Collect all tasks for this user
                    "tasks": {
                        "$push": {
                            "_id": "$_id",
                            "name": "$name",
                            "taskType": "$taskType",
                            "task_type_label": "$task_type_label",
                            "followup_date": "$followup_date",
                            "due_date": "$followup_date",
                            "completedAt": "$completedAt",
                            "createdAt": "$createdAt",
                            "status": "$status",
                            "assignedfor": "$assignedfor",
                            "type": "$type",
                            "contacted_person": "$contacted_person",
                            "isRecurring": {"$cond": [{"$ifNull": ["$recurring_day", False]}, True, False]},
                            "priority": "$priority",
                            "is_completed": "$is_completed",
                            "is_due_in_period": "$is_due_in_period",
                            "completed_in_period": "$completed_in_period",
                            "is_excluded_type": "$is_excluded_type",
                            "description": "$description"
                        }
                    },
                    # Calculate aggregate counts
                    "total_tasks": {"$sum": 1},
                    "completed_tasks": {"$sum": {"$cond": ["$is_completed", 1, 0]}},
                    "completed_in_period": {"$sum": {"$cond": ["$completed_in_period", 1, 0]}},
                    "due_in_period": {"$sum": {"$cond": ["$is_due_in_period", 1, 0]}},
                    "task_type_counts": {
                        "$push": {
                            "task_type": "$task_type_label",
                            "is_completed": "$is_completed",
                            "completed_in_period": "$completed_in_period",
                            "is_due_in_period": "$is_due_in_period",
                            "is_excluded_type": "$is_excluded_type"
                        }
                    }
                }
            },
            {"$match": {"total_tasks": {"$gt": 0}}},
            {"$sort": {"_id": 1}}
        ]
        
        cursor = tasks_collection.aggregate(pipeline)
        result = await cursor.to_list(None)
        print(f"[DASHBOARD] Successfully fetched {len(result)} task groups")
        return result
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch tasks by user: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty list instead of raising
        return []


async def _fetch_users(limit: int):
    """
    Fetch user details with limit.
    
    Returns empty list if fetch fails.
    """
    try:
        cursor = users_collection.find(
            {},
            {"_id": 1, "email": 1, "name": 1, "surname": 1}
        ).limit(limit)
        result = await cursor.to_list(limit)
        print(f"[DASHBOARD] Successfully fetched {len(result)} users")
        return result
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch users: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty list instead of raising
        return []


async def _fetch_task_types():
    """
    Fetch all available task type names from the database.
    
    Returns empty list if fetch fails.
    """
    try:
        cursor = tasktypes_collection.find({}, {"name": 1})
        task_types_list = await cursor.to_list(length=None)
        task_types = [tt.get("name") for tt in task_types_list if tt.get("name")]
        print(f"[DASHBOARD] Found {len(task_types)} task types: {task_types}")
        return task_types
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch task types: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty list instead of raising
        return []


# ============================================================================
# DATA PROCESSING FUNCTIONS (with error handling)
# ============================================================================

def _format_cells(cells: list) -> list:
    """
    Convert cell ObjectIds and dates to strings for JSON serialization.
    Handles errors gracefully by skipping malformed cells.
    """
    formatted = []
    for cell in cells:
        try:
            cell["_id"] = str(cell["_id"])
            if isinstance(cell.get("date"), datetime):
                cell["date"] = cell["date"].isoformat()
            formatted.append(cell)
        except Exception as e:
            print(f"[WARNING] Failed to format cell {cell.get('_id', 'unknown')}: {str(e)}")
            # Skip this cell and continue with others
            continue
    
    return formatted


def _create_user_map(users: list) -> dict:
    """
    Create a lookup dictionary for user information.
    Maps both email and user ID to user details.
    Handles errors by skipping malformed user records.
    """
    user_map = {}
    
    for user in users:
        try:
            uid = str(user["_id"])
            email = user.get("email", "").lower()
            
            # Handle missing email
            if not email:
                print(f"[WARNING] User {uid} has no email, skipping")
                continue
            
            full_name = f"{user.get('name', '')} {user.get('surname', '')}".strip() or email.split("@")[0]
            
            user_info = {"_id": uid, "email": email, "fullName": full_name}
            user_map[email] = user_info
            user_map[uid] = user_info
            
        except Exception as e:
            print(f"[WARNING] Failed to process user {user.get('_id', 'unknown')}: {str(e)}")
            # Skip this user and continue
            continue
    
    return user_map


def _process_task_groups(task_groups: list, user_map: dict) -> tuple:
    """
    Process task groups to calculate statistics and format data.
    
    Returns:
        - grouped_tasks: List of tasks grouped by user with counts
        - task_type_stats: Statistics breakdown by task type
        - global_stats: Overall task statistics
    """
    grouped_tasks = []
    task_type_stats = {}
    
    # Global counters
    global_stats = {
        "total_tasks": 0,
        "completed_tasks": 0,
        "completed_in_period": 0,
        "due_in_period": 0,
        "incomplete_due": 0
    }
    
    for task_group in task_groups:
        try:
            # Get user information
            email = task_group["_id"] or "unassigned@example.com"
            user_info = user_map.get(email.lower(), {
                "_id": f"unknown_{email}",
                "email": email,
                "fullName": email.split("@")[0]
            })
            
            tasks_list = task_group.get("tasks", [])
            
            # Log task types for debugging
            task_types_in_group = {t.get("taskType") for t in tasks_list if t.get("taskType")}
            if task_types_in_group:
                print(f"[DASHBOARD] Task types for {email}: {task_types_in_group}")
            
            # Format tasks and update statistics
            formatted_tasks = []
            for task in tasks_list:
                try:
                    # Convert ObjectId to string
                    task["_id"] = str(task["_id"])
                    
                    # Convert datetime fields to ISO strings
                    for date_field in ["followup_date", "due_date", "completedAt", "createdAt"]:
                        if isinstance(task.get(date_field), datetime):
                            task[date_field] = task[date_field].isoformat()
                    
                    # Update task type statistics
                    _update_task_type_stats(task, task_type_stats)
                    formatted_tasks.append(task)
                    
                except Exception as e:
                    print(f"[WARNING] Failed to format task {task.get('_id', 'unknown')}: {str(e)}")
                    # Skip this task and continue
                    continue
            
            # Calculate counts for this user
            total_for_user = task_group.get("total_tasks", 0)
            completed_all = task_group.get("completed_tasks", 0)
            completed_in_period = task_group.get("completed_in_period", 0)
            due_in_period = task_group.get("due_in_period", 0)
            incomplete_due = sum(
                1 for t in formatted_tasks 
                if t.get("is_due_in_period") and not t.get("is_completed")
            )
            
            # Update global statistics
            global_stats["total_tasks"] += total_for_user
            global_stats["completed_tasks"] += completed_all
            global_stats["completed_in_period"] += completed_in_period
            global_stats["due_in_period"] += due_in_period
            global_stats["incomplete_due"] += incomplete_due
            
            # Add to grouped tasks
            grouped_tasks.append({
                "user": user_info,
                "tasks": formatted_tasks,
                "totalCount": total_for_user,
                "completedCount": completed_all,
                "incompleteCount": total_for_user - completed_all,
                "dueInPeriodCount": due_in_period,
                "completedInPeriodCount": completed_in_period,
                "incompleteDueInPeriodCount": incomplete_due,
                "taskTypes": list({t.get("taskType") or "Uncategorized" for t in formatted_tasks})
            })
            
        except Exception as e:
            print(f"[WARNING] Failed to process task group for {task_group.get('_id', 'unknown')}: {str(e)}")
            # Skip this group and continue
            continue
    
    # Sort by user name
    try:
        grouped_tasks.sort(key=lambda x: x["user"]["fullName"].lower())
    except Exception as e:
        print(f"[WARNING] Failed to sort grouped tasks: {str(e)}")
        # Continue with unsorted list
    
    # Log task type statistics
    print(f"[DASHBOARD] Task type breakdown:")
    for task_type, stats in task_type_stats.items():
        print(f"  - {task_type}: total={stats['total']}, completed={stats['completed']}, "
              f"excluded={stats.get('is_excluded', False)}")
    
    return grouped_tasks, task_type_stats, global_stats


def _update_task_type_stats(task: dict, task_type_stats: dict):
    """
    Update task type statistics with data from a single task.
    Handles missing fields gracefully.
    """
    try:
        task_type = task.get("taskType") or "Uncategorized"
        is_excluded = task.get("is_excluded_type", False)
        
        # Initialize stats for this task type if not exists
        if task_type not in task_type_stats:
            task_type_stats[task_type] = {
                "total": 0,
                "completed": 0,
                "completed_in_period": 0,
                "due_in_period": 0,
                "incomplete_due": 0,
                "is_excluded": is_excluded
            }
        
        # Update counts
        task_type_stats[task_type]["total"] += 1
        if task.get("is_completed"):
            task_type_stats[task_type]["completed"] += 1
        if task.get("completed_in_period"):
            task_type_stats[task_type]["completed_in_period"] += 1
        if task.get("is_due_in_period"):
            task_type_stats[task_type]["due_in_period"] += 1
        if task.get("is_due_in_period") and not task.get("is_completed"):
            task_type_stats[task_type]["incomplete_due"] += 1
            
    except Exception as e:
        print(f"[WARNING] Failed to update task type stats: {str(e)}")
        # Continue without updating stats for this task


def _format_users(users: list) -> list:
    """
    Format user list for API response.
    Handles errors by skipping malformed users.
    """
    formatted_users = []
    
    for u in users:
        try:
            formatted_users.append({
                "_id": str(u["_id"]),
                "email": u.get("email", ""),
                "name": u.get("name", ""),
                "surname": u.get("surname", ""),
                "fullName": f"{u.get('name', '')} {u.get('surname', '')}".strip() 
                           or u.get("email", "").split("@")[0]
            })
        except Exception as e:
            print(f"[WARNING] Failed to format user {u.get('_id', 'unknown')}: {str(e)}")
            # Skip this user
            continue
    
    return formatted_users


# ============================================================================
# OVERVIEW BUILDING FUNCTION (with error handling)
# ============================================================================

def _build_overview(
    formatted_cells: list,
    global_stats: dict,
    task_type_stats: dict,
    grouped_tasks: list,
    users: list,
    all_task_types: list
) -> dict:
    """
    Build the overview statistics object from all collected data.
    Uses safe defaults if any calculation fails.
    """
    try:
        # Calculate completion rates with division by zero protection
        completion_rate_due = (
            round((global_stats["completed_in_period"] / global_stats["due_in_period"] * 100), 2)
            if global_stats.get("due_in_period", 0) > 0 else 0
        )
        
        completion_rate_overall = (
            round((global_stats["completed_tasks"] / global_stats["total_tasks"] * 100), 2)
            if global_stats.get("total_tasks", 0) > 0 else 0
        )
        
        # Get consolidation task statistics
        consolidation_stats = task_type_stats.get("consolidation", {})
        consolidation_completion_rate = (
            round((consolidation_stats.get("completed", 0) / consolidation_stats.get("total", 1) * 100), 2)
            if consolidation_stats.get("total", 0) > 0 else 0
        )
        
        # Safely calculate attendance
        total_attendance = 0
        try:
            total_attendance = sum(len(c.get("attendees", [])) for c in formatted_cells)
        except Exception as e:
            print(f"[WARNING] Failed to calculate total attendance: {str(e)}")
        
        # Safely count people behind
        people_behind = 0
        try:
            people_behind = len([g for g in grouped_tasks if g.get("incompleteDueInPeriodCount", 0) > 0])
        except Exception as e:
            print(f"[WARNING] Failed to count people behind: {str(e)}")
        
        return {
            # Cell statistics
            "total_attendance": total_attendance,
            "outstanding_cells": len(formatted_cells),
            
            # Task counts
            "outstanding_tasks": global_stats.get("incomplete_due", 0),
            "tasks_due_in_period": global_stats.get("due_in_period", 0),
            "tasks_completed_in_period": global_stats.get("completed_in_period", 0),
            "total_tasks_in_period": global_stats.get("total_tasks", 0),
            "total_tasks_completed": global_stats.get("completed_tasks", 0),
            "total_tasks_incomplete": global_stats.get("total_tasks", 0) - global_stats.get("completed_tasks", 0),
            
            # Consolidation-specific metrics
            "consolidation_tasks": consolidation_stats.get("total", 0),
            "consolidation_completed": consolidation_stats.get("completed", 0),
            "consolidation_completed_in_period": consolidation_stats.get("completed_in_period", 0),
            
            # User metrics
            "people_behind": people_behind,
            "total_users": len(users),
            "users_with_tasks": len(grouped_tasks),
            "users_without_tasks": len(users) - len(grouped_tasks),
            
            # Completion rates
            "completion_rate_due_tasks": completion_rate_due,
            "completion_rate_overall": completion_rate_overall,
            "consolidation_completion_rate": consolidation_completion_rate,
            
            # Task type information
            "task_type_breakdown": task_type_stats,
            "available_task_types": all_task_types,
            "task_types_found": list(task_type_stats.keys()),
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "total_unique_task_types": len(task_type_stats),
            
            # Note about excluded types
            "note": f"'{', '.join(EXCLUDED_TASK_TYPES_FROM_COMPLETED)}' task types are excluded from completed counts"
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to build overview, returning defaults: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Return safe defaults if overview building fails
        return {
            "total_attendance": 0,
            "outstanding_cells": 0,
            "outstanding_tasks": 0,
            "tasks_due_in_period": 0,
            "tasks_completed_in_period": 0,
            "total_tasks_in_period": 0,
            "total_tasks_completed": 0,
            "total_tasks_incomplete": 0,
            "consolidation_tasks": 0,
            "consolidation_completed": 0,
            "consolidation_completed_in_period": 0,
            "people_behind": 0,
            "total_users": len(users),
            "users_with_tasks": 0,
            "users_without_tasks": len(users),
            "completion_rate_due_tasks": 0,
            "completion_rate_overall": 0,
            "consolidation_completion_rate": 0,
            "task_type_breakdown": {},
            "available_task_types": [],
            "task_types_found": [],
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "total_unique_task_types": 0,
            "note": "Error occurred while building overview"
        }


async def get_dashboard_quick_stats(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get quick dashboard statistics for a given time period.
    
    Excludes task types from EXCLUDED_TASK_TYPES_FROM_COMPLETED when counting completed tasks.
    Provides a lightweight summary without fetching full task/user details.
    
    Returns:
        - Task counts (total, due, completed)
        - Consolidation task metrics
        - Overdue cells count
        - Completion rates
        - Task type breakdown
    """
    try:
        # Log request details
        print(f"[QUICK STATS] Period: {period}, User: {current_user.get('email')}")
        print(f"[QUICK STATS] Excluding types: {EXCLUDED_TASK_TYPES_FROM_COMPLETED}")
        
        # Calculate date range
        start, end = get_period_range(period)
        
        # Fetch all statistics in parallel for better performance
        (
            task_counts,
            consolidation_counts,
            overdue_cells_count,
            task_type_stats,
            excluded_counts
        ) = await asyncio.gather(
            _fetch_task_counts(start, end),
            _fetch_consolidation_counts(start, end),
            _fetch_overdue_cells_count(end),
            _fetch_task_type_breakdown(start, end),
            _fetch_excluded_task_counts(),
            return_exceptions=True
        )
        
        # Handle errors from parallel operations
        task_counts = _handle_fetch_error(task_counts, "task counts", {
            "total_tasks": 0, "tasks_due": 0, "tasks_completed_in_period": 0, "total_completed": 0
        })
        consolidation_counts = _handle_fetch_error(consolidation_counts, "consolidation counts", {
            "total": 0, "completed": 0, "completed_in_period": 0
        })
        overdue_cells_count = _handle_fetch_error(overdue_cells_count, "overdue cells", 0)
        task_type_stats = _handle_fetch_error(task_type_stats, "task type stats", {})
        excluded_counts = _handle_fetch_error(excluded_counts, "excluded counts", {
            "no_answer": 0, "awaiting_call": 0
        })
        
        # Build and return the response
        return _build_quick_stats_response(
            period=period,
            start=start,
            end=end,
            task_counts=task_counts,
            consolidation_counts=consolidation_counts,
            overdue_cells_count=overdue_cells_count,
            task_type_stats=task_type_stats,
            excluded_counts=excluded_counts
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[CRITICAL ERROR] Quick stats failed: {str(e)}")
        raise HTTPException(500, f"Error fetching quick stats: {str(e)}")


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

async def _fetch_task_counts(start_date: datetime, end_date: datetime) -> dict:
    """
    Fetch basic task counts for the period.
    
    Returns:
        - total_tasks: All tasks in period
        - tasks_due: Tasks due in period
        - tasks_completed_in_period: Tasks completed in period (excluding certain types)
        - total_completed: All completed tasks (excluding certain types)
    """
    try:
        # Define the period filter
        period_filter = {
            "$or": [
                {"followup_date": {"$gte": start_date, "$lte": end_date}},
                {"completedAt": {"$gte": start_date, "$lte": end_date}},
                {"createdAt": {"$gte": start_date, "$lte": end_date}}
            ]
        }
        
        # Run all counts in parallel
        total_tasks, tasks_due, tasks_completed_in_period, total_completed = await asyncio.gather(
            # Count all tasks in period
            tasks_collection.count_documents(period_filter),
            
            # Count tasks due in period
            tasks_collection.count_documents({
                "followup_date": {"$gte": start_date, "$lte": end_date}
            }),
            
            # Count tasks completed in period (excluding certain types)
            tasks_collection.count_documents({
                "completedAt": {"$gte": start_date, "$lte": end_date},
                "status": {"$in": ["completed", "done", "closed", "finished"]},
                "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
            }),
            
            # Count all completed tasks (excluding certain types)
            tasks_collection.count_documents({
                "status": {"$in": ["completed", "done", "closed", "finished"]},
                "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
            })
        )
        
        print(f"[QUICK STATS] Task counts - Total: {total_tasks}, Due: {tasks_due}, "
              f"Completed in period: {tasks_completed_in_period}, Total completed: {total_completed}")
        
        return {
            "total_tasks": total_tasks,
            "tasks_due": tasks_due,
            "tasks_completed_in_period": tasks_completed_in_period,
            "total_completed": total_completed
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch task counts: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def _fetch_consolidation_counts(start_date: datetime, end_date: datetime) -> dict:
    """
    Fetch consolidation-specific task counts.
    
    Returns:
        - total: Total consolidation tasks
        - completed: Total completed consolidation tasks
        - completed_in_period: Consolidation tasks completed in period
    """
    try:
        # Run all counts in parallel
        total, completed, completed_in_period = await asyncio.gather(
            # Total consolidation tasks
            tasks_collection.count_documents({
                "taskType": "consolidation"
            }),
            
            # Completed consolidation tasks
            tasks_collection.count_documents({
                "taskType": "consolidation",
                "status": {"$in": ["completed", "done", "closed", "finished"]}
            }),
            
            # Consolidation tasks completed in period
            tasks_collection.count_documents({
                "completedAt": {"$gte": start_date, "$lte": end_date},
                "status": {"$in": ["completed", "done", "closed", "finished"]},
                "taskType": "consolidation"
            })
        )
        
        print(f"[QUICK STATS] Consolidation - Total: {total}, Completed: {completed}, "
              f"Completed in period: {completed_in_period}")
        
        return {
            "total": total,
            "completed": completed,
            "completed_in_period": completed_in_period
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch consolidation counts: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def _fetch_overdue_cells_count(end_date: datetime) -> int:
    """
    Count overdue or incomplete cells up to the end date.
    Handles multiple field name variations.
    """
    try:
        count = await events_collection.count_documents({
            # Match cells using various field name patterns
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
            ],
            "date": {"$lte": end_date},
            # Match incomplete or overdue cells
            "$or": [
                {"status": "incomplete"},
                {"status": {"$exists": False}},
                {"Status": "Incomplete"},
                {"_is_overdue": True}
            ]
        })
        
        print(f"[QUICK STATS] Overdue cells: {count}")
        return count
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch overdue cells count: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def _fetch_task_type_breakdown(start_date: datetime, end_date: datetime) -> dict:
    """
    Fetch task statistics grouped by task type.
    
    Returns dictionary with task type as key and stats as value:
        - total: Total tasks of this type
        - completed: Completed tasks (excluding certain types)
        - completed_in_period: Tasks completed in period
        - due_in_period: Tasks due in period
        - is_excluded: Whether this type is excluded from completed counts
        - completion_rate: Overall completion rate
        - completion_rate_in_period: Completion rate for period
    """
    try:
        pipeline = [
            {
                "$match": {
                    # Match tasks in the period
                    "$or": [
                        {"followup_date": {"$gte": start_date, "$lte": end_date}},
                        {"completedAt": {"$gte": start_date, "$lte": end_date}},
                        {"createdAt": {"$gte": start_date, "$lte": end_date}}
                    ]
                }
            },
            {
                "$addFields": {
                    # Normalize task type
                    "task_type": {"$ifNull": ["$taskType", "Uncategorized"]},
                    
                    # Check if this type is excluded
                    "is_excluded": {
                        "$and": [
                            {"$ne": ["$taskType", None]},
                            {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                        ]
                    },
                    
                    # Check if task is completed (and not excluded type)
                    "is_completed": {
                        "$and": [
                            {
                                "$in": [
                                    {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                    ["completed", "done", "closed", "finished"]
                                ]
                            },
                            {
                                "$not": {
                                    "$and": [
                                        {"$ne": ["$taskType", None]},
                                        {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                    ]
                                }
                            }
                        ]
                    },
                    
                    # Check if completed in period
                    "completed_in_period": {
                        "$and": [
                            {"$ne": ["$completedAt", None]},
                            {"$gte": ["$completedAt", start_date]},
                            {"$lte": ["$completedAt", end_date]},
                            {
                                "$in": [
                                    {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                    ["completed", "done", "closed", "finished"]
                                ]
                            },
                            {
                                "$not": {
                                    "$and": [
                                        {"$ne": ["$taskType", None]},
                                        {"$in": ["$taskType", EXCLUDED_TASK_TYPES_FROM_COMPLETED]}
                                    ]
                                }
                            }
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$task_type",
                    "total": {"$sum": 1},
                    "completed": {"$sum": {"$cond": ["$is_completed", 1, 0]}},
                    "completed_in_period": {"$sum": {"$cond": ["$completed_in_period", 1, 0]}},
                    "due_in_period": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$followup_date", None]},
                                        {"$gte": ["$followup_date", start_date]},
                                        {"$lte": ["$followup_date", end_date]}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "is_excluded": {"$first": "$is_excluded"}
                }
            },
            {"$sort": {"total": -1}}
        ]
        
        cursor = tasks_collection.aggregate(pipeline)
        task_type_stats_raw = await cursor.to_list(None)
        
        # Process and format the statistics
        task_type_stats = {}
        for stat in task_type_stats_raw:
            task_type = stat["_id"] or "Uncategorized"
            total = stat["total"]
            completed = stat["completed"]
            completed_in_period = stat["completed_in_period"]
            due_in_period = stat["due_in_period"]
            is_excluded = stat["is_excluded"]
            
            task_type_stats[task_type] = {
                "total": total,
                "completed": completed,
                "completed_in_period": completed_in_period,
                "due_in_period": due_in_period,
                "is_excluded": is_excluded,
                "completion_rate": round((completed / total * 100), 2) if total > 0 else 0,
                "completion_rate_in_period": round((completed_in_period / due_in_period * 100), 2) if due_in_period > 0 else 0
            }
        
        print(f"[QUICK STATS] Found {len(task_type_stats)} task types")
        return task_type_stats
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch task type breakdown: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def _fetch_excluded_task_counts() -> dict:
    """
    Fetch counts for excluded task types for debugging purposes.
    
    Returns counts for:
        - no_answer: Tasks with "no answer" type
        - awaiting_call: Tasks with "Awaiting Call" type
    """
    try:
        # Run counts in parallel
        no_answer_count, awaiting_call_count = await asyncio.gather(
            tasks_collection.count_documents({
                "taskType": "no answer",
                "status": {"$in": ["completed", "done", "closed", "finished"]}
            }),
            tasks_collection.count_documents({
                "taskType": "Awaiting Call",
                "status": {"$in": ["completed", "done", "closed", "finished"]}
            })
        )
        
        print(f"[QUICK STATS] Excluded task counts - no answer: {no_answer_count}, "
              f"Awaiting Call: {awaiting_call_count}")
        
        return {
            "no_answer": no_answer_count,
            "awaiting_call": awaiting_call_count
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch excluded task counts: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _handle_fetch_error(result, operation_name: str, default_value):
    """
    Handle errors from parallel fetch operations.
    
    If result is an exception, log it and return default value.
    Otherwise return the result as-is.
    """
    if isinstance(result, Exception):
        print(f"[ERROR] Failed to fetch {operation_name}: {result}")
        import traceback
        traceback.print_exception(type(result), result, result.__traceback__)
        return default_value
    return result


def _build_quick_stats_response(
    period: str,
    start: datetime,
    end: datetime,
    task_counts: dict,
    consolidation_counts: dict,
    overdue_cells_count: int,
    task_type_stats: dict,
    excluded_counts: dict
) -> dict:
    """
    Build the final response object from all collected statistics.
    
    Includes safe calculations for completion rates with division by zero protection.
    """
    try:
        # Calculate completion rates safely
        completion_rate_due = (
            round((task_counts["tasks_completed_in_period"] / task_counts["tasks_due"] * 100), 2)
            if task_counts.get("tasks_due", 0) > 0 else 0
        )
        
        overall_completion_rate = (
            round((task_counts["total_completed"] / task_counts["total_tasks"] * 100), 2)
            if task_counts.get("total_tasks", 0) > 0 else 0
        )
        
        consolidation_completion_rate = (
            round((consolidation_counts["completed"] / consolidation_counts["total"] * 100), 2)
            if consolidation_counts.get("total", 0) > 0 else 0
        )
        
        return {
            "period": period,
            "date_range": {
                "start": start.date().isoformat(),
                "end": end.date().isoformat()
            },
            
            # Task counts
            "taskCount": task_counts.get("total_tasks", 0),
            "tasksDueInPeriod": task_counts.get("tasks_due", 0),
            "tasksCompletedInPeriod": task_counts.get("tasks_completed_in_period", 0),
            "totalCompletedTasks": task_counts.get("total_completed", 0),
            
            # Consolidation metrics
            "consolidationTasks": consolidation_counts.get("total", 0),
            "consolidationCompleted": consolidation_counts.get("completed", 0),
            "consolidationCompletedInPeriod": consolidation_counts.get("completed_in_period", 0),
            "consolidationCompletionRate": consolidation_completion_rate,
            
            # Cell metrics
            "overdueCells": overdue_cells_count,
            
            # Completion rates
            "completionRateDueTasks": completion_rate_due,
            "overallCompletionRate": overall_completion_rate,
            
            # Task type breakdown
            "taskTypeBreakdown": task_type_stats,
            "totalTaskTypesFound": len(task_type_stats),
            "excludedTaskTypes": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            
            # Metadata
            "timestamp": datetime.utcnow().isoformat(),
            "note": f"'{', '.join(EXCLUDED_TASK_TYPES_FROM_COMPLETED)}' task types are excluded from completed counts",
            
            # Debug info (excluded task counts)
            "debug": {
                "excluded_task_counts": excluded_counts
            }
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to build response, returning defaults: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Return safe defaults if response building fails
        return {
            "period": period,
            "date_range": {
                "start": start.date().isoformat(),
                "end": end.date().isoformat()
            },
            "taskCount": 0,
            "tasksDueInPeriod": 0,
            "tasksCompletedInPeriod": 0,
            "totalCompletedTasks": 0,
            "consolidationTasks": 0,
            "consolidationCompleted": 0,
            "consolidationCompletedInPeriod": 0,
            "consolidationCompletionRate": 0,
            "overdueCells": 0,
            "completionRateDueTasks": 0,
            "overallCompletionRate": 0,
            "taskTypeBreakdown": {},
            "totalTaskTypesFound": 0,
            "excludedTaskTypes": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp": datetime.utcnow().isoformat(),
            "note": "Error occurred while building response",
            "debug": {"error": str(e)}
        }


async def get_outstanding_items():
    """Get detailed outstanding cells and tasks for the dashboard"""
    try:
        # Get outstanding cells with details
        outstanding_cells = await events_collection.find({
            "eventType": "Cell",
            "status": {"$nin": ["completed", "closed", "done"]}
        }).to_list(length=None)
       
        # Get outstanding tasks with details
        outstanding_tasks = await tasks_collection.find({
            "status": {"$nin": ["completed", "closed", "done"]}
        }).to_list(length=None)
       
        # Format cells data
        cells_data = []
        for cell in outstanding_cells:
            cells_data.append({
                "name": cell.get("eventLeader", "Unknown Leader"),
                "location": cell.get("location", "Unknown Location"),
                "title": cell.get("eventName", "Untitled Cell"),
                "date": cell.get("date"),
                "status": cell.get("status", "pending")
            })
       
        # Format tasks data
        tasks_data = []
        for task in outstanding_tasks:
            tasks_data.append({
                "name": task.get("assignedTo", task.get("eventLeader", "Unassigned")),
                "email": task.get("email", ""),
                "title": task.get("taskName", task.get("title", "Untitled Task")),
                "count": task.get("priority", 1),  # Using priority as count or you can count tasks per person
                "dueDate": task.get("dueDate", task.get("date")),
                "status": task.get("status", "pending")
            })
       
        return {
            "outstanding_cells": cells_data,
            "outstanding_tasks": tasks_data
        }
       
    except Exception as e:
        print(f"Error in outstanding items: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_people_capture_stats() -> dict:
    """
    Return statistics showing how many people each team member has captured.

    Optimizations:
    - Uses MongoDB aggregation only (no Python-side grouping)
    - Limits projected fields to reduce payload size
    - Uses async Motor aggregation
    """

    try:
        # client = get_database_client()
        # db = client[DB_NAME]

        pipeline = [
            # Only documents that were actually captured by someone
            {
                "$match": {
                    "captured_by": {"$exists": True, "$ne": None}
                }
            },

            # Group by capturer
            {
                "$group": {
                    "_id": "$captured_by",
                    "people_captured_count": {"$sum": 1},

                    # Keep minimal captured person info
                    "captured_people": {
                        "$push": {
                            "name": "$fullName",
                            "email": "$email",
                            "capture_date": "$created_date"
                        }
                    }
                }
            },

            # Join capturer details
            {
                "$lookup": {
                    "from": "people",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "capturer"
                }
            },

            # Flatten capturer array (may be empty)
            {
                "$unwind": {
                    "path": "$capturer",
                    "preserveNullAndEmptyArrays": True
                }
            },

            # Shape final output
            {
                "$project": {
                    "_id": 0,
                    "capturer_id": "$_id",
                    "capturer_name": {
                        "$ifNull": [
                            "$capturer.fullName",
                            "$capturer.name",
                            "Unknown Capturer"
                        ]
                    },
                    "capturer_email": {
                        "$ifNull": ["$capturer.email", "No email"]
                    },
                    "people_captured_count": 1,
                    "captured_people": 1
                }
            },

            # Most active capturers first
            {
                "$sort": {"people_captured_count": -1}
            }
        ]

        # Execute aggregation asynchronously
        results = await db.people.aggregate(pipeline).to_list(length=None)

        if not results:
            return {
                "capture_stats": [],
                "total_capturers": 0,
                "total_people_captured": 0,
                "message": "No capture data found"
            }

        total_people_captured = sum(
            item.get("people_captured_count", 0) for item in results
        )

        return {
            "capture_stats": results,
            "total_capturers": len(results),
            "total_people_captured": total_people_captured,
            "message": (
                f"Found {len(results)} team members who captured "
                f"{total_people_captured} people total"
            )
        }

    except Exception as exc:
        logger.exception("Failed to fetch people capture statistics")

        # Avoid leaking internal errors to clients
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch capture statistics"
        ) from exc