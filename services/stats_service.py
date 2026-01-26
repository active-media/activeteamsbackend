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
from database import events_collection, tasks_collection, people_collection, consolidations_collection

from fastapi import Body, FastAPI, HTTPException, Query, Path, Request ,  Depends, BackgroundTasks
# Placeholder functions - these need to be extracted from main.py

@app.get("/stats/overview")
async def get_stats_overview(period: str = "monthly"):
    """Get overall statistics for the dashboard with time period filtering"""
    try:
        # Calculate date range based on period
        now = datetime.utcnow()
        if period == "daily":
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif period == "weekly":
            start_date = now - timedelta(days=now.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
        else:  # monthly
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 12:
                end_date = now.replace(year=now.year + 1, month=1, day=1)
            else:
                end_date = now.replace(month=now.month + 1, day=1)

        # Count outstanding cells (cells with status != "completed" or "closed")
        # Assuming cells are events with eventType "Cell" and have a status field
        outstanding_cells = await events_collection.count_documents({
            "eventType": "Cell",
            "status": {"$nin": ["completed", "closed", "done"]}
        })
       
        # Count outstanding tasks from tasks collection
        # Assuming tasks have a status field and are not completed/closed
        outstanding_tasks = await tasks_collection.count_documents({
            "status": {"$nin": ["completed", "closed", "done"]}
        })
       
        # Get total people (assuming you have a people collection)
        total_people = await people_collection.count_documents({})
       
        # Get events for the period to calculate attendance and growth
        # Only include non-cell events for attendance calculation
        period_events = await events_collection.find({
            "date": {"$gte": start_date, "$lt": end_date},
            "status": {"$in": ["completed", "closed"]},
            "eventType": {"$ne": "Cell"}  # Exclude cells from attendance calculation
        }).to_list(length=None)
       
        # Calculate total attendance for the period
        total_attendance = sum(event.get("total_attendance", 0) for event in period_events)
       
        # Calculate previous period for growth comparison
        if period == "daily":
            prev_start = start_date - timedelta(days=1)
            prev_end = start_date
        elif period == "weekly":
            prev_start = start_date - timedelta(days=7)
            prev_end = start_date
        else:  # monthly
            if start_date.month == 1:
                prev_start = start_date.replace(year=start_date.year - 1, month=12)
            else:
                prev_start = start_date.replace(month=start_date.month - 1)
            prev_end = start_date
       
        # Get previous period attendance (exclude cells)
        prev_events = await events_collection.find({
            "date": {"$gte": prev_start, "$lt": prev_end},
            "status": {"$in": ["completed", "closed"]},
            "eventType": {"$ne": "Cell"}
        }).to_list(length=None)
       
        prev_attendance = sum(event.get("total_attendance", 0) for event in prev_events)
       
        # Calculate growth rate
        if prev_attendance > 0:
            growth_rate = ((total_attendance - prev_attendance) / prev_attendance) * 100
        else:
            growth_rate = 100 if total_attendance > 0 else 0
       
        # Calculate weekly/daily attendance breakdown (exclude cells)
        attendance_breakdown = {}
        for event in period_events:
            if event.get("date"):
                event_date = event["date"]
                if period == "daily":
                    # Group by hour for daily view
                    hour = event_date.hour
                    key = f"{hour:02d}:00"
                elif period == "weekly":
                    # Group by day name for weekly view
                    key = event_date.strftime("%A")
                else:
                    # Group by week number for monthly view
                    week_num = event_date.isocalendar()[1]
                    key = f"Week {week_num}"
               
                if key not in attendance_breakdown:
                    attendance_breakdown[key] = 0
                attendance_breakdown[key] += event.get("total_attendance", 0)
       
        return {
            "outstanding_cells": outstanding_cells,
            "outstanding_tasks": outstanding_tasks,  # Changed from outstanding_events to outstanding_tasks
            "total_people": total_people,
            "total_attendance": total_attendance,
            "growth_rate": round(growth_rate, 1),
            "attendance_breakdown": attendance_breakdown,
            "period": period
        }
    except Exception as e:
        print(f"Error in stats overview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/dashboard-comprehensive")
async def get_dashboard_comprehensive(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    """
    COMPREHENSIVE DASHBOARD
    Counts completed tasks EXCLUDING "no answer" and "Awaiting Call"
    """
    try:
        print(f"[DASHBOARD] Comprehensive stats requested - Period: {period}, User: {current_user.get('email')}")
        print(f"[DASHBOARD] Excluding task types from completed count: {EXCLUDED_TASK_TYPES_FROM_COMPLETED}")

        
        start, end = get_period_range(period)
        start_date_str = start.date().isoformat()
        end_date_str = end.date().isoformat()
        print(f"[DASHBOARD] Date range: {start_date_str} → {end_date_str}")

        
        task_types_cursor = tasktypes_collection.find({}, {"name": 1})
        task_types_list = await task_types_cursor.to_list(length=None)
        all_task_types = [tt.get("name") for tt in task_types_list if tt.get("name")]
        print(f"[DASHBOARD] Found {len(all_task_types)} task types in database: {all_task_types}")

        
        overdue_cells_pipeline = [
            {
                "$match": {
                    "$or": [
                        {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                        {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
                    ],
                    "date": {"$lte": end},
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
                    "eventName": {
                        "$ifNull": ["$Event Name", "$eventName", "$EventName", "Unnamed Event"]
                    },
                    "eventType": {
                        "$ifNull": ["$Event Type", "$eventType", "$eventTypeName", "Cells"]
                    },
                    "eventLeaderName": {
                        "$ifNull": ["$Leader", "$eventLeaderName", "$EventLeaderName", "Unknown Leader"]
                    },
                    "eventLeaderEmail": {
                        "$ifNull": ["$Email", "$eventLeaderEmail", "$EventLeaderEmail", ""]
                    },
                    "leader1": {"$ifNull": ["$leader1", "$Leader @1", ""]},
                    "leader12": {
                        "$ifNull": ["$Leader at 12", "$Leader @12", "$leader12", "$Leader12", ""]
                    },
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

        
        tasks_pipeline = [
            {
                "$match": {
                    "$or": [
                        {"followup_date": {"$gte": start, "$lte": end}},
                        {"completedAt": {"$gte": start, "$lte": end}},
                        {"createdAt": {"$gte": start, "$lte": end}}
                    ]
                }
            },
            {
                "$addFields": {
                    
                    "task_type_label": {
                        "$ifNull": ["$taskType", "Uncategorized"]
                    },
                    
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
                    
                    "is_completed": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": {
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
                                        }
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    
                    "completed_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$completedAt", None]},
                                    {"$gte": ["$completedAt", start]},
                                    {"$lte": ["$completedAt", end]},
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": {
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
                                        }
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    
                    "is_due_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$followup_date", None]},
                                    {"$gte": ["$followup_date", start]},
                                    {"$lte": ["$followup_date", end]}
                                ]
                            },
                            True,
                            False
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$assignedfor",
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
                            "isRecurring": {
                                "$cond": [{"$ifNull": ["$recurring_day", False]}, True, False]
                            },
                            "priority": "$priority",
                            "is_completed": "$is_completed",
                            "is_due_in_period": "$is_due_in_period",
                            "completed_in_period": "$completed_in_period",
                            "is_excluded_type": "$is_excluded_type",
                            "description": "$description"
                        }
                    },
                    
                    "total_tasks": {"$sum": 1},
                    
                    "completed_tasks": {
                        "$sum": {
                            "$cond": ["$is_completed", 1, 0]
                        }
                    },
                    
                    "completed_in_period": {
                        "$sum": {
                            "$cond": ["$completed_in_period", 1, 0]
                        }
                    },
                    
                    "due_in_period": {
                        "$sum": {
                            "$cond": ["$is_due_in_period", 1, 0]
                        }
                    },
                    
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

        
        overdue_cells_cursor = events_collection.aggregate(overdue_cells_pipeline)
        tasks_cursor = tasks_collection.aggregate(tasks_pipeline)
        users_cursor = users_collection.find(
            {},
            {"_id": 1, "email": 1, "name": 1, "surname": 1}
        ).limit(limit)

        overdue_cells, task_groups, users = await asyncio.gather(
            overdue_cells_cursor.to_list(100),
            tasks_cursor.to_list(None),
            users_cursor.to_list(limit),
        )

        
        formatted_overdue_cells = []
        for cell in overdue_cells:
            cell["_id"] = str(cell["_id"])
            if isinstance(cell.get("date"), datetime):
                cell["date"] = cell["date"].isoformat()
            formatted_overdue_cells.append(cell)

        
        user_map = {}
        for user in users:
            uid = str(user["_id"])
            email = user.get("email", "").lower()
            full_name = f"{user.get('name', '')} {user.get('surname', '')}".strip() or email.split("@")[0]

            user_map[email] = {"_id": uid, "email": email, "fullName": full_name}
            user_map[uid] = user_map[email]

        
        grouped_tasks = []
        all_tasks_list = []
        
        
        global_total_tasks = 0
        global_completed_tasks = 0
        global_completed_in_period = 0
        global_due_in_period = 0
        global_incomplete_due = 0
        
        
        task_type_stats = {}

        for task_group in task_groups:
            email = task_group["_id"]
            if not email:
                email = "unassigned@example.com"

            user_info = user_map.get(email.lower(), {
                "_id": f"unknown_{email}",
                "email": email,
                "fullName": email.split("@")[0]
            })

            tasks_list = task_group["tasks"]
            
            
            task_types_in_group = set()
            for task in tasks_list:
                task_type = task.get("taskType")
                if task_type:
                    task_types_in_group.add(task_type)
            
            if task_types_in_group:
                print(f"[DASHBOARD DEBUG] Task types for {email}: {task_types_in_group}")
            
            
            for task in tasks_list:
                task["_id"] = str(task["_id"])
                
                for date_field in ["followup_date", "due_date", "completedAt", "createdAt"]:
                    if isinstance(task.get(date_field), datetime):
                        task[date_field] = task[date_field].isoformat()
                
                
                task_type = task.get("taskType") or "Uncategorized"
                is_excluded = task.get("is_excluded_type", False)
                
                
                if task_type not in task_type_stats:
                    task_type_stats[task_type] = {
                        "total": 0, 
                        "completed": 0, 
                        "completed_in_period": 0,
                        "due_in_period": 0,
                        "incomplete_due": 0,
                        "is_excluded": is_excluded
                    }
                
                
                task_type_stats[task_type]["total"] += 1
                if task.get("is_completed"):
                    task_type_stats[task_type]["completed"] += 1
                if task.get("completed_in_period"):
                    task_type_stats[task_type]["completed_in_period"] += 1
                if task.get("is_due_in_period"):
                    task_type_stats[task_type]["due_in_period"] += 1
                if task.get("is_due_in_period") and not task.get("is_completed"):
                    task_type_stats[task_type]["incomplete_due"] += 1

            
            total_for_user = task_group["total_tasks"]
            completed_all = task_group["completed_tasks"]
            completed_in_period = task_group["completed_in_period"]
            due_in_period = task_group["due_in_period"]
            
            
            incomplete_due = sum(
                1 for t in tasks_list 
                if t.get("is_due_in_period") and not t.get("is_completed")
            )
            
            incomplete_all = total_for_user - completed_all

            
            global_total_tasks += total_for_user
            global_completed_tasks += completed_all
            global_completed_in_period += completed_in_period
            global_due_in_period += due_in_period
            global_incomplete_due += incomplete_due

            grouped_tasks.append({
                "user": user_info,
                "tasks": tasks_list,
                "totalCount": total_for_user,
                "completedCount": completed_all,
                "incompleteCount": incomplete_all,
                "dueInPeriodCount": due_in_period,
                "completedInPeriodCount": completed_in_period,
                "incompleteDueInPeriodCount": incomplete_due,
                "taskTypes": list(set([t.get("taskType") or "Uncategorized" for t in tasks_list]))
            })

            all_tasks_list.extend(tasks_list)

        grouped_tasks.sort(key=lambda x: x["user"]["fullName"].lower())

        
        
        completion_rate_due = (
            round((global_completed_in_period / global_due_in_period * 100), 2)
            if global_due_in_period > 0 else 0
        )
        
        completion_rate_overall = (
            round((global_completed_tasks / global_total_tasks * 100), 2)
            if global_total_tasks > 0 else 0
        )

        
        unique_task_types_found = list(task_type_stats.keys())
        
        
        print(f"[DASHBOARD DEBUG] Task type stats:")
        for task_type, stats in task_type_stats.items():
            print(f"  - {task_type}: total={stats['total']}, completed={stats['completed']}, is_excluded={stats.get('is_excluded', False)}")

        overview = {
            
            "total_attendance": sum(len(c.get("attendees", [])) for c in formatted_overdue_cells),
            "outstanding_cells": len(formatted_overdue_cells),
            
            
            "outstanding_tasks": global_incomplete_due,
            "tasks_due_in_period": global_due_in_period,
            "tasks_completed_in_period": global_completed_in_period,  
            "total_tasks_in_period": global_total_tasks,
            "total_tasks_completed": global_completed_tasks,  
            "total_tasks_incomplete": global_total_tasks - global_completed_tasks,
            
            
            "consolidation_tasks": task_type_stats.get("consolidation", {}).get("total", 0),
            "consolidation_completed": task_type_stats.get("consolidation", {}).get("completed", 0),
            "consolidation_completed_in_period": task_type_stats.get("consolidation", {}).get("completed_in_period", 0),
            
            
            "people_behind": len([g for g in grouped_tasks if g["incompleteDueInPeriodCount"] > 0]),
            "total_users": len(users),
            
            
            "completion_rate_due_tasks": completion_rate_due,
            "completion_rate_overall": completion_rate_overall,
            "consolidation_completion_rate": (
                round((task_type_stats.get("consolidation", {}).get("completed", 0) / 
                      task_type_stats.get("consolidation", {}).get("total", 1) * 100), 2)
                if task_type_stats.get("consolidation", {}).get("total", 0) > 0 else 0
            ),
            
            
            "task_type_breakdown": task_type_stats,
            
            
            "users_with_tasks": len(grouped_tasks),
            "users_without_tasks": len(users) - len(grouped_tasks),
            
            
            "available_task_types": all_task_types,
            "task_types_found": unique_task_types_found,
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "total_unique_task_types": len(unique_task_types_found),
            "note": f"'no answer' and 'Awaiting Call' task types are excluded from completed counts"
        }

        return {
            "overview": overview,
            "overdueCells": formatted_overdue_cells,
            "groupedTasks": grouped_tasks,
            "allTasks": all_tasks_list,
            "allUsers": [
                {
                    "_id": str(u["_id"]),
                    "email": u.get("email", ""),
                    "name": u.get("name", ""),
                    "surname": u.get("surname", ""),
                    "fullName": f"{u.get('name', '')} {u.get('surname', '')}".strip()
                        or u.get("email", "").split("@")[0]
                }
                for u in users
            ],
            "period": period,
            "date_range": {"start": start_date_str, "end": end_date_str},
            "task_type_stats": task_type_stats,
            "available_task_types": all_task_types,
            "task_types_found": unique_task_types_found,
            "excluded_task_types": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error fetching comprehensive stats: {str(e)}")



@app.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    current_user: dict = Depends(get_current_user)
):
    """
    QUICK DASHBOARD SUMMARY - Counts completed tasks EXCLUDING "no answer" and "Awaiting Call"
    """
    try:
        start, end = get_period_range(period)

        start_str = start.date().isoformat()
        end_str = end.date().isoformat()

        print(f"[QUICK STATS] Excluding task types: {EXCLUDED_TASK_TYPES_FROM_COMPLETED}")

        
        
        total_tasks_all = await tasks_collection.count_documents({
            "$or": [
                {"followup_date": {"$gte": start, "$lte": end}},
                {"completedAt": {"$gte": start, "$lte": end}},
                {"createdAt": {"$gte": start, "$lte": end}}
            ]
        })

        
        tasks_due_in_period = await tasks_collection.count_documents({
            "followup_date": {"$gte": start, "$lte": end}
        })

        
        tasks_completed_in_period = await tasks_collection.count_documents({
            "completedAt": {"$gte": start, "$lte": end},
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
        })

        
        total_completed = await tasks_collection.count_documents({
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": {"$nin": EXCLUDED_TASK_TYPES_FROM_COMPLETED}
        })

        
        consolidation_completed_in_period = await tasks_collection.count_documents({
            "completedAt": {"$gte": start, "$lte": end},
            "status": {"$in": ["completed", "done", "closed", "finished"]},
            "taskType": "consolidation"
        })

        total_consolidation_tasks = await tasks_collection.count_documents({
            "taskType": "consolidation"
        })

        total_consolidation_completed = await tasks_collection.count_documents({
            "taskType": "consolidation",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })

        
        no_answer_count = await tasks_collection.count_documents({
            "taskType": "no answer",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })
        
        awaiting_call_count = await tasks_collection.count_documents({
            "taskType": "Awaiting Call",
            "status": {"$in": ["completed", "done", "closed", "finished"]}
        })
        
        print(f"[QUICK STATS DEBUG] Excluded task counts - no answer: {no_answer_count}, Awaiting Call: {awaiting_call_count}")

        
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"followup_date": {"$gte": start, "$lte": end}},
                        {"completedAt": {"$gte": start, "$lte": end}},
                        {"createdAt": {"$gte": start, "$lte": end}}
                    ]
                }
            },
            {
                "$addFields": {
                    
                    "task_type": {"$ifNull": ["$taskType", "Uncategorized"]},
                    
                    "is_excluded": {
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
                    
                    "is_completed": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": {
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
                                        }
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    },
                    
                    "completed_in_period": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$completedAt", None]},
                                    {"$gte": ["$completedAt", start]},
                                    {"$lte": ["$completedAt", end]},
                                    {
                                        "$in": [
                                            {"$toLower": {"$ifNull": ["$status", "pending"]}},
                                            ["completed", "done", "closed", "finished"]
                                        ]
                                    },
                                    {
                                        "$not": {
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
                                        }
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$task_type",
                    "total": {"$sum": 1},
                    "completed": {
                        "$sum": {
                            "$cond": ["$is_completed", 1, 0]
                        }
                    },
                    "completed_in_period": {
                        "$sum": {
                            "$cond": ["$completed_in_period", 1, 0]
                        }
                    },
                    "due_in_period": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$followup_date", None]},
                                        {"$gte": ["$followup_date", start]},
                                        {"$lte": ["$followup_date", end]}
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
        
        task_type_cursor = tasks_collection.aggregate(pipeline)
        task_type_stats_raw = await task_type_cursor.to_list(None)
        
        
        task_type_stats = {}
        for stat in task_type_stats_raw:
            task_type = stat["_id"] or "Uncategorized"
            total = stat["total"]
            completed = stat["completed"]
            is_excluded = stat["is_excluded"]
            
            task_type_stats[task_type] = {
                "total": total,
                "completed": completed,
                "completed_in_period": stat["completed_in_period"],
                "due_in_period": stat["due_in_period"],
                "is_excluded": is_excluded,
                "completion_rate": round((completed / total * 100), 2) if total > 0 else 0,
                "completion_rate_in_period": round((stat["completed_in_period"] / stat["due_in_period"] * 100), 2) if stat["due_in_period"] > 0 else 0
            }

        
        overdue_cells_count = await events_collection.count_documents({
            "$or": [
                {"Event Type": {"$regex": "^Cells$", "$options": "i"}},
                {"eventType": {"$regex": "^Cells$", "$options": "i"}},
                {"eventTypeName": {"$regex": "^Cells$", "$options": "i"}}
            ],
            "date": {"$lte": end},
            "$or": [
                {"status": "incomplete"},
                {"status": {"$exists": False}},
                {"Status": "Incomplete"},
                {"_is_overdue": True}
            ]
        })

        return {
            "period": period,
            "date_range": {"start": start_str, "end": end_str},
            
            
            "taskCount": total_tasks_all,
            "tasksDueInPeriod": tasks_due_in_period,
            "tasksCompletedInPeriod": tasks_completed_in_period,  
            "totalCompletedTasks": total_completed,  
            
            
            "consolidationTasks": total_consolidation_tasks,
            "consolidationCompleted": total_consolidation_completed,
            "consolidationCompletedInPeriod": consolidation_completed_in_period,
            "consolidationCompletionRate": (
                round((total_consolidation_completed / total_consolidation_tasks * 100), 2)
                if total_consolidation_tasks > 0 else 0
            ),
            
            
            "overdueCells": overdue_cells_count,
            
            
            "completionRateDueTasks": (
                round((tasks_completed_in_period / tasks_due_in_period * 100), 2)
                if tasks_due_in_period > 0 else 0
            ),
            "overallCompletionRate": (
                round((total_completed / total_tasks_all * 100), 2)
                if total_tasks_all > 0 else 0
            ),
            
            
            "taskTypeBreakdown": task_type_stats,
            "totalTaskTypesFound": len(task_type_stats),
            "excludedTaskTypes": EXCLUDED_TASK_TYPES_FROM_COMPLETED,
            
            "timestamp": datetime.utcnow().isoformat(),
            "note": "'no answer' and 'Awaiting Call' task types are excluded from completed counts"
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error fetching quick stats: {str(e)}")
    

@app.get("/ Estats/outstanding-items")
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

@app.get("/stats/people-with-tasks")
async def get_people_capture_stats():
    """
    Get team members and how many people they have captured/recruited
    """
    try:
        client = get_database_client()
        db = client[DB_NAME]
       
        # Count how many people each team member has captured
        pipeline = [
            {
                "$match": {
                    "captured_by": {"$exists": True, "$ne": None}  # Only people who were captured by someone
                }
            },
            {
                "$group": {
                    "_id": "$captured_by",  # Group by the person who captured them
                    "people_captured_count": {"$sum": 1},
                    "captured_people": {
                        "$push": {
                            "name": "$fullName",
                            "email": "$email",
                            "capture_date": "$created_date"  # or whatever field tracks when
                        }
                    }
                }
            },
            {
                "$lookup": {
                    "from": "people",
                    "localField": "_id",
                    "foreignField": "_id",  # or "email" depending on your schema
                    "as": "capturer_details"
                }
            },
            {
                "$unwind": {
                    "path": "$capturer_details",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "capturer_id": "$_id",
                    "capturer_name": {
                        "$ifNull": ["$capturer_details.fullName", "$capturer_details.name", "Unknown Capturer"]
                    },
                    "capturer_email": {
                        "$ifNull": ["$capturer_details.email", "No email"]
                    },
                    "people_captured_count": 1,
                    "captured_people": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"people_captured_count": -1}  # Sort by most captures first
            }
        ]
       
        results = list(db.people.aggregate(pipeline))  # Query the PEOPLE collection
       
        if not results:
            return {
                "capture_stats": [],
                "total_capturers": 0,
                "total_people_captured": 0,
                "message": "No capture data found"
            }
       
        total_people_captured = sum(item['people_captured_count'] for item in results)
       
        return {
            "capture_stats": results,
            "total_capturers": len(results),
            "total_people_captured": total_people_captured,
            "message": f"Found {len(results)} team members who captured {total_people_captured} people total"
        }
       
    except Exception as e:
        print(f"Error fetching capture stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch capture statistics: {str(e)}"
        )

# --- ROLE MANAGEMENT ENDPOINTS (Admin only) ---

# Role permissions configuration
ROLE_PERMISSIONS = {
    "admin": {
        "manage_users": True,
        "manage_leaders": True,
        "manage_events": True,
        "view_reports": True,
        "system_settings": True
    },
    "leader": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": True,
        "system_settings": False
    },
    "user": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": False,
        "view_reports": False,
        "system_settings": False
    },
    "registrant": {
        "manage_users": False,
        "manage_leaders": False,
        "manage_events": True,
        "view_reports": False,
        "system_settings": False
    }
}

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



