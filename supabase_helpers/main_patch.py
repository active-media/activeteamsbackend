# ============================================================
# main_patch.py  –  HOW TO WIRE supabase_stats_checkin.py
#                   INTO YOUR EXISTING main.py
# ============================================================
#
# Step 1: copy supabase_stats_checkin.py into
#         supabase_helpers/supabase_stats_checkin.py
#
# Step 2: add this import near the top of main.py
#         (after the existing supabase_helpers imports):
#
#   from supabase_helpers.supabase_stats_checkin import (
#       get_event_by_id, get_event_by_mongo_id,
#       get_session, upsert_session, get_session_attendees,
#       get_persistent_attendees, upsert_persistent_attendee,
#       get_event_consolidations, get_session_consolidations,
#       insert_consolidation, remove_consolidation, update_consolidation,
#       get_event_new_people, insert_new_person, remove_new_person,
#       get_realtime_event_data,
#       list_tasks, count_tasks, create_task, update_task, delete_task,
#       delete_task_by_consolidation_id, get_task_type_breakdown,
#       list_task_types, create_task_type, update_task_type, delete_task_type,
#       get_person_by_id, get_person_by_email,
#       get_user_by_id, get_user_by_email, list_users, count_users,
#       log_activity as sb_log_activity,
#       list_overdue_cells, toggle_event_status as sb_toggle_event_status,
#       build_quick_dashboard,
#   )
#
# ============================================================
# Below: each endpoint with its BEFORE / AFTER diff.
# Lines starting with  -  are removed; +  are added.
# ============================================================


# ──────────────────────────────────────────────────────────────
# /service-checkin/real-time-data
# ──────────────────────────────────────────────────────────────
BEFORE_realtime = """
    event = None  # TODO
    # event = await events_collection.find_one({"_id": ObjectId(base_event_id)})
    ...
    attendees = date_data.get("attendees", [])
    new_people = date_data.get("new_people", [])
    consolidations = date_data.get("consolidations", [])
"""

AFTER_realtime = """
    event = get_event_by_mongo_id(base_event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    is_recurring = bool(event.get("recurring_day") or event.get("is_recurring"))
    payload = get_realtime_event_data(
        event_id=event["event_id"],       # Supabase UUID
        session_date=instance_date,
        is_recurring=is_recurring,
    )
    return {
        "success": True,
        "event_id": event_id,
        "event_name": event.get("event_name", "Unknown Event"),
        **payload,
        "refreshed_at": datetime.utcnow().isoformat(),
    }
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/checkin  (attendee branch)
# ──────────────────────────────────────────────────────────────
BEFORE_checkin_attendee = """
    event = None  # TODO
    # event = await events_collection.find_one({"_id": ObjectId(event_id)})
    ...
    existing = None  # TODO
    # existing = await people_collection.find_one({"_id": ObjectId(person_id)})
    ...
    result = None  # TODO
    # result = await events_collection.update_one(...)
    ...
    updated_event = None  # TODO
    # updated_event = await events_collection.find_one({"_id": ObjectId(event_id)})
"""

AFTER_checkin_attendee = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    is_recurring = bool(event.get("is_recurring"))
    if is_recurring and not instance_date:
        instance_date = _sast_now().date().isoformat()

    existing = get_person_by_id(person_id)        # try Supabase people table
    if not existing:
        raise HTTPException(status_code=404, detail="Person does not exist")

    result = checkin_attendee(sb_event_id, instance_date or _today_iso(), {
        "id": person_id,
        "fullName": f"{existing.get('Name', '')} {existing.get('Surname', '')}".strip(),
        "email": existing.get("Email", ""),
        "phone": existing.get("Number", ""),
        "isPersistent": False,
    })

    present_count = len(get_session_attendees(sb_event_id, instance_date or _today_iso()))
    return {
        "message": f"{existing.get('Name')} checked in",
        "type": "attendee",
        "present_count": present_count,
        "success": True,
    }
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/checkin  (new_person branch)
# ──────────────────────────────────────────────────────────────
AFTER_checkin_new_person = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    inserted = insert_new_person(sb_event_id, person_data)
    count = len(get_event_new_people(sb_event_id))
    return {
        "message": "Visitor added to event",
        "type": "new_person",
        "new_person": inserted,
        "new_people_count": count,
        "success": True,
    }
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/remove
# ──────────────────────────────────────────────────────────────
AFTER_remove = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    is_recurring = bool(event.get("is_recurring"))

    if data_type == "attendees":
        remove_session_attendee(sb_event_id, instance_date or _today_iso(), person_id)
    elif data_type == "new_people":
        remove_new_person(person_id)
    elif data_type == "consolidations":
        remove_consolidation(person_id)

    return {
        "success": True,
        "message": f"Person removed from {data_type} successfully",
        "updated_counts": {
            "present_count":      len(get_session_attendees(sb_event_id, instance_date or _today_iso())),
            "new_people_count":   len(get_event_new_people(sb_event_id)),
            "consolidation_count":len(get_event_consolidations(sb_event_id)),
        },
    }
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/update
# ──────────────────────────────────────────────────────────────
AFTER_update_checkin = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    date = instance_date or _today_iso()

    if data_type == "attendees":
        # update the session_attendee row
        session = get_session(sb_event_id, date)
        if session:
            (
                supabase.table("event_session_attendees")
                .update({**update_fields, "updated_at": datetime.utcnow().isoformat()})
                .eq("session_id", session["session_id"])
                .eq("mongo_person_id", person_id)
                .execute()
            )
    elif data_type == "new_people":
        (
            supabase.table("event_new_people")
            .update(update_fields)
            .eq("id", person_id)
            .execute()
        )
    elif data_type == "consolidations":
        update_consolidation(person_id, update_fields)

    return {"success": True, "message": f"Person updated in {data_type} successfully"}
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/create-consolidation
# ──────────────────────────────────────────────────────────────
AFTER_create_consolidation_checkin = """
    event = get_event_by_mongo_id(base_event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]

    # Create task (keep existing task creation logic, just swap the insert):
    task_payload = { ... }   # same dict as before
    new_task = create_task(task_payload)
    task_id = new_task["_id"] if new_task else str(ObjectId())

    consolidation_record = {
        "event_id": sb_event_id,
        "person_name": person_name,
        "person_surname": person_surname,
        "person_email": person_email,
        "person_phone": person_phone,
        "decision_type": decision_type,
        "assigned_to": assigned_to,
        "assigned_to_email": consolidation_data.get("assigned_to_email", ""),
        "notes": notes,
        "status": "active",
        "created_by": current_user.get("email"),
        "created_at": datetime.utcnow().isoformat(),
    }
    inserted_cons = insert_consolidation(consolidation_record)

    sb_log_activity(
        current_user.get("email", ""),
        "CONSOLIDATION_CREATED",
        f"Created consolidation for '{person_name} {person_surname}'"
    )
    ...
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/remove-consolidation
# ──────────────────────────────────────────────────────────────
AFTER_remove_consolidation = """
    event = get_event_by_mongo_id(base_event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    removed = remove_consolidation(consolidation_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Consolidation not found")

    delete_task_by_consolidation_id(consolidation_id)

    sb_log_activity(current_user.get("email", ""), "CONSOLIDATION_REMOVED",
                    f"Removed consolidation {consolidation_id}")

    return {
        "success": True,
        "message": "Consolidation removed successfully",
        "updated_statistics": {
            "consolidations_count": len(get_event_consolidations(sb_event_id)),
            "new_people_count":     len(get_event_new_people(sb_event_id)),
        },
    }
"""


# ──────────────────────────────────────────────────────────────
# /service-checkin/validate-removal
# ──────────────────────────────────────────────────────────────
AFTER_validate_removal = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    sb_event_id = event["event_id"]
    warnings, affected_tasks = [], []

    if consolidation_id:
        consolidations = get_event_consolidations(sb_event_id)
        cons = next((c for c in consolidations if str(c.get("id")) == consolidation_id), None)
        if cons:
            # check for linked task
            tasks = list_tasks(assigned_to_email=None)  # or filter by consolidation_id
            linked = [t for t in tasks if t.get("consolidation_id") == consolidation_id]
            for t in linked:
                affected_tasks.append(t)
                warnings.append(f"Task for {t.get('contacted_person_name', 'Unknown')} will be deleted")

    return {
        "success": True,
        "validation": {
            "warnings": warnings,
            "affected_tasks": affected_tasks,
            "affected_tasks_count": len(affected_tasks),
        },
    }
"""


# ──────────────────────────────────────────────────────────────
# /stats/dashboard-quick
# ──────────────────────────────────────────────────────────────
AFTER_dashboard_quick = """
@app.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats(
    period: str = Query("today", ...),
    current_user: dict = Depends(get_current_user)
):
    org_name = current_user.get("Organization")
    if not org_name:
        raise HTTPException(status_code=403, detail="Organization not associated with user")

    start, end = get_period_range(period)    # existing helper – keep as-is
    start_str = start.date().isoformat()
    end_str   = end.date().isoformat()

    stats = build_quick_dashboard(
        org_name=org_name,
        start=start,
        end=end,
        excluded_task_types=EXCLUDED_TASK_TYPES_FROM_COMPLETED,
    )

    return {
        "period": period,
        "date_range": {"start": start_str, "end": end_str},
        **stats,
        "timestamp": datetime.utcnow().isoformat(),
    }
"""


# ──────────────────────────────────────────────────────────────
# /stats/dashboard-comprehensive  (abbreviated – key changes)
# ──────────────────────────────────────────────────────────────
AFTER_dashboard_comprehensive_key_changes = """
# Replace the three gather() calls with:

overdue_cells   = list_overdue_cells(org_name=org_name, up_to_date=end.isoformat())
task_type_stats = get_task_type_breakdown(
    org_name=org_name,
    from_date=start.isoformat(),
    to_date=end.isoformat(),
    excluded_types=EXCLUDED_TASK_TYPES_FROM_COMPLETED,
)
all_tasks_flat  = list_tasks(
    org_name=org_name,
    from_date=start.isoformat(),
    to_date=end.isoformat(),
    limit=2000,
)
all_sb_users    = list_users(org_name=org_name, limit=limit)

# Then group all_tasks_flat by assignedfor with the same logic as before
# (the pure-Python grouping loop stays unchanged – no Mongo needed).
"""


# ──────────────────────────────────────────────────────────────
# /tasks  GET  (get_user_tasks)
# ──────────────────────────────────────────────────────────────
AFTER_get_tasks = """
    tasks = list_tasks(
        org_name=org_name if not (is_leader and view_all) else None,
        assigned_to_email=user_email if not (is_leader and view_all) else None,
        limit=500,
    )
    # format & return as before
"""


# ──────────────────────────────────────────────────────────────
# /tasks  PUT  (update_task)
# ──────────────────────────────────────────────────────────────
AFTER_put_task = """
    updated = update_task(task_id, update_data)
    if not updated:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"updatedTask": updated}
"""


# ──────────────────────────────────────────────────────────────
# /tasktypes  GET / POST / PUT / DELETE
# ──────────────────────────────────────────────────────────────
AFTER_tasktypes = """
# GET
    return [{"id": t["_id"], "name": t["name"]} for t in list_task_types(org_name)]

# POST
    existing = [t for t in list_task_types(org_name) if t["name"] == task.name]
    if existing:
        raise HTTPException(400, "Task type already exists")
    created = create_task_type(task.name, org_name)
    return {"id": created["_id"], "name": created["name"]}

# PUT
    updated = update_task_type(tasktype_id, update_data.name.strip())
    return {"message": "Task type updated", "taskType": updated}

# DELETE
    delete_task_type(tasktype_id)
    return {"message": "Task type deleted successfully"}
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/statistics  (get_event_statistics)
# ──────────────────────────────────────────────────────────────
AFTER_event_statistics = """
    event = get_event_by_mongo_id(event_id)        # or by UUID
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    sessions    = list_sessions_for_event(sb_event_id)

    latest = sessions[0] if sessions else {}       # already ordered desc
    return {
        "event_id":   event_id,
        "event_name": event.get("event_name", "Unknown"),
        "statistics": {
            "latest_week": {
                "week":               latest.get("session_date"),
                "attendance_count":   latest.get("checked_in_count", 0),
                "total_headcounts":   latest.get("total_headcounts", 0),
                "status":             latest.get("status", ""),
                "did_not_meet":       latest.get("is_did_not_meet", False),
                "decisions": {
                    "first_time":   latest.get("decisions_first_time", 0),
                    "recommitment": latest.get("decisions_recommitment", 0),
                    "total":        latest.get("decisions_total", 0),
                },
            } if latest else None,
            "last_attendance_count": latest.get("checked_in_count", 0),
        },
        "has_attendance_data": len(sessions) > 0,
    }
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/attendance/{week}  (get_weekly_attendance)
# ──────────────────────────────────────────────────────────────
AFTER_weekly_attendance = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    session     = get_session(sb_event_id, week)

    if not session:
        return {"week": week, "exists": False, "message": "No attendance data for this week"}

    attendees = get_session_attendees(sb_event_id, week)
    persistent = get_persistent_attendees(sb_event_id)

    return {
        "week":                week,
        "exists":              True,
        "data":                {**session, "attendees": attendees},
        "persistent_attendees": persistent,
        "event_statistics": {
            "total_associated_count":   len(persistent),
            "last_attendance_count":    session.get("checked_in_count", 0),
            "last_decisions_count":     session.get("decisions_total", 0),
        },
    }
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/consolidations  (get_event_consolidations)
# ──────────────────────────────────────────────────────────────
AFTER_event_consolidations_endpoint = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    consolidations = get_event_consolidations(sb_event_id)
    return {"event_id": event_id, "consolidations": consolidations, "total": len(consolidations)}
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/new-people  (get_event_new_people)
# ──────────────────────────────────────────────────────────────
AFTER_event_new_people_endpoint = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    new_people = get_event_new_people(event["event_id"])
    return {
        "event_id":        event_id,
        "event_name":      event.get("event_name", "Unknown Event"),
        "new_people":      new_people,
        "total_new_people": len(new_people),
    }
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/persistent-attendees  GET + PUT
# ──────────────────────────────────────────────────────────────
AFTER_persistent_attendees_get = """
    event = get_event_by_mongo_id(actual_event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    persistent  = get_persistent_attendees(sb_event_id)
    session     = get_session(sb_event_id, exact_date_str) if target_date else None
    session_att = get_session_attendees(sb_event_id, exact_date_str) if target_date else []

    # ... build enriched list as before using `persistent` and `session_att`
"""

AFTER_persistent_attendees_put = """
    event = get_event_by_mongo_id(actual_event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    for att in enriched_attendees:
        upsert_persistent_attendee(sb_event_id, att)

    return {
        "success": True,
        "message": f"Updated {len(enriched_attendees)} persistent attendees",
        "total_associated": len(enriched_attendees),
    }
"""


# ──────────────────────────────────────────────────────────────
# /events/{event_id}/last-attendance  (get_last_attendance)
# ──────────────────────────────────────────────────────────────
AFTER_last_attendance = """
    event = get_event_by_mongo_id(event_id)
    if not event:
        raise HTTPException(404, "Event not found")

    sb_event_id = event["event_id"]
    persistent  = get_persistent_attendees(sb_event_id)
    sessions    = list_sessions_for_event(sb_event_id)

    if persistent:
        return {"has_previous_attendance": True, "attendees": persistent, ...}

    if sessions:
        last_att = get_session_attendees(sb_event_id, sessions[0]["session_date"])
        return {"has_previous_attendance": True, "attendees": last_att, ...}

    return {"has_previous_attendance": False, "attendees": [], ...}
"""


# ──────────────────────────────────────────────────────────────
# log_activity  (used throughout main.py)
# ──────────────────────────────────────────────────────────────
AFTER_log_activity = """
# Replace the existing async log_activity function body with:
async def log_activity(user_id: str, action: str, details: str):
    sb_log_activity(str(user_id), action, details)
"""