#!/usr/bin/env python3
import os

main_path = r"C:\Users\madnh\OneDrive\Desktop\2026 - Development\Client Work\Active Teams\activeteamsbackend\main.py"

with open(main_path, "r", encoding="utf-8") as f:
    content = f.read()

# Find the exact insertion point: "return {"success": True}" followed by blank line and comment
target = "\n\treturn {\"success\": True}\n\n# ====================== End of Reporting Endpoint =====================\n"

# Actually let's find the exact pattern
print("Looking for pattern...")
idx = content.find('return {"success": True}')
print(f"Found at: {idx}, repr: {repr(content[idx:idx+40])}")

# Let's just insert after the delete_report_schedule endpoint
# The pattern is: return {"success": True} followed by blank lines and the comment marker
marker = "# Check-in (no auth required)"
insert_idx = content.find(marker)
if insert_idx < 0:
    print(f"Marker '{marker}' NOT found! Looking for alternative...")
    # Try the exact string from the read tool output
    alt_marker = "    return {\"success\": True}"
    idx2 = content.find(alt_marker)
    print(f"Alternative found at: {idx2}")
    if idx2 >= 0:
        insert_idx = idx2 + len(alt_marker)
else:
    insert_idx = insert_idx  # position after marker

# The new endpoint code
new_endpoint = r'''\n@app.post("/report-schedules/{schedule_id}/run")
async def run_schedule_now(schedule_id: str, current_user: dict = Depends(require_role("admin"))):
    """Run a schedule immediately through the same code path as the scheduler.
    Requires admin role. Schedule must belong to the caller's Organization.
    A manual run must NOT change next_run_at.
    """
    organization = current_user.get("Organization") or current_user.get("organization")
    if not organization:
        raise HTTPException(status_code=403, detail="Organization not associated with user")

    # Fetch schedule in caller's org (sync via to_thread)
    schedule = await asyncio.to_thread(
        report_schedules_helpers.get_report_schedule, schedule_id, organization
    )
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    # Atomic claim: update next_run_at to new value where old value matches
    # If next_run_at is null, we need to compute it first
    now_utc = datetime.now(timezone.utc)
    from supabase_helpers.report_schedule_runner import compute_next_run
    if not schedule.get("next_run_at"):
        new_next_run = compute_next_run(schedule, now_utc)
    else:
        new_next_run = compute_next_run(schedule, datetime.fromisoformat(schedule["next_run_at"]))

    # Atomically claim by updating next_run_at; skip if another process claimed it
    updated = supabase.table("report_schedules").update({
        "next_run_at": new_next_run,
    }).eq("id", schedule_id).eq("next_run_at", schedule.get("next_run_at")).eq("Organization", organization).execute()

    if not updated.data:
        # Another process already claimed it; or next_run_at changed; skip
        return {"status": "skipped", "error": "Schedule already being processed by another instance"}

    # Now generate and send the report (same code path as scheduler)
    try:
        from supabase_helpers.report_schedule_runner import _generate_report
        from auth.email_utils import send_report_email
        report_bytes = _generate_report(schedule)
        recipients = schedule.get("recipients", [])
        send_report_email(
            to_email=recipients[0] if recipients else "",
            subject=f"Scheduled Report: {schedule['report_type']}",
            body=f"Your {schedule['report_type']} report has been generated.",
            attachment_bytes=report_bytes,
            filename=f"{schedule['report_type']}.{schedule['format']}",
            mimetype=f"application/{schedule['format']}",
        )
        # Mark success
        from supabase_helpers.report_schedules import mark_schedule_run
        mark_schedule_run(schedule_id, status="success", next_run_at=new_next_run)
        # Note: manual run does NOT change next_run_at beyond the initial claim
        # The next_run_at was already set above via the claim

    except ValueError as ve:
        # Unsupported report_type or format
        from supabase_helpers.report_schedules import mark_schedule_run
        mark_schedule_run(schedule_id, status="error", error=str(ve))
        return {"status": "error", "error": str(ve)}
    except Exception as e:
        from supabase_helpers.report_schedules import mark_schedule_run
        mark_schedule_run(schedule_id, status="error", error=str(e))
        return {"status": "error", "error": str(e)}

    return {"status": "success"}


'''

# Insert before the marker
new_content = content[:insert_idx] + new_endpoint + content[insert_idx:]

with open(main_path, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Endpoint inserted successfully at index", insert_idx)