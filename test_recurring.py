
import asyncio
from datetime import date, timedelta

async def test_recurring_instances():
    today = date(2026, 3, 9)  # today, Monday March 9

    week_start = today - timedelta(days=today.weekday())  # Monday March 9

    # Simulated attendance already captured in DB
    mock_attendance = {
        "2026-03-01": {"status": "complete", "attendees": ["person1", "person2"]},
    }

    recurring_days = ["Sunday"]
    day_mapping = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }

    print(f"Today: {today}")
    print(f"Week start (Monday): {week_start}")
    print("-" * 50)

    for day in recurring_days:
        target_weekday = day_mapping[day.lower()]
        for week_back in range(0, 4):
            instance_date = (week_start + timedelta(days=target_weekday)) - timedelta(weeks=week_back)

            exact_date_str = instance_date.isoformat()
            date_attendance = mock_attendance.get(exact_date_str, {})

            if date_attendance:
                att_status = date_attendance.get("status", "")
                attendees = date_attendance.get("attendees", [])
                if att_status == "complete" or len(attendees) > 0:
                    ev_status = "complete"
                elif att_status == "did_not_meet":
                    ev_status = "did_not_meet"
                else:
                    ev_status = "incomplete"
            else:
                ev_status = "incomplete"

            future_tag = " ← FUTURE (will show as incomplete, ready to capture)" if instance_date > today else ""
            print(f"Week -{week_back}: {instance_date} → status: {ev_status}{future_tag}")

asyncio.run(test_recurring_instances())

# Expected output:
# Today: 2026-03-09
# Week start (Monday): 2026-03-09
# --------------------------------------------------
# Week -0: 2026-03-15 → status: incomplete ← FUTURE (will show as incomplete, ready to capture)
# Week -1: 2026-03-01 → status: complete
# Week -2: 2026-02-22 → status: incomplete
# Week -3: 2026-02-15 → status: incomplete