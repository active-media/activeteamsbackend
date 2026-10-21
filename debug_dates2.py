from datetime import datetime, timedelta
from supabase_helpers.twelve_tasks import _period_range, _count_leaders, _iso

start, end = _period_range("thisWeek")
print(f"start: {start}")
print(f"end: {end}")
print(f"_iso(start): {_iso(start)}")
print(f"_iso(end): {_iso(end)}")

current_tasks = [
    {"assignedfor": "leader1@example.com", "followup_date": _iso(start)},
    {"assignedfor": "leader1@example.com", "followup_date": _iso(start + timedelta(days=5))},
    {"assignedfor": "leader2@example.com", "followup_date": _iso(start + timedelta(days=3))},
    {"assignedfor": "leader3@example.com", "followup_date": _iso(start + timedelta(days=1))},
]

counts = _count_leaders(current_tasks, start, end)
print(f"Counts: {counts}")