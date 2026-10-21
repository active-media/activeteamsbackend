from datetime import datetime, timedelta
from supabase_helpers.twelve_tasks import _period_range, _count_leaders, _iso

start, end = _period_range("thisWeek")
print(f"start ISO: {_iso(start)}")
print(f"end ISO: {_iso(end)}")

task = {"followup_date": start.date().isoformat()}
print(f"task followup_date: {task['followup_date']}")
comparison = _iso(start) <= task["followup_date"] <= _iso(end)
print(f"comparison start_iso <= task: {comparison}")

current_tasks = [
    {"assignedfor": "leader1@example.com", "followup_date": start.date().isoformat()},
    {"assignedfor": "leader1@example.com", "followup_date": (start + timedelta(days=5)).date().isoformat()},
    {"assignedfor": "leader2@example.com", "followup_date": (start + timedelta(days=3)).date().isoformat()},
    {"assignedfor": "leader3@example.com", "followup_date": (start + timedelta(days=1)).date().isoformat()},
]

counts = _count_leaders(current_tasks, start, end)
print(f"Counts: {counts}")