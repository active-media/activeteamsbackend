"""Data layer for Staff / Interns / Youth: same roster + targets + completed-task counting as Twelve Tasks,
grouped by Leader@12.

Differences from Reports/twelve_tasks.py (deliberate):
  * tasks are matched to people by person_id OR by e-mail (Tasks.assignedfor holds a lowercased e-mail,
    so matching on person_id alone always yields 0);
  * "completed" = status in completed/done/closed/finished (the values PUT /tasks normalises to).
Previous period = the window of equal length immediately before the selected period.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from . import training_exports as T
from .common import clean

DONE = {"completed", "done", "closed", "finished"}


def _window(start: str, end: str):
    s = datetime.combine(date.fromisoformat(start), time.min, tzinfo=timezone.utc)
    e = datetime.combine(date.fromisoformat(end), time.max, tzinfo=timezone.utc)
    days = (e.date() - s.date()).days + 1
    pe = s - timedelta(microseconds=1)
    ps = datetime.combine((s - timedelta(days=days)).date(), time.min, tzinfo=timezone.utc)
    return ps, pe, s, e


def fetch_staff_report(org_filter, start, end, visible_leader_ids=None, category=None) -> dict:
    org = (org_filter or {}).get("Organization")
    roster = T._select("twelve_tasks_tracked_people", "*", eq={"active": True})
    if org:
        roster = [r for r in roster if r.get("org") == org]
    if visible_leader_ids is not None:
        roster = [r for r in roster if str(r.get("person_id") or "") in visible_leader_ids]
    if category:
        roster = [r for r in roster if (r.get("category") or "").lower() == category.lower()]
    if not roster:
        return {"groups": []}

    ids = sorted({r["person_id"] for r in roster})
    leader_ids = sorted({r["leader12_id"] for r in roster})
    people = T._select("People", "_id,Email,Name,Surname", in_={"_id": sorted(set(ids) | set(leader_ids))})
    by_id = {p["_id"]: p for p in people}
    email_of = {i: clean(by_id.get(i, {}).get("Email")).lower() for i in ids}
    targets = {t["person_id"]: t["target"] for t in T._select("twelve_tasks_targets", "person_id,target", in_={"person_id": ids})}

    ps, pe, s, e = _window(start, end)
    tasks = T._select("Tasks", "status,completedAt,assignedfor,assigned_to_email,Organization",
                      eq={"Organization": org} if org else None, gte={"completedAt": ps.isoformat()}, lte={"completedAt": e.isoformat()})
    key_to_pid = {}
    for pid in ids:
        key_to_pid[pid.lower()] = pid
        if email_of[pid]:
            key_to_pid[email_of[pid]] = pid
    prev, cur = defaultdict(int), defaultdict(int)
    for t in tasks:
        if clean(t.get("status")).lower() not in DONE:
            continue
        pid = next((key_to_pid[k] for k in (clean(t.get("assignedfor")).lower(), clean(t.get("assigned_to_email")).lower())
                    if k and k in key_to_pid), None)
        if not pid:
            continue
        when = str(t.get("completedAt") or "")
        if s.isoformat() <= when <= e.isoformat():
            cur[pid] += 1
        elif ps.isoformat() <= when <= pe.isoformat():
            prev[pid] += 1

    groups = defaultdict(list)
    for r in roster:
        pid = r["person_id"]
        groups[r["leader12_id"]].append({"name": clean(r.get("display_name")) or pid, "previous": prev.get(pid, 0),
                                          "target": targets.get(pid, 0), "actual": cur.get(pid, 0)})
    def leader_name(lid):
        p = by_id.get(lid) or {}
        return clean(f"{p.get('Name', '')} {p.get('Surname', '')}") or "Unassigned"
    return {"groups": sorted(({"leader": leader_name(l), "rows": rows} for l, rows in groups.items()), key=lambda g: g["leader"].lower())}
