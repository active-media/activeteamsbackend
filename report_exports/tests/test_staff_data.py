from report_exports import training_exports as T, staff_data as S
from report_exports.staff_interns_youth import build_view
tables = {
 "twelve_tasks_tracked_people": [
   {"person_id": "P1", "leader12_id": "L1", "org": "AC", "display_name": "Birra", "active": True, "category": "staff"},
   {"person_id": "P2", "leader12_id": "L1", "org": "AC", "display_name": "Gloria", "active": True, "category": "intern"},
   {"person_id": "P3", "leader12_id": "L2", "org": "AC", "display_name": "Austin", "active": True, "category": "youth"}],
 "People": [{"_id": "P1", "Email": "Birra@x.com"}, {"_id": "P2", "Email": "gloria@x.com"}, {"_id": "P3", "Email": "a@x.com"},
            {"_id": "L1", "Name": "Bernice", "Surname": "van der Walt"}, {"_id": "L2", "Name": "Kenny", "Surname": "Bebel"}],
 "twelve_tasks_targets": [{"person_id": "P1", "target": 3}, {"person_id": "P3", "target": 10}],
 "Tasks": [
   {"status": "completed", "completedAt": "2026-10-21T08:00:00+00:00", "assignedfor": "birra@x.com", "Organization": "AC"},   # this week, matched by email
   {"status": "completed", "completedAt": "2026-10-22T08:00:00+00:00", "assignedfor": "", "assigned_to_email": "BIRRA@x.com", "Organization": "AC"},
   {"status": "done", "completedAt": "2026-10-14T08:00:00+00:00", "assignedfor": "P1", "Organization": "AC"},                  # previous week, matched by id
   {"status": "open", "completedAt": "2026-10-21T08:00:00+00:00", "assignedfor": "birra@x.com", "Organization": "AC"},         # not done
   {"status": "completed", "completedAt": "2026-10-23T08:00:00+00:00", "assignedfor": "a@x.com", "Organization": "AC"}]}
def fake(table, cols="*", eq=None, in_=None, ilike=None, gte=None, lte=None):
    rows = tables[table]
    for k, v in (eq or {}).items(): rows = [r for r in rows if r.get(k) == v]
    for k, v in (gte or {}).items(): rows = [r for r in rows if r.get(k, "") >= v]
    for k, v in (lte or {}).items(): rows = [r for r in rows if r.get(k, "") <= v]
    for k, vs in (in_ or {}).items(): rows = [r for r in rows if r.get(k) in set(vs)]
    return rows
T._select = fake
p = S.fetch_staff_report({"Organization": "AC"}, "2026-10-19", "2026-10-25")
print(p)
assert p["groups"][0]["rows"][0] == {"name": "Birra", "previous": 1, "target": 3, "actual": 2}, p
assert S.fetch_staff_report({"Organization": "AC"}, "2026-10-19", "2026-10-25", category="youth")["groups"][0]["leader"] == "Kenny Bebel"
print({k["key"]: k["value"] for k in build_view(p)["kpis"]}, "OK")
