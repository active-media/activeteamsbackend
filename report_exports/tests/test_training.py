"""Exercise the DB layer against an in-memory fake of the schema (no Supabase needed)."""
import json, io, openpyxl
from report_exports import training_exports as T

fx = json.load(open("/home/claude/report_exports/fixtures/life_class.json"))
dates = [f"2026-05-{d:02d}" for d in range(4, 31, 3)]
tables = {"events": [{"event_id": "E1", "event_name": "Life Class Term 2", "event_type_name": "Life Class", "Organization": "Active Church"}],
          "event_sessions": [{"session_id": f"S{i}", "event_id": "E1", "session_date": d, "is_did_not_meet": False} for i, d in enumerate(dates)],
          "event_attendees": [], "event_session_attendees": [], "event_training_attendance": [], "People": []}
for g in ("MEN", "WOMEN"):
    for i, p in enumerate(fx[g]):
        pid = f"{g}{i}"
        tables["People"].append({"_id": pid, "Gender": g.title()})
        tables["event_attendees"].append(dict(event_id="E1", mongo_person_id=pid, full_name=p["name"], leader12=p["leader12"],
                                              leader144=p["leader144"], registration_type=p["registration_type"]))
        for j, on in enumerate(p["sessions"]):
            if on:
                row = dict(session_id=f"S{j}", mongo_person_id=pid, full_name=p["name"])
                (tables["event_session_attendees"] if i % 2 else tables["event_training_attendance"]).append(
                    {**row, "is_checked_in": True} if i % 2 else {**row, "is_present": True})

def fake(table, cols="*", eq=None, in_=None, ilike=None, gte=None, lte=None):
    rows = tables[table]
    for k, v in (eq or {}).items(): rows = [r for r in rows if r.get(k) == v]
    for k, v in (ilike or {}).items(): rows = [r for r in rows if v.strip("%").lower() in str(r.get(k, "")).lower()]
    for k, v in (gte or {}).items(): rows = [r for r in rows if r[k] >= v]
    for k, v in (lte or {}).items(): rows = [r for r in rows if r[k] <= v]
    for k, vs in (in_ or {}).items(): rows = [r for r in rows if r.get(k) in set(vs)]
    return rows
T._select = fake
b = T.build_training_xlsx("life_class", {"Organization": "Active Church"}, "2026-05-01", "2026-07-31")
wb = openpyxl.load_workbook(io.BytesIO(b))
print(wb.sheetnames)
orig_men = sum(sum(p["sessions"]) for p in fx["MEN"]); ws = wb["MEN"]
got = sum(1 for r in ws.iter_rows(min_row=2, min_col=5, max_col=13) for c in r if c.value == "X")
print("MEN attendance marks: expected", orig_men, "got", got)
for rt in ("plan_40", "school_of_leaders"):
    print(rt, "payload ok" if T.PAYLOADS[rt](T.load_training(rt, None, "2026-05-01", "2026-07-31")) is not None else "")

v = T.build_training_view("life_class", T.load_training("life_class", None, "2026-05-01", "2026-07-31"))
print({k["key"]: k["value"] for k in v["kpis"]}, [t["key"] for t in v["tables"]], v["series"][0]["points"][:3])
for rt in ("plan_40", "school_of_leaders"):
    vv = T.build_training_view(rt, T.load_training(rt, None, "2026-05-01", "2026-07-31")); print(rt, [k["key"] for k in vv["kpis"]])
