"""Database layer for the training-style reports (Life Class, School of Leaders, Plan 40).

All three are the same shape: an event (or several) with weekly sessions, a roster of attendees
(event_attendees: registration_type, leader12, leader144) and per-session presence
(event_session_attendees.is_checked_in  OR  event_training_attendance.is_present).

Assumptions to confirm are marked ASSUMPTION.
"""
from __future__ import annotations
import os
from collections import defaultdict
from .common import clean
from .registry import build_report
from .common import reg_kind
from .life_class import SESSIONS as LC_LABELS

TRAINING_REPORT_TYPES = {"life_class", "school_of_leaders", "plan_40"}

# ASSUMPTION: event_type_name / event_name contains these strings (case-insensitive)
EVENT_MATCH = {"life_class": "life class", "school_of_leaders": "school of leaders", "plan_40": "plan 40"}
SESSIONS = {"life_class": 9, "school_of_leaders": 10, "plan_40": 5}   # Life Class slot 5 = ENCOUNTER
TOP_LEADER = {"MEN": os.getenv("TOP_LEADER_MEN", "Gavin Enslin"), "WOMEN": os.getenv("TOP_LEADER_WOMEN", "Vicky Enslin")}
YOML_MARKER = "yoml"      # ASSUMPTION: School of Leaders events with this in the name go on the YOML sheet


# ── query helper (single choke point so tests can monkeypatch it) ───────────
def _select(table, cols="*", eq=None, in_=None, ilike=None, gte=None, lte=None):
    from supabase_helpers.supabase_connection import supabase_admin
    out, page, start = [], 1000, 0
    chunks = [None]
    if in_:
        (k, vals), = in_.items()
        vals = list(vals)
        chunks = [vals[i:i + 150] for i in range(0, len(vals), 150)] or [[]]
    for chunk in chunks:
        start = 0
        while True:
            q = supabase_admin.table(table).select(cols)
            for k, v in (eq or {}).items():
                q = q.eq(k, v)
            for k, v in (ilike or {}).items():
                q = q.ilike(k, v)
            for k, v in (gte or {}).items():
                q = q.gte(k, v)
            for k, v in (lte or {}).items():
                q = q.lte(k, v)
            if chunk is not None:
                if not chunk:
                    break
                q = q.in_(list(in_)[0], chunk)
            rows = q.range(start, start + page - 1).execute().data or []
            out.extend(rows)
            if len(rows) < page:
                break
            start += page
    return out


def _person_key(mongo_id, name):
    return str(mongo_id) if mongo_id else clean(name).lower()


def _visible_names(visible_leader_ids):
    if visible_leader_ids is None:
        return None
    rows = _select("People", "Name,Surname", in_={"_id": list(visible_leader_ids)}) if visible_leader_ids else []
    return {clean(f"{r.get('Name','')} {r.get('Surname','')}").lower() for r in rows}


def load_training(report_type, org_filter, start, end, visible_leader_ids=None):
    """-> list of events, each {"event": row, "people": [ {name, gender, leader12, leader144, registration_type, present:[bool]*N} ]}"""
    org = (org_filter or {}).get("Organization")
    n = SESSIONS[report_type]
    match = EVENT_MATCH[report_type]
    events = _select("events", "event_id,event_name,event_type_name,Organization", eq={"Organization": org} if org else None,
                     ilike={"event_type_name": f"%{match}%"})
    events += [e for e in _select("events", "event_id,event_name,event_type_name,Organization",
                                  eq={"Organization": org} if org else None, ilike={"event_name": f"%{match}%"})
               if e["event_id"] not in {x["event_id"] for x in events}]
    if not events:
        return []
    ids = [e["event_id"] for e in events]
    sessions = _select("event_sessions", "session_id,event_id,session_date,is_did_not_meet", in_={"event_id": ids},
                       gte={"session_date": start}, lte={"session_date": end})
    sessions = [s for s in sessions if not s.get("is_did_not_meet") and s.get("session_date")]
    if not sessions:
        return []
    sess_ids = [s["session_id"] for s in sessions]
    roster = _select("event_attendees", "event_id,mongo_person_id,full_name,leader12,leader144,registration_type", in_={"event_id": ids})
    present = defaultdict(set)     # session_id -> {person_key}
    for r in _select("event_session_attendees", "session_id,mongo_person_id,full_name,is_checked_in", in_={"session_id": sess_ids}):
        if r.get("is_checked_in"):
            present[r["session_id"]].add(_person_key(r.get("mongo_person_id"), r.get("full_name")))
    for r in _select("event_training_attendance", "session_id,mongo_person_id,full_name,is_present", in_={"session_id": sess_ids}):
        if r.get("is_present"):
            present[r["session_id"]].add(_person_key(r.get("mongo_person_id"), r.get("full_name")))
    # people that attended but are not on the roster still belong in the report
    known = {(r["event_id"], _person_key(r.get("mongo_person_id"), r.get("full_name"))) for r in roster}
    by_session_event = {s["session_id"]: s["event_id"] for s in sessions}
    for sid, keys in present.items():
        for k in keys:
            if (by_session_event[sid], k) not in known:
                roster.append({"event_id": by_session_event[sid], "mongo_person_id": None, "full_name": k.title(),
                               "leader12": "", "leader144": "", "registration_type": ""})
                known.add((by_session_event[sid], k))
    gender_ids = [r["mongo_person_id"] for r in roster if r.get("mongo_person_id")]
    gender = {p["_id"]: clean(p.get("Gender")).lower() for p in _select("People", "_id,Gender", in_={"_id": gender_ids})} if gender_ids else {}
    vis = _visible_names(visible_leader_ids)

    out = []
    for ev in events:
        evs = sorted([s for s in sessions if s["event_id"] == ev["event_id"]], key=lambda s: s["session_date"])[:n]
        if not evs:
            continue
        people = []
        for r in roster:
            if r["event_id"] != ev["event_id"]:
                continue
            key = _person_key(r.get("mongo_person_id"), r.get("full_name"))
            if vis is not None and not ({clean(r.get("leader12")).lower(), clean(r.get("leader144")).lower(),
                                         clean(r.get("full_name")).lower()} & vis):
                continue
            flags = [key in present[s["session_id"]] for s in evs] + [False] * (n - len(evs))
            people.append({"name": clean(r.get("full_name")), "gender": gender.get(r.get("mongo_person_id"), ""),
                           "leader12": clean(r.get("leader12")), "leader144": clean(r.get("leader144")),
                           "registration_type": clean(r.get("registration_type")), "present": flags})
        people.sort(key=lambda p: (p["leader12"].lower(), p["name"].lower()))
        out.append({"event": ev, "people": people})
    return out


def _split_gender(people):
    men = [p for p in people if p["gender"].startswith("m")]
    women = [p for p in people if p["gender"].startswith("f")]
    return men, women           # unknown gender is dropped from the gendered sheets (see README)


# ── payload builders ────────────────────────────────────────────────────────
def payload_life_class(data):
    people = [p for d in data for p in d["people"]]
    men, women = _split_gender(people)
    conv = lambda ps: [dict(name=p["name"], leader12=p["leader12"], leader144=p["leader144"],
                            registration_type=p["registration_type"], sessions=p["present"]) for p in ps]
    return {"MEN": conv(men), "WOMEN": conv(women)}


def payload_plan_40(data):
    people = [p for d in data for p in d["people"]]
    men, women = _split_gender(people)
    conv = lambda ps: [dict(name=p["name"], leader12=p["leader12"], leader144=p["leader144"], weeks=p["present"]) for p in ps]
    payload = {"weeks": 5, "MEN": conv(men), "WOMEN": conv(women), "top_leader": TOP_LEADER, "leaders": {}}
    for g, ps in (("MEN", men), ("WOMEN", women)):
        names = sorted({p["leader12"] for p in ps if p["leader12"] and p["leader12"].lower() != TOP_LEADER[g].lower()}, key=str.lower)
        payload["leaders"][g] = [TOP_LEADER[g]] + names
    return payload


def payload_school_of_leaders(data):
    groups = {}
    for d in data:
        name = clean(d["event"].get("event_name"))
        yoml = YOML_MARKER in name.lower()
        g = groups.setdefault("YOML" if yoml else "Sheet1", {"sheet_name": "YOML" if yoml else "Sheet1",
                                                             "show_leader144": yoml, "cohorts": []})
        g["cohorts"].append({"label": name, "attendees": [dict(name=p["name"], leader12=p["leader12"], leader144=p["leader144"],
                                                              sessions=p["present"]) for p in d["people"]]})
    return {"groups": [groups[k] for k in ("Sheet1", "YOML") if k in groups]}


PAYLOADS = {"life_class": payload_life_class, "plan_40": payload_plan_40, "school_of_leaders": payload_school_of_leaders}


def build_training_xlsx(report_type, org_filter, start, end, visible_leader_ids=None) -> bytes:
    data = load_training(report_type, org_filter, start, end, visible_leader_ids)
    return build_report(report_type, PAYLOADS[report_type](data))


# ── on-screen view (feeds reporting_core/envelope.py::training_envelope) ─────
def _labels(rt):
    return list(LC_LABELS) if rt == "life_class" else [f"Week {i + 1}" for i in range(SESSIONS[rt])]


def _status(rt, attended):
    """Completion rules agreed for the ministries: Life Class 8-9 of 9 = completed, 6-7 = almost;
    School of Leaders 8-10 of 10 = completed, 7 = almost; Plan 40 has no pass rule."""
    if rt == "life_class":
        return "Completed" if attended >= 8 else "Almost there" if attended >= 6 else "Not completed"
    if rt == "school_of_leaders":
        return "Completed" if attended >= 8 else "Almost there" if attended == 7 else "Not completed"
    return "Attended" if attended else "Not attended"


def build_training_view(rt, data):
    people = [p for d in data for p in d["people"]]
    n, labels = SESSIONS[rt], _labels(rt)
    for p in people:
        p["_n"] = sum(p["present"])
        p["_kind"] = reg_kind(p.get("registration_type"))
    per_session = [sum(1 for p in people if p["present"][i]) for i in range(n)]
    has_rule = rt != "plan_40"
    completed = sum(1 for p in people if _status(rt, p["_n"]) == "Completed") if has_rule else None
    kpis = [
        {"key": "registered", "label": "Registered", "value": len(people), "format": "number"},
        {"key": "attended", "label": "Attended at least once", "value": sum(1 for p in people if p["_n"]), "format": "number"},
        {"key": "attendance_total", "label": "Total attendance", "value": sum(per_session), "format": "number", "hint": "Sum of all sessions"},
    ]
    if has_rule:
        kpis.append({"key": "completed", "label": "Completed", "value": completed, "format": "number"})
    if rt == "life_class":
        kpis.insert(1, {"key": "guides", "label": "Guides", "value": sum(1 for p in people if p["_kind"] == "guide"), "format": "number"})

    # weekly table
    weekly = []
    for i, label in enumerate(labels):
        row = {"session": label, "total": per_session[i]}
        if rt == "life_class":
            for k in ("guide", "first", "second"):
                row[k] = sum(1 for p in people if p["present"][i] and p["_kind"] == k)
        elif rt == "plan_40":
            row["men"] = sum(1 for p in people if p["present"][i] and p["gender"].startswith("m"))
            row["women"] = sum(1 for p in people if p["present"][i] and p["gender"].startswith("f"))
        weekly.append(row)
    wcols = [{"key": "session", "label": "Session"}]
    if rt == "life_class":
        wcols += [{"key": "guide", "label": "Guides", "format": "number"}, {"key": "first", "label": "First time", "format": "number"},
                  {"key": "second", "label": "Second time", "format": "number"}]
    if rt == "plan_40":
        wcols += [{"key": "men", "label": "Men", "format": "number"}, {"key": "women", "label": "Women", "format": "number"}]
    wcols.append({"key": "total", "label": "Total", "format": "number"})

    # per leader at 12
    by_leader = {}
    for p in people:
        by_leader.setdefault(p["leader12"] or "Unassigned", []).append(p)
    leaders = []
    for name in sorted(by_leader, key=lambda x: (x == "Unassigned", x.lower())):
        ps = by_leader[name]
        row = {"leader": name, "registered": len(ps), "attended": sum(1 for p in ps if p["_n"]),
               "attendance": sum(p["_n"] for p in ps)}
        if has_rule:
            row["completed"] = sum(1 for p in ps if _status(rt, p["_n"]) == "Completed")
        leaders.append(row)
    lcols = [{"key": "leader", "label": "Leader at 12"}, {"key": "registered", "label": "Registered", "format": "number"},
             {"key": "attended", "label": "Attended", "format": "number"}, {"key": "attendance", "label": "Total attendance", "format": "number"}]
    if has_rule:
        lcols.append({"key": "completed", "label": "Completed", "format": "number"})

    acols = [{"key": "name", "label": "Name"}, {"key": "leader12", "label": "Leader at 12"}, {"key": "leader144", "label": "Leader at 144"}]
    if rt == "life_class":
        acols.append({"key": "registration_type", "label": "Registration"})
    acols += [{"key": "sessions", "label": f"Sessions (of {n})", "format": "number"}, {"key": "status", "label": "Status"}]
    attendees = [{"name": p["name"], "leader12": p["leader12"], "leader144": p["leader144"],
                  "registration_type": p["registration_type"], "sessions": p["_n"], "status": _status(rt, p["_n"])} for p in people]

    tables = [{"key": "weekly", "title": "Attendance by session", "columns": wcols, "rows": weekly},
              {"key": "leaders", "title": "By Leader at 12", "columns": lcols, "rows": leaders}]
    if rt == "school_of_leaders":
        tables.insert(1, {"key": "cohorts", "title": "By cohort", "columns": [
            {"key": "cohort", "label": "Cohort"}, {"key": "registered", "label": "Registered", "format": "number"},
            {"key": "completed", "label": "Completed", "format": "number"}],
            "rows": [{"cohort": clean(d["event"].get("event_name")), "registered": len(d["people"]),
                      "completed": sum(1 for p in d["people"] if _status(rt, sum(p["present"])) == "Completed")} for d in data]})
    tables.append({"key": "attendees", "title": "Attendees", "columns": acols, "rows": attendees})
    series = [{"key": "session_attendance", "title": "Attendance by session", "type": "bar",
               "points": [{"x": l, "y": v} for l, v in zip(labels, per_session)]}]
    return {"kpis": kpis, "tables": tables, "series": series}
