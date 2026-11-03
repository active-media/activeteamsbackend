"""Reverse-parse the admin team's original workbooks into builder payloads (fixtures / round-trip tests)."""
import openpyxl
from report_exports.common import clean

U = "/mnt/user-data/uploads/"


def _x(c): return bool(c.value) and str(c.value).strip() != ""


def life_class():
    wb = openpyxl.load_workbook(U + "LifeClass_Term_2_-_2026.xlsx")
    out = {}
    for g in ("MEN", "WOMEN"):
        ws, rows = wb[g], []
        for r in range(2, ws.max_row + 1):
            if not clean(ws.cell(r, 1).value): continue
            rows.append(dict(name=clean(ws.cell(r, 1).value), leader12=clean(ws.cell(r, 2).value),
                             leader144=clean(ws.cell(r, 3).value), registration_type=clean(ws.cell(r, 4).value),
                             sessions=[_x(ws.cell(r, c)) for c in range(5, 14)]))
        out[g] = rows
    return out


def school_of_leaders():
    wb = openpyxl.load_workbook(U + "School_of_Leaders_Term_2-_2026.xlsx")
    groups = []
    for ws in wb:
        l144 = clean(ws.cell(1, 3).value).lower() == "leader at 144"
        first = 4 if l144 else 3
        cohorts, cur = [], None
        for r in range(1, ws.max_row + 1):
            a = clean(ws.cell(r, 1).value)
            if not a: continue
            if a.lower().startswith("destiny"):
                cur = dict(label=a, attendees=[]); cohorts.append(cur); continue
            cur["attendees"].append(dict(name=a, leader12=clean(ws.cell(r, 2).value),
                leader144=clean(ws.cell(r, 3).value) if l144 else "",
                sessions=[_x(ws.cell(r, c)) for c in range(first, first + 10)]))
        groups.append(dict(sheet_name=ws.title, show_leader144=l144, cohorts=cohorts))
    return dict(groups=groups)


def plan_40():
    wb = openpyxl.load_workbook(U + "Plan_40_Term_2_2026.xlsx")
    p = {"weeks": 5}
    for g in ("MEN", "WOMEN"):
        ws, rows = wb[g], []
        for r in range(2, ws.max_row + 1):
            if not clean(ws.cell(r, 1).value): continue
            rows.append(dict(name=clean(ws.cell(r, 1).value), leader12=clean(ws.cell(r, 2).value),
                             leader144=clean(ws.cell(r, 3).value), weeks=[_x(ws.cell(r, c)) for c in range(4, 9)]))
        p[g] = rows
    ws = wb["STATS W1"]
    roster = {"MEN": [], "WOMEN": []}; cur = None
    for r in range(1, ws.max_row + 1):
        a = clean(ws.cell(r, 1).value)
        if a in ("MEN", "WOMEN"): cur = a; continue
        if cur and a and a not in ("Name & Surname",) and not a.startswith("WEEK"): roster[cur].append(a)
    p["leaders"] = roster
    p["top_leader"] = {"MEN": roster["MEN"][0], "WOMEN": roster["WOMEN"][0]}
    return p


def potential_leaders():
    wb = openpyxl.load_workbook(U + "Potential_Leaders.xlsx")
    out = {}
    for name, key, with_l in (("Men", "MEN", True), ("Women", "WOMEN", False)):
        ws, blocks = wb[name], []
        ncols = 4 if with_l else 3; stride = ncols + 2
        for r in range(1, ws.max_row + 1):
            if clean(ws.cell(r, 1).value) != "Leader @12": continue
            for k in range(3):
                c0 = 1 + k * stride
                if clean(ws.cell(r, c0).value) != "Leader @12" or not clean(ws.cell(r + 1, c0).value): continue
                b = dict(leader12=clean(ws.cell(r + 1, c0).value), leaders=[], potential=[])
                rr = r + 1
                while rr <= ws.max_row and (rr == r + 1 or not clean(ws.cell(rr, c0).value)) and clean(ws.cell(rr, 1).value) != "Leader @12":
                    if with_l and clean(ws.cell(rr, c0 + 1).value): b["leaders"].append(clean(ws.cell(rr, c0 + 1).value))
                    pc = c0 + (2 if with_l else 1)
                    if clean(ws.cell(rr, pc).value): b["potential"].append(dict(name=clean(ws.cell(rr, pc).value), comment=clean(ws.cell(rr, pc + 1).value)))
                    rr += 1
                blocks.append(b)
        out[key] = blocks
    return out


def twelve_tasks():
    ws = openpyxl.load_workbook(U + "12_Tasks.xlsx").active
    return dict(rows=[dict(name=clean(ws.cell(r, 1).value), last_week=int(ws.cell(r, 2).value or 0),
                           target=int(ws.cell(r, 3).value or 0), actual=int(ws.cell(r, 4).value or 0))
                      for r in range(2, ws.max_row + 1) if clean(ws.cell(r, 1).value)])


def service_target():
    ws = openpyxl.load_workbook(U + "Service_Target.xlsx").active
    groups, cur = [], []
    for r in range(2, ws.max_row + 1):
        n = clean(ws.cell(r, 1).value)
        if not n:
            if cur: groups.append(cur); cur = []
            continue
        cur.append(dict(name=n, targets={"Friday": ws.cell(r, 2).value, "Sunday": ws.cell(r, 4).value},
                        actuals={"Friday": ws.cell(r, 3).value, "Sunday": ws.cell(r, 5).value}))
    if cur: groups.append(cur)
    return dict(services=["Friday", "Sunday"], groups=groups)


ALL = dict(life_class=life_class, school_of_leaders=school_of_leaders, plan_40=plan_40,
           potential_leaders=potential_leaders, twelve_tasks=twelve_tasks, service_target=service_target)
