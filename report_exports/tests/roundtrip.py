"""Rebuild each report from a payload parsed out of the original workbook, then diff against the original.
Run from the parent dir:  python -m report_exports.tests.roundtrip"""
import io, json, openpyxl
from report_exports import REPORTS, build_report
from report_exports.tests.excel_to_payload import ALL, U

FILES = dict(life_class="LifeClass_Term_2_-_2026.xlsx", school_of_leaders="School_of_Leaders_Term_2-_2026.xlsx",
             plan_40="Plan_40_Term_2_2026.xlsx", potential_leaders="Potential_Leaders.xlsx",
             twelve_tasks="12_Tasks.xlsx", service_target="Service_Target.xlsx")


def sig(c):
    f = c.fill.fgColor.rgb if c.fill.fill_type else None
    v = c.value
    if isinstance(v, str): v = " ".join(v.split()).lower() if not v.startswith("=") else v.upper().replace(" ", "")
    if isinstance(v, float) and v == int(v): v = int(v)
    return v, (f[-6:] if isinstance(f, str) and f.startswith("FF") else None), bool(c.font.b)


def cells(ws):
    return {c.coordinate: sig(c) for row in ws.iter_rows() for c in row if c.value not in (None, "")}


for rt in REPORTS:
    payload = ALL[rt]()
    open(f"/home/claude/report_exports/fixtures/{rt}.json", "w").write(json.dumps(payload, indent=1))
    mine = openpyxl.load_workbook(io.BytesIO(build_report(rt, payload)))
    orig = openpyxl.load_workbook(U + FILES[rt])
    print(f"\n=== {rt}: sheets mine={mine.sheetnames} orig={orig.sheetnames}")
    for name in orig.sheetnames:
        if name not in mine.sheetnames: print("  MISSING sheet", name); continue
        a, b = cells(orig[name]), cells(mine[name])
        keys = set(a) | set(b)
        diff = [(k, a.get(k), b.get(k)) for k in sorted(keys) if (a.get(k) or (None,))[0] != (b.get(k) or (None,))[0]]
        fdiff = [(k, a[k][1], b[k][1]) for k in keys if k in a and k in b and a[k][1] != b[k][1]]
        print(f"  {name}: orig cells={len(a)} mine={len(b)} value-diffs={len(diff)} fill-diffs={len(fdiff)}")
        for d in diff[:6]: print("     val", d)
        for d in fdiff[:3]: print("     fill", d)
