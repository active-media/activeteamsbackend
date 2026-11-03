import re, subprocess, io, json, openpyxl
from report_exports import build_report
from report_exports.staff_interns_youth import build_view
txt = subprocess.run(["pdftotext", "-layout", "/mnt/user-data/uploads/Staff_and_interns_youth_stats_-_Sheet1.pdf", "-"], capture_output=True, text=True).stdout
groups, cur = [], None
for line in txt.splitlines():
    line = line.strip()
    if not line or line.startswith("Names") or line.lower().startswith("total"): continue
    ms = re.findall(r"([A-Za-z][A-Za-z\- ]*?)\s+(\d+)\s+(\d+)\s+(\d+)", line)
    if ms and not re.fullmatch(r"[A-Za-z\- ]+", line):
        for n, a, b, c in ms: cur["rows"].append(dict(name=n.strip(), previous=int(a), target=int(b), actual=int(c)))
    else:
        cur = dict(leader=line, rows=[]); groups.append(cur)
payload = dict(groups=groups)
json.dump(payload, open("/home/claude/report_exports/tests/staff_payload.json", "w"), indent=1)
print(len(groups), "groups,", sum(len(g["rows"]) for g in groups), "people")
ws = openpyxl.load_workbook(io.BytesIO(build_report("staff_interns_youth", payload))).active
print([ws.cell(r, 1).value for r in range(1, 9)], ws.merged_cells.ranges and len(ws.merged_cells.ranges))
v = build_view(payload); print({k["key"]: k["value"] for k in v["kpis"]})
