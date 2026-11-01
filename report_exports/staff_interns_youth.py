"""Staff / Interns / Youth stats workbook (Staff_and_interns_youth_stats - Sheet1.pdf)

One block per Leader@12: a centred leader-name row, one row per person, then a Total row.
Columns: Names | Previous Week Stats | Target | Actual Stats

payload = {"groups": [{"leader": "Bernice van der Walt",
                       "rows": [{"name": "Birra", "previous": 0, "target": 3, "actual": 0}, ...]}, ...]}
"""
from openpyxl import Workbook
from openpyxl.utils import get_column_letter as L
from .common import *


def build(payload: dict) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    for c, t in enumerate(["Names", "Previous Week Stats", "Target", "Actual Stats"], 1):
        put(ws, 1, c, t, bold=True, h="center")
    r = 2
    for g in payload.get("groups", []):
        put(ws, r, 1, clean(g["leader"]), bold=True, h="center")
        for c in (2, 3, 4):
            put(ws, r, c)
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)
        r += 1
        first = r
        for row in g.get("rows", []):
            put(ws, r, 1, clean(row["name"]), h="center")
            put(ws, r, 2, row.get("previous", 0), h="center")
            put(ws, r, 3, row.get("target", 0), h="center")
            put(ws, r, 4, row.get("actual", 0), h="center")
            r += 1
        put(ws, r, 1, "Total :", h="center")
        for c in (2, 3, 4):
            col = L(c)
            put(ws, r, c, f"=SUM({col}{first}:{col}{r - 1})" if r > first else 0, bold=(c != 3), h="center")
        r += 1
    widths(ws, {"A": 22, "B": 20, "C": 14, "D": 16})
    return to_bytes(wb)


def build_view(payload: dict) -> dict:
    """Envelope content for the on-screen report (kpis / tables / series)."""
    rows, leaders = [], []
    for g in payload.get("groups", []):
        t = {k: sum(x.get(k, 0) or 0 for x in g["rows"]) for k in ("previous", "target", "actual")}
        leaders.append({"leader": clean(g["leader"]), "people": len(g["rows"]), **t})
        rows += [{"leader": clean(g["leader"]), "name": clean(x["name"]), "previous": x.get("previous", 0),
                  "target": x.get("target", 0), "actual": x.get("actual", 0)} for x in g["rows"]]
    tot = lambda k: sum(x[k] for x in leaders)
    return {
        "kpis": [{"key": "people", "label": "People tracked", "value": len(rows), "format": "number"},
                 {"key": "actual", "label": "Actual stats", "value": tot("actual"), "format": "number"},
                 {"key": "target", "label": "Target", "value": tot("target"), "format": "number"},
                 {"key": "previous", "label": "Previous week", "value": tot("previous"), "format": "number"}],
        "tables": [
            {"key": "leaders", "title": "By Leader at 12", "columns": [
                {"key": "leader", "label": "Leader"}, {"key": "people", "label": "People", "format": "number"},
                {"key": "previous", "label": "Previous week", "format": "number"}, {"key": "target", "label": "Target", "format": "number"},
                {"key": "actual", "label": "Actual", "format": "number"}], "rows": leaders},
            {"key": "people", "title": "People", "columns": [
                {"key": "name", "label": "Name"}, {"key": "leader", "label": "Leader at 12"},
                {"key": "previous", "label": "Previous week", "format": "number"}, {"key": "target", "label": "Target", "format": "number"},
                {"key": "actual", "label": "Actual", "format": "number"}], "rows": rows}],
        "series": [{"key": "by_leader", "title": "Actual vs target by leader", "type": "bar",
                    "points": [{"x": x["leader"], "y": x["actual"]} for x in leaders]}],
    }
