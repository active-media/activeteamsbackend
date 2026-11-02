"""School of Leaders workbook (School_of_Leaders_Term_2-_2026.xlsx)

Each sheet holds several stacked cohorts ("Destiny 1 & 2", "Destiny 3 & 4", ...), each with its own
repeated header row and a 10-week attendance grid. Source sheets: "Sheet1" (Leader@12 only) and
"YOML" (Leader@12 + Leader@144).

payload = {"groups": [
   {"sheet_name": "Sheet1", "show_leader144": False,
    "cohorts": [{"label": "Destiny 1 & 2", "attendees": [a, ...]}, {"label": "Destiny 3 & 4", ...}]},
   {"sheet_name": "YOML", "show_leader144": True, "cohorts": [...]},
]}
a = {"name", "leader12", "leader144", "sessions": [bool x10]}
"""
from openpyxl import Workbook
from openpyxl.utils import get_column_letter as L
from .common import *

WEEKS = 10


def build(payload: dict) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    for g in payload.get("groups", []):
        ws = wb.create_sheet(g["sheet_name"][:31])
        l144 = g.get("show_leader144", False)
        first = 4 if l144 else 3
        last = first + WEEKS - 1
        r = 1
        for cohort in g.get("cohorts", []):
            heads = [cohort["label"], "Leader at 12"] + (["Leader at 144"] if l144 else []) \
                + [f"Week {i}" for i in range(1, WEEKS + 1)] + ["Total"]
            for c, t in enumerate(heads, 1):
                put(ws, r, c, t, bold=True, bg=WHITE, h="center" if c >= first else None)
            head = r
            r += 1
            for a in cohort.get("attendees", []):
                put(ws, r, 1, clean(a["name"]), bg=WHITE, border=False)
                put(ws, r, 2, clean(a.get("leader12")), bg=WHITE, border=False)
                if l144:
                    put(ws, r, 3, clean(a.get("leader144")), bg=WHITE, border=False)
                s = (list(a.get("sessions") or []) + [False] * WEEKS)[:WEEKS]
                for i, on in enumerate(s):
                    put(ws, r, first + i, "X" if on else None, bg=WHITE, h="center")
                put(ws, r, last + 1, f"=COUNTA({L(first)}{r}:{L(last)}{r})", bold=True, bg=WHITE, h="center")
                r += 1
            # per-week column totals directly under each cohort, then one blank row
            for i in range(WEEKS):
                col = L(first + i)
                put(ws, r, first + i, f"=COUNTA({col}{head + 1}:{col}{r - 1})", bold=True, bg=WHITE, h="center")
            r += 2
        widths(ws, {"A": 22.5, "B": 16.9, "C": 10.2 if not l144 else 16.9})
    return to_bytes(wb)
