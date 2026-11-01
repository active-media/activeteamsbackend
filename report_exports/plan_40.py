"""Plan 40 workbook (Plan_40_Term_2_2026.xlsx)

Sheets: MEN | WOMEN | STATS W1..W5
payload = {
  "weeks": 5,
  "leaders": {"MEN": ["Gavin Enslin", "Esdras Muta", ...], "WOMEN": ["Vicky Enslin", ...]},   # STATS roster, top leader first
  "top_leader": {"MEN": "Gavin Enslin", "WOMEN": "Vicky Enslin"},  # attendees with no Leader@12 are counted here
  "MEN": [a, ...], "WOMEN": [a, ...]
}
a = {"name", "leader12", "leader144", "weeks": [bool x5]}
"""
from collections import Counter
from openpyxl import Workbook
from .common import *


def _people_sheet(ws, people, n):
    heads = ["Name & Surname", "Leader at 12", "Leader at 144"] + [f"Week {i}" for i in range(1, n + 1)] + ["Total Attendance"]
    for c, t in enumerate(heads, 1):
        put(ws, 1, c, t, bold=True, h="center" if c in (1, len(heads)) else None)
    from openpyxl.utils import get_column_letter as L
    for r, a in enumerate(people, 2):
        put(ws, r, 1, clean(a["name"]), bg=WHITE)
        put(ws, r, 2, clean(a.get("leader12")))
        put(ws, r, 3, clean(a.get("leader144")))
        w = (list(a.get("weeks") or []) + [False] * n)[:n]
        for i, on in enumerate(w):
            put(ws, r, 4 + i, "X" if on else None, h="center")
        put(ws, r, 4 + n, f"=COUNTA(D{r}:{L(3 + n)}{r})", bold=True, h="center")
    last = 1 + len(people)
    for i in range(n):
        col = L(4 + i)
        put(ws, last + 1, 4 + i, f"=COUNTA({col}2:{col}{last})", bold=True, h="center")
    widths(ws, {"A": 19.5, "B": 20.2})


def _stats_sheet(ws, payload, week_idx):
    r = 1
    put(ws, r, 1, f"WEEK {week_idx + 1}", bold=True, border=False)
    r += 1
    totals = []
    for gender in ("MEN", "WOMEN"):
        top = payload.get("top_leader", {}).get(gender, "")
        counts = Counter()
        for a in payload.get(gender, []):
            w = a.get("weeks") or []
            if week_idx < len(w) and w[week_idx]:
                counts[clean(a.get("leader12")) or clean(top)] += 1
        put(ws, r, 1, gender, bold=True)
        put(ws, r + 1, 1, "Name & Surname", bold=True)
        put(ws, r + 1, 2, "Number ", bold=True)
        first = r + 2
        roster = payload.get("leaders", {}).get(gender, [])
        for i, name in enumerate(roster):
            put(ws, first + i, 1, name)
            put(ws, first + i, 2, counts.get(clean(name), 0), h="center")
        last = first + len(roster) - 1
        put(ws, last, 3, f"=SUM(B{first}:B{last})", bold=True, h="center", border=False)
        totals.append(last)
        r = last + 2
    put(ws, totals[1] + 1, 3, f"=SUM(C{totals[0]},C{totals[1]})", bold=True, h="center", border=False)
    widths(ws, {"A": 17.5})


def build(payload: dict) -> bytes:
    n = payload.get("weeks", 5)
    wb = Workbook()
    wb.remove(wb.active)
    for g in ("MEN", "WOMEN"):
        _people_sheet(wb.create_sheet(g), payload.get(g, []), n)
    for w in range(n):
        _stats_sheet(wb.create_sheet(f"STATS W{w + 1}"), payload, w)
    return to_bytes(wb)
