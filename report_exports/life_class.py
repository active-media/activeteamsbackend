"""Life Class workbook  (LifeClass_Term_2_-_2026.xlsx)

Sheets (per gender):  MEN | GUIDE STATE MEN | GUIDE STATE FOR MEN | WOMEN | GUIDE STATE WOMEN | GUIDE STATE FOR WOMEN

payload = {
  "MEN":   [attendee, ...],
  "WOMEN": [attendee, ...],
}
attendee = {
  "name": str, "leader12": str, "leader144": str,         # leader144 == the delegate's guide ('' if none)
  "registration_type": "Guide" | "Delegate: First Time" | "Delegate: Second Time",
  "sessions": [bool x9],   # order: W1, W2, W3, W4, ENCOUNTER, W5, W6, W7, W8
}
The two GUIDE STATE sheets are *derived* from the attendee rows, so the backend only
has to supply the attendee list.
"""
from __future__ import annotations
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.utils import get_column_letter as L
from .common import *

SESSIONS = ["WEEK 1", "WEEK 2", "WEEK 3", "WEEK 4", "ENCOUNTER", "WEEK 5", "WEEK 6", "WEEK 7", "WEEK 8"]
SHORT = ["W1", "W2", "W3", "W4", "ENCOUNTER", "W5", "W6", "W7", "W8"]
NOLEAD = "Unassigned"


def _sess(a):
    s = list(a.get("sessions") or [])
    return (s + [False] * 9)[:9]


# ── derived stats ───────────────────────────────────────────────────────────
def guide_state(attendees):
    """[(leader12, [(guide_name, [9 delegate counts]), ...]), ...]  leader's own row first."""
    by_leader = defaultdict(list)
    for a in attendees:
        by_leader[clean(a.get("leader12")) or NOLEAD].append(a)
    out = []
    for leader in sorted(by_leader, key=lambda x: (x == NOLEAD, x.lower())):
        people = by_leader[leader]
        guides = {clean(a["name"]) for a in people if reg_kind(a.get("registration_type")) == "guide"}
        guides |= {clean(a.get("leader144")) for a in people
                   if reg_kind(a.get("registration_type")) != "guide" and clean(a.get("leader144"))}
        guides.discard(leader)
        rows = {leader: [0] * 9, **{g: [0] * 9 for g in guides}}
        for a in people:
            if reg_kind(a.get("registration_type")) == "guide":
                continue                      # guides are not delegates
            key = clean(a.get("leader144")) or leader
            if key not in rows:
                key = leader
            for i, on in enumerate(_sess(a)):
                rows[key][i] += 1 if on else 0
        ordered = [(leader, rows[leader])] + [(g, rows[g]) for g in sorted(guides, key=str.lower)]
        out.append((leader, ordered))
    return out


def leader_state(attendees):
    """[(leader12, guides[9], first[9], second[9])]"""
    agg = defaultdict(lambda: {"guide": [0] * 9, "first": [0] * 9, "second": [0] * 9})
    for a in attendees:
        k = reg_kind(a.get("registration_type"))
        if not k:
            continue
        leader = clean(a.get("leader12")) or NOLEAD
        for i, on in enumerate(_sess(a)):
            agg[leader][k][i] += 1 if on else 0
    return [(l, agg[l]["guide"], agg[l]["first"], agg[l]["second"])
            for l in sorted(agg, key=lambda x: (x == NOLEAD, x.lower()))]


# ── sheet writers ───────────────────────────────────────────────────────────
def _attendance_sheet(ws, attendees, women):
    heads = ["Names & Surname", "Leader at 12", "Leader at 144", "Registration type", *SESSIONS, "TOTAL"]
    for c, t in enumerate(heads, 1):
        put(ws, 1, c, t, bold=(c == 1), h="center" if c >= 9 else None)
    for r, a in enumerate(attendees, 2):
        kind = reg_kind(a.get("registration_type"))
        put(ws, r, 1, clean(a["name"]), bg=WHITE)
        put(ws, r, 2, clean(a.get("leader12")))
        put(ws, r, 3, clean(a.get("leader144")))
        put(ws, r, 4, REG_LABEL.get(kind, clean(a.get("registration_type"))), bg=REG_FILL.get(kind))
        for i, on in enumerate(_sess(a)):
            put(ws, r, 5 + i, "X" if on else None, h="center")
        put(ws, r, 14, f"=COUNTA(E{r}:M{r})", h="center")
    last = 1 + len(attendees)
    # column totals directly under the roster
    tot = last + 1
    for i in range(9):
        col = L(5 + i)
        put(ws, tot, 5 + i, f"=COUNTA({col}2:{col}{last})", bold=True, h="center")
    # STATS block: attendance per Leader@12 per session
    s0 = tot + 5
    put(ws, s0, 4, "STATS", bold=True)
    for i, t in enumerate(["Week 1", "Week 2", "Week 3", "Week 4", "Encounter", "Week 5", "Week 6", "Week 7", "Week 8", "Total"]):
        put(ws, s0, 5 + i, t, bold=True, h="center")
    for k, (leader, g, f, s) in enumerate(leader_state(attendees), 1):
        put(ws, s0 + k, 4, leader)
        for i in range(9):
            put(ws, s0 + k, 5 + i, g[i] + f[i] + s[i], h="center")
        put(ws, s0 + k, 14, f"=SUM(E{s0 + k}:M{s0 + k})", bold=True, h="center")
    widths(ws, {"A": 21.5, "B": 19.8, "C": 21.1, "D": 21.0, "E": 14.0, "I": 15.5})


def _guide_state_sheet(ws, attendees, women):
    heads = ["Leader", "Guide", "Delegate W1", "Delegate W2", "Delegate W3", "Delegate W4",
             "Encounter " if women else "ENCOUNTER", "Delegate W5", "Delegate W6", "Delegate W7", "Delegate W8"]
    for c, t in enumerate(heads, 1):
        put(ws, 1, c, t, bold=True, h="center" if c >= 3 else None)
    r = 2
    for leader, rows in guide_state(attendees):
        for n, (guide, counts) in enumerate(rows):
            put(ws, r, 1, leader if n == 0 else None)
            put(ws, r, 2, guide)
            for i, v in enumerate(counts):
                put(ws, r, 3 + i, v, h="center")
            r += 1
    widths(ws, {"A": 17.1, "B": 20.9, "C": 12.2, "D": 14.0})


def _leader_state_sheet(ws, attendees):
    stats = leader_state(attendees)
    bands = [(0, [0, 1, 2, 3, 4], ["WEEK 1", "WEEK 2", "WEEK 3", "WEEK 4", "ENCOUNTER"]),
             (None, [5, 6, 7, 8], ["WEEK 5", "WEEK 6", "WEEK 7", "WEEK 8"])]
    r = 1
    for _, idxs, labels in bands:
        for n, (idx, label) in enumerate(zip(idxs, labels)):
            c = 2 + n * 3
            put(ws, r, c, f"{label} DELEGATES", bold=True, h="center")
            for k in (1, 2):
                put(ws, r, c + k)
            ws.merge_cells(start_row=r, start_column=c, end_row=r, end_column=c + 2)
        put(ws, r + 1, 1, "Leader", bold=True)
        for n in range(len(idxs)):
            c = 2 + n * 3
            for k, t in enumerate(["Guides", "First Time", "Second Time"]):
                put(ws, r + 1, c + k, t, bold=(k > 0), h="center" if k else None)
        rr = r + 2
        for leader, g, f, s in stats:
            put(ws, rr, 1, leader)
            for n, idx in enumerate(idxs):
                c = 2 + n * 3
                for k, arr in enumerate((g, f, s)):
                    put(ws, rr, c + k, arr[idx], h="center")
            rr += 1
        r = rr + 1
    widths(ws, {"A": 19.8})


def build(payload: dict) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    for gender in ("MEN", "WOMEN"):
        att = payload.get(gender) or []
        women = gender == "WOMEN"
        _attendance_sheet(wb.create_sheet(gender), att, women)
        _guide_state_sheet(wb.create_sheet(f"GUIDE STATE {gender}"), att, women)
        _leader_state_sheet(wb.create_sheet(f"GUIDE STATE FOR {gender}"), att)
    return to_bytes(wb)
