"""Service Target workbook (Service_Target.xlsx)

payload = {
  "services": ["Friday", "Sunday"],
  "groups": [ [row, ...], [row, ...] ],      # groups are separated by one blank row
}
row = {"name": str, "targets": {"Friday": int|None, "Sunday": ...}, "actuals": {"Friday": int|None, ...}}
"""
from openpyxl import Workbook
from .common import *


def build(payload: dict) -> bytes:
    svcs = payload.get("services", ["Friday", "Sunday"])
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    put(ws, 1, 1, "Name", bold=True, h="center")
    for i, s in enumerate(svcs):
        put(ws, 1, 2 + i * 2, f"Target \n{s}", bold=True, bg=RED, h="center", wrap=True)
        put(ws, 1, 3 + i * 2, s, bold=True, bg=WHITE, h="center")
    r = 2
    for gi, group in enumerate(payload.get("groups", [])):
        for row in group:
            put(ws, r, 1, clean(row["name"]))
            for i, s in enumerate(svcs):
                put(ws, r, 2 + i * 2, (row.get("targets") or {}).get(s), bg=WHITE, h="center")
                put(ws, r, 3 + i * 2, (row.get("actuals") or {}).get(s), bg=WHITE, h="center")
            r += 1
        r += 1  # blank separator row
    widths(ws, {"A": 14.0, "B": 12.9, "D": 13.1})
    return to_bytes(wb)
