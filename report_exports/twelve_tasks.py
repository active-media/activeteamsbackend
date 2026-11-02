"""Twelve Tasks workbook (12_Tasks.xlsx)

payload = {"rows": [{"name", "last_week": int, "target": int, "actual": int}, ...]}
Colour key (from the sheet): orange = on target, red = less than target, green = more than target.
Applied to the 'Actual this week' column.
"""
from openpyxl import Workbook
from .common import *


def status_colour(actual, target):
    if actual == target:
        return ORANGE
    return RED if actual < target else GREEN


def build(payload: dict) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    for c, (t, b) in enumerate([("Name", False), (" Last week ", True), ("Targets", True), ("Actual  this week", True)], 1):
        put(ws, 1, c, t, bold=b, bg=WHITE, h="center" if c > 1 else None)
    put(ws, 1, 6, "Key", bg=WHITE)
    for r, (label, col) in enumerate([("On target", ORANGE), ("Less than target", RED), ("More than target ", GREEN)], 2):
        put(ws, r, 6, None, bg=col)
        put(ws, r, 7, label)
    for r, row in enumerate(payload.get("rows", []), 2):
        put(ws, r, 1, clean(row["name"]), bg=WHITE)
        put(ws, r, 2, row.get("last_week", 0), bg=WHITE, h="center")
        put(ws, r, 3, row.get("target", 0), bg=WHITE, h="center")
        put(ws, r, 4, row.get("actual", 0), bg=status_colour(row.get("actual", 0), row.get("target", 0)), h="center")
    widths(ws, {"A": 16.6, "B": 14.9, "C": 12.6, "D": 15.4, "G": 15.5})
    return to_bytes(wb)
