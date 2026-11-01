"""Potential Leaders workbook (Potential_Leaders.xlsx)

Leader@12 blocks laid out three across, stacked in bands.
payload = {"MEN": [block], "WOMEN": [block]}
block = {"leader12": str, "leaders": [str], "potential": [{"name": str, "comment": str}]}
MEN blocks carry a 'Leaders' column; WOMEN blocks do not (as in the source).
"""
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from .common import *

PER_ROW = 3


def _sheet(ws, blocks, with_leaders):
    ncols = 4 if with_leaders else 3
    stride = ncols + 2
    heads = ["Leader @12"] + (["Leaders"] if with_leaders else []) + ["Potential Leader(s)", "Comment"]
    row = 1
    for start in range(0, max(len(blocks), 1), PER_ROW):
        band = blocks[start:start + PER_ROW]
        depth = max([max(len(b.get("leaders", [])), len(b.get("potential", [])), 1) for b in band] or [1])
        for k, b in enumerate(band):
            c0 = 1 + k * stride
            for i, t in enumerate(heads):
                put(ws, row, c0 + i, t)
            put(ws, row + 1, c0, clean(b["leader12"]), bg=WHITE)
            for i in range(depth):
                rr = row + 1 + i
                if i > 0:
                    put(ws, rr, c0)
                col = c0 + 1
                if with_leaders:
                    ld = b.get("leaders", [])
                    put(ws, rr, col, clean(ld[i]) if i < len(ld) else None)
                    col += 1
                pl = b.get("potential", [])
                put(ws, rr, col, clean(pl[i]["name"]) if i < len(pl) else None)
                put(ws, rr, col + 1, (pl[i].get("comment") or None) if i < len(pl) else None)
        row += max(13, depth + 2)
    w = (13, 22, 20, 18) if with_leaders else (13, 20, 18)
    for k in range(PER_ROW):
        for i in range(ncols):
            ws.column_dimensions[get_column_letter(1 + k * stride + i)].width = w[i]


def build(payload: dict) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    _sheet(wb.create_sheet("Men"), payload.get("MEN", []), True)
    _sheet(wb.create_sheet("Women"), payload.get("WOMEN", []), False)
    return to_bytes(wb)
