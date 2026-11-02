from __future__ import annotations
import io
from typing import Optional
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

THIN = Side(style="thin")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
GREEN, RED, ORANGE, WHITE = "00FF00", "FF0000", "FF9900", "FFFFFF"


def fill(hex6: Optional[str]):
    return PatternFill("solid", fgColor="FF" + hex6) if hex6 else PatternFill(fill_type=None)


def put(ws, r, c, v=None, *, bold=False, bg=None, h=None, border=True, nf=None,
        size=10, wrap=False, v_align=None):
    cell = ws.cell(row=r, column=c)
    if v is not None:
        cell.value = v
    cell.font = Font(name="Arial", size=size, bold=bold)
    if bg:
        cell.fill = fill(bg)
    if h or wrap or v_align:
        cell.alignment = Alignment(horizontal=h, vertical=v_align, wrap_text=wrap)
    if border:
        cell.border = BOX
    if nf:
        cell.number_format = nf
    return cell


def widths(ws, spec: dict):
    for col, w in spec.items():
        ws.column_dimensions[col].width = w


def clean(s) -> str:
    """Collapse the stray double spaces / trailing spaces the manual sheets are full of."""
    return " ".join(str(s or "").split())


def to_bytes(wb: Workbook) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Registration type handling (Life Class) ────────────────────────────────
REG_LABEL = {"guide": "Guide", "first": "Delegate: First Time", "second": "Delegate: Second Time"}
REG_FILL = {"guide": RED, "first": GREEN, "second": ORANGE}


def reg_kind(raw) -> Optional[str]:
    """Normalise the many spellings found in the sheets
    ('Delegate:Second Time', 'Delage: First Time', 'Delegate: Second', ...)."""
    s = clean(raw).lower()
    if "guide" in s:
        return "guide"
    if "second" in s:
        return "second"
    if "first" in s or "delage" in s:
        return "first"
    return None
