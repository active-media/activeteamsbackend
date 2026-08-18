"""
main.py — Supabase-only branch
Scope: Stats Dashboard + Service Check-in endpoints only.
All MongoDB-backed endpoints (people, events CRUD, tasks, consolidations,
admin, signup) have been intentionally removed from this branch.
"""

import os
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ── Supabase-backed stats + service check-in modules ──────────────────────
from supabase_helpers.supabase_stats import (
    sb_get_stats_overview,
    sb_get_outstanding_items,
    sb_get_dashboard_quick,
    sb_get_dashboard_comprehensive,
)
from supabase_helpers.service_checkin_routes import router as service_checkin_router

# TODO: confirm this is the Supabase-backed auth dependency for this branch.
# If get_current_user in auth.utils still does a Motor/Mongo lookup against
# users_collection, it needs to be swapped for a Supabase Users-table lookup
# (e.g. supabase_helpers.supabase_auth.get_current_user) before this branch
# is usable end-to-end.
from auth.utils import get_current_user

import asyncio
import traceback
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(title="Active Teams API — Supabase Branch")

app.include_router(service_checkin_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://teams.theactivechurch.org",
        "http://localhost:8000",
        "http://localhost:5173",
        "https://new-active-teams.netlify.app",
        "https://activeteams.netlify.app",
        "https://activeteamsbackend2.0.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Credentials": "true",
        },
    )


@app.get("/")
def root():
    return {"message": "Supabase-only branch is live"}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/ping")
async def ping():
    return JSONResponse(content={"message": "Server is alive"}, status_code=200)


# ── Org scoping helper (shared by all stats endpoints) ─────────────────────
def _build_stats_org_filter(current_user: dict) -> Optional[dict]:
    """
    Returns None for super-admins (no restriction) or a dict that
    supabase_stats helpers will apply as a WHERE clause.
    """
    is_super = (
        current_user.get("is_supreme_admin")
        or current_user.get("role") == "super_admin"
    )
    if is_super:
        return None
    org = current_user.get("Organization") or current_user.get("organization")
    if not org:
        return None
    return {"organization": org}


# ── Period range helper (used by dashboard-comprehensive / dashboard-quick) ─
from datetime import timedelta


def get_period_range(period: str):
    """
    Matches frontend's period filters:
    today | thisWeek | thisMonth | previous7 | previousWeek | previousMonth
    """
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "today":
        start = today
        end = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end

    if period == "thisWeek":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end

    if period == "thisMonth":
        start = today.replace(day=1)
        if today.month == 12:
            end = datetime(today.year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(today.year, today.month + 1, 1) - timedelta(microseconds=1)
        return start, end

    if period == "previous7":
        end = today - timedelta(days=1)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        start = end - timedelta(days=6)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end

    if period == "previousWeek":
        last_week = today - timedelta(weeks=1)
        start = last_week - timedelta(days=last_week.weekday())
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
        return start, end

    if period == "previousMonth":
        year = today.year
        month = today.month - 1
        if month == 0:
            month = 12
            year -= 1
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end = datetime(year, month + 1, 1) - timedelta(microseconds=1)
        return start, end

    raise ValueError(f"Invalid period '{period}'")


EXCLUDED_TASK_TYPES_FROM_COMPLETED = ["no answer", "Awaiting Call"]


# ── STATS ENDPOINTS ─────────────────────────────────────────────────────────

from fastapi import Query


@app.get("/stats/overview")
async def get_stats_overview(
    period: str = "monthly",
    current_user: dict = Depends(get_current_user),
):
    org_filter = _build_stats_org_filter(current_user)
    try:
        return await asyncio.to_thread(sb_get_stats_overview, period, org_filter)
    except Exception as e:
        logger.error(f"Error in stats overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/outstanding-items")
async def get_outstanding_items(
    current_user: dict = Depends(get_current_user),
):
    org_filter = _build_stats_org_filter(current_user)
    try:
        return await asyncio.to_thread(sb_get_outstanding_items, org_filter)
    except Exception as e:
        logger.error(f"Error in outstanding items: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats(
    period: str = Query(
        "today",
        pattern="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$",
    ),
    current_user: dict = Depends(get_current_user),
):
    org_name = current_user.get("Organization")
    if not org_name and current_user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Organization not associated with user")

    org_filter = _build_stats_org_filter(current_user)
    try:
        return await asyncio.to_thread(sb_get_dashboard_quick, period, org_filter)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching quick stats: {str(e)}")


@app.get("/stats/dashboard-comprehensive")
async def get_dashboard_comprehensive(
    period: str = Query(
        "today",
        pattern="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$",
    ),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user),
):
    org_name = current_user.get("Organization")
    if not org_name and current_user.get("role") != "super_admin":
        raise HTTPException(status_code=403, detail="Organization not associated with user")

    org_filter = _build_stats_org_filter(current_user)
    try:
        return await asyncio.to_thread(
            sb_get_dashboard_comprehensive, period, limit, org_filter
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching comprehensive stats: {str(e)}",
        )


# Note: /stats/people-with-tasks (sb_get_people_capture_stats) is referenced
# in your stats_queries.py per memory notes, but wasn't shown in the
# supabase_stats import list. Add it back here once confirmed available:
#
# from supabase_helpers.supabase_stats import sb_get_people_capture_stats
#
# @app.get("/stats/people-with-tasks")
# async def get_people_capture_stats(current_user: dict = Depends(get_current_user)):
#     org_filter = _build_stats_org_filter(current_user)
#     try:
#         return await asyncio.to_thread(sb_get_people_capture_stats, org_filter)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to fetch capture statistics: {str(e)}")