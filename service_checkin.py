import os
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ── Supabase-backed stats + service check-in modules ──────────────────────
from supabase_helpers.service_checkin_routes import router as service_checkin_router
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