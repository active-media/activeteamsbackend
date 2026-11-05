import os
from typing import Optional
from functools import lru_cache
from supabase import create_client, Client
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


@lru_cache(maxsize=1)
def get_supabase() -> Client:
    if not SUPABASE_URL or not (SUPABASE_KEY or SUPABASE_SERVICE_KEY):
        raise RuntimeError("Supabase not configured: missing SUPABASE_URL and a key")
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    return create_client(SUPABASE_URL, key)


@lru_cache(maxsize=1)
def get_supabase_admin() -> Client:
    if not SUPABASE_URL:
        raise RuntimeError("Supabase not configured")
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    if not key:
        raise RuntimeError("Supabase not configured")
    return create_client(SUPABASE_URL, key)


TABLES = {
    "USERS": "Users",
    "PEOPLE": "People",
    "ORGANIZATIONS": "Organizations",
    "ACTIVITY_LOGS": "Activity Logs",
    "EVENTS": "events",
    "EVENT_TYPES": "event_types",
    "TASK_TYPES": "Task Types",
    "TASKS": "Tasks",
    "EVENT_ATTENDEES": "event_attendees",
    "EVENT_SESSION_ATTENDEES": "event_session_attendees",
    "EVENT_NEW_PEOPLE": "event_new_people",
    "EVENT_CONSOLIDATIONS": "event_consolidations",
    "EVENT_SESSIONS": "event_sessions",
    "CONSOLIDATIONS": "consolidations",
    "PEOPLE_LOWER": "people",
}
