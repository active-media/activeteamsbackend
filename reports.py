import json

from fastapi import APIRouter, HTTPException, Query, Path, Body, Depends, params
from datetime import datetime, timedelta, date, timezone
from typing import Optional, List, Dict, Any 
from bson import ObjectId
import uuid
from auth.models import EventCreate, EventTypeCreate, LeaderStatusResponse,AttendanceSubmission,ConsolidationCreate
import pytz
import re
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from time import sleep
from urllib.parse import unquote
from database import db, events_collection, people_collection, users_collection, tasks_collection ,tasktypes_collection,consolidations_collection, organizations_collection, org_config_collection
from auth.utils import hash_password, verify_password, get_next_occurrence_single, parse_time_string, get_leader_cell_name_async, create_access_token, decode_access_token , task_type_serializer, get_current_user 
from supabase_helpers.supabase_connection import supabase
import os


router = APIRouter()

@router.get("/school_of_leaders/{event_type_id}")
async def generate_school_cell_report(event_type_id: str):
    try:
        today = datetime.utcnow().isoformat()

        # STEP 1: Get all running School of Leaders event types from events table
        events_query = (
            supabase.table("events")
            .select("id, event_name, event_type, is_training")
            .eq("is_training", True)
            .eq("is_active", True)
            .eq("event_type_name", "School Of Leaders")
            .execute()
        )
        running_events = events_query.data
        print("FETCHED RUNNING EVENTS", running_events)

        response_payload = []

        for event in running_events:
            event_id = event["id"]

            # STEP 0: Create 10 session instances in event_sessions if none exist
            existing_sessions = (
                supabase.table("event_sessions")
                .select("id")
                .eq("event_id", event_id)
                .execute()
            )

            if not existing_sessions.data:
                initial_sessions = [
                    {
                        "event_id": event_id,
                        "session_number": i + 1,
                        "name": f"Session {i + 1}",
                    }
                    for i in range(10)
                ]
                supabase.table("event_sessions").insert(initial_sessions).execute()

            # STEP 2: Fetch associated people from event_training_attendees with leader_at_12
            associated_attendees_query = (
                supabase.table("event_training_attendees")
                .select("person_id, people(full_name, leader_at_12)")
                .eq("event_id", event_id)
                .execute()
            )

            formatted_associated_attendees = [
                {
                    "full_name": att.get("people", {}).get("full_name", "Unknown"),
                    "leader12": att.get("people", {}).get("leader_at_12", "N/A"),
                }
                for att in associated_attendees_query.data
            ]

            # STEP 3: Determine sessions that already happened by querying by date
            past_sessions_query = (
                supabase.table("event_sessions")
                .select("id, session_number, date")
                .eq("event_id", event_id)
                .lte("date", today)
                .order("session_number", desc=False)
                .execute()
            )

            sessions_data = {}

            # STEP 4: Retrieve attendees for each passed session & calculate attendance counts
            for session in past_sessions_query.data:
                session_id = session["id"]
                session_num = session["session_number"]

                session_attendees_query = (
                    supabase.table("session_attendees")
                    .select("person_id, people(full_name, leader_at_12)")
                    .eq("session_id", session_id)
                    .execute()
                )

                session_attendees_list = []
                for att in session_attendees_query.data:
                    person_id = att["person_id"]

                    # Count total sessions this person has attended across all sessions
                    total_attended_query = (
                        supabase.table("session_attendees")
                        .select("id", count="exact")
                        .eq("person_id", person_id)
                        .execute()
                    )

                    session_attendees_list.append(
                        {
                            "fullname": att.get("people", {}).get("full_name", "Unknown"),
                            "leader12": att.get("people", {}).get("leader_at_12", "N/A"),
                            "total_attended": total_attended_query.count or 0,
                        }
                    )

                sessions_data[f"session_{session_num}"] = {
                    "attendees": session_attendees_list
                }

            # STEP 5: Construct event response object matching your schema
            response_payload.append(
                {
                    "training_class": event["name"],
                    "associated_attendees": formatted_associated_attendees,
                    "sessions": sessions_data,
                }
            )

        return response_payload

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))