#!/usr/bin/env python3
"""Populate database with test data for lifeclass report testing."""

from supabase_helpers.supabase_connection import supabase
from datetime import datetime, timedelta, timezone
import uuid

def populate_events():
    """Insert test events."""
    events_data = [
        {
            "event_name": "Nhlakanipho Madlanga - Die Fakkel High School - Tuesday",
            "event_type_name": "Cells",
            "location": " Carter Rd, Forest Hill",
            "event_leader": "Nhlakanipho Madlanga",
            "event_leader_email": "madnhlaka@gmail.com",
            "organization": "Active Church",
            "event_date": "2025-10-21 00:00:00+00",
            "recurring_day": "Tuesday",
            "is_recurring": False,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
        {
            "event_name": "Tegra Mungudi - Rosettenville - School Cell - Wednesday",
            "event_type_name": "School Cell",
            "location": " Rosettenville",
            "event_leader": "Tegra Mungudi",
            "event_leader_email": "terga.mungudi@example.com",
            "organization": "Active Church",
            "event_date": "2025-10-22 00:00:00+00",
            "recurring_day": "Wednesday",
            "is_recurring": False,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
        {
            "event_name": "Life Class - Men Group - Week 1",
            "event_type_name": "Life Class",
            "location": " Church Building",
            "event_leader": "Leader1",
            "event_leader_email": "leader1@example.com",
            "organization": "Active Church",
            "event_date": "2025-10-28 00:00:00+00",
            "recurring_day": "Tuesday",
            "is_recurring": True,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
        {
            "event_name": "Life Class - Women Group - Week 1",
            "event_type_name": "Life Class",
            "event_leader": "Leader3",
            "event_leader_email": "leader3@example.com",
            "organization": "Active Church",
            "event_date": "2025-10-28 00:00:00+00",
            "recurring_day": "Tuesday",
            "is_recurring": True,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
        {
            "event_name": "Life Class - Men Group - Week 2",
            "event_type_name": "Life Class",
            "event_leader": "Leader1",
            "event_leader_email": "leader1@example.com",
            "organization": "Active Church",
            "event_date": "2025-11-04 00:00:00+00",
            "recurring_day": "Tuesday",
            "is_recurring": True,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
        {
            "event_name": "Life Class - Women Group - Week 2",
            "event_type_name": "Life Class",
            "event_leader": "Leader3",
            "event_leader_email": "leader3@example.com",
            "organization": "Active Church",
            "event_date": "2025-11-04 00:00:00+00",
            "recurring_day": "Tuesday",
            "is_recurring": True,
            "is_active": True,
            "is_ticketed": False,
            "is_global": False,
            "status": "Complete",
            "source_format": "old",
            "is_training": False,
            "is_permanent_deact": False,
        },
    ]
    
    for ev in events_data:
        supabase.table('events').insert(ev).execute()
    print("Events populated")


def populate_event_types():
    """Insert event type for Life Class."""
    event_type_data = {
        "name": "Life Class",
        "description": "bfghbjnbvc",
        "is_ticketed": False,
        "is_global": False,
        "has_person_steps": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "is_training": False,
        "org_id": str(uuid.uuid4()),
    }
    supabase.table('event_types').insert(event_type_data).execute()
    print("Event types populated")


def populate_event_attendees():
    """Insert test event attendees with attendance data for 8 weeks + encounter."""
    # First get the event types and events to reference
    events = supabase.table('events').select('event_id, event_name').execute()
    event_types = supabase.table('event_types').select('event_type_id, name').execute()
    
    # Create attendees with weekly attendance (X for some weeks, blank for others)
    attendees_data = [
        # Attendee 1: John Doe - Leader1, Guide registration, attended weeks 1,2,4,6,8 + encounter
        {
            "full_name": "Kungawo Nolongeza",
            "email": "kungawonolongeza@gmail.com",
            "phone": "082 662 4543",
            "leader12": "Nash Bobo",
            "leader144": "Terga Mungudi",
            "event_id": events.data[0]['event_id'] if events.data else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
            # Weekly attendance - weeks 1-8 + encounter
            # For lifeclass report, we need to map this to the CSV format
            # But the database doesn't have week-by-week, so we'll create synthetic attendance
        },
        # Attendee 2: Jane Smith - Leader1, Delegate: First Time
        {
            "full_name": "Jane Smith",
            "email": "jane.smith@example.com",
            "phone": "082 123 4567",
            "leader12": "Leader1",
            "leader144": "",
            "event_id": events.data[2]['event_id'] if events.data and len(events.data) > 2 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 3: Bob Johnson - Leader2, Guide
        {
            "full_name": "Bob Johnson",
            "email": "bob.johnson@example.com",
            "phone": "082 987 6543",
            "leader12": "Leader2",
            "leader144": "Guide2",
            "event_id": events.data[3]['event_id'] if events.data and len(events.data) > 3 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 4: Alice Williams - Leader2, Delegate: Second Time
        {
            "full_name": "Alice Williams",
            "email": "alice.williams@example.com",
            "phone": "082 456 7890",
            "leader12": "Leader2",
            "leader144": "",
            "event_id": events.data[4]['event_id'] if events.data and len(events.data) > 4 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 5: Carol - Leader3, Guide
        {
            "full_name": "Carol",
            "email": "carol@example.com",
            "phone": "082 789 1234",
            "leader12": "Leader3",
            "leader144": "",
            "event_id": events.data[5]['event_id'] if events.data and len(events.data) > 5 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 6: Dave - Leader3, Delegate: First Time
        {
            "full_name": "Dave",
            "email": "dave@example.com",
            "phone": "082 234 5678",
            "leader12": "Leader3",
            "leader144": "",
            "event_id": events.data[5]['event_id'] if events.data and len(events.data) > 5 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 7: Eve - Leader4, Guide, with leader144
        {
            "full_name": "Eve",
            "email": "eve@example.com",
            "phone": "082 345 6789",
            "leader12": "Leader4",
            "leader144": "Guide4",
            "event_id": events.data[5]['event_id'] if events.data and len(events.data) > 5 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
        # Attendee 8: Frank - Leader4, Delegate: Second Time
        {
            "full_name": "Frank",
            "email": "frank@example.com",
            "phone": "082 456 1230",
            "leader12": "Leader4",
            "leader144": "",
            "event_id": events.data[5]['event_id'] if events.data and len(events.data) > 5 else None,
            "event_type_id": event_types.data[0]['event_type_id'] if event_types.data else None,
            "is_persistent": True,
            "is_checked_in": True,
        },
    ]
    
    for att in attendees_data:
        supabase.event_attendees.insert(att).execute()
    print("Event attendees populated")


def main():
    print("Populating database with test data...")
    populate_event_types()
    populate_events()
    populate_event_attendees()
    print("\nDatabase population complete!")


if __name__ == "__main__":
    main()