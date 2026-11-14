import asyncio
import importlib
import sys
from datetime import datetime
from types import SimpleNamespace

for package_name in ("auth", "supabase_helpers"):
    package = sys.modules.get(package_name)
    if package is not None and not hasattr(package, "__path__"):
        for module_name in tuple(sys.modules):
            if module_name == package_name or module_name.startswith(package_name + "."):
                sys.modules.pop(module_name, None)

connection_module = sys.modules.get("supabase_helpers.supabase_connection")
if connection_module is not None and not getattr(connection_module, "__file__", None):
    sys.modules.pop("supabase_helpers.supabase_connection", None)

events = importlib.import_module("events")
EventCreate = importlib.import_module("auth.models").EventCreate


class FakeSupabaseQuery:
    def __init__(self, table, inserted_rows):
        self.table_name = table
        self.inserted_rows = inserted_rows
        self.payload = None

    def select(self, *_args):
        return self

    def eq(self, *_args):
        return self

    def insert(self, payload):
        self.payload = payload
        return self

    def execute(self):
        if self.table_name == "event_types":
            data = [{
                "event_type_id": "cells-type-id",
                "name": "cells",
                "is_ticketed": False,
            }]
        else:
            data = [{"event_id": "created-event-id", **self.payload}]
            self.inserted_rows.append(self.payload)
        return SimpleNamespace(data=data)


class FakeSupabase:
    def __init__(self):
        self.inserted_rows = []

    def table(self, name):
        return FakeSupabaseQuery(name, self.inserted_rows)


class FakeReadQuery:
    def select(self, *_args):
        return self

    def in_(self, *_args):
        return self

    def execute(self):
        return SimpleNamespace(data=[])


class FakeReadSupabase:
    def __init__(self):
        self.rpc_calls = []

    def rpc(self, function_name, params=None):
        self.rpc_calls.append((function_name, params))
        return FakeReadQuery()

    def table(self, _name):
        return FakeReadQuery()


def test_create_non_recurring_cell(monkeypatch):
    fake_supabase = FakeSupabase()
    monkeypatch.setattr(events, "supabase", fake_supabase)
    event = EventCreate(
        eventTypeName="cells",
        eventName="Test Cell",
        date=datetime(2026, 11, 13, 10, 0),
        time="10:00",
        recurring_day=[],
        location="Test location",
        eventLeader="Test Leader",
        eventLeaderEmail="leader@example.org",
        description="Test description",
    )

    result = asyncio.run(events.create_event(event, {
        "org_id": "test-org-id",
        "Organization": "Test Organization",
    }))

    assert result["success"] is True
    assert result["id"] == "created-event-id"
    assert len(fake_supabase.inserted_rows) == 1
    inserted_event = fake_supabase.inserted_rows[0]
    assert inserted_event["event_type_id"] == "cells-type-id"
    assert inserted_event["event_type_name"] == "Cells"
    assert inserted_event["event_date"] == "2026-11-13T10:00:00"
    assert inserted_event["is_recurring"] is False
    assert inserted_event["recurring_day"] == ""
    assert inserted_event["org_id"] == "test-org-id"
    assert "Organization" not in inserted_event
    assert "class_level" not in inserted_event
    assert "total_sessions" not in inserted_event


def test_eventsdata_uses_general_events_rpc_parameter_name(monkeypatch):
    fake_supabase = FakeReadSupabase()
    monkeypatch.setattr(events, "supabase", fake_supabase)

    result = asyncio.run(events.get_other_events(
        current_user={"email": "leader@example.org", "role": "admin"},
        event_type="cells",
        status=None,
        day="all",
        is_global=False,
    ))

    assert result["events"] == []
    assert fake_supabase.rpc_calls == [
        ("get_general_events", {"p_event_type": "cells"}),
    ]
