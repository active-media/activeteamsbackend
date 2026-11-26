import asyncio

import events


def test_get_cell_events_returns_only_requested_page(monkeypatch):
    event_rows = [
        {"event_id": f"event-{index}", "mongo_id": f"mongo-{index}"}
        for index in range(5)
    ]
    session_rows = [
        {
            "event_id": f"event-{index}",
            "session_date": f"2026-10-{index + 1:02d}",
            "status": "incomplete",
        }
        for index in range(5)
    ]

    class PeopleCollection:
        async def find_one(self, *_args, **_kwargs):
            return None

    class SupabaseResponse:
        data = event_rows

        def execute(self):
            return self

    class Supabase:
        def rpc(self, *_args, **_kwargs):
            return SupabaseResponse()

    monkeypatch.setattr(events, "people_collection", PeopleCollection())
    monkeypatch.setattr(events, "supabase", Supabase())
    monkeypatch.setattr(
        events,
        "_get_event_type_id",
        lambda *_args: "cells-event-type",
    )
    monkeypatch.setattr(
        events,
        "_fetch_all_event_sessions",
        lambda *_args, **_kwargs: session_rows,
    )

    response = asyncio.run(
        events.get_cell_events(
            page=2,
            limit=2,
            status="incomplete",
            event_type="Cells",
            personal=False,
            all_events=False,
            start_date="2026-10-01",
            end_date="2026-10-31",
            day=None,
            leader_at_12_view=False,
            show_personal_cells=False,
            show_all_authorized=False,
            include_subordinate_cells=False,
            leader_at_1_identifier=None,
            isLeaderAt12=False,
            firstName=None,
            userSurname=None,
            must_paginate=True,
        )
    )

    assert [cell["event_id"] for cell in response["events"]] == [
        "event-2",
        "event-3",
    ]
    assert response["total_events"] == 5
    assert response["total_pages"] == 3
    assert response["current_page"] == 2
