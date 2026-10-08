"""End-to-end test of /events/eventsdata against a mock Mongo.

The unit tests in test_events_visibility.py exercise the query builder. This one
drives the actual FastAPI endpoint with a real HTTP request against an in-memory
Mongo, to prove the wiring holds: that a legacy event with no org_id is actually
returned in the response body, that pagination and has_more behave, and that the
status filter no longer nukes the list when "all" is requested.

Run:
    venv/bin/python -m pytest tests/test_eventsdata_endpoint.py -v
"""

import asyncio
import os
import sys
import unittest
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test-active-teams-db")

from unittest.mock import patch  # noqa: E402

from mongomock_motor import AsyncMongoMockClient  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402


def legacy_event(name, date_str, event_type="Church Service"):
    """An event exactly as it existed before multi-tenant support: no org_id,
    no Organization. This is the shape of the 3392 hidden production documents."""
    return {
        "Event Name": name,
        "Event Type": event_type,
        "date": date_str,
        "recurring_day": [],
        "Day": "Sunday",
        "Leader": "Test Leader",
        "eventLeaderEmail": "leader@test.com",
        "attendance": {},
    }


class TestEventsDataEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = AsyncMongoMockClient()
        self.db = self.client["test-active-teams-db"]

        main.events_collection = self.db["Events"]
        main.people_collection = self.db["People"]
        main.users_collection = self.db["Users"]
        main.tasks_collection = self.db["tasks"]
        main.tasktypes_collection = self.db["TaskTypes"]
        main.org_config_collection = self.db["OrgConfig"]
        main.consolidations_collection = self.db["consolidations"]
        main.organizations_collection = self.db["organizations"]

        # get_event_counts_documents() memoises phase-1 reads on the query alone,
        # which is correct in production (one module-level collection) but would
        # hand a later test the previous test's documents.
        main.clear_event_counts_cache()

        self.app = main.app
        # get_current_user is a FastAPI dependency; override it so the test
        # exercises the events query rather than the auth path.
        from fastapi import FastAPI
        app = self.app

        async def fake_user():
            return {"email": "admin@test.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "Admin", "surname": "User"}

        app.dependency_overrides[main.get_current_user] = fake_user
        self.test_client = TestClient(app)

    def seed(self, docs):
        """Insert fixture documents.

        mongomock_motor is async, so these calls must be awaited. TestClient runs
        the app on its own event loop, so seeding happens on a separate one -
        the mock client supports both.
        """
        asyncio.run(self.db["Events"].insert_many(docs))

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_legacy_event_with_no_org_id_is_returned(self):
        """THE regression test. Before the fix this returned an empty list."""
        self.seed([
            legacy_event("J Activation", "2026-03-08"),
            legacy_event("J Activation", "2026-09-27"),
        ])

        r = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "show_all_dates": "true"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertIn("J Activation", names,
                      f"legacy event was filtered out; got {names}")
        self.assertGreaterEqual(body["total_events"], 2)

    def test_status_all_is_not_treated_as_a_filter(self):
        """status=all must not blank the list (it used to match nothing)."""
        self.seed([
            legacy_event("J Activation", "2026-03-08"),
        ])
        r = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "status": "all"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertGreaterEqual(
            r.json()["total_events"], 1,
            "status=all filtered everything out",
        )

    def test_pagination_and_has_more(self):
        self.seed([
            legacy_event(f"Event {i}", f"2026-0{i+1}-01") for i in range(7)
        ])

        page1 = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "limit": 3, "page": 1},
        ).json()
        self.assertEqual(len(page1["events"]), 3)
        self.assertTrue(page1["has_more"], "has_more must be true on page 1 of 3")
        self.assertEqual(page1["total_events"], 7)

        last = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "limit": 3, "page": 3},
        ).json()
        self.assertEqual(len(last["events"]), 1)
        self.assertFalse(last["has_more"], "has_more must be false on the final page")

    def test_other_org_events_are_not_leaked(self):
        self.seed([
            legacy_event("Legacy Visible", "2026-03-08"),
            {"Event Name": "Other Org", "Event Type": "Church Service",
             "date": "2026-03-08", "org_id": "some-other-church", "recurring_day": []},
        ])
        body = self.test_client.get(
            "/events/eventsdata", params={"start_date": "2015-01-01"}
        ).json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertIn("Legacy Visible", names)
        self.assertNotIn("Other Org", names,
                         "another org's event leaked into the results")

    def test_is_global_true_returns_only_global_events(self):
        """The filter Service Check-in uses to avoid downloading the whole grid.

        The endpoint returns 3,628 rows for this org and that page only ever
        rendered the 1,261 global ones, so it now asks for just those instead of
        paging over everything and discarding 65% of it.
        """
        self.seed([
            {**legacy_event("Global Service", "2026-03-08"), "isGlobal": True},
            {**legacy_event("Local Cell", "2026-03-08"), "isGlobal": False},
            legacy_event("Unflagged", "2026-03-08"),
        ])

        body = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "is_global": "true"},
        ).json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertIn("Global Service", names)
        self.assertNotIn("Local Cell", names, "isGlobal=False was returned")
        self.assertNotIn("Unflagged", names, "a missing isGlobal is not global")
        self.assertEqual(body["total_events"], 1)

    def test_is_global_false_excludes_only_the_globals(self):
        self.seed([
            {**legacy_event("Global Service", "2026-03-08"), "isGlobal": True},
            {**legacy_event("Local Cell", "2026-03-08"), "isGlobal": False},
            legacy_event("Unflagged", "2026-03-08"),
        ])

        body = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "is_global": "false"},
        ).json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertNotIn("Global Service", names)
        self.assertIn("Local Cell", names)
        self.assertIn("Unflagged", names,
                      "a document with no isGlobal key is not a global event")

    def test_is_global_true_and_false_partition_the_unfiltered_result(self):
        """Neither half may drop or double-count a row."""
        self.seed([
            {**legacy_event("Global Service", "2026-03-08"), "isGlobal": True},
            {**legacy_event("Local Cell", "2026-03-08"), "isGlobal": False},
            legacy_event("Unflagged", "2026-03-08"),
        ])

        def total(**params):
            params["start_date"] = "2015-01-01"
            return self.test_client.get("/events/eventsdata", params=params).json()["total_events"]

        self.assertEqual(total(is_global="true") + total(is_global="false"), total())

    def test_is_global_absent_keeps_everything(self):
        """The default must not narrow the result - the Events grid relies on it."""
        self.seed([
            {**legacy_event("Global Service", "2026-03-08"), "isGlobal": True},
            {**legacy_event("Local Cell", "2026-03-08"), "isGlobal": False},
        ])

        body = self.test_client.get(
            "/events/eventsdata", params={"start_date": "2015-01-01"}
        ).json()
        self.assertEqual(body["total_events"], 2)

    def test_is_global_combines_with_the_type_filter(self):
        self.seed([
            {**legacy_event("Global Service", "2026-03-08"), "isGlobal": True},
            {**legacy_event("Global Conference", "2026-03-08",
                            event_type="Conference"), "isGlobal": True},
        ])

        body = self.test_client.get(
            "/events/eventsdata",
            params={"start_date": "2015-01-01", "is_global": "true",
                    "event_type": "Conference"},
        ).json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertEqual(names, ["Global Conference"])

    def test_date_filter_still_applies_when_not_disabled(self):
        self.seed([
            legacy_event("Old Event", "2020-01-01"),
            legacy_event("Recent Event", "2026-09-01"),
        ])
        body = self.test_client.get(
            "/events/eventsdata", params={"start_date": "2026-01-01"}
        ).json()
        names = [e.get("eventName") for e in body["events"]]
        self.assertIn("Recent Event", names)
        self.assertNotIn("Old Event", names,
                         "date filter stopped working")


def recurring_event(name, days, **over):
    """A recurring event. Its instances are synthesised from recurring_day."""
    doc = legacy_event(name, "2026-01-01", event_type="Service")
    doc["recurring_day"] = days
    doc["Day"] = ""
    doc.update(over)
    return doc


class TestRecurringEventsExpandBackwards(unittest.TestCase):
    """The actual root cause of disappearing events.

    The recurring branch looped `for week_back in range(0, 1)`, so a recurring
    event produced at most one instance - the current week's. Production data
    showed 10 of 15 recurring events rendering as zero rows whenever that week's
    occurrence was still in the future.
    """

    def setUp(self):
        self.client = AsyncMongoMockClient()
        self.db = self.client["test-active-teams-db"]
        main.events_collection = self.db["Events"]
        main.people_collection = self.db["People"]
        main.users_collection = self.db["Users"]
        main.tasks_collection = self.db["tasks"]
        main.tasktypes_collection = self.db["TaskTypes"]
        main.org_config_collection = self.db["OrgConfig"]
        main.consolidations_collection = self.db["consolidations"]
        main.organizations_collection = self.db["organizations"]

        # See TestEventsDataEndpoint.setUp - the phase-1 cache is keyed on the
        # query and would otherwise carry documents across tests.
        main.clear_event_counts_cache()

        async def fake_user():
            return {"email": "admin@test.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "Admin", "surname": "User"}

        self.app = main.app
        self.app.dependency_overrides[main.get_current_user] = fake_user
        self.test_client = TestClient(self.app)

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def seed(self, docs):
        asyncio.run(self.db["Events"].insert_many(docs))

    def get(self, **params):
        r = self.test_client.get("/events/eventsdata", params=params)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def test_a_weekly_event_produces_many_past_instances(self):
        """A single weekly document must expand into a row per past week."""
        self.seed([recurring_event("Weekly Service", ["Monday"])])

        # 12 weeks back, capped by the start date.
        start = (date.today() - timedelta(weeks=12)).isoformat()
        body = self.get(start_date=start, limit=500)

        weekly = [e for e in body["events"] if e.get("eventName") == "Weekly Service"]
        self.assertGreaterEqual(
            len(weekly), 8,
            f"expected ~12 past instances, got {len(weekly)}",
        )

    def test_instance_dates_span_multiple_distinct_dates(self):
        self.seed([recurring_event("Weekly Service", ["Monday"])])

        start = (date.today() - timedelta(weeks=8)).isoformat()
        body = self.get(start_date=start, limit=500)

        dates = {e.get("date") for e in body["events"]
                 if e.get("eventName") == "Weekly Service"}
        self.assertGreaterEqual(
            len(dates), 5,
            f"instances did not spread across weeks; dates={sorted(dates)}",
        )

    def test_a_future_weekday_still_shows_its_past_occurrences(self):
        """The case that made events vanish entirely.

        If the only occurrence this week is still in the future, the old
        range(0, 1) loop produced nothing and the event disappeared.
        """
        # Tomorrow's weekday, so this week's instance is in the future.
        tomorrow = (date.today() + timedelta(days=1)).strftime("%A")
        self.seed([recurring_event("Future Weekday Service", [tomorrow])])

        start = (date.today() - timedelta(weeks=6)).isoformat()
        body = self.get(start_date=start, limit=500)

        names = [e.get("eventName") for e in body["events"]]
        self.assertIn(
            "Future Weekday Service", names,
            "a recurring event vanished when this week's instance was in the future",
        )

    def test_multi_day_recurring_expands_each_day(self):
        self.seed([recurring_event("Midweek + Sunday", ["Monday", "Sunday"])])

        start = (date.today() - timedelta(weeks=4)).isoformat()
        body = self.get(start_date=start, limit=500)

        rows = [e for e in body["events"] if e.get("eventName") == "Midweek + Sunday"]
        self.assertGreaterEqual(len(rows), 5, f"got {len(rows)} rows for a 2-day weekly event")

    def test_start_date_still_bounds_the_walk_back(self):
        """The walk-back must not ignore the requested window."""
        self.seed([recurring_event("Weekly Service", ["Monday"])])

        recent = (date.today() - timedelta(weeks=2)).isoformat()
        body = self.get(start_date=recent, limit=500)

        weekly = [e for e in body["events"] if e.get("eventName") == "Weekly Service"]
        self.assertLessEqual(
            len(weekly), 3,
            f"start_date was ignored; {len(weekly)} instances from a 2-week window",
        )

    def test_future_instances_are_still_not_shown(self):
        """Walking back must not start leaking future dates."""
        self.seed([recurring_event("Weekly Service", ["Monday"])])

        start = (date.today() - timedelta(weeks=4)).isoformat()
        body = self.get(start_date=start, limit=500)

        today = date.today().isoformat()
        for e in body["events"]:
            d = e.get("date")
            if d and str(d)[:10] > today:
                self.fail(f"future instance leaked into the response: {d}")


if __name__ == "__main__":
    unittest.main()
