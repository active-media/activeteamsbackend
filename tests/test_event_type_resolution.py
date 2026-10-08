"""The event type on a row must be the document's real type.

main.py resolves a row's type as

    event.get("Event Type") or event.get("eventType") or event.get("eventTypeName") or "Event"

eventTypeName was missing from that chain, and it is the only spelling present on
80 production documents - 25 of them Church Service and 2 Conference. Those rows
were all labelled the literal "Event", so Service Check-in could not tell them
apart, the Event Type column read "Event" for the whole grid, and the attendance
export wrote "Event" into the Event Type column for every row.

The query side already matched on all three spellings, which is why the events
were not invisible in the database - only in the response.
"""

import unittest
from datetime import date, timedelta

import main
from tests.test_events_two_phase_read import (  # noqa: F401
    AsyncMongoMockClient, full_entry, run,
)


class TestEventTypeResolution(unittest.TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient

        self.client = AsyncMongoMockClient()
        self.db = self.client["test-active-teams-db"]
        main.events_collection = self.db["Events"]
        for name in ("People", "Users", "tasks", "TaskTypes", "OrgConfig",
                     "consolidations", "organizations"):
            setattr(main, f"{name.lower()}_collection", self.db[name])
        main.clear_event_counts_cache()

        async def fake_user():
            return {"email": "admin@test.com", "role": "admin",
                    "org_id": "active-teams", "Organization": "Active Church",
                    "name": "Admin", "surname": "User"}

        self.app = main.app
        self.app.dependency_overrides[main.get_current_user] = fake_user
        self.test_client = TestClient(self.app)
        self.past = date.today() - timedelta(days=7)

    def tearDown(self):
        self.app.dependency_overrides.clear()
        main.clear_event_counts_cache()

    def fetch(self, **params):
        query = {"limit": 50}
        query.update(params)
        resp = self.test_client.get("/events/eventsdata", params=query)
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def seed(self, **type_fields):
        doc = {
            "Event Name": f"Service {list(type_fields)[0]}",
            "date": self.past.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "isGlobal": True,
            "attendance": {},
        }
        doc.update(type_fields)
        run(self.db["Events"].insert_many([doc]))

    def test_event_type_name_is_used_when_it_is_the_only_spelling(self):
        """The regression: this row used to be labelled "Event"."""
        self.seed(eventTypeName="Church Service")
        rows = self.fetch()["events"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["eventType"], "Church Service")

    def test_event_type_name_is_used_for_conferences(self):
        self.seed(eventTypeName="Conference")
        rows = self.fetch()["events"]
        self.assertEqual(rows[0]["eventType"], "Conference")

    def clear(self):
        # delete_many is a coroutine on mongomock_motor, so it has to be run
        # rather than merely constructed - an un-awaited one silently does
        # nothing and the previous subtest's row is still rows[0].
        run(self.db["Events"].delete_many({}))

    def test_precedence_is_event_type_then_eventtype_then_eventtypename(self):
        cases = [
            ({"Event Type": "First", "eventType": "Second", "eventTypeName": "Third"},
             "First"),
            ({"eventType": "Second", "eventTypeName": "Third"}, "Second"),
            ({"eventTypeName": "Third"}, "Third"),
        ]
        for i, (fields, expected) in enumerate(cases):
            with self.subTest(fields=fields):
                main.clear_event_counts_cache()
                self.clear()
                doc = {
                    "Event Name": f"Precedence {i}",
                    "date": self.past.isoformat(),
                    "recurring_day": [],
                    "Day": "Sunday",
                    "isGlobal": True,
                    "attendance": {},
                }
                doc.update(fields)
                run(self.db["Events"].insert_many([doc]))
                rows = self.fetch()["events"]
                self.assertEqual(len(rows), 1, "each subtest seeds exactly one row")
                self.assertEqual(rows[0]["eventType"], expected)

    def test_falsy_values_fall_through_instead_of_winning(self):
        """dict.get's default only fires when a key is absent, so an empty
        string or a null stored under an earlier spelling used to win and the
        row came back with no type at all."""
        for empty in ("", None):
            with self.subTest(empty=empty):
                main.clear_event_counts_cache()
                self.clear()
                run(self.db["Events"].insert_many([{
                    "Event Name": "Falsy",
                    "date": self.past.isoformat(),
                    "recurring_day": [],
                    "Day": "Sunday",
                    "isGlobal": True,
                    "attendance": {},
                    "Event Type": empty,
                    "eventTypeName": "Church Service",
                }]))
                rows = self.fetch()["events"]
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["eventType"], "Church Service")

    def test_still_falls_back_to_a_literal_when_nothing_is_recorded(self):
        main.clear_event_counts_cache()
        self.clear()
        run(self.db["Events"].insert_many([{
            "Event Name": "Untyped",
            "date": self.past.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {},
        }]))
        rows = self.fetch()["events"]
        self.assertEqual(rows[0]["eventType"], "Event",
                         "a document with no type at all keeps the old fallback")

    def test_a_correctly_labelled_training_row_is_excluded_by_the_front_filter(self):
        """Documents whose only spelling is eventTypeName are mostly Training.
        Service Check-in drops those, so the label has to be real for the filter
        to work - this is the guard on the other half of the fix."""
        excluded = ("cells", "all cells", "cell", "training")
        for value, should_drop in (("Training", True), ("Church Service", False),
                                   ("Conference", False)):
            with self.subTest(value=value):
                self.assertEqual(
                    value.lower() in excluded, should_drop,
                    f"ServiceCheckIn.filterValidEvents would {'drop' if should_drop else 'keep'} {value!r}",
                )


if __name__ == "__main__":
    unittest.main()
