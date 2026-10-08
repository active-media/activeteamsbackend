"""Tests for the two-phase read that replaced the ~48s /events/eventsdata query.

The measured problem: explain("executionStats") showed MongoDB running the
filter in 2ms, but the request still took ~48s, because the handler read every
matching document in full (4.35 MB at ~0.09 MB/s) and only then paginated in
Python down to 25 rows. 77.6% of those bytes were nested attendee person
records inside `attendance`.

The fix splits the read:
  phase 1  build_event_counts_pipeline() - counts, no people
  phase 2  fetch_event_page_details()   - people, only for the returned page

The risk this file guards is the one that would reintroduce "events disappear":
phase 1 no longer has arrays to call len() on, so every place that derived a
status or a count from an array had to be rewritten to read the count instead.
If one of those is wrong, a completed event silently reads as incomplete and
drops out of the COMPLETED tab.

Run:
    venv/bin/python -m pytest tests/test_events_two_phase_read.py -v
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test-active-teams-db")

from unittest.mock import patch  # noqa: E402
import unittest.mock  # noqa: E402

from mongomock_motor import AsyncMongoMockClient  # noqa: E402

import main  # noqa: E402
from database import MAX_RECURRING_WEEKS_BACK  # noqa: E402


def run(coro):
    return asyncio.run(coro)


def attended_entry(date_str, count, status="complete"):
    """A counts-only attendance entry, as phase 1 produces it."""
    return {
        "status": status,
        "is_did_not_meet": False,
        "closed_by": "",
        "closed_at": "",
        "event_date_iso": date_str,
        "event_date_exact": date_str,
        "attendees": [],
        "new_people": [],
        "consolidations": [],
        "attendees_count": count,
        "new_people_count": 0,
        "consolidation_count": 0,
    }


def full_entry(date_str, names, status="complete"):
    """The same record as it exists in the database, with real people in it."""
    return {
        "status": status,
        "is_did_not_meet": False,
        "attendees": [{"id": str(i), "name": n, "price": 100, "paid": 50}
                      for i, n in enumerate(names)],
        "new_people": [],
        "consolidations": [],
    }


class TestCountsPipelineShape(unittest.TestCase):
    """The pipeline must stay within the operator set the query planner and the
    test double both accept, and must not reintroduce a full-document read."""

    def setUp(self):
        self.pipeline = main.build_event_counts_pipeline({"Event Type": "Church Service"})

    def test_starts_with_the_original_match(self):
        self.assertEqual(
            self.pipeline[0],
            {"$match": {"Event Type": "Church Service"}},
            "the pipeline must filter with the caller's query, unchanged",
        )

    def test_uses_only_supported_operators(self):
        """$isObject and $mergeObjects are unavailable in mongomock, so the
        pipeline is restricted to the operators both sides support."""
        allowed = {
            "$match", "$addFields", "$project", "$arrayToObject", "$objectToArray",
            "$map", "$size", "$isArray", "$cond", "$ifNull", "$literal",
        }

        def walk(node):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key.startswith("$"):
                        self.assertIn(key, allowed, f"unsupported operator {key}")
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        for stage in self.pipeline:
            walk(stage)

    def test_keeps_the_scalars_the_handler_reads(self):
        entry = main.build_event_counts_pipeline({})[1]["$addFields"]["attendance"]
        entry = entry["$arrayToObject"]["$map"]["in"]["v"]
        for field in ("status", "is_did_not_meet", "closed_by", "closed_at",
                      "event_date_iso", "event_date_exact"):
            self.assertIn(field, entry,
                          f"phase 1 must still supply {field} or statuses drift")

    def test_emits_counts_for_every_array_it_empties(self):
        entry = main.build_event_counts_pipeline({})[1]["$addFields"]["attendance"]
        entry = entry["$arrayToObject"]["$map"]["in"]["v"]
        for array, count in (("attendees", "attendees_count"),
                             ("new_people", "new_people_count"),
                             ("consolidations", "consolidation_count")):
            self.assertEqual(entry[array], [],
                             f"{array} should be emptied in phase 1")
            self.assertIn(count, entry,
                          f"emptying {array} without {count} loses the count")

    def test_drops_the_person_arrays_in_the_final_projection(self):
        excluded = main.build_event_counts_pipeline({})[2]["$project"]
        for field in main.ATTENDANCE_DETAIL_ARRAYS:
            self.assertEqual(excluded[field], 0,
                             f"{field} must be excluded or phase 1 defeats the point")


class TestCountsPipelineResults(unittest.TestCase):
    """What phase 1 actually returns, against a mock Mongo."""

    def setUp(self):
        self.client = AsyncMongoMockClient()
        self.events = self.client["test-active-teams-db"]["Events"]
        main.clear_event_counts_cache()

    def test_counts_survive_and_people_are_dropped(self):
        run(self.events.insert_many([
            {"_id": 1, "attendance": {
                "2025-03-02": {"status": "complete", "attendees": [{"n": 1}, {"n": 2}],
                               "new_people": [{"n": 3}], "consolidations": []}}},
            {"_id": 2, "attendance": {
                "2025-03-09": {"status": "open", "attendees": []}}},
        ]))

        docs = run(main.get_event_counts_documents(self.events, {}))

        first = next(d for d in docs if d["_id"] == 1)["attendance"]["2025-03-02"]
        self.assertEqual(first["attendees_count"], 2)
        self.assertEqual(first["new_people_count"], 1)
        self.assertEqual(first["consolidation_count"], 0)
        self.assertEqual(first["attendees"], [], "people must not be transferred")
        self.assertEqual(first["status"], "complete")

        second = next(d for d in docs if d["_id"] == 2)["attendance"]["2025-03-09"]
        self.assertEqual(second["attendees_count"], 0)
        self.assertEqual(second["status"], "open")

    def test_event_level_arrays_become_counts(self):
        run(self.events.insert_many([{
            "_id": 1, "attendance": {},
            "attendees": [{"n": 1}, {"n": 2}, {"n": 3}],
            "persistent_attendees": [{"n": 1}],
            "new_people": [{"n": 9}, {"n": 8}],
            "consolidations": [{"n": 7}],
        }]))

        doc = run(main.get_event_counts_documents(self.events, {}))[0]

        self.assertEqual(doc["attendees_count"], 3)
        self.assertEqual(doc["persistent_attendees_count"], 1)
        self.assertEqual(doc["new_people_count"], 2)
        self.assertEqual(doc["consolidation_count"], 1)
        for field in main.ATTENDANCE_DETAIL_ARRAYS:
            self.assertNotIn(field, doc, f"{field} should not survive phase 1")

    def test_malformed_documents_do_not_break_the_read(self):
        """One bad record must not 500 the whole page - it used to be a
        collection scan over real data with legacy shapes mixed in."""
        run(self.events.insert_many([
            {"_id": 1, "attendance": {"2025-03-02": {"status": "complete",
                                                      "attendees": [{"n": 1}]}}},
            {"_id": 2, "attendance": {}},
            {"_id": 3},
            {"_id": 4, "attendance": {"2025-03-02": {"status": "open",
                                                      "attendees": "not-a-list"}}},
            {"_id": 5, "attendance": {"2025-03-02": None}},
        ]))

        docs = run(main.get_event_counts_documents(self.events, {}))

        self.assertEqual(len(docs), 5, "every document should still come back")
        self.assertEqual(docs[0]["attendance"]["2025-03-02"]["attendees_count"], 1)
        self.assertEqual(
            next(d for d in docs if d["_id"] == 4)["attendance"]["2025-03-02"]["attendees_count"],
            0, "a non-array attendees field counts as zero, not an error")

    def test_page_size_limit_is_still_passed_to_the_cursor(self):
        """The document cap must survive the move from find() to aggregate().

        Asserted on the length argument rather than on the result count, because
        mongomock's cursor ignores `length` entirely (verified: aggregate and find
        both return all 50 seeded rows for to_list(length=10)), so asserting on
        len() would pass whether or not the cap was applied.
        """
        run(self.events.insert_many([{"_id": i, "attendance": {}} for i in range(50)]))

        seen = {}
        original = self.events.aggregate

        def recording_aggregate(*args, **kwargs):
            cursor = original(*args, **kwargs)
            real_to_list = cursor.to_list

            async def to_list(length=None, *a, **kw):
                seen["length"] = length
                return await real_to_list(length, *a, **kw)

            cursor.to_list = to_list
            return cursor

        self.events.aggregate = recording_aggregate
        with patch.object(main, "MAX_EVENT_DOCUMENTS", 10):
            run(main.get_event_counts_documents(self.events, {}))

        self.assertEqual(seen.get("length"), 10,
                         "the document cap must be applied to the aggregation cursor")


class TestPhase1Cache(unittest.TestCase):
    def setUp(self):
        self.client = AsyncMongoMockClient()
        self.events = self.client["test-active-teams-db"]["Events"]
        main.clear_event_counts_cache()

    def tearDown(self):
        main.clear_event_counts_cache()

    def test_repeat_read_of_the_same_query_hits_the_cache(self):
        """Pagination happens after the read, so page 2..N issue an identical
        query. That is the case the cache exists for."""
        run(self.events.insert_many([{"_id": 1, "attendance": {}}]))

        with patch.object(main, "_event_counts_cache_ttl", lambda: 30.0):
            first = run(main.get_event_counts_documents(self.events, {"a": 1}))
            calls = {"n": 0}
            original = self.events.aggregate

            def counting(*args, **kwargs):
                calls["n"] += 1
                return original(*args, **kwargs)

            self.events.aggregate = counting
            second = run(main.get_event_counts_documents(self.events, {"a": 1}))

        self.assertEqual(calls["n"], 0, "second read should have been served from cache")
        self.assertEqual(first, second)

    def test_different_queries_do_not_share_an_entry(self):
        run(self.events.insert_many([
            {"_id": 1, "attendance": {}, "Event Type": "A"},
            {"_id": 2, "attendance": {}, "Event Type": "B"},
        ]))

        with patch.object(main, "_event_counts_cache_ttl", lambda: 30.0):
            a = run(main.get_event_counts_documents(self.events, {"Event Type": "A"}))
            b = run(main.get_event_counts_documents(self.events, {"Event Type": "B"}))

        self.assertEqual([d["_id"] for d in a], [1])
        self.assertEqual([d["_id"] for d in b], [2])

    def test_ttl_zero_disables_caching(self):
        run(self.events.insert_many([{"_id": 1, "attendance": {}}]))

        with patch.object(main, "_event_counts_cache_ttl", lambda: 0.0):
            run(main.get_event_counts_documents(self.events, {"a": 1}))
            self.assertEqual(main._EVENTS_COUNTS_CACHE, {},
                             "no entry should be stored when caching is off")

    def test_expired_entries_are_reread(self):
        run(self.events.insert_many([{"_id": 1, "attendance": {}}]))

        with patch.object(main, "_event_counts_cache_ttl", lambda: 0.001):
            run(main.get_event_counts_documents(self.events, {}))
            import time as _time
            _time.sleep(0.02)
            run(self.events.insert_many([{"_id": 2, "attendance": {}}]))
            docs = run(main.get_event_counts_documents(self.events, {}))

        self.assertEqual(len(docs), 2, "an expired entry must be re-read, not reused")

    def test_clear_empties_the_cache(self):
        run(self.events.insert_many([{"_id": 1, "attendance": {}}]))
        with patch.object(main, "_event_counts_cache_ttl", lambda: 30.0):
            run(main.get_event_counts_documents(self.events, {"a": 1}))
            self.assertTrue(main._EVENTS_COUNTS_CACHE)
            main.clear_event_counts_cache()
            self.assertEqual(main._EVENTS_COUNTS_CACHE, {})

    def test_ttl_parsing_is_defensive(self):
        with patch.dict(os.environ, {"EVENTS_COUNTS_CACHE_TTL": "not-a-number"}):
            self.assertEqual(main._event_counts_cache_ttl(), 30.0)
        with patch.dict(os.environ, {"EVENTS_COUNTS_CACHE_TTL": "-5"}):
            self.assertEqual(main._event_counts_cache_ttl(), 0.0)
        with patch.dict(os.environ, {"EVENTS_COUNTS_CACHE_TTL": "0"}):
            self.assertEqual(main._event_counts_cache_ttl(), 0.0)


class TestAttendanceEntryCount(unittest.TestCase):
    def test_prefers_the_count(self):
        self.assertEqual(
            main.attendance_entry_count({"attendees_count": 7, "attendees": []}, "attendees_count", "attendees"),
            7,
            "a non-empty list with a stale count must not win - phase 1 empties arrays",
        )

    def test_falls_back_to_the_array(self):
        self.assertEqual(
            main.attendance_entry_count({"attendees": [1, 2, 3]}, "attendees_count", "attendees"),
            3,
            "documents that never went through the pipeline must still work",
        )

    def test_zero_count_is_not_treated_as_missing(self):
        self.assertEqual(
            main.attendance_entry_count({"attendees_count": 0, "attendees": [1]}, "attendees_count", "attendees"),
            0,
        )

    def test_booleans_are_not_counts(self):
        self.assertEqual(
            main.attendance_entry_count({"attendees_count": True, "attendees": [1, 2]},
                                        "attendees_count", "attendees"),
            2, "True is an int in Python; it must not be read as a count of 1",
        )

    def test_handles_junk(self):
        for entry in (None, "nope", 42, [], {}):
            self.assertEqual(main.attendance_entry_count(entry, "attendees_count", "attendees"), 0)


class TestPageDetailProjection(unittest.TestCase):
    def test_requests_only_the_needed_dates(self):
        projection = main.build_page_detail_projection({
            "attendance.2025-03-02", "attendance.2025-03-09",
        })
        self.assertEqual(projection["attendance.2025-03-02"], 1)
        self.assertEqual(projection["attendance.2025-03-09"], 1)
        self.assertNotIn("attendance", projection,
                         "requesting the whole map would pull every dated record "
                         "of a 27-week recurring event back in")

    def test_also_requests_the_event_level_arrays(self):
        projection = main.build_page_detail_projection({"attendance.2025-03-02"})
        for field in main.ATTENDANCE_DETAIL_ARRAYS:
            if field == "persistent_attendees":
                continue
            self.assertEqual(projection[field], 1,
                             f"{field} is read off the row by Service Check-in")

    def test_persistent_attendees_is_opt_in(self):
        """It is the largest array on the document and nothing reads it off a
        list row, so the default projection must leave it out and the opt-in
        must bring it back."""
        default = main.build_page_detail_projection({"attendance.2025-03-02"})
        self.assertNotIn("persistent_attendees", default)

        opt_in = main.build_page_detail_projection({"attendance.2025-03-02"}, True)
        self.assertEqual(opt_in["persistent_attendees"], 1)


class TestFetchPageDetails(unittest.TestCase):
    def setUp(self):
        self.client = AsyncMongoMockClient()
        self.events = self.client["test-active-teams-db"]["Events"]
        main.clear_event_counts_cache()

    def test_returns_people_for_the_requested_page(self):
        run(self.events.insert_many([{
            "_id": "ev1",
            "attendance": {
                "2025-03-02": full_entry("2025-03-02", ["Ada", "Grace"]),
                "2025-03-09": full_entry("2025-03-09", ["Linus"]),
            },
            "persistent_attendees": [{"id": "p1", "name": "Grace"}],
        }]))

        detail = run(main.fetch_event_page_details(
            self.events, [{"original_event_id": "ev1", "date": "2025-03-02"}],
            True,
        ))

        names = [a["name"] for a in detail[("ev1", "2025-03-02")]["attendees"]]
        self.assertEqual(names, ["Ada", "Grace"])
        self.assertEqual(
            [a["name"] for a in detail[("ev1", "2025-03-02")]["persistent_attendees"]],
            ["Grace"],
        )

    def test_omits_persistent_attendees_unless_asked(self):
        """The default read must not pay for the largest array on the document.

        Nothing reads persistent_attendees off a list row, and it measured 69%
        of a 25-row page and 83% of the 500-row Service Check-in request, so the
        projection drops it and the detail payload omits the key entirely.
        """
        run(self.events.insert_many([{
            "_id": "ev1",
            "attendance": {"2025-03-02": full_entry("2025-03-02", ["Ada"])},
            "persistent_attendees": [{"id": "p1", "name": "Grace"}],
        }]))

        detail = run(main.fetch_event_page_details(
            self.events, [{"original_event_id": "ev1", "date": "2025-03-02"}]
        ))

        self.assertNotIn("persistent_attendees", detail[("ev1", "2025-03-02")])
        # The other event-level arrays are still there - only this one is gated.
        self.assertEqual(
            [a["name"] for a in detail[("ev1", "2025-03-02")]["attendees"]], ["Ada"],
        )

    def test_does_not_leak_other_dates_into_the_page(self):
        """The whole point of phase 2: 25 rows must not drag in 27 weeks."""
        run(self.events.insert_many([{
            "_id": "ev1",
            "attendance": {f"2025-03-{d:02d}": full_entry(f"2025-03-{d:02d}", ["P"])
                           for d in range(1, 28)},
        }]))

        detail = run(main.fetch_event_page_details(
            self.events, [{"original_event_id": "ev1", "date": "2025-03-02"}]
        ))

        self.assertEqual(list(detail.keys()), [("ev1", "2025-03-02")])

    def test_resolves_a_legacy_week_key(self):
        """Records stored as "2025-W03" are found by the exact-date lookup, so
        phase 2 must fall back to re-reading that one event rather than
        silently returning no people."""
        # 2025-03-02 is in ISO week 2025-W09 - get_attendance_by_date()'s legacy
        # fallback looks the date up by that key.
        run(self.events.insert_many([{
            "_id": "ev1",
            "attendance": {"2025-W09": full_entry("2025-03-02", ["Ada"])},
        }]))

        detail = run(main.fetch_event_page_details(
            self.events, [{"original_event_id": "ev1", "date": "2025-03-02"}]
        ))

        self.assertIn(("ev1", "2025-03-02"), detail)
        self.assertEqual(
            [a["name"] for a in detail[("ev1", "2025-03-02")]["attendees"]], ["Ada"])

    def test_missing_document_is_not_fatal(self):
        detail = run(main.fetch_event_page_details(
            self.events, [{"original_event_id": "nope", "date": "2025-03-02"}],
            True,
        ))
        payload = detail[("nope", "2025-03-02")]
        self.assertFalse(payload["has_date_record"],
                         "an event that no longer exists has no dated record")
        for field in ("attendees", "new_people", "consolidations"):
            self.assertIsNone(payload[field],
                              f"a vanished event must not claim dated {field}")
        for field in ("event_attendees", "event_new_people",
                      "event_consolidations", "persistent_attendees"):
            self.assertEqual(payload[field], [],
                             f"{field} should be empty for a vanished event")

    def test_empty_and_junk_input(self):
        for instances in ([], None, [{}], [None], ["string"],
                          [{"original_event_id": "", "date": ""}]):
            self.assertEqual(run(main.fetch_event_page_details(self.events, instances)), {})


class TestEnrichAttendeesWithFinancials(unittest.TestCase):
    """Hoisted to module level; both branches and phase 2 now share one copy."""

    def test_owing_and_change(self):
        out = main.enrich_attendees_with_financials([
            {"name": "A", "price": 100, "paid": 100},
            {"name": "B", "price": 100, "paid": 50},
            {"name": "C", "price": 100, "paid": 0},
        ])
        self.assertEqual([(a["owing"], a["change"]) for a in out],
                         [(0, 0), (50, 0), (100, 0)])

    def test_overpayment_becomes_change(self):
        out = main.enrich_attendees_with_financials([{"name": "A", "price": 100, "paid": 150}])
        self.assertEqual((out[0]["owing"], out[0]["change"]), (0, 50))

    def test_skips_non_dicts_and_tolerates_junk(self):
        self.assertEqual(main.enrich_attendees_with_financials([None, 1, "x"]), [])
        self.assertEqual(main.enrich_attendees_with_financials(None), [])
        out = main.enrich_attendees_with_financials([{"name": "A", "price": "bad", "paid": None}])
        self.assertEqual((out[0]["price"], out[0]["paid"]), (0, 0))

    def test_reads_paid_amount_alias(self):
        out = main.enrich_attendees_with_financials([{"name": "A", "paidAmount": 75}])
        self.assertEqual(out[0]["paid"], 75.0)


class TestTwoPhaseMatchesFullRead(unittest.TestCase):
    """The decisive test: the two-phase response must be identical to the old
    full-document response, row for row.

    The old behaviour is reproduced faithfully by reading every document in full
    and skipping phase 2 - which is exactly what the handler used to do, since it
    paginated in Python after the read. If phase 1's counts ever disagree with the
    real arrays, a status or a count drifts here and an event silently moves
    between the COMPLETED and INCOMPLETE tabs.
    """

    def setUp(self):
        from fastapi.testclient import TestClient

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
        main.clear_event_counts_cache()

        async def fake_user():
            return {"email": "admin@test.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "Admin", "surname": "User"}

        self.app = main.app
        self.app.dependency_overrides[main.get_current_user] = fake_user
        self.test_client = TestClient(self.app)

    def tearDown(self):
        self.app.dependency_overrides.clear()
        main.clear_event_counts_cache()

    def seed(self):
        from datetime import date, timedelta
        today = date.today()
        past = today - timedelta(days=7)
        older = today - timedelta(days=14)

        run(self.db["Events"].insert_many([
            {   # one-time event, completed, with people
                "Event Name": "Sunday Service",
                "Event Type": "Church Service",
                "date": past.isoformat(),
                "recurring_day": [],
                "Day": "Sunday",
                "attendance": {past.isoformat(): full_entry(
                    past.isoformat(), ["Ada", "Grace"])},
                "persistent_attendees": [{"id": "p1", "name": "Grace", "price": 0, "paid": 0}],
            },
            {   # one-time event, never captured -> must stay incomplete
                "Event Name": "Quiet Service",
                "Event Type": "Church Service",
                "date": older.isoformat(),
                "recurring_day": [],
                "Day": "Sunday",
                "attendance": {},
                "persistent_attendees": [],
            },
            {   # recurring event with several weeks of attendance
                "Event Name": "Weekly Prayer",
                "Event Type": "Prayer",
                "date": older.isoformat(),
                "recurring_day": ["Sunday"],
                "Day": "Sunday",
                "attendance": {
                    older.isoformat(): full_entry(older.isoformat(), ["Linus"]),
                    past.isoformat(): full_entry(past.isoformat(), ["Ada", "Grace", "Linus"]),
                },
                "persistent_attendees": [{"id": "p2", "name": "Ada", "price": 0, "paid": 0}],
            },
            {   # explicitly did_not_meet
                "Event Name": "Cancelled Service",
                "Event Type": "Church Service",
                "date": past.isoformat(),
                "recurring_day": [],
                "Day": "Sunday",
                "attendance": {past.isoformat(): {
                    "status": "did_not_meet", "is_did_not_meet": True, "attendees": []}},
                "persistent_attendees": [],
            },
        ]))
        return past, older

    def fetch(self, **params):
        # The opt-in is on by default here so the row-for-row comparison always
        # covers the richest payload. The default (field omitted) path is covered
        # separately by TestPhase2EdgeCasesFoundByTheEquivalenceTest.
        query = {"limit": 50, "include_persistent_attendees": "true"}
        query.update(params)
        resp = self.test_client.get("/events/eventsdata", params=query)
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    async def old_style_read(self, collection, query):
        """The pre-split read: every document, in full, no phase 2.

        Async because it is substituted into the running request, not called
        from a test body.
        """
        return await collection.find(query).to_list(length=3000)

    def assert_same(self, **params):
        new = self.fetch(**params)

        with patch.object(main, "get_event_counts_documents", self.old_style_read), \
             patch.object(main, "fetch_event_page_details",
                          new=unittest.mock.AsyncMock(return_value={})):
            old = self.fetch(**params)

        self.assertEqual(new["total_events"], old["total_events"],
                         "the number of matching instances must not change")
        self.assertEqual(new["total_pages"], old["total_pages"])
        self.assertEqual(new["has_more"], old["has_more"])

        by_id_new = {e["_id"]: e for e in new["events"]}
        by_id_old = {e["_id"]: e for e in old["events"]}
        self.assertEqual(set(by_id_new), set(by_id_old),
                         "the same rows must be returned")

        for key, old_row in by_id_old.items():
            self.assertEqual(by_id_new[key], old_row,
                             f"row {key} differs between the two-phase and full reads")
        return new

    def test_unfiltered_response_is_identical(self):
        self.seed()
        rows = self.assert_same()
        self.assertTrue(rows["events"], "fixture should produce rows")

    def test_status_filter_complete_is_identical(self):
        self.seed()
        self.assert_same(status="complete")

    def test_status_filter_incomplete_is_identical(self):
        self.seed()
        self.assert_same(status="incomplete")

    def test_status_filter_all_is_identical(self):
        self.seed()
        self.assert_same(status="all")

    def test_completed_events_survive(self):
        """The specific regression we are guarding: a captured event must still
        read as complete once its people are no longer in phase 1.

        Asserted both through the COMPLETED tab (the tab the bug emptied) and
        through the row's own status, so a status that merely fails to be
        filtered out still gets caught.
        """
        self.seed()
        rows = self.fetch(status="complete")
        names = sorted(e["eventName"] for e in rows["events"])
        self.assertIn("Sunday Service", names,
                      "a captured event lost its completed status")
        self.assertNotIn("Quiet Service", names)

        every = {e["eventName"]: e for e in self.fetch(status="all")["events"]}
        self.assertEqual(every["Sunday Service"]["status"], "complete",
                         "a captured event must still be reported as complete")
        self.assertEqual(every["Quiet Service"]["status"], "incomplete")

    def test_counts_match_the_people_returned(self):
        self.seed()
        for row in self.fetch()["events"]:
            self.assertEqual(
                row["total_attendance"], len(row["attendees"]),
                f"{row['eventName']} {row['date']}: total_attendance must equal "
                "the number of attendees actually returned",
            )
            self.assertEqual(
                row["new_people_count"], len(row["new_people"]),
                f"{row['eventName']} {row['date']}: new_people_count mismatch")

    def test_pagination_returns_the_same_pages(self):
        self.seed()
        page1 = self.assert_same(page=1, limit=2)
        self.assertEqual(len(page1["events"]), 2)
        self.assertTrue(page1["has_more"])
        self.assert_same(page=2, limit=2)

    def test_show_all_dates_is_identical(self):
        self.seed()
        self.assert_same(show_all_dates="true")


class TestPhase2EdgeCasesFoundByTheEquivalenceTest(unittest.TestCase):
    """Regressions for two bugs the equivalence test caught.

    Both were invisible in a benchmark and only showed up as a diff against the
    old full-document read, which is why they are pinned here explicitly.
    """

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
            return {"email": "admin@test.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "Admin", "surname": "User"}

        self.app = main.app
        self.app.dependency_overrides[main.get_current_user] = fake_user
        self.test_client = TestClient(self.app)

    def tearDown(self):
        self.app.dependency_overrides.clear()
        main.clear_event_counts_cache()

    def fetch(self, **params):
        query = {"limit": 100}
        query.update(params)
        resp = self.test_client.get("/events/eventsdata", params=query)
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def test_persistent_attendees_survive_on_rows_with_no_attendance(self):
        """The first bug: phase 2 only produced detail for dates that had an
        attendance record, so the many recurring rows with no capture lost the
        event-level persistent_attendees list the old code attached to every row.
        """
        from datetime import date, timedelta
        older = date.today() - timedelta(days=14)
        run(self.db["Events"].insert_many([{
            "Event Name": "Weekly Prayer",
            "Event Type": "Prayer",
            "date": older.isoformat(),
            "recurring_day": ["Sunday"],
            "Day": "Sunday",
            "attendance": {},          # nothing captured at all
            "persistent_attendees": [{"id": "p1", "name": "Grace", "price": 0, "paid": 0}],
        }]))

        rows = self.fetch(include_persistent_attendees="true")["events"]
        self.assertTrue(rows, "the recurring event should still produce rows")
        for row in rows:
            self.assertEqual(
                [a["name"] for a in row["persistent_attendees"]], ["Grace"],
                f"row {row['date']} lost its persistent attendees",
            )

    def test_persistent_attendees_are_empty_by_default(self):
        """Opting in is required to get the list at all, so the default page
        response no longer repeats it once per row.

        Both event shapes are seeded because the list is attached in two
        separate places - the recurring branch and the one-time branch - and
        phase 2 overwrites it a third time. All three have to honour the flag.
        """
        from datetime import date, timedelta
        older = date.today() - timedelta(days=14)
        people = [{"id": "p1", "name": "Grace", "price": 0, "paid": 0}]
        run(self.db["Events"].insert_many([
            {   # recurring -> exercises the recurring branch
                "Event Name": "Weekly Prayer",
                "Event Type": "Prayer",
                "date": older.isoformat(),
                "recurring_day": ["Sunday"],
                "Day": "Sunday",
                "attendance": {},
                "persistent_attendees": people,
            },
            {   # one-time -> exercises the one-time branch
                "Event Name": "Quiet Service",
                "Event Type": "Church Service",
                "date": older.isoformat(),
                "recurring_day": [],
                "Day": "Sunday",
                "attendance": {},
                "persistent_attendees": people,
            },
        ]))

        rows = self.fetch()["events"]
        self.assertTrue(rows, "the seeded events should still produce rows")
        for row in rows:
            self.assertEqual(row["persistent_attendees"], [],
                             f"row {row['eventName']} {row['date']} should not "
                             f"carry the list")

    def test_recurring_root_attendee_fallback_still_fires(self):
        """The second bug: the fallback keyed off the event-level attendees
        array, which phase 1 empties, so a recurring event whose attendees live
        at the root level stopped being treated as complete.

        This must be a genuinely recurring event - the fallback under test lives
        in the recurring branch, which a one-time event never reaches.
        """
        from datetime import date, timedelta
        # A real past Sunday, at least a week back. The recurring branch only
        # emits the weekdays named in recurring_day, so anchoring to "a week ago"
        # rather than to a Sunday would never match any instance.
        today = date.today()
        days_since_last_sunday = (today.weekday() + 1) % 7
        sunday = today - timedelta(days=days_since_last_sunday + 7)
        run(self.db["Events"].insert_many([{
            "Event Name": "Recurring Root Attendees",
            "Event Type": "Church Service",
            "date": sunday.isoformat(),
            "recurring_day": ["Sunday"],
            "Day": "Sunday",
            "status": "complete",
            "attendance": {},           # no per-date record at all
            "attendees": [{"id": "a1", "name": "Ada", "price": 0, "paid": 0}],
        }]))

        rows = self.fetch(status="all")["events"]
        row = next(r for r in rows
                   if r["eventName"] == "Recurring Root Attendees"
                   and r["date"] == sunday.isoformat())

        self.assertTrue(row["is_recurring"], "fixture must be a recurring instance")
        self.assertEqual(row["status"], "complete",
                         "a recurring event with root attendees must read as complete")
        self.assertEqual(row["total_attendance"], 1)
        self.assertEqual([a["name"] for a in row["attendees"]], ["Ada"],
                         "phase 2 must still supply the people")

    def test_one_time_event_prefers_root_attendees(self):
        from datetime import date, timedelta
        older = date.today() - timedelta(days=14)
        run(self.db["Events"].insert_many([{
            "Event Name": "Root Only",
            "Event Type": "Church Service",
            "date": older.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {},
            "attendees": [{"id": "a1", "name": "Ada"}, {"id": "a2", "name": "Grace"}],
        }]))

        row = next(r for r in self.fetch()["events"] if r["eventName"] == "Root Only")
        self.assertEqual(row["status"], "complete")
        self.assertEqual(row["total_attendance"], 2)
        self.assertEqual(len(row["attendees"]), 2)

    def test_record_with_attendees_but_no_status_reads_as_complete(self):
        """has_weekly_attendees is the deciding term for a record whose own
        status is missing.

        The status ladder is: did_not_meet -> open/incomplete/reopened/active ->
        `has_weekly_attendees or complete/closed` -> incomplete. A record with no
        status therefore falls through to the has_weekly_attendees term, so
        reading that from the (now empty) array instead of the count demotes it
        to incomplete. Production has 11 such records - 13 with a null status, of
        which 11 carry attendees.
        """
        from datetime import date, timedelta
        older = date.today() - timedelta(days=14)
        run(self.db["Events"].insert_many([{
            "Event Name": "No Status But People",
            "Event Type": "Church Service",
            "date": older.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {older.isoformat(): {
                "attendees": [{"id": "a1", "name": "Ada"}],
                "new_people": [],
                "consolidations": [],
                # deliberately no "status" and no "is_did_not_meet"
            }},
        }]))

        row = next(r for r in self.fetch()["events"]
                   if r["eventName"] == "No Status But People")

        self.assertEqual(row["status"], "complete",
                         "attendees with no status must read as complete")
        self.assertEqual(row["total_attendance"], 1)
        self.assertEqual([a["name"] for a in row["attendees"]], ["Ada"])

    def test_recurring_record_with_attendees_but_no_status_reads_as_complete(self):
        """The same ladder, on the recurring branch, which has its own copy."""
        from datetime import date, timedelta
        today = date.today()
        days_since_last_sunday = (today.weekday() + 1) % 7
        sunday = today - timedelta(days=days_since_last_sunday + 7)
        run(self.db["Events"].insert_many([{
            "Event Name": "Recurring No Status",
            "Event Type": "Church Service",
            "date": sunday.isoformat(),
            "recurring_day": ["Sunday"],
            "Day": "Sunday",
            "attendance": {sunday.isoformat(): {
                "attendees": [{"id": "a1", "name": "Ada"}, {"id": "a2", "name": "Grace"}],
                "new_people": [],
                "consolidations": [],
            }},
        }]))

        rows = self.fetch()["events"]
        row = next(r for r in rows
                   if r["eventName"] == "Recurring No Status"
                   and r["date"] == sunday.isoformat())

        self.assertEqual(row["status"], "complete")
        self.assertEqual(row["total_attendance"], 2)

    def test_event_with_no_attendees_anywhere_stays_incomplete(self):
        from datetime import date, timedelta
        older = date.today() - timedelta(days=14)
        run(self.db["Events"].insert_many([{
            "Event Name": "Never Captured",
            "Event Type": "Church Service",
            "date": older.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {},
        }]))

        row = next(r for r in self.fetch()["events"] if r["eventName"] == "Never Captured")
        self.assertEqual(row["status"], "incomplete")
        self.assertEqual(row["total_attendance"], 0)
        self.assertEqual(row["attendees"], [])


class TestEndpointActuallyUsesTheTwoPhaseRead(unittest.TestCase):
    """Guards against the split being quietly undone.

    Every other test here exercises the helpers directly, so they would keep
    passing if the handler stopped calling them and went back to reading whole
    documents. These assert the wiring.
    """

    def test_handler_reads_through_the_counts_pipeline(self):
        import inspect
        source = inspect.getsource(main.get_other_events)
        self.assertIn("get_event_counts_documents(", source,
                      "the handler must read through the counts pipeline")
        self.assertIn("fetch_event_page_details(", source,
                      "the handler must run the phase-2 detail pass")
        self.assertNotIn("events_collection.find(query)\n", source,
                         "the handler must not read whole documents directly")

    def test_endpoint_runs_both_phases(self):
        from fastapi.testclient import TestClient
        from datetime import date, timedelta

        client = AsyncMongoMockClient()
        db = client["test-active-teams-db"]
        main.events_collection = db["Events"]
        for name in ("People", "Users", "tasks", "TaskTypes", "OrgConfig",
                     "consolidations", "organizations"):
            setattr(main, f"{name.lower()}_collection", db[name])
        main.clear_event_counts_cache()

        past = date.today() - timedelta(days=7)
        run(db["Events"].insert_many([{
            "Event Name": "Traced Service",
            "Event Type": "Church Service",
            "date": past.isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {past.isoformat(): full_entry(past.isoformat(), ["Ada"])},
            "persistent_attendees": [{"id": "p1", "name": "Grace"}],
        }]))

        calls = {"phase1": 0, "phase2": 0, "flags": []}
        real_phase1 = main.get_event_counts_documents
        real_phase2 = main.fetch_event_page_details

        async def traced_phase1(collection, query):
            calls["phase1"] += 1
            return await real_phase1(collection, query)

        async def traced_phase2(collection, instances, include_persistent_attendees=False):
            calls["phase2"] += 1
            calls["flags"].append(include_persistent_attendees)
            return await real_phase2(collection, instances, include_persistent_attendees)

        async def fake_user():
            return {"email": "a@b.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "A", "surname": "B"}

        main.app.dependency_overrides[main.get_current_user] = fake_user
        try:
            with patch.object(main, "get_event_counts_documents", traced_phase1), \
                 patch.object(main, "fetch_event_page_details", traced_phase2):
                with TestClient(main.app) as http:
                    resp = http.get("/events/eventsdata", params={"limit": 25})
        finally:
            main.app.dependency_overrides.clear()
            main.clear_event_counts_cache()

        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(calls["phase1"], 1, "phase 1 must run")
        self.assertEqual(calls["phase2"], 1, "phase 2 must run")
        self.assertEqual(calls["flags"], [False],
                         "phase 2 must receive the opt-in flag so it can skip the "
                         "largest array unless it was asked for")
        row = next(e for e in resp.json()["events"] if e["eventName"] == "Traced Service")
        self.assertEqual([a["name"] for a in row["attendees"]], ["Ada"],
                         "the row must still carry its attendees")
        self.assertEqual(row["persistent_attendees"], [],
                         "the row must not carry the opt-in list by default")


class TestCacheInvalidatedOnWrite(unittest.TestCase):
    """A submission changes exactly the counts phase 1 memoises, so it must
    clear them - otherwise a leader captures attendance and the Events page
    keeps showing the pre-capture status for up to the TTL."""

    def test_submit_attendance_clears_the_cache(self):
        import inspect
        source = inspect.getsource(main.submit_attendance)
        self.assertIn("clear_event_counts_cache()", source,
                      "submit_attendance must invalidate the counts cache")


class TestReadPathHasNoWrites(unittest.TestCase):
    """The GET handler used to issue an update_one per synthesised instance to
    migrate legacy attendance keys. With the corrected recurring walk-back that
    became up to one write per instance per page load."""

    def test_eventsdata_performs_no_updates(self):
        client = AsyncMongoMockClient()
        db = client["test-active-teams-db"]
        main.events_collection = db["Events"]
        main.people_collection = db["People"]
        main.users_collection = db["Users"]
        main.tasks_collection = db["tasks"]
        main.tasktypes_collection = db["TaskTypes"]
        main.org_config_collection = db["OrgConfig"]
        main.consolidations_collection = db["consolidations"]
        main.organizations_collection = db["organizations"]
        main.clear_event_counts_cache()

        from fastapi.testclient import TestClient
        from datetime import date, timedelta

        today = date.today()
        # A legacy-keyed record, which is exactly what used to trigger the write.
        run(db["Events"].insert_many([{
            "_id": "ev1",
            "Event Name": "Legacy Service",
            "Event Type": "Church Service",
            "date": (today - timedelta(days=3)).isoformat(),
            "recurring_day": [],
            "Day": "Sunday",
            "attendance": {f"{today - timedelta(days=3):%G-W%V}": full_entry(
                (today - timedelta(days=3)).isoformat(), ["Ada"])},
        }]))

        updates = {"n": 0}
        original = db["Events"].update_one

        def counting(*args, **kwargs):
            updates["n"] += 1
            return original(*args, **kwargs)

        db["Events"].update_one = counting

        async def fake_user():
            return {"email": "a@b.com", "role": "admin", "org_id": "active-teams",
                    "Organization": "Active Church", "name": "A", "surname": "B"}

        main.app.dependency_overrides[main.get_current_user] = fake_user
        try:
            with TestClient(main.app) as client:
                resp = client.get("/events/eventsdata", params={"limit": 25})
        finally:
            main.app.dependency_overrides.clear()

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(updates["n"], 0,
                         "a GET handler must not write to the Events collection")
        # The legacy record must still be visible - the write-back was never
        # needed to read it.
        self.assertTrue(any(e["eventName"] == "Legacy Service" for e in resp.json()["events"]))


if __name__ == "__main__":
    unittest.main()
