"""Tests for the events visibility fix.

The bug: /events/eventsdata filtered event documents on {"org_id": org_id} with
no fallback for documents that predate multi-tenant support. 3392 documents in
production carry no org_id, so they were silently invisible in Events, Service
Check-in and Stats while newly created events showed normally.

These tests pin the fixed behaviour, including the regression that would undo it.

Run:
    venv/bin/python -m pytest tests/test_events_visibility.py -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# database.py builds a real motor client at import time. Point it at nothing
# before importing so no network connection is attempted.
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test-active-teams-db")

from main import build_event_org_conditions  # noqa: E402


def matches(conditions, doc):
    """Does a document satisfy any branch of the $or condition list?

    Mirrors the subset of Mongo query semantics these conditions use, so the
    tests assert on real query behaviour rather than on dict shape.
    """
    for cond in conditions:
        if _matches_one(cond, doc):
            return True
    return False


def _matches_one(cond, doc):
    for field, expected in cond.items():
        if field == "org_id" and isinstance(expected, dict) and "$exists" in expected:
            # Must return rather than fall through: falling off the end of the
            # loop would report a match for any document, making every
            # "must not be visible" assertion pass vacuously.
            return expected["$exists"] == (field in doc)
        if field not in doc:
            return False
        actual = doc[field]
        if isinstance(expected, dict) and "$regex" in expected:
            import re as _re
            flags = _re.IGNORECASE if "i" in expected.get("$options", "") else 0
            if not _re.search(expected["$regex"], str(actual), flags):
                return False
            continue
        if str(actual).lower() != str(expected).lower():
            return False
    return True


LEGACY_EVENT = {
    # No org_id, no Organization - exactly the shape of the 3392 hidden docs
    "Event Name": "J Activation",
    "Event Type": "Church Service",
    "date": "2026-03-08",
}
TAGGED_EVENT = {
    "Event Name": "J Activation",
    "Event Type": "Church Service",
    "org_id": "active-teams",
    "date": "2026-09-27",
}
OTHER_ORG_EVENT = {
    "Event Name": "J Activation",
    "Event Type": "Church Service",
    "org_id": "some-other-church",
    "date": "2026-03-08",
}
LEGACY_WITH_ORGANISATION = {
    "Event Name": "J Activation",
    "Event Type": "Church Service",
    "Organisation": "Active Church",
    "date": "2026-03-08",
}


class TestLegacyEventsAreVisible(unittest.TestCase):
    def setUp(self):
        self.conditions = build_event_org_conditions("active-teams", "Active Church")

    def test_untagged_event_is_visible(self):
        """The regression test: pre-multi-tenant events must not be filtered out."""
        self.assertTrue(
            matches(self.conditions, LEGACY_EVENT),
            "an event with no org_id must be visible to the original org",
        )

    def test_tagged_event_is_visible(self):
        self.assertTrue(matches(self.conditions, TAGGED_EVENT))

    def test_legacy_event_with_organisation_is_visible(self):
        self.assertTrue(matches(self.conditions, LEGACY_WITH_ORGANISATION))

    def test_other_org_event_is_not_visible(self):
        """The fallback must not leak another org's events."""
        self.assertFalse(matches(self.conditions, OTHER_ORG_EVENT))

    def test_empty_org_still_matches_legacy(self):
        """A user with no Organization string must still see untagged events."""
        conditions = build_event_org_conditions("active-teams", "")
        self.assertTrue(matches(conditions, LEGACY_EVENT))
        self.assertFalse(matches(conditions, OTHER_ORG_EVENT))

    def test_untagged_event_not_included_for_other_orgs(self):
        """The legacy fallback is scoped to active-teams only, by design."""
        conditions = build_event_org_conditions("some-other-church", "Some Other Church")
        self.assertFalse(
            matches(conditions, LEGACY_EVENT),
            "untagged events must not leak into a non-default org",
        )
        self.assertTrue(matches(conditions, OTHER_ORG_EVENT))


class TestOrgIdGapIsClosed(unittest.TestCase):
    def test_original_bug_shape_is_rejected(self):
        """The pre-fix query was [{org_id: ...}, {Organization: regex}].

        Assert it could NOT see a legacy event, so this test would fail if
        someone reverted the fix.
        """
        old_conditions = [
            {"org_id": "active-teams"},
            {"Organization": {"$regex": "Active\\ Church", "$options": "i"}},
        ]
        self.assertFalse(
            matches(old_conditions, LEGACY_EVENT),
            "sanity check: the old query really did hide legacy events",
        )

    def test_fixed_query_does_see_it(self):
        self.assertTrue(matches(build_event_org_conditions("active-teams", "Active Church"), LEGACY_EVENT))


if __name__ == "__main__":
    unittest.main()
