"""Tests for reactivate_events.py.

The safety property that matters most: the script must not write anything unless
--apply is passed. A production run without that flag must be a no-op.

Run:
    venv/bin/python -m pytest tests/test_reactivate_events.py -v
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test-active-teams-db")

from mongomock_motor import AsyncMongoMockClient  # noqa: E402

import reactivate_events  # noqa: E402


# Factories, not shared dicts: mongomock's insert_many stamps an _id onto the
# dicts it is handed, so reusing one module-level literal across tests would make
# every later copy collide on _id.
def perm_deact(**over):
    return {
        "Event Name": "Dormant Cell",
        "org_id": "active-teams",
        "is_permanent_deact": True,
        "is_active": False,
        "deactivation_reason": "Season ended",
        "deactivation_start": "2025-06-01",
        **over,
    }


def other_org(**over):
    return {
        "Event Name": "Someone Elses Cell",
        "org_id": "some-other-church",
        "is_permanent_deact": True,
        "is_active": False,
        **over,
    }


def healthy(**over):
    return {
        "Event Name": "Running Cell",
        "org_id": "active-teams",
        "is_active": True,
        **over,
    }


def run_script(argv, docs=None):
    """Run the script's main() against an in-memory Mongo. Returns (rc, db).

    DB_NAME is forced rather than merely defaulted: importing main (as the other
    test modules do) runs load_dotenv(), which can put a real DB_NAME into the
    environment and silently point the script at a different - and here empty -
    database.
    """
    client = AsyncMongoMockClient()
    db = client["test-active-teams-db"]
    if docs is None:
        docs = [perm_deact(), other_org(), healthy()]
    asyncio.run(db["Events"].insert_many(docs))

    with patch.dict(
        os.environ,
        {"DB_NAME": "test-active-teams-db", "MONGO_URI": "mongodb://localhost:27017"},
    ):
        with patch.object(reactivate_events, "AsyncIOMotorClient", return_value=client):
            with patch.object(sys, "argv", ["reactivate_events.py"] + argv):
                rc = asyncio.run(reactivate_events.main())
    return rc, db


def fetch(db):
    return asyncio.run(db["Events"].find({}).to_list(length=100))


def by_name(db, name):
    return next(d for d in fetch(db) if d.get("Event Name") == name)


class TestDryRunIsSafe(unittest.TestCase):
    def test_dry_run_writes_nothing(self):
        """The default invocation must not modify a single document."""
        before = fetch(run_script([])[1])
        rc, db = run_script([])
        after = fetch(db)

        self.assertEqual(rc, 0)
        self.assertEqual(
            [(d.get("Event Name"), d.get("is_active")) for d in before],
            [(d.get("Event Name"), d.get("is_active")) for d in after],
            "a dry run modified documents",
        )

    def test_dry_run_leaves_the_target_inactive(self):
        _, db = run_script([])
        self.assertFalse(by_name(db, "Dormant Cell")["is_active"])


class TestApply(unittest.TestCase):
    def test_apply_reactivates_matching_org(self):
        _, db = run_script(["--apply"])
        self.assertTrue(by_name(db, "Dormant Cell")["is_active"])

    def test_apply_preserves_the_deactivation_audit_trail(self):
        """The nightly job nulls these fields; this script must not."""
        _, db = run_script(["--apply"])
        doc = by_name(db, "Dormant Cell")
        self.assertEqual(doc.get("deactivation_reason"), "Season ended")
        self.assertEqual(doc.get("deactivation_start"), "2025-06-01")

    def test_apply_does_not_touch_other_orgs(self):
        _, db = run_script(["--apply"])
        self.assertFalse(by_name(db, "Someone Elses Cell")["is_active"])

    def test_apply_leaves_healthy_events_alone(self):
        _, db = run_script(["--apply"])
        self.assertTrue(by_name(db, "Running Cell")["is_active"])

    def test_include_other_orgs_opts_in_explicitly(self):
        _, db = run_script(["--apply", "--include-other-orgs"])
        self.assertTrue(
            by_name(db, "Someone Elses Cell")["is_active"],
            "--include-other-orgs did not reach the other org",
        )

    def test_limit_caps_the_number_touched(self):
        _, db = run_script(
            ["--apply", "--limit", "2"],
            docs=[perm_deact(**{"Event Name": f"Cell {i}"}) for i in range(5)],
        )

        active = [d for d in fetch(db) if d.get("is_active")]
        self.assertEqual(len(active), 2, "--limit did not cap the update")


class TestArgumentParsing(unittest.TestCase):
    def test_help_exits_cleanly(self):
        # main() is a coroutine, so the body - and therefore argparse - only runs
        # when awaited. Without asyncio.run the help text never prints.
        with patch.object(sys, "argv", ["reactivate_events.py", "--help"]):
            with self.assertRaises(SystemExit) as ctx:
                asyncio.run(reactivate_events.main())
        self.assertEqual(ctx.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
