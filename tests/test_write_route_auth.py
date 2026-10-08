"""Locks down which write routes may be called without a bearer token.

The /eventsdata latency work did not touch auth, but the same audit that found
the unauthenticated write routes also found that nothing was stopping the next
one from being added. These tests fail if a new write route shows up without
get_current_user, or if a route that is supposed to be public is quietly locked
down (which would break login, and is the more expensive mistake of the two).
"""

import inspect
import unittest

import main
from fastapi.routing import APIRoute

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def write_routes():
    """Yield (method, path, requires_auth, endpoint_name) once per method.

    One entry per method rather than per route, so a path that accepts both
    POST and PUT yields two rows and a single unguarded verb cannot hide behind
    a guarded one.
    """
    for route in main.app.routes:
        if not isinstance(route, APIRoute):
            continue
        methods = {m for m in route.methods if m not in SAFE_METHODS}
        if not methods:
            continue
        requires_auth = "get_current_user" in inspect.getsource(route.endpoint)
        for method in sorted(methods):
            yield method, route.path, requires_auth, route.endpoint.__name__


def unauthenticated_write_routes():
    return {(m, p) for m, p, auth, _ in write_routes() if not auth}


# The auth flows cannot require a token by definition - the caller has none yet.
# That is the whole list: every other write route in the app requires a bearer
# token, including the legacy /checkin and /uncapture pair, which had no caller
# in this repo or the frontend and were open to anyone who knew the URL.
EXPECTED_PUBLIC = {
    ("POST", "/login"),
    ("POST", "/logout"),
    ("POST", "/signup"),
    ("POST", "/refresh-token"),
    ("POST", "/forgot-password"),
    ("POST", "/reset-password"),
}


class TestWriteRoutesRequireAuth(unittest.TestCase):
    def test_no_write_route_is_public_beyond_the_known_set(self):
        unexpected = unauthenticated_write_routes() - EXPECTED_PUBLIC
        self.assertEqual(
            unexpected, set(),
            "these write routes have no get_current_user dependency; add the "
            "dependency or extend EXPECTED_PUBLIC if it is meant to be public",
        )

    def test_the_public_set_has_not_shrunk(self):
        """Guards the other direction: locking down an auth flow would break
        login for every user, which is far worse than an extra public route."""
        missing = EXPECTED_PUBLIC - unauthenticated_write_routes()
        self.assertEqual(
            missing, set(),
            "these routes were expected to stay public but now require auth",
        )

    def test_the_scan_actually_finds_routes(self):
        """Sanity check on the scan itself: if inspect stopped resolving the
        endpoint source, every route would look public and the test above would
        pass for the wrong reason."""
        authed = [p for _, p, auth, _ in write_routes() if auth]
        self.assertGreater(len(authed), 50,
                           "the auth scan found almost nothing - is it working?")
        self.assertIn(("POST", "/events"),
                      {(m, p) for m, p, auth, _ in write_routes() if auth},
                          "/events should be present and authed")


class TestPreviouslyPublicRoutesAreNowGuarded(unittest.TestCase):
    """These were all callable by anyone who knew the URL. Each one is named
    here so that re-opening it is a deliberate, visible edit."""

    ROUTES = [
        ("POST", "/cache/people/refresh"),
        ("POST", "/checkin"),
        ("PUT", "/event-types/{event_type_name}"),
        ("DELETE", "/events/cell/{event_id}/members/{member_id}"),
        ("POST", "/migrate-event-types-uuids"),
        ("POST", "/organizations"),
        ("PUT", "/organizations/{org_id}"),
        ("DELETE", "/organizations/{org_id}"),
        ("POST", "/people/import/preview-columns"),
        ("POST", "/uncapture"),
    ]

    def test_each_route_still_exists(self):
        """A rename would otherwise make the auth assertion below pass silently
        because the path no longer matches anything."""
        actual = {(m, p) for m, p, _, _ in write_routes()}
        for method, path in self.ROUTES:
            self.assertIn((method, path), actual,
                          f"{method} {path} disappeared; update this test")

    def test_each_is_authed(self):
        public = unauthenticated_write_routes()
        for method, path in self.ROUTES:
            self.assertNotIn(
                (method, path), public,
                f"{method} {path} is callable without a token again",
            )
