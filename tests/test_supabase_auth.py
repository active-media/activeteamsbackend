import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bson import ObjectId
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from jose import jwt

import auth.utils as auth_utils


class FakeSupabaseResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


class FakeSupabaseClient:
    response = None
    requested_url = None
    requested_headers = None

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    async def get(self, url, headers):
        type(self).requested_url = url
        type(self).requested_headers = headers
        return type(self).response


class TestSupabaseAuthentication(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = patch.multiple(
            auth_utils,
            SUPABASE_URL="https://project.example",
            SUPABASE_API_KEY="public-test-key",
            SUPABASE_JWT_ISSUER="https://project.example/auth/v1",
            SUPABASE_JWT_AUDIENCE="authenticated",
        )
        self.settings.start()
        self.addCleanup(self.settings.stop)
        FakeSupabaseClient.response = None
        FakeSupabaseClient.requested_url = None
        FakeSupabaseClient.requested_headers = None

    def make_token(self, **claim_overrides):
        claims = {
            "iss": "https://project.example/auth/v1",
            "aud": "authenticated",
            "sub": "supabase-user-id",
            "exp": (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp(),
        }
        claims.update(claim_overrides)
        return jwt.encode(claims, "test-signing-key", algorithm="HS256")

    async def test_supabase_token_authenticates_and_maps_to_application_user(self):
        token = self.make_token()
        FakeSupabaseClient.response = FakeSupabaseResponse(
            200,
            {
                "id": "supabase-user-id",
                "email": "person@example.com",
                "email_confirmed_at": "2026-01-01T00:00:00Z",
            },
        )
        application_user = {
            "_id": ObjectId("68b53616e9432f87e7b1d808"),
            "email": "person@example.com",
            "name": "Test",
            "role": "user",
            "organization": "Active Teams",
        }
        users_collection = SimpleNamespace(
            find_one=AsyncMock(side_effect=[None, application_user])
        )
        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials=token,
        )

        with (
            patch.object(
                auth_utils,
                "httpx",
                SimpleNamespace(AsyncClient=FakeSupabaseClient),
            ),
            patch.object(auth_utils, "users_collection", users_collection),
        ):
            current_user = await auth_utils.get_current_user(credentials)

        self.assertEqual(
            FakeSupabaseClient.requested_url,
            "https://project.example/auth/v1/user",
        )
        self.assertEqual(
            FakeSupabaseClient.requested_headers["apikey"],
            "public-test-key",
        )
        self.assertEqual(
            FakeSupabaseClient.requested_headers["Authorization"],
            f"Bearer {token}",
        )
        self.assertEqual(current_user["user_id"], "68b53616e9432f87e7b1d808")
        self.assertEqual(current_user["organization"], "Active Teams")

    async def test_invalid_claims_are_rejected_before_supabase_request(self):
        invalid_claims = (
            {"iss": "https://wrong.example/auth/v1"},
            {"aud": "other"},
            {"exp": 0},
        )

        with patch.object(
            auth_utils,
            "httpx",
            SimpleNamespace(AsyncClient=FakeSupabaseClient),
        ):
            for claims in invalid_claims:
                with self.subTest(claims=claims):
                    with self.assertRaises(HTTPException) as raised:
                        await auth_utils._get_supabase_user(
                            self.make_token(**claims)
                        )
                    self.assertEqual(raised.exception.status_code, 401)
                    self.assertIsNone(FakeSupabaseClient.requested_url)

    async def test_supabase_rejection_is_unauthorized(self):
        FakeSupabaseClient.response = FakeSupabaseResponse(401, {"message": "invalid"})
        with patch.object(
            auth_utils,
            "httpx",
            SimpleNamespace(AsyncClient=FakeSupabaseClient),
        ):
            with self.assertRaises(HTTPException) as raised:
                await auth_utils._get_supabase_user(self.make_token())

        self.assertEqual(raised.exception.status_code, 401)

    async def test_unconfirmed_email_cannot_link_a_mongo_account(self):
        token = self.make_token()
        FakeSupabaseClient.response = FakeSupabaseResponse(
            200,
            {
                "id": "supabase-user-id",
                "email": "person@example.com",
                "email_confirmed_at": None,
            },
        )
        users_collection = SimpleNamespace(find_one=AsyncMock(return_value=None))
        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials=token,
        )

        with (
            patch.object(
                auth_utils,
                "httpx",
                SimpleNamespace(AsyncClient=FakeSupabaseClient),
            ),
            patch.object(auth_utils, "users_collection", users_collection),
        ):
            with self.assertRaises(HTTPException) as raised:
                await auth_utils.get_current_user(credentials)

        self.assertEqual(raised.exception.status_code, 403)
        users_collection.find_one.assert_awaited_once_with(
            {"supabase_user_id": "supabase-user-id"}
        )
