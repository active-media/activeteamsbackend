"""Tests for the legacy (Mongo) -> Supabase account migration."""

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import auth_migration
from auth.utils import hash_password

LEGACY_PASSWORD = "legacy-secret-123"
PERSON_UUID = "11111111-1111-4111-8111-111111111111"


class FakeResult:
    def __init__(self, data):
        self.data = data


class FakeTable:
    def __init__(self, rows):
        self.rows = rows
        self.inserted = []
        self.updates = []

    def select(self, *args, **kwargs):
        return self

    def eq(self, column, value):
        self._filter = (column, value)
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        column, value = getattr(self, "_filter", (None, None))
        if column in ("email", "_id"):
            return FakeResult([r for r in self.rows if r.get(column) == value])
        return FakeResult(list(self.rows))

    def insert(self, row):
        self.inserted.append(row)
        return self

    def update(self, values):
        self.updates.append(values)
        return self

    def _run_update(self):
        column, value = getattr(self, "_filter", (None, None))
        for row in self.rows:
            if row.get(column) == value:
                row.update(self.updates[-1])
        return FakeResult(self.rows)

    def execute_insert(self):
        self.rows.extend(self.inserted)
        return FakeResult(self.inserted)


class FakeTableChain(FakeTable):
    def execute(self):
        # Distinguish the update chain from the select chain.
        if self.updates:
            return self._run_update()
        return FakeTable.execute(self)


class FakeAuth:
    def __init__(self, sign_in_result=None, sign_up_result=None, admin=None, fail_sign_in=0):
        self._sign_in_result = sign_in_result
        self._sign_up_result = sign_up_result
        self._fail_sign_in = fail_sign_in
        self.admin = admin

    def sign_in_with_password(self, credentials):
        if self._fail_sign_in > 0:
            self._fail_sign_in -= 1
            raise RuntimeError("Invalid login credentials")
        return self._sign_in_result

    def sign_up(self, credentials):
        return self._sign_up_result


class FakeSupabase:
    def __init__(self, rows=None, auth=None, people=None):
        self.table_data = rows if rows is not None else []
        self.people_data = people if people is not None else []
        self.auth = auth or FakeAuth()
        self.tables = {}

    def table(self, name):
        if name not in self.tables:
            data = self.people_data if name == "People" else self.table_data
            self.tables[name] = FakeTableChain(data)
        return self.tables[name]


class FakeSession:
    def __init__(self):
        self.access_token = "access-token"
        self.refresh_token = "refresh-token"
        self.expires_in = 3600


class FakeUser:
    def __init__(self, email="legacy@example.com", uid="auth-uuid"):
        self.email = email
        self.id = uid


class FakeSignInResult:
    def __init__(self, session=None, user=None, error=None):
        self.session = session
        self.user = user
        self.error = error


def make_legacy_user(**overrides):
    user = {
        "_id": "6aebadaedc8ece3b7ccce6d4",
        "email": "legacy@example.com",
        "name": "Legacy",
        "surname": "User",
        "role": "leader",
        "Organization": "Active Church",
        "org_id": "active-teams",
        "password": hash_password(LEGACY_PASSWORD),
        "person_id": "person-1",
    }
    user.update(overrides)
    return user


@pytest.fixture
def client(monkeypatch):
    legacy = {"value": make_legacy_user()}
    supabase = {
        "value": FakeSupabase(
            people=[{"_id": "person-1", "person_id": PERSON_UUID}]
        )
    }
    admin = {"value": None}

    async def fake_find_one(query):
        email = query.get("email", {})
        wanted = email.get("$regex", "").lower().lstrip("^").rstrip("$").replace("\\", "")
        if legacy["value"] and legacy["value"]["email"].lower() == wanted:
            return legacy["value"]
        return None

    monkeypatch.setattr(auth_migration.users_collection, "find_one", fake_find_one)
    monkeypatch.setattr(auth_migration, "_admin_client", lambda: admin["value"])
    monkeypatch.setattr(auth_migration, "supabase_anon", supabase["value"])

    app = FastAPI()
    app.include_router(auth_migration.router)
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client, legacy, supabase, admin


def test_unknown_email_is_rejected(client):
    test_client, legacy, _, _ = client
    legacy["value"] = None

    response = test_client.post(
        "/auth/migrate-legacy", json={"email": "nobody@example.com", "password": LEGACY_PASSWORD}
    )

    assert response.status_code == 404


def test_wrong_password_is_rejected(client):
    test_client, _, _, _ = client

    response = test_client.post(
        "/auth/migrate-legacy", json={"email": "legacy@example.com", "password": "wrong"}
    )

    assert response.status_code == 401
    assert "Invalid credentials" in response.json()["detail"]


def test_missing_fields_are_rejected(client):
    test_client, _, _, _ = client

    response = test_client.post("/auth/migrate-legacy", json={"email": ""})

    assert response.status_code == 400


def test_migration_returns_a_session_and_creates_the_profile(client):
    test_client, _, supabase, _ = client
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser())
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["access_token"] == "access-token"
    assert body["refresh_token"] == "refresh-token"
    assert body["migrated"] is True
    assert body["profile"]["role"] == "leader"

    created = supabase["value"].table("Users").inserted
    assert len(created) == 1
    assert created[0]["email"] == "legacy@example.com"
    assert created[0]["_id"] == "6aebadaedc8ece3b7ccce6d4"
    assert created[0]["person_id"] == PERSON_UUID


def test_a_person_id_without_a_supabase_match_is_left_out(client):
    test_client, legacy, supabase, _ = client
    legacy["value"]["person_id"] = "unknown-object-id"
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser())
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    created = supabase["value"].table("Users").inserted
    assert len(created) == 1
    assert "person_id" not in created[0]


def test_existing_profile_keeps_its_values_and_fills_the_gaps(client):
    test_client, _, supabase, _ = client
    supabase["value"].table_data = [
        {
            "_id": "supabase-uuid",
            "email": "legacy@example.com",
            "role": "admin",
            "org_id": None,
            "person_id": None,
        }
    ]
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser())
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    table = supabase["value"].table("Users")
    assert table.inserted == []
    assert table.updates, "expected the missing fields to be filled in"
    updates = table.updates[-1]
    assert "role" not in updates, "an existing role must not be overwritten"
    assert updates["org_id"] == "active-teams"
    assert updates["person_id"] == PERSON_UUID


def test_service_role_creates_the_auth_account(client):
    test_client, _, supabase, admin = client

    class FakeAdminAuth:
        def __init__(self):
            self.created = []
            self.updated = []

        def list_users(self, page=1, per_page=200):
            class Response:
                users = []

            return Response()

        def create_user(self, payload):
            self.created.append(payload)
            return FakeUser()

        def update_user_by_id(self, uid, payload):
            self.updated.append((uid, payload))
            return FakeUser()

    class FakeAdmin:
        def __init__(self, supabase):
            self.auth = FakeAdminAuth()
            self.auth.admin = self.auth
            self._supabase = supabase

        def table(self, name):
            return self._supabase.table(name)

    fake_admin = FakeAdmin(supabase["value"])
    admin["value"] = fake_admin
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser()),
        fail_sign_in=1,
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    assert fake_admin.auth.created[0]["email"] == "legacy@example.com"
    assert fake_admin.auth.created[0]["email_confirm"] is True


def test_admin_rest_api_creates_the_auth_account(client, monkeypatch):
    test_client, _, supabase, _ = client
    calls = []

    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_admin_rest(method, path, payload=None):
        calls.append((method, path, payload))
        if method == "GET":
            return FakeResponse(200, {"users": []})
        return FakeResponse(200, {"id": "auth-uuid"})

    monkeypatch.setattr(auth_migration, "_admin_rest", fake_admin_rest)
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser()),
        fail_sign_in=1,
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    creates = [call for call in calls if call[0] == "POST" and call[1] == "admin/users"]
    assert creates, "expected the auth account to be created through the admin API"
    assert creates[0][2]["email"] == "legacy@example.com"
    assert creates[0][2]["password"] == LEGACY_PASSWORD


def test_admin_rest_api_updates_an_existing_auth_account(client, monkeypatch):
    test_client, _, supabase, _ = client
    calls = []

    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_admin_rest(method, path, payload=None):
        calls.append((method, path, payload))
        if method == "GET":
            return FakeResponse(200, {"users": [{"id": "auth-uuid", "email": "legacy@example.com"}]})
        return FakeResponse(200, {"id": "auth-uuid"})

    monkeypatch.setattr(auth_migration, "_admin_rest", fake_admin_rest)
    supabase["value"].auth = FakeAuth(
        sign_in_result=FakeSignInResult(session=FakeSession(), user=FakeUser()),
        fail_sign_in=1,
    )

    response = test_client.post(
        "/auth/migrate-legacy",
        json={"email": "legacy@example.com", "password": LEGACY_PASSWORD},
    )

    assert response.status_code == 200
    updates = [call for call in calls if call[0] == "PUT"]
    assert updates, "expected the existing auth account to be updated"
    assert updates[0][1] == "admin/users/auth-uuid"
    assert updates[0][2]["password"] == LEGACY_PASSWORD


def test_account_exists_endpoint_does_not_leak_information(client):
    test_client, legacy, _, _ = client
    legacy["value"] = None

    response = test_client.post("/auth/legacy-account-exists", json={"email": "nobody@example.com"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}
