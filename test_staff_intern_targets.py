"""Tests for the weekly Staff & Intern service-target endpoints.

Supabase and Mongo are mocked so the tests run without the live database (the
``service_targets`` columns must be added via
``migrations/001_service_targets_staff_intern.sql`` before the real endpoints
work end-to-end).
"""

from datetime import date, datetime, timedelta

import pytest
import pytz
from bson import ObjectId
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock, patch

import main
from auth.utils import get_current_user
from routers import staff_intern as si

app = main.app
SAST = pytz.timezone("Africa/Johannesburg")

PERSON_ID = "507f1f77bcf86cd799439011"


def _override_user(role="admin", email="admin@example.com", supreme=False):
    def _dep():
        return {
            "_id": PERSON_ID,
            "role": role,
            "email": email,
            "is_supreme_admin": supreme,
            "organization": "Active Church",
        }
    return _dep


def _row(**overrides):
    base = {
        "id": 1,
        "person_id": PERSON_ID,
        "service": "friday",
        "week_identifier": "2026-W41",
        "target_count": 5,
        "actual": 3,
        "met": False,
        "closed": False,
        "closed_at": None,
        "closed_by": None,
        "leader_name": "Jane Doe",
        "email": "jane@example.com",
        "organization": "Active Church",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def test_current_week_identifier_format():
    assert si.current_week_identifier().count("-W") == 1


def test_parse_week_identifier_returns_monday():
    monday = si.parse_week_identifier("2026-W41")
    assert isinstance(monday, date)
    assert monday.weekday() == 0


def test_service_date_friday_and_sunday():
    friday = si.service_date("2026-W41", "friday")
    sunday = si.service_date("2026-W41", "sunday")
    assert friday.weekday() == 4
    assert sunday.weekday() == 6
    assert (sunday - friday).days == 2


def test_service_has_passed():
    # A week far in the past must be closed; a week far in the future must not.
    assert si.service_has_passed("2000-W01", "friday") is True
    assert si.service_has_passed("2999-W01", "friday") is False


def test_is_authorized_roles():
    assert si.is_authorized({"role": "admin"}) is True
    assert si.is_authorized({"role": "leaderAt12"}) is True
    assert si.is_authorized({"role": "leader12"}) is True
    assert si.is_authorized({"role": "leader@12"}) is True
    assert si.is_authorized({"role": "user", "email": main.SUPREME_ADMIN_EMAIL, "is_supreme_admin": True}) is True
    assert si.is_authorized({"role": "leader"}) is False
    assert si.is_authorized({"role": "user"}) is False


# --------------------------------------------------------------------------- #
# GET /staff-intern/targets
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_list_targets_forbidden_for_other_roles():
    app.dependency_overrides[get_current_user] = _override_user("leader")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.get("/staff-intern/targets")
    assert res.status_code == 403


@pytest.mark.asyncio
async def test_list_targets_returns_week_services_and_targets():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch", MagicMock(return_value=[_row()])), \
         patch.object(si, "_service_info_with_events", AsyncMock(return_value={
             "friday": {"label": "Friday Service", "date": "2026-10-09", "passed": False, "event": None},
             "sunday": {"label": "Sunday Service", "date": "2026-10-11", "passed": False, "event": None},
         })):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get("/staff-intern/targets?week=2026-W41")
    assert res.status_code == 200
    body = res.json()
    assert body["week_identifier"] == "2026-W41"
    assert set(body["services"].keys()) == {"friday", "sunday"}
    assert body["targets"][0]["person_id"] == PERSON_ID
    assert body["targets"][0]["target"] == 5
    assert body["targets"][0]["actual"] == 3


@pytest.mark.asyncio
async def test_list_targets_auto_closes_passed_service():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    closed_patch = {}

    def _fake_update(row_id, patch):
        closed_patch.update(patch)
        return {**_row(), **patch}

    with patch.object(si, "_fetch", MagicMock(return_value=[_row(actual=6, week_identifier="2000-W01")])), \
         patch.object(si, "_update", MagicMock(side_effect=_fake_update)), \
         patch.object(si, "_service_info_with_events", AsyncMock(return_value={})):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get("/staff-intern/targets?week=2000-W01")
    assert res.status_code == 200
    assert closed_patch.get("closed") is True
    assert closed_patch.get("met") is True  # actual 6 >= target 5
    assert res.json()["targets"][0]["closed"] is True


# --------------------------------------------------------------------------- #
# PUT /staff-intern/targets/{person_id}
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_upsert_target_forbidden():
    app.dependency_overrides[get_current_user] = _override_user("user")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.put(f"/staff-intern/targets/{PERSON_ID}", json={"service": "friday", "target": 5})
    assert res.status_code == 403


@pytest.mark.asyncio
async def test_upsert_target_creates_and_computes_met():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    inserted = {}

    def _fake_insert(payload):
        inserted.update(payload)
        return {**_row(), **payload, "id": 99}

    with patch.object(si, "_fetch_one", MagicMock(return_value=None)), \
         patch.object(si, "_person_identity", AsyncMock(return_value={
             "leader_name": "Jane Doe", "email": "jane@example.com", "organization": "Active Church"})), \
         patch.object(si, "_insert", MagicMock(side_effect=_fake_insert)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/staff-intern/targets/{PERSON_ID}",
                json={"service": "friday", "target": 4, "actual": 4, "week_identifier": "2026-W41"},
            )
    assert res.status_code == 200
    assert inserted["service"] == "friday"
    assert inserted["target_count"] == 4
    assert inserted["met"] is True
    assert res.json()["target"]["met"] is True


@pytest.mark.asyncio
async def test_upsert_target_updates_existing():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    updated = {}

    def _fake_update(row_id, patch):
        updated.update(patch)
        return {**_row(), **patch}

    with patch.object(si, "_fetch_one", MagicMock(return_value=_row(actual=1))), \
         patch.object(si, "_update", MagicMock(side_effect=_fake_update)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/staff-intern/targets/{PERSON_ID}",
                json={"service": "friday", "target": 3, "actual": 1, "week_identifier": "2026-W41"},
            )
    assert res.status_code == 200
    assert updated["target_count"] == 3
    assert updated["met"] is False


@pytest.mark.asyncio
async def test_upsert_target_rejects_closed():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch_one", MagicMock(return_value=_row(closed=True))):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/staff-intern/targets/{PERSON_ID}",
                json={"service": "friday", "target": 3, "actual": 1, "week_identifier": "2026-W41"},
            )
    assert res.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {"service": "monday", "target": 1},
    {"service": "friday", "target": -1},
    {"service": "friday", "target": True},
    {"service": "friday", "target": "x"},
    {"service": "friday", "target": 1, "week_identifier": "nope"},
])
async def test_upsert_target_validation(payload):
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch_one", MagicMock(return_value=None)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(f"/staff-intern/targets/{PERSON_ID}", json=payload)
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_upsert_target_invalid_person_id():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch_one", MagicMock(return_value=None)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put("/staff-intern/targets/not-an-id", json={"service": "friday", "target": 1})
    assert res.status_code == 400


# --------------------------------------------------------------------------- #
# close / reopen
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_close_target_sets_met():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    updated = {}

    def _fake_update(row_id, patch):
        updated.update(patch)
        return {**_row(actual=6), **patch}

    with patch.object(si, "_fetch_one", MagicMock(return_value=_row(actual=6))), \
         patch.object(si, "_update", MagicMock(side_effect=_fake_update)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post(
                f"/staff-intern/targets/{PERSON_ID}/close",
                json={"service": "friday", "week_identifier": "2026-W41"},
            )
    assert res.status_code == 200
    assert updated["closed"] is True
    assert updated["met"] is True


@pytest.mark.asyncio
async def test_close_target_not_found():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch_one", MagicMock(return_value=None)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post(
                f"/staff-intern/targets/{PERSON_ID}/close",
                json={"service": "friday", "week_identifier": "2026-W41"},
            )
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_reopen_target_clears_closed():
    app.dependency_overrides[get_current_user] = _override_user("leaderAt12")
    updated = {}

    def _fake_update(row_id, patch):
        updated.update(patch)
        return {**_row(closed=True), **patch}

    with patch.object(si, "_fetch_one", MagicMock(return_value=_row(closed=True))), \
         patch.object(si, "_update", MagicMock(side_effect=_fake_update)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.post(
                f"/staff-intern/targets/{PERSON_ID}/reopen",
                json={"service": "friday", "week_identifier": "2026-W41"},
            )
    assert res.status_code == 200
    assert updated["closed"] is False
    assert updated["closed_at"] is None


# --------------------------------------------------------------------------- #
# GET /staff-intern/targets?start_date=&end_date= (date-range history)
# --------------------------------------------------------------------------- #
def test_week_identifier_for_date():
    assert si.week_identifier_for_date(date(2026, 10, 9)) == "2026-W41"


def test_service_dates_in_range_lists_every_friday_and_sunday():
    occurrences = si.service_dates_in_range(date(2026, 10, 3), date(2026, 10, 25))
    assert len(occurrences) == 7
    assert all(day.weekday() in (4, 6) for _, day in occurrences)
    assert occurrences[0][1] == date(2026, 10, 4)
    assert occurrences[-1][1] == date(2026, 10, 25)


@pytest.mark.asyncio
@pytest.mark.parametrize("query", [
    "start_date=2026-10-01",
    "end_date=2026-10-31",
    "start_date=nope&end_date=2026-10-31",
    "start_date=2026-11-01&end_date=2026-10-01",
    "start_date=2026-10-01&end_date=2026-10-31&service=monday",
])
async def test_list_targets_range_validation(query):
    app.dependency_overrides[get_current_user] = _override_user("admin")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.get(f"/staff-intern/targets?{query}")
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_list_targets_range_returns_history_with_dates():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    monday = si.parse_week_identifier("2026-W41")
    friday = (monday + timedelta(days=4)).isoformat()
    sunday = (monday + timedelta(days=6)).isoformat()
    rows = [
        _row(service="friday", week_identifier="2026-W41"),
        _row(id=2, service="sunday", week_identifier="2026-W41"),
    ]
    with patch.object(si, "_fetch_range", MagicMock(return_value=rows)), \
         patch.object(si, "_auto_close", MagicMock()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get(
                "/staff-intern/targets?start_date=2026-10-05&end_date=2026-10-11"
            )
    assert res.status_code == 200
    body = res.json()
    assert body["start_date"] == "2026-10-05"
    assert body["end_date"] == "2026-10-11"
    assert len(body["targets"]) == 2
    by_service = {t["service"]: t for t in body["targets"]}
    assert by_service["friday"]["date"] == friday
    assert by_service["sunday"]["date"] == sunday
    assert body["service_count"] == 2
    assert {s["service"] for s in body["services"]} == {"friday", "sunday"}
    assert all(s["has_targets"] for s in body["services"])


@pytest.mark.asyncio
async def test_list_targets_range_filters_by_service_date():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    rows = [
        _row(service="friday", week_identifier="2026-W41"),
        _row(id=2, service="sunday", week_identifier="2026-W41"),
    ]
    monday = si.parse_week_identifier("2026-W41")
    friday = (monday + timedelta(days=4)).isoformat()
    with patch.object(si, "_fetch_range", MagicMock(return_value=rows)), \
         patch.object(si, "_auto_close", MagicMock()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get(
                f"/staff-intern/targets?start_date={friday}&end_date={friday}"
            )
    assert res.status_code == 200
    body = res.json()
    assert [t["service"] for t in body["targets"]] == ["friday"]
    assert [s["service"] for s in body["services"]] == ["friday"]


@pytest.mark.asyncio
async def test_list_targets_range_filters_by_service_for_leader12():
    app.dependency_overrides[get_current_user] = _override_user("leader12")
    rows = [
        _row(service="friday", week_identifier="2026-W41"),
        _row(id=2, service="sunday", week_identifier="2026-W41"),
    ]
    with patch.object(si, "_fetch_range", MagicMock(return_value=rows)), \
         patch.object(si, "_auto_close", MagicMock()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get(
                "/staff-intern/targets?start_date=2026-10-05&end_date=2026-10-11&service=sunday"
            )
    assert res.status_code == 200
    assert [target["service"] for target in res.json()["targets"]] == ["sunday"]


@pytest.mark.asyncio
async def test_list_targets_range_empty_returns_message():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(si, "_fetch_range", MagicMock(return_value=[])), \
         patch.object(si, "_auto_close", MagicMock()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.get(
                "/staff-intern/targets?start_date=2026-10-03&end_date=2026-10-25"
            )
    assert res.status_code == 200
    body = res.json()
    assert body["targets"] == []
    assert body["count"] == 0
    assert body["service_count"] == 7
    assert all(not s["has_targets"] for s in body["services"])
    assert {s["date"] for s in body["services"]} == {
        "2026-10-04", "2026-10-09", "2026-10-11", "2026-10-16",
        "2026-10-18", "2026-10-23", "2026-10-25",
    }
    assert "No targets were recorded between 2026-10-03 and 2026-10-25" in body["message"]
