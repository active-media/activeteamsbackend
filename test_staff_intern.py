import pytest
from bson import ObjectId
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock, patch

import main
from auth.utils import get_current_user

app = main.app


def _override_user(role="admin", email="admin@example.com", supreme=False):
    def _dep():
        return {"_id": "507f1f77bcf86cd799439011", "role": role, "email": email,
                "is_supreme_admin": supreme}
    return _dep


def _mock_collection(matched=1):
    coll = MagicMock()
    result = MagicMock()
    result.matched_count = matched
    coll.update_one = AsyncMock(return_value=result)
    return coll


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["user", "leader", "registrant"])
async def test_staff_intern_forbidden_for_other_roles(role):
    app.dependency_overrides[get_current_user] = _override_user(role)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.put(
            f"/people/{ObjectId()}/staff-intern",
            json={"staffStatus": "staff"},
        )
    assert res.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["admin", "leaderAt12", "leaderat12", "leader@12", "leader at 12"])
async def test_staff_intern_allowed_for_admin_and_leader12(role):
    app.dependency_overrides[get_current_user] = _override_user(role)
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"staffStatus": "intern", "fridayServiceTarget": 5, "sundayServiceTarget": 3,
                      "fridayActual": 4, "sundayActual": 2},
            )
    assert res.status_code == 200
    assert res.json()["message"] == "Staff/intern data updated successfully"


@pytest.mark.asyncio
async def test_staff_intern_allowed_for_supreme_admin_email():
    app.dependency_overrides[get_current_user] = _override_user(
        "user", email=main.SUPREME_ADMIN_EMAIL, supreme=True)
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"staffStatus": "staff"},
            )
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_staff_intern_invalid_status():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"staffStatus": "manager"},
            )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_negative_target_rejected():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"fridayServiceTarget": -5},
            )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_non_integer_rejected():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"sundayActual": "abc"},
            )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_bool_rejected():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"fridayActual": True},
            )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_no_valid_fields():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection()):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"unrelated": 1},
            )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_invalid_person_id():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.put(
            "/people/not-an-objectid/staff-intern",
            json={"staffStatus": "staff"},
        )
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_staff_intern_person_not_found():
    app.dependency_overrides[get_current_user] = _override_user("admin")
    with patch.object(main, "people_collection", _mock_collection(matched=0)):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            res = await client.put(
                f"/people/{ObjectId()}/staff-intern",
                json={"staffStatus": "staff"},
            )
    assert res.status_code == 404
