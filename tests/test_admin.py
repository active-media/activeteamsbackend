import pytest
from fastapi import HTTPException
from datetime import datetime
from bson import ObjectId

# Import functions under test
from services.admin_service import (
    create_user,
    get_all_users,
    update_user_role,
    delete_user,
    update_role_permissions,
    get_role_permissions,
)

# Import models
from auth.models import UserCreater, RoleUpdate, PermissionUpdate


# ---------------------------
# FIXTURES
# ---------------------------

@pytest.fixture
def admin_user():
    return {
        "_id": ObjectId(),
        "role": "admin",
        "email": "admin@test.com"
    }


@pytest.fixture
def normal_user():
    return {
        "_id": ObjectId(),
        "role": "user",
        "email": "user@test.com"
    }


@pytest.fixture
def sample_user_data():
    return UserCreater(
        name="John",
        surname="Doe",
        email="john@test.com",
        password="password123",
        phone_number="0123456789",
        date_of_birth=None,
        address=None,
        gender=None,
        invitedBy=None,
        leader12=None,
        leader144=None,
        leader1728=None,
        stage="Win",
        role="user"
    )


# ---------------------------
# FAKE DATABASE / COLLECTIONS
# ---------------------------

class FakeInsertResult:
    inserted_id = ObjectId()


class FakeUpdateResult:
    modified_count = 1


class FakeDeleteResult:
    deleted_count = 1


class FakeUsersCollection:
    def __init__(self):
        self.users = {}

    async def find_one(self, query):
        if "email" in query:
            return None
        if "_id" in query:
            return self.users.get(str(query["_id"]))
        return None

    async def insert_one(self, doc):
        _id = ObjectId()
        doc["_id"] = _id
        self.users[str(_id)] = doc
        return FakeInsertResult()

    def find(self, query):
        async def generator():
            for user in self.users.values():
                yield user
        return generator()

    async def update_one(self, query, update):
        user = self.users.get(str(query["_id"]))
        if not user:
            return FakeUpdateResult()
        user.update(update["$set"])
        return FakeUpdateResult()

    async def delete_one(self, query):
        self.users.pop(str(query["_id"]), None)
        return FakeDeleteResult()


class FakeActivityLogs:
    async def insert_one(self, doc):
        return True

    def find(self, query):
        async def generator():
            yield {
                "_id": ObjectId(),
                "action": "TEST",
                "details": "Test log",
                "timestamp": datetime.utcnow(),
                "user_id": "123"
            }
        return generator()


class FakeDB:
    activity_logs = FakeActivityLogs()


# ---------------------------
# MONKEYPATCH DATABASE
# ---------------------------

@pytest.fixture(autouse=True)
def patch_db(monkeypatch):
    fake_users = FakeUsersCollection()
    fake_db = FakeDB()

    monkeypatch.setattr("services.admin_service.users_collection", fake_users)
    monkeypatch.setattr("services.admin_service.db", fake_db)

    return fake_users


# ---------------------------
# TESTS
# ---------------------------

@pytest.mark.asyncio
async def test_create_user_admin_only(sample_user_data, normal_user):
    with pytest.raises(HTTPException) as exc:
        await create_user(sample_user_data, normal_user)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_create_user_success(sample_user_data, admin_user):
    response = await create_user(sample_user_data, admin_user)

    assert "created successfully" in response.message


@pytest.mark.asyncio
async def test_get_all_users_admin_only(normal_user):
    with pytest.raises(HTTPException):
        await get_all_users(normal_user)


@pytest.mark.asyncio
async def test_get_all_users_success(admin_user):
    result = await get_all_users(admin_user)

    assert hasattr(result, "users")
    assert isinstance(result.users, list)


@pytest.mark.asyncio
async def test_update_user_role_invalid_role(admin_user):
    role_update = RoleUpdate(role="invalid")

    with pytest.raises(HTTPException) as exc:
        await update_user_role(str(ObjectId()), role_update, admin_user)

    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_update_role_permissions_success(admin_user):
    permission_update = PermissionUpdate(
        permission="manage_users",
        enabled=False
    )

    response = await update_role_permissions(
        "admin",
        permission_update,
        admin_user
    )

    assert "updated" in response.message


@pytest.mark.asyncio
async def test_get_role_permissions_success(admin_user):
    result = await get_role_permissions("admin", admin_user)

    assert result["role"] == "admin"
    assert "permissions" in result


@pytest.mark.asyncio
async def test_delete_user_prevent_self_delete(admin_user):
    with pytest.raises(HTTPException) as exc:
        await delete_user(str(admin_user["_id"]), admin_user)

    assert exc.value.status_code == 400
