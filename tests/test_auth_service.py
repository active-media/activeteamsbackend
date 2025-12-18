# import pytest
# from unittest.mock import AsyncMock
# from bson import ObjectId
# from fastapi import HTTPException

# from services.auth_service import (
#     signup,
#     login,
#     forgot_password,
#     reset_password,
#     refresh_token,
#     logout,
#     users_collection,
#     people_collection,
# )
# from auth.models import (
#     UserCreate,
#     UserLogin,
#     ForgotPasswordRequest,
#     ResetPasswordRequest,
#     RefreshTokenRequest
# )
# from auth.utils import create_access_token, decode_access_token

# # ---------------------------
# # Test Signup
# # ---------------------------
# @pytest.mark.asyncio
# async def test_signup_success(mocker):
#     # Mock DB calls
#     mocker.patch("services.auth_service.db.Users.find_one", new_callable=AsyncMock, return_value=None)
#     mocker.patch("services.auth_service.db.Users.insert_one", new_callable=AsyncMock)

#     # Mock people_cache
#     mocker.patch("services.auth_service.people_cache", {"data": []})

#     # Mock password hashing
#     mocker.patch("services.auth_service.hash_password", return_value="$hashed")

#     user = UserCreate(
#         name="John",
#         surname="Doe",
#         email="john@example.com",
#         password="password123",
#         confirm_password="password123",
#         date_of_birth="1990-01-01",
#         phone_number="1234567890",
#         home_address="123 Street",
#         gender="male",
#         invited_by="Jane Smith"
#     )

#     response = await signup(user)
#     assert response["email"] == "john@example.com"

# @pytest.mark.asyncio
# async def test_signup_existing_email(mocker):
#     mocker.patch(
#         "services.auth_service.db.Users.find_one",
#         new_callable=AsyncMock,
#         return_value={"email": "john@example.com"}
#     )

#     user = UserCreate(
#         name="John",
#         surname="Doe",
#         email="john@example.com",
#         password="password123",
#         confirm_password="password123",
#         date_of_birth="1990-01-01",
#         phone_number="1234567890",
#         home_address="123 Street",
#         gender="male",
#         invited_by="Jane Smith"
#     )

#     with pytest.raises(HTTPException) as exc:
#         await signup(user)
#     assert exc.value.status_code == 400
#     assert "Email already registered" in exc.value.detail

# # ---------------------------
# # Test Login
# # ---------------------------
# @pytest.mark.asyncio
# async def test_login_success(mocker):
#     mock_user_doc = {"_id": ObjectId(), "email": "john@example.com", "password": "$hashed"}

#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "services.auth_service.people_collection.find_one",
#         new_callable=AsyncMock,
#         return_value={}
#     )
#     mocker.patch("services.auth_service.verify_password", return_value=True)
#     mocker.patch("auth.utils.create_access_token", return_value="access-token")
#     mocker.patch("services.auth_service.users_collection.update_one", new_callable=AsyncMock)

#     user = UserLogin(email="john@example.com", password="password123")
#     response = await login(user)
#     assert response["access_token"] == "access-token"

# @pytest.mark.asyncio
# async def test_login_invalid_credentials(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=None
#     )

#     user = UserLogin(email="john@example.com", password="wrongpassword")
#     with pytest.raises(HTTPException):
#         await login(user)

# # ---------------------------
# # Test Forgot Password
# # ---------------------------
# @pytest.mark.asyncio
# async def test_forgot_password_existing_user(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value={"_id": ObjectId(), "email": "john@example.com"}
#     )
#     mocker.patch("auth.utils.create_access_token", return_value="reset-token")

#     background_tasks = mocker.Mock()
#     payload = ForgotPasswordRequest(email="john@example.com")
#     response = await forgot_password(payload, background_tasks)
#     assert response["message"] == "If your email exists, a reset link has been sent."

# @pytest.mark.asyncio
# async def test_forgot_password_unknown_user(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=None
#     )
#     background_tasks = mocker.Mock()
#     payload = ForgotPasswordRequest(email="unknown@example.com")
#     response = await forgot_password(payload, background_tasks)
#     assert response["message"] == "If your email exists, a reset link has been sent."

# # ---------------------------
# # Test Reset Password
# # ---------------------------
# @pytest.mark.asyncio
# async def test_reset_password_success(mocker):
#     mock_user_doc = {"_id": ObjectId(), "email": "john@example.com", "password": "$hashed"}

#     mocker.patch(
#         "auth.utils.decode_access_token",
#         return_value={"user_id": str(mock_user_doc["_id"])}
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "services.auth_service.hash_password",
#         return_value="$hashed"
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock
#     )

#     payload = ResetPasswordRequest(token="fake-token", new_password="newpassword")
#     response = await reset_password(payload)
#     assert response is None

# # ---------------------------
# # Test Refresh Token
# # ---------------------------
# @pytest.mark.asyncio
# async def test_refresh_token_success(mocker):
#     mock_user_doc = {"_id": ObjectId(), "email": "john@example.com", "password": "$hashed"}

#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "auth.utils.create_access_token",
#         return_value="new-access-token"
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock
#     )

#     payload = RefreshTokenRequest(refresh_token="abc")
#     response = await refresh_token(payload)
#     assert response["access_token"] == "new-access-token"

# # ---------------------------
# # Test Logout
# # ---------------------------
# @pytest.mark.asyncio
# async def test_logout(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock
#     )

#     response = await logout(str(ObjectId()))
#     assert response["message"] == "Logged out successfully"

# import pytest
# from unittest.mock import AsyncMock, MagicMock
# from bson import ObjectId
# from fastapi import HTTPException
# from datetime import datetime, timedelta

# from services.auth_service import (
#     signup,
#     login,
#     forgot_password,
#     reset_password,
#     refresh_token,
#     logout,
# )
# from auth.models import (
#     UserCreate,
#     UserLogin,
#     ForgotPasswordRequest,
#     ResetPasswordRequest,
#     RefreshTokenRequest
# )

# # ---------------------------
# # Test Signup
# # ---------------------------
# @pytest.mark.asyncio
# async def test_signup_success(mocker):
#     # Mock DB calls with proper async mocks
#     mock_find_one = mocker.patch(
#         "services.auth_service.db.__getitem__",
#         return_value=MagicMock(
#             find_one=AsyncMock(return_value=None),
#             insert_one=AsyncMock(return_value=MagicMock(inserted_id=ObjectId()))
#         )
#     )
    
#     # Mock people_collection.insert_one
#     mocker.patch(
#         "services.auth_service.people_collection.insert_one",
#         new_callable=AsyncMock,
#         return_value=MagicMock(inserted_id=ObjectId())
#     )

#     # Mock get_people_cache - return a callable that returns the mock data
#     mock_cache = {"data": []}
#     mocker.patch(
#         "services.auth_service.get_people_cache",
#         mock_cache
#     )
    
#     # Mock people_cache for appending
#     mocker.patch("services.auth_service.people_cache", mock_cache)

#     # Mock password hashing
#     mocker.patch("services.auth_service.hash_password", return_value="$hashed")

#     user = UserCreate(
#         name="John",
#         surname="Doe",
#         email="jo124@example.com",
#         password="password123",
#         confirm_password="password123",
#         date_of_birth="1990-01-01",
#         phone_number="1234567890",
#         home_address="123 Street",
#         gender="male",
#         invited_by="Jane Smith"
#     )

#     response = await signup(user)
#     assert response["message"] == "User created successfully"


# @pytest.mark.asyncio
# async def test_signup_existing_email(mocker):
#     # Use proper collection mock structure
#     mock_collection = MagicMock()
#     mock_collection.find_one = AsyncMock(return_value={"email": "jo124@example.com"})
    
#     mocker.patch(
#         "services.auth_service.db.__getitem__",
#         return_value=mock_collection
#     )

#     user = UserCreate(
#         name="John",
#         surname="Doe",
#         email="jo124@example.com",
#         password="password123",
#         confirm_password="password123",
#         date_of_birth="1990-01-01",
#         phone_number="1234567890",
#         home_address="123 Street",
#         gender="male",
#         invited_by="Jane Smith"
#     )

#     with pytest.raises(HTTPException) as exc:
#         await signup(user)
#     assert exc.value.status_code == 400
#     assert "Email already registered" in exc.value.detail

# # ---------------------------
# # Test Login
# # ---------------------------
# @pytest.mark.asyncio
# async def test_login_success(mocker):
#     # Mock user with all required fields
#     mock_user_doc = {
#         "_id": ObjectId(),
#         "email": "jo124@example.com",
#         "password": "$hashed",
#         "name": "John",
#         "surname": "Doe",
#         "role": "user",
#         "date_of_birth": "1990-01-01",
#         "home_address": "123 Street",
#         "phone_number": "1234567890",
#         "gender": "male",
#         "invited_by": "Jane Smith"
#     }

#     # Mock person document
#     mock_person_doc = {
#         "Name": "John",
#         "Surname": "Doe",
#         "Email": "jo124@example.com",
#         "Leader @1": "Leader One",
#         "Leader @12": "Leader Twelve",
#         "Leader @144": "Leader 144"
#     }

#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "services.auth_service.people_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_person_doc
#     )
#     mocker.patch(
#         "services.auth_service.events_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=None
#     )
#     mocker.patch("services.auth_service.verify_password", return_value=True)
#     mocker.patch("services.auth_service.create_access_token", return_value="access-token")
#     mocker.patch("services.auth_service.users_collection.update_one", new_callable=AsyncMock)
#     mocker.patch("services.auth_service.hash_password", return_value="$hashed_refresh")

#     user = UserLogin(email="jo124@example.com", password="password123")
#     response = await login(user)
#     assert response["access_token"] == "access-token"
#     assert "user" in response
#     assert response["user"]["email"] == "jo124@example.com"


# @pytest.mark.asyncio
# async def test_login_invalid_credentials(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=None
#     )

#     user = UserLogin(email="jo124@example.com", password="wrongpassword")
#     with pytest.raises(HTTPException) as exc:
#         await login(user)
#     assert exc.value.status_code == 401

# # ---------------------------
# # Test Forgot Password
# # ---------------------------
# @pytest.mark.asyncio
# async def test_forgot_password_existing_user(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value={"_id": ObjectId(), "email": "jo124@example.com", "name": "John"}
#     )
#     mocker.patch("services.auth_service.create_access_token", return_value="reset-token")

#     background_tasks = MagicMock()
#     payload = ForgotPasswordRequest(email="jo124@example.com")
#     response = await forgot_password(payload, background_tasks)
#     assert response["message"] == "If your email exists, a reset link has been sent."


# @pytest.mark.asyncio
# async def test_forgot_password_unknown_user(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=None
#     )
#     background_tasks = MagicMock()
#     payload = ForgotPasswordRequest(email="unknown@example.com")
#     response = await forgot_password(payload, background_tasks)
#     assert response["message"] == "If your email exists, a reset link has been sent."

# # ---------------------------
# # Test Reset Password
# # ---------------------------
# @pytest.mark.asyncio
# async def test_reset_password_success(mocker):
#     user_id = ObjectId()
#     mock_user_doc = {
#         "_id": user_id,
#         "email": "jo124@example.com",
#         "password": "$hashed",
#         "role": "user"
#     }

#     # Patch decode_access_token in auth.utils where it's actually defined
#     mocker.patch(
#         "auth.utils.decode_access_token",
#         return_value={"user_id": str(user_id)}
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "services.auth_service.hash_password",
#         return_value="$hashed_new"
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock,
#         return_value=MagicMock(modified_count=1)
#     )
#     mocker.patch(
#         "services.auth_service.create_access_token",
#         return_value="new-access-token"
#     )

#     payload = ResetPasswordRequest(token="fake-token", new_password="newpassword")
#     response = await reset_password(payload)
#     assert response["message"] == "Password has been reset successfully."
#     assert response["access_token"] == "new-access-token"

# # ---------------------------
# # Test Refresh Token
# # ---------------------------
# @pytest.mark.asyncio
# async def test_refresh_token_success(mocker):
#     refresh_token_id = str(ObjectId())
#     mock_user_doc = {
#         "_id": ObjectId(),
#         "email": "jo124@example.com",
#         "password": "$hashed",
#         "role": "user",
#         "refresh_token_id": refresh_token_id,
#         "refresh_token_hash": "$hashed_refresh",
#         "refresh_token_expires": datetime.utcnow() + timedelta(days=30)
#     }

#     mocker.patch(
#         "services.auth_service.users_collection.find_one",
#         new_callable=AsyncMock,
#         return_value=mock_user_doc
#     )
#     mocker.patch(
#         "services.auth_service.verify_password",
#         return_value=True
#     )
#     mocker.patch(
#         "services.auth_service.create_access_token",
#         return_value="new-access-token"
#     )
#     mocker.patch(
#         "services.auth_service.hash_password",
#         return_value="$new_hashed_refresh"
#     )
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock
#     )

#     payload = RefreshTokenRequest(
#         refresh_token="abc",
#         refresh_token_id=refresh_token_id
#     )
#     response = await refresh_token(payload)
#     assert response["access_token"] == "new-access-token"

# # ---------------------------
# # Test Logout
# # ---------------------------
# @pytest.mark.asyncio
# async def test_logout(mocker):
#     mocker.patch(
#         "services.auth_service.users_collection.update_one",
#         new_callable=AsyncMock
#     )

#     response = await logout(str(ObjectId()))
#     assert response["message"] == "Logged out successfully"

import pytest
from unittest.mock import AsyncMock, MagicMock
from bson import ObjectId
from fastapi import HTTPException
from datetime import datetime, timedelta

from services.auth_service import (
    signup,
    login,
    forgot_password,
    reset_password,
    refresh_token,
    logout,
)
from auth.models import (
    UserCreate,
    UserLogin,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    RefreshTokenRequest
)

# ---------------------------
# Test Signup
# ---------------------------
@pytest.mark.asyncio
async def test_signup_success(mocker):
    # Mock DB calls with proper async mocks
    mock_find_one = mocker.patch(
        "services.auth_service.db.__getitem__",
        return_value=MagicMock(
            find_one=AsyncMock(return_value=None),
            insert_one=AsyncMock(return_value=MagicMock(inserted_id=ObjectId()))
        )
    )
    
    # Mock people_collection.insert_one
    mocker.patch(
        "services.auth_service.people_collection.insert_one",
        new_callable=AsyncMock,
        return_value=MagicMock(inserted_id=ObjectId())
    )

    # Mock get_people_cache - it's accessed as get_people_cache["data"] in the code
    mock_cache = {"data": []}
    mocker.patch(
        "services.auth_service.get_people_cache",
        mock_cache
    )
    
    # Mock people_cache from people_service since that's where it's defined
    mocker.patch("services.people_service.people_cache", mock_cache)

    # Mock password hashing
    mocker.patch("services.auth_service.hash_password", return_value="$hashed")

    user = UserCreate(
        name="John",
        surname="Doe",
        email="john126@example.com",
        password="password123",
        confirm_password="password123",
        date_of_birth="1990-01-01",
        phone_number="1234567890",
        home_address="123 Street",
        gender="male",
        invited_by="Jane Smith"
    )

    response = await signup(user)
    assert response["message"] == "User created successfully"


@pytest.mark.asyncio
async def test_signup_existing_email(mocker):
    # Mock the entire db dictionary access pattern
    mock_users_collection = MagicMock()
    mock_users_collection.find_one = AsyncMock(return_value={"email": "john126@example.com"})
    
    # Create a mock db that returns our collection
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_users_collection)
    
    # Patch the db object itself
    mocker.patch("services.auth_service.db", mock_db)

    user = UserCreate(
        name="John",
        surname="Doe",
        email="john126@example.com",
        password="password123",
        confirm_password="password123",
        date_of_birth="1990-01-01",
        phone_number="1234567890",
        home_address="123 Street",
        gender="male",
        invited_by="Jane Smith"
    )

    with pytest.raises(HTTPException) as exc:
        await signup(user)
    assert exc.value.status_code == 400
    assert "Email already registered" in exc.value.detail

# ---------------------------
# Test Login
# ---------------------------
@pytest.mark.asyncio
async def test_login_success(mocker):
    # Mock user with all required fields
    mock_user_doc = {
        "_id": ObjectId(),
        "email": "john126@example.com",
        "password": "$hashed",
        "name": "John",
        "surname": "Doe",
        "role": "user",
        "date_of_birth": "1990-01-01",
        "home_address": "123 Street",
        "phone_number": "1234567890",
        "gender": "male",
        "invited_by": "Jane Smith"
    }

    # Mock person document
    mock_person_doc = {
        "Name": "John",
        "Surname": "Doe",
        "Email": "john126@example.com",
        "Leader @1": "Leader One",
        "Leader @12": "Leader Twelve",
        "Leader @144": "Leader 144"
    }

    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value=mock_user_doc
    )
    mocker.patch(
        "services.auth_service.people_collection.find_one",
        new_callable=AsyncMock,
        return_value=mock_person_doc
    )
    mocker.patch(
        "services.auth_service.events_collection.find_one",
        new_callable=AsyncMock,
        return_value=None
    )
    mocker.patch("services.auth_service.verify_password", return_value=True)
    mocker.patch("services.auth_service.create_access_token", return_value="access-token")
    mocker.patch("services.auth_service.users_collection.update_one", new_callable=AsyncMock)
    mocker.patch("services.auth_service.hash_password", return_value="$hashed_refresh")

    user = UserLogin(email="john126@example.com", password="password123")
    response = await login(user)
    assert response["access_token"] == "access-token"
    assert "user" in response
    assert response["user"]["email"] == "john126@example.com"


@pytest.mark.asyncio
async def test_login_invalid_credentials(mocker):
    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value=None
    )

    user = UserLogin(email="john126@example.com", password="wrongpassword")
    with pytest.raises(HTTPException) as exc:
        await login(user)
    assert exc.value.status_code == 401

# ---------------------------
# Test Forgot Password
# ---------------------------
@pytest.mark.asyncio
async def test_forgot_password_existing_user(mocker):
    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value={"_id": ObjectId(), "email": "john126@example.com", "name": "John"}
    )
    mocker.patch("services.auth_service.create_access_token", return_value="reset-token")

    background_tasks = MagicMock()
    payload = ForgotPasswordRequest(email="john126@example.com")
    response = await forgot_password(payload, background_tasks)
    assert response["message"] == "If your email exists, a reset link has been sent."


@pytest.mark.asyncio
async def test_forgot_password_unknown_user(mocker):
    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value=None
    )
    background_tasks = MagicMock()
    payload = ForgotPasswordRequest(email="unknown@example.com")
    response = await forgot_password(payload, background_tasks)
    assert response["message"] == "If your email exists, a reset link has been sent."

# ---------------------------
# Test Reset Password
# ---------------------------
@pytest.mark.asyncio
async def test_reset_password_success(mocker):
    user_id = ObjectId()
    mock_user_doc = {
        "_id": user_id,
        "email": "john126@example.com",
        "password": "$hashed",
        "role": "user"
    }

    # Patch decode_access_token where it's USED in auth_service, not where it's defined
    mocker.patch(
        "services.auth_service.decode_access_token",
        return_value={"user_id": str(user_id)}
    )
    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value=mock_user_doc
    )
    mocker.patch(
        "services.auth_service.hash_password",
        return_value="$hashed_new"
    )
    mocker.patch(
        "services.auth_service.users_collection.update_one",
        new_callable=AsyncMock,
        return_value=MagicMock(modified_count=1)
    )
    mocker.patch(
        "services.auth_service.create_access_token",
        return_value="new-access-token"
    )

    payload = ResetPasswordRequest(token="fake-token", new_password="newpassword")
    response = await reset_password(payload)
    assert response["message"] == "Password has been reset successfully."
    assert response["access_token"] == "new-access-token"

# ---------------------------
# Test Refresh Token
# ---------------------------
@pytest.mark.asyncio
async def test_refresh_token_success(mocker):
    refresh_token_id = str(ObjectId())
    mock_user_doc = {
        "_id": ObjectId(),
        "email": "john126@example.com",
        "password": "$hashed",
        "role": "user",
        "refresh_token_id": refresh_token_id,
        "refresh_token_hash": "$hashed_refresh",
        "refresh_token_expires": datetime.utcnow() + timedelta(days=30)
    }

    mocker.patch(
        "services.auth_service.users_collection.find_one",
        new_callable=AsyncMock,
        return_value=mock_user_doc
    )
    mocker.patch(
        "services.auth_service.verify_password",
        return_value=True
    )
    mocker.patch(
        "services.auth_service.create_access_token",
        return_value="new-access-token"
    )
    mocker.patch(
        "services.auth_service.hash_password",
        return_value="$new_hashed_refresh"
    )
    mocker.patch(
        "services.auth_service.users_collection.update_one",
        new_callable=AsyncMock
    )

    payload = RefreshTokenRequest(
        refresh_token="abc",
        refresh_token_id=refresh_token_id
    )
    response = await refresh_token(payload)
    assert response["access_token"] == "new-access-token"

# ---------------------------
# Test Logout
# ---------------------------
@pytest.mark.asyncio
async def test_logout(mocker):
    mocker.patch(
        "services.auth_service.users_collection.update_one",
        new_callable=AsyncMock
    )

    response = await logout(str(ObjectId()))
    assert response["message"] == "Logged out successfully"