"""Legacy (Mongo) account migration into Supabase.

Users who signed up before the Supabase migration still have their account in
the Mongo `users` collection: a profile plus a bcrypt password hash. Supabase
keeps credentials in `auth.users` and the profile in `public.Users`, so those
people cannot sign in until their account is carried across.

This module bridges the two:

* `POST /auth/migrate-legacy` verifies the old Mongo credentials and, if they
  are correct, makes sure a Supabase auth account and a `public.Users` profile
  row exist for that email. It hands back a real Supabase session, so the user
  keeps the password they already know and never has to register again.
* `POST /auth/legacy-account-exists` only makes sure an auth account exists,
  without checking a password. The forgot-password flow uses it so a user who
  never migrated can still receive a reset email.

Credentials live in `auth.users`, which is normally managed with a service role
key. The backend's server key may also use the Supabase admin API, in which
case an account that already exists is updated with the legacy password. When
neither works, the account is signed up through the anon client, which only
succeeds as long as the project does not require email confirmation.
"""

import os
import re
import secrets
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Body, HTTPException
from supabase import create_client

from auth.utils import verify_password
from database import users_collection
from supabase_helpers.supabase_connection import supabase as supabase_anon

router = APIRouter(tags=["auth-migration"])

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Fields copied from the Mongo user document into the Supabase profile row.
PROFILE_FIELDS = (
    "name",
    "surname",
    "date_of_birth",
    "home_address",
    "invited_by",
    "phone_number",
    "gender",
    "role",
    "Organization",
    "org_id",
    "is_supreme_admin",
    "profile_picture",
)

# Mongo stores people as ObjectId strings, Supabase stores them as UUIDs, so
# these fields have to be translated through the `People` table.
ID_MAPPED_FIELDS = (
    "person_id",
    "LeaderId",
    "leader12",
    "leader144",
    "leader1728",
    "LeaderPath[0]",
    "LeaderPath[1]",
    "LeaderPath[2]",
    "LeaderPath[3]",
)

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

EMPTY_VALUES = (None, "", [], {})


def _admin_client():
    """Service role client, or None when the key is not configured."""
    if not (SUPABASE_URL and SUPABASE_SERVICE_KEY):
        return None
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def _is_blank(value: Any) -> bool:
    return value in EMPTY_VALUES


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


async def _find_legacy_user(email: str) -> Optional[Dict[str, Any]]:
    """Mongo user for this email, matched case-insensitively."""
    if not email:
        return None
    import re

    return await users_collection.find_one({"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}})


def _legacy_profile(legacy: Dict[str, Any], email: str) -> Dict[str, Any]:
    row: Dict[str, Any] = {"_id": str(legacy.get("_id")), "email": email}
    for field in PROFILE_FIELDS:
        if field in legacy:
            row[field] = legacy.get(field)
    if _is_blank(row.get("org_id")):
        organization = legacy.get("Organization") or legacy.get("organization") or ""
        row["org_id"] = organization.strip().lower().replace(" ", "-") or "active-teams"
    if _is_blank(row.get("Organization")):
        row["Organization"] = legacy.get("organization") or "Active Church"
    if "is_supreme_admin" not in row:
        row["is_supreme_admin"] = False
    return row


def _coerce_stage(value: Any) -> Any:
    """Supabase keeps `stage` as a number, Mongo sometimes holds a label."""
    if _is_blank(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_person_uuid(client, value: Any) -> Optional[str]:
    """Translate a Mongo ObjectId into the Supabase UUID of that person."""
    if _is_blank(value):
        return None
    text = str(value).strip()
    if UUID_PATTERN.match(text):
        return text
    try:
        rows = client.table("People").select("person_id").eq("_id", text).limit(1).execute().data
    except Exception:  # noqa: BLE001 - the lookup is best effort
        return None
    if rows and rows[0].get("person_id"):
        return str(rows[0]["person_id"])
    return None


def _mapped_id_fields(client, legacy: Dict[str, Any]) -> Dict[str, Any]:
    """Leader / person references translated into Supabase UUIDs."""
    mapped: Dict[str, Any] = {}
    for field in ID_MAPPED_FIELDS:
        if field not in legacy:
            continue
        value = legacy.get(field)
        if field.startswith("LeaderPath"):
            path = value if isinstance(value, list) else [value]
            resolved = [_resolve_person_uuid(client, item) for item in path]
            resolved = [item for item in resolved if item]
            if resolved:
                mapped[field] = resolved
            continue
        resolved = _resolve_person_uuid(client, value)
        if resolved:
            mapped[field] = resolved
    return mapped


async def _sync_profile(legacy: Dict[str, Any], email: str) -> Dict[str, Any]:
    """Make sure `public.Users` holds this user's profile.

    An existing row wins for every field it already has - a user who was
    promoted or moved organisation after the migration keeps that. Only empty
    fields are filled in from the legacy document.
    """
    client = _admin_client() or supabase_anon
    profile = _legacy_profile(legacy, email)
    profile.update(_mapped_id_fields(client, legacy))
    if "stage" in legacy:
        stage = _coerce_stage(legacy.get("stage"))
        if stage is not None:
            profile["stage"] = stage

    result = client.table("Users").select("*").eq("email", email).limit(1).execute()
    rows = getattr(result, "data", None) or []

    if rows:
        current = rows[0]
        updates = {
            field: value
            for field, value in profile.items()
            if field != "_id" and _is_blank(current.get(field)) and not _is_blank(value)
        }
        if updates:
            client.table("Users").update(updates).eq("_id", current.get("_id")).execute()
            current.update(updates)
        return current

    insert_row = dict(profile)
    if legacy.get("password"):
        insert_row["password"] = legacy.get("password")
        insert_row["confirm_password"] = legacy.get("confirm_password") or legacy.get("password")
    if legacy.get("created_at"):
        insert_row["created_at"] = legacy.get("created_at")

    return _insert_profile(client, insert_row)


def _insert_profile(client, insert_row: Dict[str, Any]) -> Dict[str, Any]:
    """Insert the profile, dropping optional columns Supabase rejects."""
    optional_fields = [
        "profile_picture",
        "date_of_birth",
        "home_address",
        "invited_by",
        "phone_number",
        "gender",
        "stage",
        "person_id",
        "LeaderId",
        "leader12",
        "leader144",
        "leader1728",
        "LeaderPath[0]",
        "LeaderPath[1]",
        "LeaderPath[2]",
        "LeaderPath[3]",
    ]

    row = dict(insert_row)
    last_error: Optional[Exception] = None
    while True:
        try:
            created = client.table("Users").insert(row).execute()
            rows = getattr(created, "data", None) or []
            return rows[0] if rows else row
        except Exception as exc:  # noqa: BLE001 - retried without the offending column
            last_error = exc
            removable = [field for field in optional_fields if field in row]
            if not removable:
                raise HTTPException(
                    status_code=502,
                    detail=f"Could not create the Supabase profile: {exc}",
                ) from exc
            row.pop(removable[0], None)


def _find_auth_user(admin, email: str):
    """Auth user with this email, looked up through the admin API."""
    page = 1
    while page <= 20:
        response = admin.auth.admin.list_users(page=page, per_page=200)
        for user in getattr(response, "users", []) or []:
            if (user.email or "").strip().lower() == email:
                return user
        if len(getattr(response, "users", []) or []) < 200:
            return None
        page += 1
    return None


def _admin_rest(method: str, path: str, payload: Optional[Dict[str, Any]] = None):
    """Call the Supabase admin REST API with the configured server key."""
    if not (SUPABASE_URL and SUPABASE_ANON_KEY):
        return None
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }
    try:
        return httpx.request(
            method,
            f"{SUPABASE_URL}/auth/v1/{path}",
            headers=headers,
            json=payload,
            timeout=30,
        )
    except Exception:  # noqa: BLE001 - the caller falls back to the anon client
        return None


def _find_auth_user_rest(email: str) -> Optional[Dict[str, Any]]:
    """Auth user with this email, looked up through the admin REST API."""
    page = 1
    while page <= 50:
        response = _admin_rest("GET", f"admin/users?page={page}&per_page=200")
        if response is None or response.status_code != 200:
            return None
        try:
            users = response.json().get("users") or []
        except Exception:  # noqa: BLE001
            return None
        for user in users:
            if (user.get("email") or "").strip().lower() == email:
                return user
        if len(users) < 200:
            return None
        page += 1
    return None


def _create_with_admin_rest(email: str, password: str) -> bool:
    """Create or update the auth account through the admin REST API.

    Returns False when the configured key may not use the admin API, so the
    caller can fall back to signing the user up through the anon client.
    """
    existing = _find_auth_user_rest(email)
    if existing:
        response = _admin_rest(
            "PUT", f"admin/users/{existing.get('id')}", {"password": password, "email_confirm": True}
        )
        if response is None or response.status_code >= 400:
            return False
        return True

    response = _admin_rest(
        "POST",
        "admin/users",
        {"email": email, "password": password, "email_confirm": True},
    )
    if response is None or response.status_code >= 400:
        return False
    return True


def _session_payload(result) -> Dict[str, Any]:
    session = getattr(result, "session", None)
    user = getattr(result, "user", None)
    if not session:
        raise HTTPException(status_code=500, detail="Supabase did not return a session")
    return {
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "expires_in": getattr(session, "expires_in", None),
        "token_type": "bearer",
        "user": {
            "id": getattr(user, "id", None),
            "email": getattr(user, "email", None),
        },
    }


def _sign_in(email: str, password: str) -> Dict[str, Any]:
    try:
        result = supabase_anon.auth.sign_in_with_password({"email": email, "password": password})
    except Exception as exc:  # noqa: BLE001 - the client raises on bad credentials
        raise HTTPException(status_code=401, detail=f"Supabase sign-in failed: {exc}") from exc
    if getattr(result, "session", None):
        return _session_payload(result)
    error = getattr(result, "error", None)
    raise HTTPException(status_code=401, detail=str(error or "Supabase sign-in failed"))


def _try_sign_in(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Sign in, returning None when the account or the password does not fit."""
    try:
        result = supabase_anon.auth.sign_in_with_password({"email": email, "password": password})
    except Exception:  # noqa: BLE001 - unknown account or wrong password
        return None
    if result is not None and getattr(result, "session", None):
        return _session_payload(result)
    return None


def _create_with_service_role(admin, email: str, password: str) -> None:
    auth_user = _find_auth_user(admin, email)
    if auth_user is None:
        try:
            admin.auth.admin.create_user(
                {"email": email, "password": password, "email_confirm": True}
            )
            return
        except Exception as exc:  # noqa: BLE001 - surfaced to the caller below
            if "already" not in str(exc).lower():
                raise HTTPException(
                    status_code=502, detail=f"Could not create the Supabase account: {exc}"
                ) from exc
            auth_user = _find_auth_user(admin, email)

    if auth_user is not None:
        try:
            admin.auth.admin.update_user_by_id(
                auth_user.id, {"password": password, "email_confirm": True}
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=502, detail=f"Could not update the Supabase account: {exc}"
            ) from exc


def _create_with_anon_key(email: str, password: str) -> Dict[str, Any]:
    try:
        result = supabase_anon.auth.sign_up({"email": email, "password": password})
    except Exception as exc:  # noqa: BLE001 - the client raises when the user exists
        message = str(exc)
        if "already" in message.lower() or "registered" in message.lower():
            raise HTTPException(
                status_code=409,
                detail=(
                    "This account already exists in Supabase with a different password. "
                    "Use “Forgot password” to set a new one, or ask an administrator to "
                    "reset it for you."
                ),
            ) from exc
        raise HTTPException(
            status_code=502,
            detail=(
                "The account could not be created automatically. Ask an administrator "
                "for the service role key, or use “Forgot password”."
            ),
        ) from exc

    session = getattr(result, "session", None)
    if session:
        return _session_payload(result)

    error = getattr(result, "error", None)
    message = str(error or "")
    if "already" in message.lower() or "registered" in message.lower():
        raise HTTPException(
            status_code=409,
            detail=(
                "This account already exists in Supabase with a different password. "
                "Use “Forgot password” to set a new one, or ask an administrator to "
                "reset it for you."
            ),
        )
    raise HTTPException(
        status_code=502,
        detail=(
            "The account could not be created automatically. Ask an administrator "
            "for the service role key, or use “Forgot password”."
        ),
    )


async def _ensure_session(email: str, password: str) -> Tuple[Dict[str, Any], bool]:
    """Return a Supabase session for this email, creating the account if needed."""
    # The account may already exist and work with the password we were given.
    signed_in = _try_sign_in(email, password)
    if signed_in is not None:
        return signed_in, False

    # Otherwise the account is missing, or it exists with another password: the
    # Mongo credentials were verified already, so carry that password over.
    admin = _admin_client()
    if admin is not None:
        _create_with_service_role(admin, email, password)
        return _sign_in(email, password), True

    if _create_with_admin_rest(email, password):
        return _sign_in(email, password), True

    # No key that may create accounts: a fresh sign-up may still work.
    return _create_with_anon_key(email, password), True


async def _ensure_auth_account(email: str) -> None:
    """Create the auth account without a password, for the reset-password flow."""
    admin = _admin_client()
    if admin is not None:
        auth_user = _find_auth_user(admin, email)
        if auth_user is None:
            try:
                admin.auth.admin.create_user(
                    {
                        "email": email,
                        "password": secrets.token_urlsafe(32),
                        "email_confirm": False,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                if "already" not in str(exc).lower():
                    raise HTTPException(
                        status_code=502, detail=f"Could not prepare the account: {exc}"
                    ) from exc
        return

    existing = _find_auth_user_rest(email)
    if existing:
        return
    response = _admin_rest(
        "POST",
        "admin/users",
        {"email": email, "password": secrets.token_urlsafe(32), "email_confirm": False},
    )
    if response is not None and response.status_code < 400:
        return

    try:
        result = supabase_anon.auth.sign_up(
            {"email": email, "password": secrets.token_urlsafe(32)}
        )
    except Exception as exc:  # noqa: BLE001 - already registered is fine
        message = str(exc)
        if "already" in message.lower() or "registered" in message.lower():
            return
        raise HTTPException(status_code=502, detail="Could not prepare the account") from exc

    error = getattr(result, "error", None)
    message = str(error or "")
    if "already" in message.lower() or "registered" in message.lower():
        return
    if getattr(result, "session", None) is None and message:
        raise HTTPException(status_code=502, detail="Could not prepare the account")


@router.post("/auth/migrate-legacy")
async def migrate_legacy_account(payload: dict = Body(...)):
    """Sign a pre-Supabase user in, creating their Supabase account on the fly."""
    email = _normalize_email(str(payload.get("email") or ""))
    password = str(payload.get("password") or "")

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password are required")

    legacy = await _find_legacy_user(email)
    if not legacy:
        raise HTTPException(status_code=404, detail="No legacy account found for this email")

    stored_hash = legacy.get("password")
    if not stored_hash or not verify_password(password, str(stored_hash)):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session, created = await _ensure_session(email, password)
    profile = await _sync_profile(legacy, email)

    return {
        **session,
        "migrated": True,
        "account_created": created,
        "profile": {
            "_id": profile.get("_id"),
            "name": profile.get("name"),
            "surname": profile.get("surname"),
            "email": profile.get("email"),
            "role": profile.get("role"),
            "Organization": profile.get("Organization"),
            "org_id": profile.get("org_id"),
        },
    }


@router.post("/auth/legacy-account-exists")
async def legacy_account_exists(payload: dict = Body(...)):
    """Prepare the auth account so the reset-password email can be sent.

    Always answers the same way so it cannot be used to discover which email
    addresses are registered.
    """
    email = _normalize_email(str(payload.get("email") or ""))
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")

    legacy = await _find_legacy_user(email)
    if legacy:
        try:
            await _ensure_auth_account(email)
        except HTTPException:
            pass

    return {"ok": True}
