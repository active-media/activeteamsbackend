"""
profile_router.py  –  All /profile/* and /users/* endpoints, fully migrated
to Supabase (no MongoDB / ObjectId anywhere).

Mount in main.py:
    from profile_router import router as profile_router
    app.include_router(profile_router)

Remove from main.py:
    - @app.get("/profile/{user_id}")
    - @app.put("/profile/{user_id}")
    - @app.post("/users/{user_id}/avatar")
    - @app.put("/users/{user_id}/password")
    - @app.get("/users")          ← the invited-by dropdown endpoint
    - normalize_gender_value()
    - format_user_response()
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.security import HTTPAuthorizationCredentials

from auth.utils import get_current_user
from supabase_helpers.supabase_connection import supabase as _supabase, supabase_admin as _supabase_admin

router = APIRouter(tags=["profile"])

SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _normalize_gender(gender: Optional[str]) -> str:
    if not gender:
        return ""
    gender_map = {
        "male": "Male", "female": "Female",
        "m": "Male",    "f": "Female",
        "Male": "Male", "Female": "Female",
        "Other": "Other", "Prefer not to say": "Prefer not to say",
    }
    return gender_map.get(str(gender).strip(), str(gender).strip())


def _format_user(user: dict) -> dict:
    """Flatten a Users row into the standard profile response shape."""
    org = user.get("Organization") or user.get("organization") or ""
    return {
        "id":             user.get("_id", ""),
        "name":           user.get("name", ""),
        "surname":        user.get("surname", ""),
        "date_of_birth":  user.get("date_of_birth", ""),
        "home_address":   user.get("home_address", ""),
        "invited_by":     user.get("invited_by", ""),
        "phone_number":   user.get("phone_number", ""),
        "email":          user.get("email", ""),
        "gender":         _normalize_gender(user.get("gender", "")),
        "role":           user.get("role", "user"),
        "profile_picture":user.get("profile_picture", ""),
        "organization":   org,
        "Organization":   org,
        "org_id":         user.get("org_id", ""),
        "leader12":       user.get("leader12", ""),
        "leader144":      user.get("leader144", ""),
        "leader1728":     user.get("leader1728", ""),
        "stage":          user.get("stage", ""),
        "is_supreme_admin": user.get("is_supreme_admin") or False,
    }


def _resolve_leader_name(leader_id: Optional[str], users_by_id: dict) -> str:
    """Return 'Name Surname' for a leader _id from a pre-fetched id→row map."""
    if not leader_id:
        return ""
    row = users_by_id.get(leader_id)
    if not row:
        return ""
    return f"{row.get('name', '')} {row.get('surname', '')}".strip()


def _resolve_leader_obj(leader_id: Optional[str], users_by_id: dict) -> Optional[dict]:
    """Return a LeaderInfo dict for a leader _id, or None."""
    if not leader_id:
        return None
    row = users_by_id.get(leader_id)
    if not row:
        return None
    return {
        "id":           row["_id"],
        "name":         row.get("name", ""),
        "surname":      row.get("surname", ""),
        "email":        row.get("email", ""),
        "phone_number": row.get("phone_number", ""),
    }


# ─── GET /profile/{user_id} ───────────────────────────────────────────────────

@router.get("/profile/{user_id}")
async def get_profile(
    user_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Return a user's full profile including resolved leader names.
    Users can only fetch their own profile; admins / supreme admins can fetch any.
    user_id is the legacy Mongo text _id stored in the Users table.
    """
    token_user_id = current_user.get("user_id") or current_user.get("_id")
    is_supreme    = current_user.get("is_supreme_admin", False)
    is_admin      = current_user.get("role") == "admin"

    if not is_supreme and not is_admin and str(token_user_id) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized to access this profile")

    # ── Fetch the target user row ──────────────────────────────────────────
    try:
        res = (
            _supabase_admin.table("Users")
            .select("*")
            .eq("_id", user_id)
            .limit(1)
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User lookup failed: {e}")

    if not rows:
        raise HTTPException(status_code=404, detail=f"User not found: {user_id}")

    user = rows[0]

    # ── Resolve leader IDs → names ─────────────────────────────────────────
    leader_ids = [
        uid for uid in [
            user.get("leader12"),
            user.get("leader144"),
            user.get("leader1728"),
        ] if uid
    ]

    users_by_id: dict = {}
    if leader_ids:
        try:
            lr = (
                _supabase_admin.table("Users")
                .select("_id, name, surname, email, phone_number")
                .in_("_id", leader_ids)
                .execute()
            )
            for row in (lr.data or []):
                users_by_id[row["_id"]] = row
        except Exception:
            pass  # non-fatal — leaders will just be empty

    leader_at_1   = _resolve_leader_obj(user.get("leader12"),   users_by_id)
    leader_at_12  = _resolve_leader_obj(user.get("leader144"),  users_by_id)
    leader_at_144 = _resolve_leader_obj(user.get("leader1728"), users_by_id)

    # ── Also try to enrich invited_by from People table ───────────────────
    invited_by = user.get("invited_by", "")
    if not invited_by and leader_at_1:
        invited_by = f"{leader_at_1['name']} {leader_at_1['surname']}".strip()

    base = _format_user(user)
    base["invited_by"] = invited_by
    base["leader_path"] = [uid for uid in [
        user.get("leader12"), user.get("leader144"), user.get("leader1728")
    ] if uid]
    base["leaders"] = {
        "leaderAt1":   leader_at_1,
        "leaderAt12":  leader_at_12,
        "leaderAt144": leader_at_144,
    }
    return base


# ─── GET /profile/me  (convenience — resolves from auth token) ────────────────

@router.get("/profile/me/details")
async def get_my_profile(current_user: dict = Depends(get_current_user)):
    """Return the authenticated user's own profile. Uses email from token."""
    user_id = current_user.get("_id") or current_user.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Could not resolve user ID from token")
    return await get_profile(user_id, current_user)


# ─── PUT /profile/{user_id} ───────────────────────────────────────────────────

@router.put("/profile/{user_id}")
async def update_profile(
    user_id: str,
    profile_update: dict,
    current_user: dict = Depends(get_current_user),
):
    """
    Update mutable profile fields.
    Users may only update their own profile.
    Admins / supreme admins may update any profile.
    """
    token_user_id = current_user.get("user_id") or current_user.get("_id")
    is_supreme    = current_user.get("is_supreme_admin", False)
    is_admin      = current_user.get("role") == "admin"

    if not is_supreme and not is_admin and str(token_user_id) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized to update this profile")

    # Confirm the user exists
    try:
        res = (
            _supabase_admin.table("Users")
            .select("_id, email, Organization, org_id")
            .eq("_id", user_id)
            .limit(1)
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User lookup failed: {e}")

    if not rows:
        raise HTTPException(status_code=404, detail="User not found")

    # ── Build update payload from allowed fields only ──────────────────────
    ALLOWED = {
        "name", "surname", "date_of_birth", "home_address",
        "phone_number", "invited_by", "gender",
        "email", "organization", "Organization",
    }
    payload: dict = {}

    for field in ALLOWED:
        if field in profile_update and profile_update[field] is not None:
            value = profile_update[field]
            if field == "gender":
                value = _normalize_gender(value)
            payload[field] = value

    # Keep both Organization (capital O) and organization in sync
    if "organization" in payload and "Organization" not in payload:
        payload["Organization"] = payload["organization"]
    elif "Organization" in payload and "organization" not in payload:
        payload["organization"] = payload["Organization"]

    if "organization" in payload:
        payload["org_id"] = payload["organization"].lower().replace(" ", "-")

    if not payload:
        # Nothing to update — just return current state
        return await get_profile(user_id, current_user)

    payload["updated_at"] = datetime.utcnow().isoformat()

    try:
        _supabase_admin.table("Users").update(payload).eq("_id", user_id).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profile update failed: {e}")

    return await get_profile(user_id, current_user)


# ─── POST /users/{user_id}/avatar ─────────────────────────────────────────────

@router.post("/users/{user_id}/avatar")
async def upload_avatar(
    user_id: str,
    avatar: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    """
    Upload a profile picture.
    Stores the image in Supabase Storage bucket 'avatars' and saves the public
    URL to Users.profile_picture.  Falls back to base64 data-url if storage
    upload fails (e.g. bucket not yet created).
    """
    token_user_id = current_user.get("user_id") or current_user.get("_id")
    is_supreme    = current_user.get("is_supreme_admin", False)
    is_admin      = current_user.get("role") == "admin"

    if not is_supreme and not is_admin and str(token_user_id) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized to update this profile")

    if not avatar.content_type or not avatar.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    contents = await avatar.read()
    if len(contents) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large — maximum 5 MB")

    ext       = (avatar.content_type or "image/png").split("/")[-1] or "png"
    storage_path = f"{user_id}/avatar.{ext}"
    avatar_url: str

    # ── Try Supabase Storage first ─────────────────────────────────────────
    try:
        _supabase_admin.storage.from_("avatars").upload(
            storage_path, contents,
            {"upsert": "true", "content-type": avatar.content_type}
        )
        pub = _supabase_admin.storage.from_("avatars").get_public_url(storage_path)
        avatar_url = f"{pub}?t={int(datetime.utcnow().timestamp())}"
    except Exception as storage_err:
        # Graceful fallback — encode as data-url
        import base64
        b64 = base64.b64encode(contents).decode("utf-8")
        avatar_url = f"data:{avatar.content_type};base64,{b64}"

    # ── Persist URL in Users row ───────────────────────────────────────────
    try:
        _supabase_admin.table("Users").update({
            "profile_picture": avatar_url,
            "updated_at": datetime.utcnow().isoformat(),
        }).eq("_id", user_id).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save avatar URL: {e}")

    return {"message": "Avatar uploaded successfully", "avatarUrl": avatar_url}


# ─── PUT /users/{user_id}/password ────────────────────────────────────────────

@router.put("/users/{user_id}/password")
async def change_password(
    user_id: str,
    password_data: dict,
    current_user: dict = Depends(get_current_user),
):
    """
    Change a user's password via Supabase Auth (admin API).
    The frontend also calls supabase.auth.updateUser() directly for the
    currently-logged-in user — this endpoint exists as a server-side fallback
    and for admin-initiated resets.
    """
    token_user_id = current_user.get("user_id") or current_user.get("_id")
    is_supreme    = current_user.get("is_supreme_admin", False)
    is_admin      = current_user.get("role") == "admin"

    if not is_supreme and not is_admin and str(token_user_id) != str(user_id):
        raise HTTPException(status_code=403, detail="Not authorized to update this profile")

    new_password = password_data.get("newPassword") or password_data.get("new_password")
    if not new_password:
        raise HTTPException(status_code=400, detail="newPassword is required")
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # Look up the Supabase Auth UUID for this user (stored in Users._id or supabase_uuid)
    supabase_uuid = current_user.get("supabase_uuid")
    if not supabase_uuid or str(token_user_id) != str(user_id):
        # Admin changing someone else's password — look up their auth UUID via email
        try:
            res = (
                _supabase_admin.table("Users")
                .select("_id, email")
                .eq("_id", user_id)
                .limit(1)
                .execute()
            )
            target = res.data[0] if res.data else None
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"User lookup failed: {e}")

        if not target:
            raise HTTPException(status_code=404, detail="User not found")

        # Use admin API to find auth user by email
        try:
            auth_users = _supabase_admin.auth.admin.list_users()
            supabase_uuid = next(
                (u.id for u in auth_users if u.email == target["email"]),
                None
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Auth user lookup failed: {e}")

        if not supabase_uuid:
            raise HTTPException(status_code=404, detail="Auth user not found for this account")

    # ── Update password via Supabase Admin Auth API ────────────────────────
    try:
        _supabase_admin.auth.admin.update_user_by_id(
            supabase_uuid,
            {"password": new_password}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Password update failed: {e}")

    return {"message": "Password updated successfully"}


# ─── GET /users  (invited-by dropdown) ───────────────────────────────────────

@router.get("/users")
async def get_users_for_dropdown(
    organization: Optional[str] = Query(None),
):
    """
    Return a lightweight list of users for the 'Invited By' signup dropdown.
    No auth required — returns names + emails only.
    """
    try:
        query = (
            _supabase_admin.table("Users")
            .select("_id, name, surname, email")
            .limit(200)
        )
        if organization:
            query = query.eq("Organization", organization)

        res = query.execute()
        users = res.data or []

        formatted = [
            {
                "_id":     u["_id"],
                "name":    u.get("name", ""),
                "surname": u.get("surname", ""),
                "email":   u.get("email", ""),
                "label":   f"{u.get('name', '')} {u.get('surname', '')}".strip(),
            }
            for u in users
            if f"{u.get('name', '')} {u.get('surname', '')}".strip()
        ]

        return {"users": formatted, "total": len(formatted)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {e}")