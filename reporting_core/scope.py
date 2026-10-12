from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
from typing import Any

from fastapi import Depends, HTTPException

from auth.utils import get_current_user
from supabase_helpers.supabase_connection import supabase


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportScope:
    org_filter: dict | None
    is_super: bool
    role: str
    user_id: str
    visible_leader_ids: set[str] | None


def _normal(value: Any) -> str:
    return str(value or "").strip().casefold()


def _leader_ids(current_user: dict, people: list[dict]) -> set[str]:
    user_id = str(current_user.get("user_id") or current_user.get("_id") or "")
    email = _normal(current_user.get("email"))
    root = next(
        (
            person for person in people
            if (email and _normal(person.get("Email") or person.get("email")) == email)
            or (user_id and str(person.get("_id") or "") == user_id)
        ),
        None,
    )
    if not root:
        return set()

    root_id = str(root.get("_id") or user_id)
    root_name = " ".join(
        str(root.get(key) or "").strip()
        for key in ("Name", "Surname")
        if str(root.get(key) or "").strip()
    )
    root_values = {_normal(root_id), _normal(root_name)} - {""}
    visible = {root_id} if root_id else set()

    for person in people:
        person_id = str(person.get("_id") or "")
        path = person.get("LeaderPath") or []
        if not isinstance(path, (list, tuple, set)):
            path = [path]
        path_values = {_normal(value) for value in path}
        parent_value = _normal(person.get("LeaderId"))
        level_values = {
            _normal(person.get(key))
            for key in ("leader12", "leader144", "leader1728", "Leader @12", "Leader @144", "Leader @1728")
        }
        if root_values.intersection(path_values) or _normal(root_id) == parent_value or root_values.intersection(level_values):
            if person_id:
                visible.add(person_id)

    return visible


def build_report_scope(current_user: dict, people: list[dict] | None = None) -> ReportScope:
    role = str(current_user.get("role") or "").strip()
    normalized_role = role.casefold()
    is_super = bool(current_user.get("is_supreme_admin")) or normalized_role == "super_admin"
    user_id = str(current_user.get("user_id") or current_user.get("_id") or current_user.get("id") or "")

    if not is_super and normalized_role not in {"admin", "leader", "leaderat12"}:
        raise HTTPException(status_code=403, detail="This role cannot access reporting endpoints")

    organization = current_user.get("Organization") or current_user.get("organization")
    if not is_super and not str(organization or "").strip():
        raise HTTPException(status_code=403, detail="Organization not associated with user")

    if is_super:
        return ReportScope(None, True, role, user_id, None)

    org_filter = {"Organization": str(organization).strip()}
    if normalized_role == "admin":
        return ReportScope(org_filter, False, role, user_id, None)
    if people is None:
        return ReportScope(org_filter, False, role, user_id, None)

    leader_ids = _leader_ids(current_user, people)
    if not leader_ids:
        raise HTTPException(status_code=403, detail="Leader hierarchy could not be resolved")
    return ReportScope(org_filter, False, role, user_id, leader_ids)


def _load_organization_people(org_filter: dict) -> list[dict]:
    result = supabase.table("People").select("*").execute()
    organization = _normal(org_filter.get("Organization"))
    return [
        person for person in (result.data or [])
        if _normal(person.get("Organization") or person.get("organization")) == organization
    ]


async def get_report_scope(
    current_user: dict = Depends(get_current_user),
) -> ReportScope:
    preliminary = build_report_scope(current_user)
    if preliminary.is_super or preliminary.role.casefold() == "admin":
        return preliminary

    try:
        people = await asyncio.to_thread(_load_organization_people, preliminary.org_filter or {})
        return build_report_scope(current_user, people)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unable to resolve reporting hierarchy")
        raise HTTPException(status_code=500, detail="Unable to resolve reporting hierarchy") from exc