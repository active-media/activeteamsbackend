from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from supabase_helpers.supabase_connection import supabase_admin as supabase


def _org_from_filter(org_filter: Optional[dict]) -> Optional[str]:
    if not org_filter:
        return None
    return org_filter.get("Organization") or org_filter.get("organization")


def sb_upsert_service_target(
    leader_name: Optional[str],
    target_count: int,
    email: str,
    org_filter: Optional[dict],
    week_identifier: Optional[str] = None,
    person_id: Optional[str] = None,    # accepted for compatibility, ignored
    target_type: Optional[str] = None,  # accepted for compatibility, ignored
) -> dict:
    """Upsert a service target keyed on (leader_name, week_identifier, organization).

    Errors are raised, not swallowed, so the route returns a real error.
    """
    org_value = _org_from_filter(org_filter)
    if not org_value:
        raise ValueError("organization is required to save a service target")
    if not week_identifier:
        raise ValueError("week_identifier is required to save a service target")

    data = {
        "leader_name": leader_name or "",
        "target_count": target_count,
        "email": email,
        "organization": org_value,
        "week_identifier": week_identifier,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    result = (
        supabase.table("service_targets")
        .upsert(data, on_conflict="leader_name,week_identifier,organization")
        .execute()
    )
    if not result.data:
        raise RuntimeError("service target upsert returned no data")
    return result.data[0]


def sb_list_service_targets(
    week: Optional[str] = None,
    org_filter: Optional[dict] = None,
    visible_leader_ids: Optional[set] = None,
    person_id: Optional[str] = None,
    target_type: Optional[str] = None,
    **_ignored,
) -> list[dict]:
    """Flat rows: leader_name, target_count, email, organization, week_identifier, actual_count."""
    query = supabase.table("service_targets").select("*")

    org_value = _org_from_filter(org_filter)
    if org_value:
        query = query.eq("organization", org_value)
    if week:
        query = query.eq("week_identifier", week)

    rows = query.execute().data or []
    for r in rows:
        r.setdefault("actual_count", 0)  # TODO: wire real check-in counts
    return rows


def sb_get_service_target_report(
    week: Optional[str] = None,
    org_filter: Optional[dict] = None,
    visible_leader_ids: Optional[set] = None,
    **_ignored,
) -> dict:
    targets = sb_list_service_targets(week=week, org_filter=org_filter, visible_leader_ids=visible_leader_ids)
    total_target = sum(t.get("target_count") or 0 for t in targets)
    return {"targets": targets, "total_target": total_target, "total_targets": len(targets)}


def sb_delete_service_target(target_id: str) -> bool:
    """Delete a service target by ID. Returns True if deleted."""
    try:
        result = supabase.table("service_targets").delete().eq("id", target_id).execute()
        return bool(result.data)
    except Exception:
        return False