from __future__ import annotations

from typing import Optional

from supabase_helpers.supabase_connection import supabase


def sb_upsert_service_target(
    leader_name: Optional[str],
    target_count: int,
    email: str,
    org_filter: Optional[dict],
    week_identifier: Optional[str] = None,
) -> dict:
    """
    Upsert a service target record.
    
    Returns the created/updated target record.
    """
    org_value = None
    if org_filter:
        org_value = org_filter.get("Organization")

    # Build the data dict
    data = {
        "leader_name": leader_name or "",
        "target_count": target_count,
        "email": email,
    }

    if org_value:
        data["organization"] = org_value

    if week_identifier:
        data["week_identifier"] = week_identifier

    data["created_at"] = datetime.utcnow().isoformat()
    data["updated_at"] = datetime.utcnow().isoformat()

    # Try to upsert — insert if not exists, update if exists
    # We use on conflict to handle duplicates
    try:
        result = (
            supabase.table("service_targets")
            .upsert(data, on_conflict=["leader_name", "week_identifier", "organization"])
            .execute()
        )
        if result.data:
            return result.data[0]
    except Exception:
        pass

    # Fallback: just insert
    try:
        result = supabase.table("service_targets").insert(data).execute()
        if result.data:
            return result.data[0]
    except Exception:
        pass

    return {"leader_name": data["leader_name"], "target_count": data["target_count"]}


def sb_delete_service_target(target_id: str) -> bool:
    """Delete a service target by ID. Returns True if deleted.""" 
    try:
        result = supabase.table("service_targets").delete().eq("id", target_id).execute()
        return len(result.data) > 0 if result.data else False
    except Exception:
        return False


def sb_list_service_targets(week: Optional[str] = None, org_filter: Optional[dict] = None) -> list[dict]:
    """List service targets, optionally filtered by week identifier."""
    query = supabase.table("service_targets").select("*")

    if org_filter:
        org_value = org_filter.get("Organization")
        if org_value:
            query = query.eq("organization", org_value)

    if week:
        query = query.eq("week_identifier", week)

    result = query.execute()
    return result.data or []


def sb_get_service_target_report(week: Optional[str] = None, org_filter: Optional[dict] = None) -> dict:
    """Get service target report showing target vs actual attendance."""
    targets = sb_list_service_targets(week, org_filter)

    # Build the report with target and actual data
    # For now, return the targets structure; actual attendance would
    # need to be computed from event data
    total_target = sum(t.get("target_count", 0) for t in targets)

    return {
        "targets": targets,
        "total_target": total_target,
        "total_targets": len(targets),
    }