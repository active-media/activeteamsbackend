"""Small, non-blocking audit-trail helper shared by API modules.

The Admin dashboard reads the persistent Supabase ``Activity Logs`` table, so
every important mutation should append a row here rather than keeping the log
only in the browser session.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from supabase_helpers.supabase_client import supabase_admin

logger = logging.getLogger(__name__)


def log_activity(
    user_id: str,
    action: str,
    details: str,
    organization: Optional[str] = None,
) -> None:
    """Best-effort append to the persistent audit trail.

    Audit logging must never make the user-facing mutation fail, so errors are
    logged and swallowed.
    """

    payload = {
        "_id": str(uuid.uuid4()),
        "user_id": str(user_id or ""),
        "action": str(action or "UNKNOWN").upper(),
        "details": str(details or ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if organization:
        payload["Organization"] = organization

    try:
        supabase_admin.table("Activity Logs").insert(payload).execute()
    except Exception as exc:  # pragma: no cover - depends on external Supabase
        logger.warning("Could not write activity log: %s", exc)
