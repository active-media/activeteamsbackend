from fastapi import HTTPException

try:
    from postgrest.exceptions import APIError
except Exception:  # pragma: no cover
    APIError = Exception


def _code(e: Exception) -> str:
    d = getattr(e, "code", "") or getattr(getattr(e, "error", e), "code", "")
    return str(d)


def _message(e: Exception) -> str:
    return str(getattr(e, "message", "") or e)


def handle_supabase_error(
    e: Exception,
    *,
    not_found_msg: str | None = None,
    conflict_msg: str | None = None,
    forbidden_msg: str | None = None,
    bad_request_msg: str | None = None,
    unprocessable_msg: str | None = None,
) -> HTTPException:
    c = _code(e)
    m = _message(e).lower()
    if c == "23505" or "duplicate" in m or "unique" in m:
        return HTTPException(status_code=409, detail=conflict_msg or "Duplicate entry")
    if c == "23503" or "foreign key" in m:
        return HTTPException(status_code=409, detail=bad_request_msg or "Foreign key constraint violation")
    if c == "23502" or c == "23514" or "not null" in m or "check" in m:
        return HTTPException(status_code=422, detail=unprocessable_msg or "Validation constraint failed")
    if "row-level security" in m or "rls" in m:
        return HTTPException(status_code=403, detail=forbidden_msg or "Access denied (RLS)")
    if "no rows" in m or "not found" in m or not_found_msg:
        return HTTPException(status_code=404, detail=not_found_msg or "Not found")
    return HTTPException(status_code=500, detail="Unexpected error")
