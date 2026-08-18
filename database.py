import os
import re
import uuid
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from supabase_helpers.supabase_connection import supabase

load_dotenv()

# ============================================================================
# SUPABASE — People (fully migrated off MongoDB)
# ============================================================================
# `supabase` is the shared client from supabase_helpers.supabase_connection.
# PEOPLE_TABLE is used directly by people.py (the People router) for the
# standard REST endpoints (GET/POST/PATCH/DELETE /people, search, etc).
PEOPLE_TABLE = "People"


# --- thin Mongo-compatible adapter over Supabase ---------------------------
# main.py has ~30 call sites that use `people_collection.find_one(...)`,
# `.aggregate([...])`, `.update_one(...)` etc. outside of the standard CRUD
# routes (leader-hierarchy resolution, the in-memory people cache, the
# spreadsheet importer, check-in, admin tools...). Rewriting every one of
# those individually is a much bigger job, so this adapter lets them keep
# working unchanged while everything underneath is actually Supabase.
# New code should call `supabase.table(PEOPLE_TABLE)` directly instead of
# going through this adapter (see people.py for the pattern).

class _MongoLikeResult:
    def __init__(self, data=None, inserted_id=None, matched_count=0, modified_count=0, deleted_count=0):
        self.data = data or []
        self.inserted_id = inserted_id
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count


def _get_field_value(document: Dict[str, Any], field: str) -> Any:
    if field.startswith("$"):
        field = field[1:]
    value: Any = document
    for part in field.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


def _evaluate_expression(expr: Any, document: Dict[str, Any]) -> Any:
    if isinstance(expr, dict):
        if "$eq" in expr:
            return _evaluate_expression(expr["$eq"][0], document) == _evaluate_expression(expr["$eq"][1], document)
        if "$ne" in expr:
            return _evaluate_expression(expr["$ne"][0], document) != _evaluate_expression(expr["$ne"][1], document)
        if "$and" in expr:
            return all(_evaluate_expression(i, document) for i in expr["$and"])
        if "$or" in expr:
            return any(_evaluate_expression(i, document) for i in expr["$or"])
        if "$concat" in expr:
            parts = [_evaluate_expression(i, document) for i in expr["$concat"]]
            return "".join(str(p or "") for p in parts)
        if "$regexMatch" in expr:
            params = expr["$regexMatch"]
            text = str(_evaluate_expression(params.get("input"), document) or "")
            pattern = params.get("regex", "")
            flags = re.IGNORECASE if "i" in (params.get("options", "") or "").lower() else 0
            try:
                return re.search(pattern, text, flags) is not None
            except re.error:
                return False
        if "$ifNull" in expr:
            value = _evaluate_expression(expr["$ifNull"][0], document)
            return value if value is not None else _evaluate_expression(expr["$ifNull"][1], document)
        if "$toString" in expr:
            return str(_evaluate_expression(expr["$toString"], document) or "")
        if len(expr) == 1:
            _, value = next(iter(expr.items()))
            if isinstance(value, str) and value.startswith("$"):
                return _get_field_value(document, value)
            return _evaluate_expression(value, document)
    if isinstance(expr, list):
        return [_evaluate_expression(i, document) for i in expr]
    if isinstance(expr, str) and expr.startswith("$"):
        return _get_field_value(document, expr)
    return expr


def _matches_query(document: Dict[str, Any], query: Any) -> bool:
    if not query:
        return True
    if not isinstance(query, dict):
        return False
    for key, value in query.items():
        if key == "$or":
            if not any(_matches_query(document, cond) for cond in value):
                return False
            continue
        if key == "$and":
            if not all(_matches_query(document, cond) for cond in value):
                return False
            continue
        if key == "$expr":
            if not bool(_evaluate_expression(value, document)):
                return False
            continue
        doc_value = _get_field_value(document, key)
        if isinstance(value, dict):
            if "$ne" in value:
                if doc_value == value["$ne"]:
                    return False
            elif "$in" in value:
                if doc_value not in value["$in"]:
                    return False
            elif "$nin" in value:
                if doc_value in value["$nin"]:
                    return False
            elif "$exists" in value:
                exists = doc_value not in (None, "")
                if bool(value["$exists"]) != exists:
                    return False
            elif "$regex" in value:
                flags = re.IGNORECASE if "i" in (value.get("$options", "") or "").lower() else 0
                try:
                    if doc_value is None or not re.search(value["$regex"], str(doc_value), flags):
                        return False
                except re.error:
                    return False
            else:
                if not _matches_query({key: doc_value}, {key: value}):
                    return False
        else:
            if doc_value != value:
                return False
    return True


def _apply_projection(document: Dict[str, Any], projection: Any) -> Dict[str, Any]:
    if not projection or not isinstance(projection, dict):
        return document
    includes = [k for k, v in projection.items() if v]
    if not includes:
        return document
    return {k: document.get(k) for k in includes if k in document}


def _apply_sort(rows: List[Dict[str, Any]], sort_spec: Any) -> List[Dict[str, Any]]:
    if not sort_spec:
        return rows
    pairs = sort_spec.items() if isinstance(sort_spec, dict) else sort_spec
    for field, direction in reversed(list(pairs)):
        rows = sorted(
            rows,
            key=lambda item: (_get_field_value(item, field) is None, _get_field_value(item, field) or ""),
            reverse=direction < 0,
        )
    return rows


class SupabaseCursor:
    def __init__(self, collection: "SupabasePeopleCollection", query=None, projection=None, pipeline=None):
        self._collection = collection
        self._query = query or {}
        self._projection = projection
        self._pipeline = pipeline
        self._sort_spec = None
        self._skip_n = 0
        self._limit_n: Optional[int] = None
        self._rows: Optional[List[Dict[str, Any]]] = None
        self._iter = None

    def sort(self, sort_spec):
        self._sort_spec = sort_spec
        return self

    def skip(self, n):
        self._skip_n = n
        return self

    def limit(self, n):
        self._limit_n = n
        return self

    async def _load(self):
        if self._rows is None:
            if self._pipeline is not None:
                rows = await self._collection._run_pipeline(self._pipeline)
            else:
                rows = await self._collection._fetch(self._query, self._projection)
            if self._sort_spec:
                rows = _apply_sort(rows, self._sort_spec)
            if self._skip_n:
                rows = rows[self._skip_n:]
            if self._limit_n is not None:
                rows = rows[: self._limit_n]
            self._rows = rows
        return self._rows

    def __aiter__(self):
        self._iter = None
        return self

    async def __anext__(self):
        if self._iter is None:
            self._iter = iter(await self._load())
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def to_list(self, length: Optional[int] = None):
        rows = await self._load()
        return rows if length is None else rows[:length]


class SupabasePeopleCollection:
    """Mongo-collection-shaped wrapper around `supabase.table(PEOPLE_TABLE)`."""

    def __init__(self, client, table_name: str):
        self._client = client
        self._table_name = table_name

    def _table(self):
        return self._client.table(self._table_name)

    @staticmethod
    def _is_simple(query) -> bool:
        if not query:
            return True
        for key, value in query.items():
            if key in ("$or", "$and", "$expr"):
                return False
            if isinstance(value, dict):
                for op in value:
                    if op not in ("$ne", "$in", "$nin", "$exists", "$gt", "$gte", "$lt", "$lte", "$regex", "$options"):
                        return False
        return True

    @staticmethod
    def _select_clause(projection) -> str:
        if not projection or not isinstance(projection, dict):
            return "*"
        includes = [k for k, v in projection.items() if v]
        if not includes:
            return "*"
        return ",".join(f'"{f}"' if any(c in f for c in " @.-") else f for f in includes)

    def _apply_filters(self, qb, query):
        if not query:
            return qb
        for key, value in query.items():
            if key in ("$or", "$and", "$expr"):
                continue
            if isinstance(value, dict):
                if "$ne" in value:
                    qb = qb.neq(key, value["$ne"])
                elif "$in" in value:
                    qb = qb.in_(key, list(value["$in"]))
                elif "$nin" in value:
                    qb = qb.not_.in_(key, list(value["$nin"]))
                elif "$gt" in value:
                    qb = qb.gt(key, value["$gt"])
                elif "$gte" in value:
                    qb = qb.gte(key, value["$gte"])
                elif "$lt" in value:
                    qb = qb.lt(key, value["$lt"])
                elif "$lte" in value:
                    qb = qb.lte(key, value["$lte"])
                elif "$exists" in value:
                    qb = qb.not_.is_(key, "null") if value["$exists"] else qb.is_(key, "null")
                elif "$regex" in value:
                    pattern = str(value["$regex"])
                    anchored_start, anchored_end = pattern.startswith("^"), pattern.endswith("$")
                    core = pattern[1:-1] if anchored_start and anchored_end else (
                        pattern[1:] if anchored_start else (pattern[:-1] if anchored_end else pattern)
                    )
                    core = re.sub(r"\\(.)", r"\1", core)  # best-effort un-escape from re.escape()
                    prefix = "" if anchored_start else "%"
                    suffix = "" if anchored_end else "%"
                    qb = qb.ilike(key, f"{prefix}{core}{suffix}")
            else:
                qb = qb.eq(key, value)
        return qb

    async def _fetch(self, query, projection) -> List[Dict[str, Any]]:
        select_clause = self._select_clause(projection)
        if self._is_simple(query):
            qb = self._table().select(select_clause)
            qb = self._apply_filters(qb, query)
            resp = qb.execute()
            rows = resp.data or []
        else:
            # Complex query ($or / $and / $expr) — Postgrest can't express these
            # generically, so pull a broad page and filter client-side.
            resp = self._table().select("*").limit(5000).execute()
            rows = [r for r in (resp.data or []) if _matches_query(r, query)]
        if projection:
            rows = [_apply_projection(r, projection) for r in rows]
        return rows

    def find(self, query=None, projection=None) -> SupabaseCursor:
        return SupabaseCursor(self, query=query, projection=projection)

    async def find_one(self, query=None, projection=None) -> Optional[Dict[str, Any]]:
        rows = await self.find(query, projection).limit(1).to_list(1)
        return rows[0] if rows else None

    async def count_documents(self, query=None) -> int:
        if self._is_simple(query):
            qb = self._table().select("_id", count="exact")
            qb = self._apply_filters(qb, query)
            resp = qb.execute()
            return resp.count or 0
        return len(await self._fetch(query, None))

    async def insert_one(self, document: Dict[str, Any]) -> _MongoLikeResult:
        doc = dict(document)
        if not doc.get("_id"):
            doc["_id"] = str(uuid.uuid4())
        resp = self._table().insert(doc).execute()
        inserted = resp.data[0] if resp.data else doc
        return _MongoLikeResult(data=[inserted], inserted_id=inserted.get("_id"))

    @staticmethod
    def _apply_update_ops(row: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        changed = dict(row)
        if "$set" in update:
            changed.update(update["$set"])
        if "$unset" in update:
            for k in update["$unset"]:
                changed.pop(k, None)
        if "$inc" in update:
            for k, amt in update["$inc"].items():
                changed[k] = (changed.get(k) or 0) + amt
        if "$push" in update:
            for k, v in update["$push"].items():
                target = changed.get(k) or []
                if not isinstance(target, list):
                    target = [target]
                target = target + (v["$each"] if isinstance(v, dict) and "$each" in v else [v])
                changed[k] = target
        if "$pull" in update:
            for k, cond in update["$pull"].items():
                target = changed.get(k) or []
                if isinstance(target, list):
                    if isinstance(cond, dict):
                        changed[k] = [item for item in target if not _matches_query(item, cond)]
                    else:
                        changed[k] = [item for item in target if item != cond]
        changed.pop("_id", None)
        return changed

    async def update_one(self, query, update) -> _MongoLikeResult:
        rows = await self.find(query).limit(1).to_list(1)
        if not rows:
            return _MongoLikeResult(matched_count=0, modified_count=0)
        row = rows[0]
        changes = self._apply_update_ops(row, update)
        self._table().update(changes).eq("_id", row["_id"]).execute()
        return _MongoLikeResult(matched_count=1, modified_count=1)

    async def update_many(self, query, update) -> _MongoLikeResult:
        rows = await self.find(query).to_list(None)
        for row in rows:
            changes = self._apply_update_ops(row, update)
            self._table().update(changes).eq("_id", row["_id"]).execute()
        return _MongoLikeResult(matched_count=len(rows), modified_count=len(rows))

    async def delete_one(self, query) -> _MongoLikeResult:
        rows = await self.find(query).limit(1).to_list(1)
        if not rows:
            return _MongoLikeResult(deleted_count=0)
        self._table().delete().eq("_id", rows[0]["_id"]).execute()
        return _MongoLikeResult(deleted_count=1)

    async def delete_many(self, query) -> _MongoLikeResult:
        rows = await self.find(query).to_list(None)
        for row in rows:
            self._table().delete().eq("_id", row["_id"]).execute()
        return _MongoLikeResult(deleted_count=len(rows))

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> SupabaseCursor:
        return SupabaseCursor(self, pipeline=pipeline)

    async def create_index(self, *args, **kwargs) -> bool:
        return True  # indexes are managed in Supabase directly now

    async def _run_pipeline(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        resp = self._table().select("*").limit(5000).execute()
        rows = resp.data or []
        for stage in pipeline:
            if "$match" in stage:
                rows = [r for r in rows if _matches_query(r, stage["$match"])]
            elif "$project" in stage:
                rows = [
                    {k: (_evaluate_expression(v, r) if isinstance(v, dict) else r.get(k)) for k, v in stage["$project"].items() if v}
                    for r in rows
                ]
            elif "$sort" in stage:
                rows = _apply_sort(rows, stage["$sort"])
            elif "$limit" in stage:
                rows = rows[: stage["$limit"]]
            elif "$skip" in stage:
                rows = rows[stage["$skip"]:]
            elif "$addFields" in stage:
                rows = [{**r, **{k: _evaluate_expression(v, r) for k, v in stage["$addFields"].items()}} for r in rows]
            elif "$replaceRoot" in stage:
                root_expr = stage["$replaceRoot"]["newRoot"]
                rows = [_evaluate_expression(root_expr, r) for r in rows]
            elif "$group" in stage:
                rows = self._apply_group(rows, stage["$group"])
            # $lookup / $unwind intentionally unsupported here — not used on People pipelines.
        return rows

    @staticmethod
    def _apply_group(rows, group_spec):
        groups: Dict[str, List[Dict[str, Any]]] = {}
        group_id_spec = group_spec.get("_id")
        for row in rows:
            key = _evaluate_expression(group_id_spec, row) if group_id_spec is not None else None
            groups.setdefault(str(key), []).append(row)
        results = []
        for docs in groups.values():
            result = {"_id": _evaluate_expression(group_id_spec, docs[0]) if group_id_spec is not None else None}
            for field, expr in group_spec.items():
                if field == "_id":
                    continue
                if isinstance(expr, dict) and "$sum" in expr:
                    result[field] = len(docs) if expr["$sum"] == 1 else sum(
                        float(_evaluate_expression(expr["$sum"], d) or 0) for d in docs
                    )
                elif isinstance(expr, dict) and "$push" in expr:
                    result[field] = [_evaluate_expression(expr["$push"], d) for d in docs]
                elif isinstance(expr, dict) and "$first" in expr:
                    result[field] = _evaluate_expression(expr["$first"], docs[0])
                else:
                    result[field] = _evaluate_expression(expr, docs[0])
            results.append(result)
        return results


# The one export the rest of main.py already imports as `people_collection`.
# It looks and behaves like a Motor collection but every call goes to Supabase.
people_collection = SupabasePeopleCollection(supabase, PEOPLE_TABLE)


# ============================================================================
# MONGODB — everything else (Events, Users, Tasks, TaskTypes, Organizations,
# OrgConfig, Consolidations) — unchanged for now.
# ============================================================================
MONGO_URI = os.getenv("MONGO_URI", "None")
DB_NAME = os.getenv("DB_NAME", "active-teams-db")

print(f"--- CONNECTING TO DB: {DB_NAME} ---")

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]

events_collection = db["Events"]
users_collection = db["Users"]
tasks_collection = db["tasks"]
tasktypes_collection = db["TaskTypes"]
org_config_collection = db["OrgConfig"]
consolidations_collection = db["consolidations"]
organizations_collection = db["organizations"]


def get_database():
    return db