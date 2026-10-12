"""
inspect_supabase_data.py  (READ-ONLY)

Prints and saves a snapshot of what is currently in the Active Teams Supabase
project so a realistic test-data seeder can be written against the real schema.

Run from the backend root (the folder that contains `supabase_helpers/`):

    python inspect_supabase_data.py
    python inspect_supabase_data.py --samples 5 --out schema_snapshot.json
    python inspect_supabase_data.py --tables events Users tasks

What it does
  1. Discovers every table + column + type from the PostgREST OpenAPI spec
     (needs the service-role key, which `supabase_admin` already uses).
  2. For each table: exact row count, N sample rows, and the distinct values of
     low-cardinality columns (status, role, event type, Organization, ...).
  3. Flags things the reporting dashboard cares about (campus, cells, leaders).
  4. Masks obvious PII / secrets in samples, writes everything to a JSON file.

It never inserts, updates or deletes anything.
"""
import argparse
import json
import re
import sys
from collections import Counter

import os
import httpx

# Make `supabase_helpers` importable whether this file sits in the backend root
# or inside supabase_helpers/, and whichever folder it is run from.
_here = os.path.dirname(os.path.abspath(__file__))
for _p in (_here, os.path.dirname(_here)):
    if os.path.isdir(os.path.join(_p, "supabase_helpers")) and _p not in sys.path:
        sys.path.insert(0, _p)
        os.chdir(_p)  # so .env files and relative paths resolve like the backend
        break

try:
    from supabase_helpers.supabase_connection import supabase_admin as client
except Exception as exc:  # pragma: no cover
    print(f"Could not import supabase_admin from supabase_helpers.supabase_connection: {exc}")
    print("Run this script from the backend root folder.")
    sys.exit(1)

SENSITIVE = re.compile(
    r"(pass(word)?|token|secret|hash|api[_-]?key|email|phone|mobile|cell_?number|"
    r"id_?number|address|birth|dob)", re.I)

# Used only if the OpenAPI spec cannot be read.
FALLBACK_TABLES = [
    "events", "Users", "tasks", "task_types", "event_types", "attendance",
    "service_targets", "report_schedules", "campuses", "cells", "organizations",
]


def mask(key, value):
    if value is None or not SENSITIVE.search(key):
        return value
    return "***masked***"


def get_spec():
    url = f"{client.supabase_url}/rest/v1/"
    headers = {"apikey": client.supabase_key, "Authorization": f"Bearer {client.supabase_key}"}
    r = httpx.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    spec = r.json()
    defs = spec.get("definitions") or spec.get("components", {}).get("schemas") or {}
    tables = {}
    for name, d in defs.items():
        props = d.get("properties", {})
        required = set(d.get("required", []))
        tables[name] = {
            col: {
                "type": p.get("format") or p.get("type"),
                "required": col in required,
                "default": p.get("default"),
                "description": p.get("description"),  # often contains "Primary Key" / "Foreign Key"
            }
            for col, p in props.items()
        }
    return tables


def inspect_table(name, cols, n_samples):
    out = {"columns": cols}
    try:
        head = client.table(name).select("*", count="exact").limit(1).execute()
        out["row_count"] = head.count
    except Exception as exc:
        out["error"] = str(exc)[:300]
        return out

    try:
        rows = client.table(name).select("*").limit(1000).execute().data or []
    except Exception as exc:
        out["error"] = str(exc)[:300]
        return out

    out["samples"] = [{k: mask(k, v) for k, v in r.items()} for r in rows[:n_samples]]

    if not cols and rows:  # fallback table with no spec: derive columns from data
        out["columns"] = {k: {"type": type(v).__name__} for k, v in rows[0].items()}

    distinct = {}
    for col in (out["columns"] or {}):
        if SENSITIVE.search(col):
            continue
        vals = [r.get(col) for r in rows if not isinstance(r.get(col), (dict, list))]
        if not vals:
            continue
        counts = Counter(vals)
        if len(counts) <= 15 and len(rows) > 0:
            distinct[col] = dict(counts.most_common())
    out["low_cardinality_values"] = distinct
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=3)
    ap.add_argument("--out", default="schema_snapshot.json")
    ap.add_argument("--tables", nargs="*", help="limit to these tables")
    args = ap.parse_args()

    try:
        spec = get_spec()
        print(f"Discovered {len(spec)} tables/views from the API schema.\n")
    except Exception as exc:
        print(f"[warn] OpenAPI discovery failed ({exc}); falling back to a guessed table list.\n")
        spec = {t: {} for t in FALLBACK_TABLES}

    names = args.tables or sorted(spec)
    snapshot = {}
    for name in names:
        info = inspect_table(name, spec.get(name, {}), args.samples)
        snapshot[name] = info
        if "error" in info:
            print(f"- {name}: ERROR {info['error']}")
            continue
        print(f"- {name}: {info['row_count']} rows, {len(info['columns'])} columns")
        print("    columns: " + ", ".join(f"{c}:{m.get('type')}" for c, m in info["columns"].items()))
        for col, vals in info.get("low_cardinality_values", {}).items():
            print(f"    {col} -> {vals}")

    # Dashboard-specific checks
    print("\n=== Reporting dashboard checks ===")
    ev = snapshot.get("events", {}).get("columns", {})
    print(f"events.campus_id exists: {'campus_id' in ev}")
    campus_like = [t for t in snapshot if "campus" in t.lower()]
    print(f"campus-like tables: {campus_like or 'none found'}")
    for t, info in snapshot.items():
        cols = info.get("columns") or {}
        hits = [c for c in cols if re.search(r"campus|cell|leader|org", c, re.I)]
        if hits:
            print(f"  {t}: {hits}")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, default=str)
    print(f"\nFull snapshot written to {args.out}")


if __name__ == "__main__":
    main()