"""READ-ONLY. Measures the serialised HTTP response size of /events/eventsdata.

The DB read is 4.35 MB, but the endpoint synthesises one instance per week and
attaches an enriched attendee list to each. The response actually sent to the
browser may be far larger, and it is serialised, transferred and parsed on the
critical path.
"""

import asyncio
import contextlib
import io
import json
import os
import sys
import time

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import resolve_mongo_uri, redact_uri  # noqa: E402
import main  # noqa: E402

FAKE_USER = {
    "email": "diag@local", "role": "admin", "org_id": "active-teams",
    "Organization": "Active Church", "name": "Diag", "surname": "User",
}


async def call(start, limit):
    buf = io.StringIO()
    t0 = time.perf_counter()
    with contextlib.redirect_stdout(buf):
        res = await main.get_other_events(
            current_user=FAKE_USER, page=1, limit=limit, status=None,
            event_type=None, search=None, personal=None,
            start_date=start, end_date=None, show_all_dates=False,
        )
    return res, time.perf_counter() - t0


async def run():
    client = AsyncIOMotorClient(resolve_mongo_uri())
    db = client[os.getenv("DB_NAME", "active-teams-db")]
    for attr, coll in [
        ("events_collection", "Events"), ("people_collection", "People"),
        ("users_collection", "Users"), ("tasks_collection", "tasks"),
        ("tasktypes_collection", "TaskTypes"),
        ("org_config_collection", "OrgConfig"),
        ("consolidations_collection", "consolidations"),
        ("organizations_collection", "organizations"),
    ]:
        setattr(main, attr, db[coll])

    print(f"DB: {os.getenv('DB_NAME', 'active-teams-db')} @ {redact_uri(resolve_mongo_uri())}\n")
    print("=== response size actually sent to the browser ===\n")
    print(f"{'start_date':<14} {'page limit':>10} {'instances':>10} "
          f"{'JSON size':>12} {'serialise':>10} {'handler':>9}")
    print("-" * 72)

    for start, limit in [("2025-10-10", 25), ("2025-10-10", 100),
                         ("2025-01-01", 25), ("2025-01-01", 100)]:
        res, handler_s = await call(start, limit)
        t0 = time.perf_counter()
        blob = json.dumps(res, default=str)
        ser_s = time.perf_counter() - t0
        print(f"{start:<14} {limit:>10} {res['total_events']:>10} "
              f"{len(blob)/1e6:>10.2f}MB {ser_s:>9.2f}s {handler_s:>8.1f}s")

    print("\n=== which instance fields dominate the response? ===")
    res, _ = await call("2025-10-10", 25)
    totals = {}
    for ev in res["events"]:
        for k, v in ev.items():
            totals[k] = totals.get(k, 0) + len(json.dumps(v, default=str))
    total = sum(totals.values())
    for k, v in sorted(totals.items(), key=lambda kv: -kv[1])[:12]:
        print(f"  {v/1e6:>7.2f}MB {100*v/total:>5.1f}%  {k}")


if __name__ == "__main__":
    asyncio.run(run())
