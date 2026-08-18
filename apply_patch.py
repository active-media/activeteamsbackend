"""
apply_patch.py
--------------
Run this from the same directory as main.py to apply all three changes:

    python apply_patch.py

It makes a backup (main.py.bak) before modifying anything.
"""

import re
import shutil
import sys
from pathlib import Path

MAIN = Path("main.py")

if not MAIN.exists():
    sys.exit("ERROR: main.py not found in current directory.")

# ── Backup ────────────────────────────────────────────────────────────────────
shutil.copy(MAIN, MAIN.with_suffix(".py.bak"))
print("Backup created: main.py.bak")

src = MAIN.read_text(encoding="utf-8")
original_len = len(src)


# ── CHANGE 1: add router imports ──────────────────────────────────────────────
OLD1 = "from supreme_admin import router as supreme_admin_router\nfrom contextlib import asynccontextmanager"
NEW1 = (
    "from supreme_admin import router as supreme_admin_router\n"
    "from contextlib import asynccontextmanager\n"
    "from daily_tasks import router as tasks_router\n"
    "from task_types import router as task_types_router"
)

if OLD1 not in src:
    print("WARN: Change-1 anchor not found — skipping (imports may already exist).")
else:
    src = src.replace(OLD1, NEW1, 1)
    print("✓ Change 1 applied: router imports added.")


# ── CHANGE 2: register routers in include_router() ────────────────────────────
OLD2 = "def include_router(app):\n    #app.include_router(events_router)\n    pass"
NEW2 = (
    "def include_router(app):\n"
    "    # Task endpoints (Supabase-backed)\n"
    "    app.include_router(tasks_router)\n"
    "    app.include_router(task_types_router)"
)

if OLD2 not in src:
    print("WARN: Change-2 anchor not found — skipping (include_router may already be updated).")
else:
    src = src.replace(OLD2, NEW2, 1)
    print("✓ Change 2 applied: routers registered in include_router().")


# ── CHANGE 3: remove duplicate route handlers ─────────────────────────────────
# Each pattern matches from the @app decorator to (but NOT including) the next
# top-level @app or async def that is not one of the deleted handlers.
# We use DOTALL so '.' matches newlines, and we stop at the *next* @app.

HANDLERS_TO_REMOVE = [
    # POST /tasks  (the MongoDB version — the one that calls db["tasks"])
    r'@app\.post\("/tasks"\)\nasync def create_task\(task: TaskModel.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /tasks/my-special-tasks
    r'@app\.get\("/tasks/my-special-tasks"\)\nasync def get_my_special_tasks\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /tasks  (the main listing endpoint)
    r'@app\.get\("/tasks"\)\nasync def get_user_tasks\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /tasktypes
    r'@app\.get\("/tasktypes".*?\)\nasync def get_task_types\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # POST /tasktypes
    r'@app\.post\("/tasktypes".*?\)\nasync def create_task_type\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # PUT /tasktypes/{tasktype_id}
    r'@app\.put\("/tasktypes/\{tasktype_id\}"\)\nasync def update_task_type\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # DELETE /tasktypes/{tasktype_id}
    r'@app\.delete\("/tasktypes/\{tasktype_id\}"\)\nasync def delete_task_type\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # PUT /tasks/{task_id}
    r'@app\.put\("/tasks/\{task_id\}"\)\nasync def update_task\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /tasks/all  — the standalone async def (not the inner one)
    r'@app\.get\("/tasks/all"\)\nasync def get_all_tasks\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /tasks/leader/{leader_email}
    r'@app\.get\("/tasks/leader/\{leader_email\}"\)\nasync def get_leader_tasks\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # DELETE /tasks/cleanup-orphaned
    r'@app\.delete\("/tasks/cleanup-orphaned"\)\nasync def cleanup_orphaned_tasks\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',

    # GET /stats/dashboard-quick  (the MongoDB version — keep comprehensive)
    r'@app\.get\("/stats/dashboard-quick"\)\nasync def get_dashboard_quick_stats\(.*?(?=\n@app\.|\nfrom |\nclass |\Z)',
]

REPLACEMENT_COMMENT = (
    "\n"
    "# ─────────────────────────────────────────────────────────────────────────────\n"
    "# Task & Task-Type routes are handled by the dedicated routers:\n"
    "#   daily_tasks.py  →  /tasks, /tasks/all, /tasks/leader/{email},\n"
    "#                       /tasks/my-special-tasks, /tasks/cleanup-orphaned,\n"
    "#                       /stats/dashboard-quick\n"
    "#   task_types.py   →  /tasktypes  (GET / POST / PUT / DELETE)\n"
    "# Both routers are registered in include_router() above.\n"
    "# ─────────────────────────────────────────────────────────────────────────────\n"
)

removed = 0
for pattern in HANDLERS_TO_REMOVE:
    new_src, n = re.subn(pattern, "", src, flags=re.DOTALL)
    if n:
        src = new_src
        removed += n
        print(f"  ✓ Removed handler matching: {pattern[:60]}...")
    else:
        print(f"  WARN: No match for pattern: {pattern[:60]}...")

# Insert the comment before the stats/dashboard-comprehensive handler
# so there's a clear marker where the deleted routes used to be.
ANCHOR = '@app.get("/stats/dashboard-comprehensive")'
if ANCHOR in src:
    src = src.replace(ANCHOR, REPLACEMENT_COMMENT + ANCHOR, 1)
    print("✓ Replacement comment inserted before /stats/dashboard-comprehensive.")
else:
    # Append at end of file as fallback
    src += REPLACEMENT_COMMENT
    print("WARN: /stats/dashboard-comprehensive not found; comment appended at EOF.")

print(f"\n✓ Change 3 applied: {removed} duplicate handler(s) removed.")


# ── Write result ───────────────────────────────────────────────────────────────
MAIN.write_text(src, encoding="utf-8")
print(f"\nDone. main.py updated ({original_len} → {len(src)} chars).")
print("Run `grep -n 'async def create_task\\|async def get_task_types' main.py` to verify.")