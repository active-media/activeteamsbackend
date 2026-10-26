#!/usr/bin/env python3
import os

path = r"C:\Users\madnh\OneDrive\Desktop\2026 - Development\Client Work\Active Teams\activeteamsbackend"


def show(title, file_rel, searches):
    full = os.path.join(path, file_rel)
    print("=" * 60)
    print(title)
    print("=" * 60)
    with open(full, "r") as f:
        lines = f.read().split("\n")
    for search in searches:
        found = False
        for i, line in enumerate(lines, 1):
            if search in line:
                print(f"  Line {i}: {line.strip()}")
                found = True
        if not found:
            print(f"  No match for: {search}")
    print()


# 1. Period tokens used
show(
    "1. PERIOD TOKENS USED",
    "supabase_helpers/report_schedule_runner.py",
    [
        "period_token",
        "previousWeek",
        "previousMonth",
        "previous7",
        "thisMonth",
    ],
)

# Also show the compute_next_run period logic
with open(os.path.join(path, "supabase_helpers", "report_schedule_runner.py"), "r") as f:
    c = f.read()
lines = c.split("\n")
print("  compute_next_run period token assignments:")
for i, line in enumerate(lines, 1):
    if 'period_token' in line and ('freq' in line or 'f ==' in line):
        print(f"    Line {i}: {line.strip()}")

# 2. Manual run endpoint
show(
    "2. MANUAL RUN ENDPOINT - does NOT claim/modify next_run_at",
    "main.py",
    [
        "run_schedule_now",
        "atomic claim",
        "next_run_at",
    ],
)

# 3. Claim query
show(
    "3. CLAIM QUERY behavior when next_run_at is NULL",
    "supabase_helpers/report_schedule_runner.py",
    [
        'eq("next_run_at"',
        "updated.data",
        "schedule.get",
        "if not schedule",
    ],
)

# 4. compute_next_run call points
show(
    "4. compute_next_run CALL POINTS",
    "supabase_helpers/report_schedule_runner.py",
    ["compute_next_run"],
)

with open(os.path.join(path, "supabase_helpers", "report_schedules.py"), "r") as f:
    c = f.read()
lines = c.split("\n")
print("  In report_schedules.py:")
for i, line in enumerate(lines, 1):
    if "compute_next_run" in line or ("mark_schedule_run" in line and "next_run_at" in line):
        print(f"    Line {i}: {line.strip()}")

# 5. send_report_email choice
show(
    "5. send_report_email: Gmail vs Resend selection",
    "auth/email_utils.py",
    [
        "send_report_email",
        "GMAIL_APP_PASSWORD",
        "timeout",
        "return False",
    ],
)

# 6. _is_due when NULL
show(
    "6. _is_due when next_run_at is NULL",
    "supabase_helpers/report_schedule_runner.py",
    [
        "_is_due",
        "next_run",
        "timeofday",
        "active",
    ],
)