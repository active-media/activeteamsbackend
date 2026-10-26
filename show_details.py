#!/usr/bin/env python3
import os
import sys

path = r"C:\Users\madnh\OneDrive\Desktop\2026 - Development\Client Work\Active Teams\activeteamsbackend"
errors = "replace"


def show_section(title, file_rel, searches):
    full = os.path.join(path, file_rel)
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
    try:
        with open(full, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")
    except Exception:
        with open(full, "r", encoding="utf-8", errors=errors) as f:
            lines = f.read().split("\n")
    for search in searches:
        found = False
        for i, line in enumerate(lines, 1):
            if search in line:
                print(f"  Line {i}: {line.strip()}")
                found = True
        if not found:
            print(f"  No match for: {search}")


# 1. Period tokens used
show_section(
    "1. PERIOD TOKENS USED",
    "supabase_helpers/report_schedule_runner.py",
    ["period_token", "previousWeek", "previousMonth", "previous7", "thisMonth"],
)

# Also manually show the period token assignments
with open(os.path.join(path, "supabase_helpers", "report_schedule_runner.py"), "r", encoding="utf-8", errors=errors) as f:
    lines = f.read().split("\n")
print("\n  compute_next_run period token assignments:")
for i, line in enumerate(lines, 1):
    if "period_token" in line and ("freq" in line or "f ==" in line or "schedule.get" in line):
        print(f"    Line {i}: {line.strip()}")

print("\n  No 'yesterday' token exists in resolve_period")
print("  resolve_period supports: today, thisWeek, thisMonth, previous7,")
print("    previousWeek, previousMonth, thisQuarter, yearly, YYYY-MM")
print()


# 2. Manual run endpoint
show_section(
    "2. MANUAL RUN ENDPOINT - does NOT claim/modify next_run_at",
    "main.py",
    ["run_schedule_now", "next_run_at"],
)

# 3. Claim query behavior
show_section(
    "3. CLAIM QUERY behavior when next_run_at is NULL",
    "supabase_helpers/report_schedule_runner.py",
    ["eq(\"next_run_at\"", "updated.data", "schedule.get", "if not schedule"],
)

# 4. compute_next_run call points
show_section(
    "4. compute_next_run CALL POINTS",
    "supabase_helpers/report_schedule_runner.py",
    ["compute_next_run"],
)

with open(os.path.join(path, "supabase_helpers", "report_schedules.py"), "r", encoding="utf-8", errors=errors) as f:
    lines = f.read().split("\n")
print("\n  In report_schedules.py:")
for i, line in enumerate(lines, 1):
    if "compute_next_run" in line or ("mark_schedule_run" in line and "next_run_at" in line):
        print(f"    Line {i}: {line.strip()}")

print("\n  Summary: _run_due_schedules() calls it per schedule claimed")


# 5. send_report_email choice
show_section(
    "5. send_report_email: Gmail vs Resend selection",
    "auth/email_utils.py",
    ["send_report_email", "GMAIL_APP_PASSWORD", "timeout", "return False"],
)

# 6. _is_due when NULL
show_section(
    "6. _is_due when next_run_at is NULL",
    "supabase_helpers/report_schedule_runner.py",
    ["_is_due", "next_run", "timeofday", "active"],
)

print("\n--- DETAILED ANALYSIS ---")

# Detailed analysis sections
print("\n1. PERIOD TOKENS - DETAIL:")
with open(os.path.join(path, "supabase_helpers", "report_schedule_runner.py"), "r", encoding="utf-8", errors=errors) as f:
    c = f.read()
lines = c.split("\n")
for i, line in enumerate(lines, 1):
    if "period_token" in line:
        print(f"  Line {i}: {line.strip()}")

print("\n2. CLAIM QUERY - DETAIL:")
for i, line in enumerate(lines, 1):
    if "eq(" in line and "next_run_at" in line:
        print(f"  Line {i}: {line.strip()}")
    if "updated.data" in line:
        print(f"  Line {i}: {line.strip()}")
    if "schedule.get" in line and "next_run_at" in line:
        print(f"  Line {i}: {line.strip()}")

print("\n3. _is_due - DETAIL:")
for i, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped and '_is_due' in stripped.lower():
        print(f"  Line {i}: {stripped}")

print("\n4. MANUAL RUN ENDPOINT - DETAIL (first 50 lines):")
with open(os.path.join(path, "main.py"), "r", encoding="utf-8", errors=errors) as f:
    lines = f.read().split("\n")
found = False
for i, line in enumerate(lines, 1):
    if "run_schedule_now" in line:
        found = True
    if found:
        print(f"  Line {i}: {line.strip()}")
        if i > 100:  # limit output
            break

print("\n5. SEND REPORT EMAIL - DETAIL:")
with open(os.path.join(path, "auth", "email_utils.py"), "r", encoding="utf-8", errors=errors) as f:
    lines = f.read().split("\n")
for i, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped and ("GMAIL_APP_PASSWORD" in stripped or "send_report_email" in stripped or "timeout" in stripped or "return False" in stripped):
        print(f"  Line {i}: {stripped}")