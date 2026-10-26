#!/usr/bin/env python3
import os

path = r"C:\Users\madnh\OneDrive\Desktop\2026 - Development\Client Work\Active Teams\activeteamsbackend"

for name in ["report_schedule_runner.py", "email_utils.py", "report_schedules.py"]:
    full = os.path.join(path, "supabase_helpers" if name != "email_utils.py" else "auth", name)
    if not os.path.exists(full):
        full = os.path.join(path, "auth", name) if name == "email_utils.py" else full
    print(f"=== {name} ===")
    with open(full, "r") as f:
        c = f.read()
    checks = {}
    if name == "report_schedule_runner.py":
        checks = {
            "compute_next_run": "def compute_next_run" in c,
            "_send_report_email": "def _send_report_email" in c,
            "_generate_report": "def _generate_report" in c,
            "atomic claim 'eq':": 'eq("next_run_at"' in c,
            "asyncio.to_thread (BAD for sync)": "asyncio.to_thread" in c,
        }
    elif name == "email_utils.py":
        checks = {
            "send_report_email": "def send_report_email" in c,
            "timeout=30": "timeout=30" in c,
        }
    elif name == "report_schedules.py":
        checks = {
            "compute_next_run": "def compute_next_run" in c,
            "mark_schedule_run enhanced": "def mark_schedule_run" in c,
            "SAST = ZoneInfo": "SAST = ZoneInfo" in c,
        }
    for chk, result in checks.items():
        print(f"  {chk}: {'YES' if result else 'NO'}")
    print()