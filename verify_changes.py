#!/usr/bin/env python3
import os

path = r"C:\Users\madnh\OneDrive\Desktop\2026 - Development\Client Work\Active Teams\activeteamsbackend"

files = {
    "report_schedule_runner.py": "supabase_helpers/report_schedule_runner.py",
    "email_utils.py": "auth/email_utils.py",
    "report_schedules.py": "supabase_helpers/report_schedules.py",
}

for name, rel in files.items():
    full = os.path.join(path, rel)
    print(f"=== {name} ===")
    with open(full, "r") as f:
        c = f.read()
    checks = {
        "report_schedule_runner.py": [
            "def compute_next_run",
            "def _send_report_email",
            "def _generate_report",
            'eq("next_run_at"',  # atomic claim
            "asyncio.to_thread",  # should NOT be here (sync runner)
        ],
        "email_utils.py": [
            "def send_report_email",
            "timeout=30",
        ],
        "report_schedules.py": [
            "def compute_next_run",
            "def mark_schedule_run",
            "SAST = ZoneInfo",
        ],
    }
    for chk in checks.get(name, []):
        present = chk in c
        print(f"  {chk}: {'YES' if present else 'NO'}")
    print()
PYEOF