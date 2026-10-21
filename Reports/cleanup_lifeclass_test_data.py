#!/usr/bin/env python3
"""
cleanup_lifeclass_test_data.py
=================================
Deletes exactly the rows recorded in test_manifest.json (written by
populate_lifeclass_test_data.py) from your real Supabase project.
Deletes in FK-safe order: attendees, then events, then people.

Run this after every test run, even if the test failed - the orchestrator
(test_lifeclass_report_supabase.py) already does this automatically via
try/finally, so you normally won't need to run this by hand. It's here as
a manual safety net if a run gets interrupted (Ctrl-C, crash, etc.).
"""

import json
import os
import sys

from supabase_lifeclass_data import get_client

MANIFEST_PATH = 'test_manifest.json'


def chunked(seq, size=200):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def main():
    if not os.path.exists(MANIFEST_PATH):
        print(f'No {MANIFEST_PATH} found - nothing to clean up.')
        return

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    client = get_client()

    n_att = n_ev = n_ppl = 0
    for chunk in chunked(manifest.get('attendee_ids', [])):
        if chunk:
            client.table('event_attendees').delete().in_('attendee_id', chunk).execute()
            n_att += len(chunk)
    for chunk in chunked(manifest.get('event_ids', [])):
        if chunk:
            client.table('events').delete().in_('event_id', chunk).execute()
            n_ev += len(chunk)
    for chunk in chunked(manifest.get('people_ids', [])):
        if chunk:
            client.table('People').delete().in_('_id', chunk).execute()
            n_ppl += len(chunk)

    print(f'Deleted {n_att} attendee rows, {n_ev} events, {n_ppl} people '
          f'(run tag: {manifest.get("run_tag")}).')
    os.remove(MANIFEST_PATH)


if __name__ == '__main__':
    main()