#!/usr/bin/env python3
"""
populate_lifeclass_test_data.py
==================================
Writes a small, clearly-tagged set of fixture data into your REAL Supabase
project so test_lifeclass_report_supabase.py has something realistic to
pull the report from.

SAFETY
------
Every row this script creates is tagged so it can never be mistaken for
real congregation data and can always be fully removed:
  - every person's full_name is prefixed "ZZTEST "
  - every cohort's events.event_name is prefixed "ZZTEST_LifeClass_"
  - every id created is written to test_manifest.json as it's inserted,
    so cleanup_lifeclass_test_data.py can delete exactly those rows even
    if this script is interrupted partway through.

This does NOT touch any existing data - it only INSERTs new rows.

ASSUMPTIONS ABOUT THE PEOPLE TABLE
-----------------------------------
We only confirmed _id / email / gender on People from your message. This
script also writes full_name and Organization (matching the "Active Church"
value seen in your sample events row) - if People has other NOT NULL
columns with no default, the insert will fail with a clear Postgres error
naming the column; add it to PEOPLE_EXTRA_FIELDS below and re-run.

Requires: supabase-py, and SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY set.
"""

import json
import os
import uuid
from datetime import datetime, timedelta

from supabase_lifeclass_data import get_client

RUN_TAG = 'ZZTEST_' + datetime.utcnow().strftime('%Y%m%d%H%M%S')
ORG = 'Active Church'  # matches the sample org from your events row
PEOPLE_EXTRA_FIELDS = {}  # add e.g. {'Organization': ORG} overrides here if inserts fail

manifest = {'people_ids': [], 'event_ids': [], 'attendee_ids': [], 'run_tag': RUN_TAG,
            'men_cohort_name': f'{RUN_TAG}_Men', 'women_cohort_name': f'{RUN_TAG}_Women'}


def new_person_id():
    return 'ZZTEST' + uuid.uuid4().hex[:20]


def insert_person(client, name, gender, email):
    pid = new_person_id()
    row = {'_id': pid, 'Name': f'ZZTEST {name}', 'Email': email, 'Gender': gender,
           'Organization': ORG}
    row.update(PEOPLE_EXTRA_FIELDS)
    client.table('People').insert(row).execute()
    manifest['people_ids'].append(pid)
    return pid, row['Name']


def insert_cohort_events(client, cohort_name, n_sessions=9, start_date=None):
    """Creates n_sessions events sharing event_name=cohort_name, one week apart."""
    start_date = start_date or (datetime.utcnow() - timedelta(weeks=n_sessions))
    event_ids = []
    for i in range(n_sessions):
        eid = str(uuid.uuid4())
        row = {
            'event_id': eid,
            'event_name': cohort_name,
            'event_type_name': 'Life Class',
            'Organization': ORG,  # capital O on events, confirmed against live schema
            'event_leader': 'ZZTEST Fixture',
            'event_date': (start_date + timedelta(weeks=i)).strftime('%Y-%m-%d 00:00:00+00'),
            'is_active': True,
            'status': 'Complete',
            'source_format': 'old',
        }
        client.table('events').insert(row).execute()
        manifest['event_ids'].append(eid)
        event_ids.append(eid)
    return event_ids


def insert_attendee(client, event_id, person_id, full_name, email, leader12, leader144,
                     regtype, checked_in):
    aid = str(uuid.uuid4())
    row = {
        'attendee_id': aid,
        'event_id': event_id,
        'mongo_person_id': person_id,
        'full_name': full_name,
        'email': email,
        'leader12': leader12,
        'leader144': leader144,
        'registration_type': regtype,
        'is_persistent': checked_in,
    }
    client.table('event_attendees').insert(row).execute()
    manifest['attendee_ids'].append(aid)


def build_cohort(client, cohort_name, roster):
    """
    roster: list of dicts:
        {name, gender, regtype, leader12_key, leader144_key, attendance}
    leader12_key / leader144_key refer to the *name* (not id) of another
    roster member, or None. attendance: dict {week_index: True/False} - only
    weeks present get an event_attendees row written (weeks omitted entirely
    are left as "never registered" to also exercise the missing-row path).
    """
    event_ids = insert_cohort_events(client, cohort_name)
    name_to_person = {}
    for person in roster:
        pid, tagged_name = insert_person(client, person['name'], person['gender'],
                                          f"{person['name'].lower().replace(' ', '.')}@zztest.example.com")
        name_to_person[person['name']] = (pid, tagged_name)

    for person in roster:
        pid, tagged_name = name_to_person[person['name']]
        leader12_name = name_to_person[person['leader12_key']][1] if person['leader12_key'] else None
        leader144_name = name_to_person[person['leader144_key']][1] if person['leader144_key'] else None
        for wi, checked_in in person['attendance'].items():
            insert_attendee(client, event_ids[wi], pid, tagged_name,
                             f"{person['name'].lower().replace(' ', '.')}@zztest.example.com",
                             leader12_name, leader144_name, person['regtype'], checked_in)
    return {p['name']: name_to_person[p['name']][1] for p in roster}


def men_roster():
    return [
        {'name': 'John Leader', 'gender': 'male', 'regtype': 'Guide',
         'leader12_key': None, 'leader144_key': None,
         'attendance': {i: True for i in range(9)}},
        {'name': 'Mike Guide', 'gender': 'male', 'regtype': 'Guide',
         'leader12_key': 'John Leader', 'leader144_key': None,
         'attendance': {0: True, 1: True, 2: True, 3: False}},
        {'name': 'Steve Delegate', 'gender': 'male', 'regtype': 'Delegate: First Time',
         'leader12_key': 'John Leader', 'leader144_key': 'Mike Guide',
         'attendance': {0: True, 1: True}},
        {'name': 'Tom Delegate', 'gender': 'male', 'regtype': 'Delegate: Second Time',
         'leader12_key': 'John Leader', 'leader144_key': 'Mike Guide',
         'attendance': {2: True, 3: True}},
        {'name': 'Second Leader', 'gender': 'male', 'regtype': 'Guide',
         'leader12_key': None, 'leader144_key': None,
         'attendance': {i: True for i in range(9)}},
        {'name': 'Alex Delegate', 'gender': 'male', 'regtype': 'Delage: First Time ',  # messy text on purpose
         'leader12_key': 'Second Leader', 'leader144_key': None,
         'attendance': {0: True, 4: True}},
        {'name': 'Nina Delegate', 'gender': 'male', 'regtype': 'Guide ',  # messy text on purpose
         'leader12_key': 'Second Leader', 'leader144_key': None,
         'attendance': {4: True, 5: True, 6: True, 7: True}},
        {'name': 'Priya Delegate', 'gender': 'male', 'regtype': 'Delegate:Second Time',  # messy text on purpose
         'leader12_key': 'Second Leader', 'leader144_key': 'Nina Delegate',
         'attendance': {5: True, 6: True}},
    ]


def women_roster():
    return [
        {'name': 'Mary Leader', 'gender': 'female', 'regtype': 'Guide',
         'leader12_key': None, 'leader144_key': None,
         'attendance': {i: True for i in range(9)}},
        {'name': 'Grace Guide', 'gender': 'female', 'regtype': 'Guide',
         'leader12_key': 'Mary Leader', 'leader144_key': None,
         'attendance': {0: True, 1: True, 2: True, 3: True}},
        {'name': 'Sarah Delegate', 'gender': 'female', 'regtype': 'Delegate: First Time',
         'leader12_key': 'Mary Leader', 'leader144_key': 'Grace Guide',
         'attendance': {0: True}},
        {'name': 'Ruth Delegate', 'gender': 'female', 'regtype': 'delegate: second time',  # messy casing
         'leader12_key': 'Mary Leader', 'leader144_key': 'Grace Guide',
         'attendance': {1: True, 2: True}},
    ]


def main():
    client = get_client()
    print(f'Populating fixture data (run tag: {RUN_TAG}) ...')
    try:
        men_names = build_cohort(client, manifest['men_cohort_name'], men_roster())
        women_names = build_cohort(client, manifest['women_cohort_name'], women_roster())
    finally:
        with open('test_manifest.json', 'w') as f:
            json.dump(manifest, f, indent=2)
        print(f"Manifest written to test_manifest.json "
              f"({len(manifest['people_ids'])} people, {len(manifest['event_ids'])} events, "
              f"{len(manifest['attendee_ids'])} attendee rows so far).")

    print(f"Men cohort:   {manifest['men_cohort_name']}")
    print(f"Women cohort: {manifest['women_cohort_name']}")
    return manifest, men_names, women_names


if __name__ == '__main__':
    main()