#!/usr/bin/env python3
"""
supabase_lifeclass_data.py
============================
Loads Life Class attendance straight from Supabase and reshapes it into the
same row format lifeclass_report.build_report() already consumes:

    {'name': str, 'leader12': str|None, 'leader144': str|None,
     'regtype': str|None, 'weeks': [ 'X'|None, ... 9 items ]}

This deliberately does NOT touch lifeclass_report.py's sheet-building logic
(that's already tested against 239 independent checks) - it only replaces
the CSV-reading step with a Supabase-reading step.

SCHEMA ASSUMPTIONS (confirmed with you):
  - events.event_type_name == 'Life Class' marks a session
  - the 9 sessions of one cohort share the same events.event_name; ordering
    by event_date ascending gives session #1..#9 -> WEEK1,2,3,4,ENCOUNTER,
    WEEK5,6,7,8. A cohort with fewer than 9 sessions so far is normal (report
    run mid-term) - only the sessions that exist are mapped/counted.
  - "attended" == event_attendees.is_persistent is true for that session
    (confirmed: is_persistent is the per-session attendance toggle here,
    despite the name suggesting something else)
  - event_attendees.leader12 / leader144 are already present
  - event_attendees.registration_type is a NEW column (see
    001_add_registration_type.sql) - not yet populated for real data
  - gender lives on People, joined via event_attendees.mongo_person_id ==
    People._id (falls back to matching on email if mongo_person_id is null)

Env vars required:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   (falls back to SUPABASE_KEY if not set -
                                  service role is needed to bypass RLS,
                                  per this project's existing convention)

Requires: supabase-py  (pip install supabase --break-system-packages)
"""

import os
from collections import OrderedDict, defaultdict

from dotenv import find_dotenv, load_dotenv
from supabase import create_client

# Auto-load a .env file if one exists anywhere from the current directory
# upward (e.g. activeteamsbackend/.env) - so this works the same way your
# FastAPI backend already picks up SUPABASE_URL / the service key, without
# you having to $env:-set them by hand every session.
load_dotenv(find_dotenv(usecwd=True))

WEEK_HEADERS = ['WEEK 1', 'WEEK 2', 'WEEK 3', 'WEEK 4', 'ENCOUNTER',
                'WEEK 5', 'WEEK 6', 'WEEK 7', 'WEEK 8']
N_WEEKS = len(WEEK_HEADERS)


def get_client():
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')
    if not url or not key:
        raise RuntimeError(
            'Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) '
            'in your environment before running this.')
    return create_client(url, key)


def check_schema(client):
    """
    Preflight check: confirms every column this module relies on actually
    exists before we make any real calls, so a mismatch fails fast with one
    clear message instead of an opaque PGRST204 deep in a stack trace.
    """
    required = {
        'events': ['event_id', 'event_name', 'event_date', 'event_type_name'],
        'event_attendees': ['event_id', 'mongo_person_id', 'full_name', 'email',
                             'leader12', 'leader144', 'is_persistent', 'registration_type'],
        'People': ['_id', 'Name', 'Email', 'Gender'],
    }
    missing = []
    for table, cols in required.items():
        try:
            # Selecting with limit 0 still validates column names against the
            # PostgREST schema cache without needing information_schema access
            # (some service-role setups restrict that).
            client.table(table).select(','.join(cols)).limit(0).execute()
        except Exception as e:
            missing.append(f'{table}: {e}')
    if missing:
        raise RuntimeError(
            'Schema check failed - one or more expected columns are missing or '
            'the schema cache is stale (try Settings -> API -> Reload schema cache '
            'in the Supabase dashboard):\n  ' + '\n  '.join(missing))


def _norm_gender(value):
    if not value:
        return None
    v = str(value).strip().lower()
    if v in ('male', 'm'):
        return 'male'
    if v in ('female', 'f'):
        return 'female'
    return None


def fetch_lifeclass_rows(client, cohort_event_names=None, organization=None, verbose=True):
    """
    Returns (men_rows, women_rows, warnings) in the shape build_report() needs.

    cohort_event_names: optional list of events.event_name values to restrict
        to (used by the test script to isolate its fixture cohorts from real
        data). If None, pulls every Life Class cohort found.
    organization: optional filter on events.organization.
    """
    warnings = []
    check_schema(client)

    # 1. Find every Life Class session
    q = client.table('events').select(
        'event_id, event_name, event_date, event_leader'
    ).eq('event_type_name', 'Life Class')
    if cohort_event_names is not None:
        q = q.in_('event_name', cohort_event_names)
    if organization is not None:
        q = q.eq('Organization', organization)  # capital O on events, confirmed against live schema
    events_resp = q.execute()
    events = events_resp.data or []
    if not events:
        warnings.append('No Life Class events found matching the given filters.')
        return [], [], warnings

    # 2. Group into cohorts by event_name, order by event_date -> week index
    cohorts = defaultdict(list)
    for ev in events:
        cohorts[ev['event_name']].append(ev)

    event_id_to_week = {}      # event_id -> week index (0-8)
    all_event_ids = []
    for cohort_name, sessions in cohorts.items():
        sessions.sort(key=lambda e: e['event_date'] or '')
        if len(sessions) > N_WEEKS:
            warnings.append(
                f'Cohort "{cohort_name}" has {len(sessions)} Life Class sessions '
                f'(expected at most {N_WEEKS}) - using the first {N_WEEKS} by date, '
                f'ignoring the rest.')
            sessions = sessions[:N_WEEKS]
        for wi, ev in enumerate(sessions):
            event_id_to_week[ev['event_id']] = wi
            all_event_ids.append(ev['event_id'])

    # 3. Fetch attendees for those sessions (paginate .in_() in chunks - PostgREST
    #    has a practical limit on IN-list size)
    attendees = []
    CHUNK = 200
    for i in range(0, len(all_event_ids), CHUNK):
        chunk_ids = all_event_ids[i:i + CHUNK]
        resp = client.table('event_attendees').select(
            'event_id, mongo_person_id, full_name, email, leader12, leader144, '
            'is_persistent, registration_type'
        ).in_('event_id', chunk_ids).execute()
        attendees.extend(resp.data or [])

    if not attendees:
        warnings.append('Life Class events were found but event_attendees has no rows for them.')
        return [], [], warnings

    # 4. Fetch gender for everyone involved, via People, joined on mongo_person_id
    #    (falls back to email match for rows with no mongo_person_id)
    person_ids = sorted({a['mongo_person_id'] for a in attendees if a.get('mongo_person_id')})
    emails = sorted({a['email'] for a in attendees if a.get('email') and not a.get('mongo_person_id')})

    gender_by_person_id = {}
    gender_by_email = {}
    if person_ids:
        for i in range(0, len(person_ids), CHUNK):
            resp = client.table('People').select('_id, Email, Gender').in_('_id', person_ids[i:i + CHUNK]).execute()
            for p in (resp.data or []):
                gender_by_person_id[p['_id']] = _norm_gender(p.get('Gender'))
    if emails:
        for i in range(0, len(emails), CHUNK):
            resp = client.table('People').select('_id, Email, Gender').in_('Email', emails[i:i + CHUNK]).execute()
            for p in (resp.data or []):
                if p.get('Email'):
                    gender_by_email[p['Email'].lower()] = _norm_gender(p.get('Gender'))

    def lookup_gender(a):
        if a.get('mongo_person_id') and a['mongo_person_id'] in gender_by_person_id:
            return gender_by_person_id[a['mongo_person_id']]
        if a.get('email'):
            return gender_by_email.get(a['email'].lower())
        return None

    # 5. Aggregate per person (keyed by mongo_person_id, falling back to
    #    email, falling back to full_name - in that order of reliability)
    people = OrderedDict()  # key -> accumulator dict
    unknown_gender_names = set()

    for a in attendees:
        wi = event_id_to_week.get(a['event_id'])
        if wi is None:
            continue  # attendee row for a session we trimmed off above
        key = a.get('mongo_person_id') or (a.get('email') or '').lower() or a.get('full_name')
        if key not in people:
            people[key] = {
                'name': a.get('full_name'),
                'leader12': None,
                'leader144': None,
                'regtype_raw': None,
                'weeks': [None] * N_WEEKS,
                'gender': None,
            }
        rec = people[key]
        if a.get('leader12'):
            rec['leader12'] = a['leader12']
        if a.get('leader144'):
            rec['leader144'] = a['leader144']
        if a.get('registration_type'):
            rec['regtype_raw'] = a['registration_type']
        if a.get('is_persistent'):
            rec['weeks'][wi] = 'X'
        if rec['gender'] is None:
            rec['gender'] = lookup_gender(a)

    from lifeclass_report import norm_name, norm_regtype

    men_rows, women_rows = [], []
    for key, rec in people.items():
        row = {
            'name': norm_name(rec['name']),
            'leader12': norm_name(rec['leader12']),
            'leader144': norm_name(rec['leader144']),
            'regtype': norm_regtype(rec['regtype_raw']),
            'weeks': rec['weeks'],
        }
        if not row['name']:
            continue
        if rec['gender'] == 'male':
            men_rows.append(row)
        elif rec['gender'] == 'female':
            women_rows.append(row)
        else:
            unknown_gender_names.add(row['name'])

    if unknown_gender_names:
        warnings.append(
            f'{len(unknown_gender_names)} attendee(s) skipped - no gender on People: '
            + ', '.join(sorted(unknown_gender_names)))

    if verbose:
        for w in warnings:
            print('WARNING:', w)
        print(f'Loaded {len(men_rows)} men, {len(women_rows)} women '
              f'across {len(cohorts)} cohort(s).')

    return men_rows, women_rows, warnings