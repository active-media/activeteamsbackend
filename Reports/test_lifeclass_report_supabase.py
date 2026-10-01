#!/usr/bin/env python3
"""
test_lifeclass_report_supabase.py
====================================
End-to-end test of the Life Class report AGAINST YOUR REAL SUPABASE PROJECT.

    1. Writes two tagged fixture cohorts (ZZTEST_...) into Supabase.
    2. Fetches them back via supabase_lifeclass_data.fetch_lifeclass_rows()
       - the exact same function real report runs will use.
    3. Runs them through lifeclass_report.build_report() (unchanged, already
       unit-tested against 239 CSV-based checks).
    4. Recalculates the formulas with LibreOffice headless.
    5. Independently recomputes every expected number in pure Python from
       the fixture definitions and diffs against the recalculated workbook,
       looking cells up BY NAME rather than assuming row order (Supabase
       doesn't guarantee row order).
    6. ALWAYS deletes the fixture data afterwards (try/finally), success or
       failure.

Run:
    export SUPABASE_URL=...
    export SUPABASE_SERVICE_ROLE_KEY=...
    python3 test_lifeclass_report_supabase.py

Requires: supabase-py, openpyxl, LibreOffice (soffice) on PATH.
"""

import os
import shutil
import subprocess
import sys
from collections import OrderedDict

import openpyxl

import lifeclass_report as report
import populate_lifeclass_test_data as populate
import cleanup_lifeclass_test_data as cleanup
from supabase_lifeclass_data import get_client, fetch_lifeclass_rows, WEEK_HEADERS

N_WEEKS = len(WEEK_HEADERS)
failures = []
checks_run = 0


def find_soffice():
    """Locate the LibreOffice binary even when it isn't on PATH (common on Windows)."""
    on_path = shutil.which('soffice')
    if on_path:
        return on_path
    candidates = [
        r'C:\Program Files\LibreOffice\program\soffice.exe',
        r'C:\Program Files (x86)\LibreOffice\program\soffice.exe',
        '/usr/bin/soffice',
        '/opt/libreoffice/program/soffice',
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise RuntimeError(
        "Couldn't find the LibreOffice 'soffice' binary on PATH or in the usual "
        "install locations. Set SOFFICE_PATH in your environment to its full path "
        "(e.g. C:\\Program Files\\LibreOffice\\program\\soffice.exe) and re-run.")


SOFFICE = os.environ.get('SOFFICE_PATH') or find_soffice()


def check(label, actual, expected):
    global checks_run
    checks_run += 1
    if actual != expected:
        failures.append(f'{label}: expected {expected!r}, got {actual!r}')


def find_row(ws, col, value, row_start=2, row_end=None):
    row_end = row_end or ws.max_row
    for r in range(row_start, row_end + 1):
        if ws.cell(r, col).value == value:
            return r
    return None


def find_all_rows(ws, col, value):
    return [r for r in range(1, ws.max_row + 1) if ws.cell(r, col).value == value]


def find_guide_state_row(ws, leader, guide):
    current_leader = None
    for r in range(2, ws.max_row + 1):
        a = ws.cell(r, 1).value
        b = ws.cell(r, 2).value
        if a:
            current_leader = a
        if current_leader == leader and b == guide:
            return r
    return None


# --------------------------------------------------------------------------
# Ground truth: reuse the exact rosters populate_lifeclass_test_data.py
# will insert, so expectations are computed from the same source, not the
# report script's own formula-building code.
# --------------------------------------------------------------------------
def roster_to_rows(roster, names_map):
    rows = []
    for person in roster:
        weeks = ['X' if person['attendance'].get(i) is True else None for i in range(N_WEEKS)]
        rows.append({
            'name': names_map[person['name']],
            'leader12': names_map[person['leader12_key']] if person['leader12_key'] else None,
            'leader144': names_map[person['leader144_key']] if person['leader144_key'] else None,
            'regtype': report.norm_regtype(person['regtype']),
            'weeks': weeks,
        })
    return rows


def effective_guide(rec):
    return rec['leader144'] if rec['leader144'] else rec['leader12']


def build_expectations(rows):
    exp = {'rows': rows}
    exp['row_totals'] = [sum(1 for w in r['weeks'] if w == 'X') for r in rows]
    exp['weekly_totals'] = [sum(1 for r in rows if r['weeks'][wi] == 'X') for wi in range(N_WEEKS)]

    leaders12 = list(OrderedDict.fromkeys(r['leader12'] for r in rows if r['leader12']))
    exp['leaders12'] = leaders12
    exp['leader_stats'] = {
        leader: [sum(1 for r in rows if r['leader12'] == leader and r['weeks'][wi] == 'X')
                 for wi in range(N_WEEKS)]
        for leader in leaders12
    }

    all_guide_names = {r['name'] for r in rows if r['regtype'] == 'Guide'}
    guides_by_leader = OrderedDict()
    for leader in leaders12:
        glist = []
        if leader in all_guide_names:
            glist.append(leader)
        for rec in rows:
            if rec['regtype'] == 'Guide' and rec['leader12'] == leader and rec['name'] not in glist:
                glist.append(rec['name'])
        guides_by_leader[leader] = glist
    exp['guides_by_leader'] = guides_by_leader
    exp['guide_state'] = {
        (leader, guide): [sum(1 for r in rows if effective_guide(r) == guide and r['weeks'][wi] == 'X')
                          for wi in range(N_WEEKS)]
        for leader, glist in guides_by_leader.items() for guide in glist
    }

    exp['guide_state_for'] = {
        leader: {
            'Guide': [sum(1 for r in rows if r['leader12'] == leader and r['weeks'][wi] == 'X'
                          and r['regtype'] == 'Guide') for wi in range(N_WEEKS)],
            'Delegate: First Time': [sum(1 for r in rows if r['leader12'] == leader and r['weeks'][wi] == 'X'
                                          and r['regtype'] == 'Delegate: First Time') for wi in range(N_WEEKS)],
            'Delegate: Second Time': [sum(1 for r in rows if r['leader12'] == leader and r['weeks'][wi] == 'X'
                                           and r['regtype'] == 'Delegate: Second Time') for wi in range(N_WEEKS)],
        } for leader in leaders12
    }
    return exp


def verify_group(sheet_prefix, exp):
    ws = wb[sheet_prefix]
    total_col = 5 + N_WEEKS

    for i, rec in enumerate(exp['rows']):
        r = find_row(ws, 1, rec['name'])
        if r is None:
            check(f'{sheet_prefix}: row for "{rec["name"]}" exists', False, True)
            continue
        check(f'{sheet_prefix} "{rec["name"]}" TOTAL', ws.cell(r, total_col).value, exp['row_totals'][i])

    sum_row = find_row(ws, 4, 'WEEKLY TOTAL')
    check(f'{sheet_prefix}: WEEKLY TOTAL row found', sum_row is not None, True)
    if sum_row:
        for wi in range(N_WEEKS):
            check(f'{sheet_prefix} WEEKLY TOTAL {WEEK_HEADERS[wi]}',
                  ws.cell(sum_row, 5 + wi).value, exp['weekly_totals'][wi])

    stats_title_row = find_row(ws, 4, 'LEADER @12 STATS')
    check(f'{sheet_prefix}: LEADER @12 STATS title found', stats_title_row is not None, True)
    for leader in exp['leaders12']:
        r = find_row(ws, 4, leader, row_start=(stats_title_row or 0) + 1)
        check(f'{sheet_prefix}: leader-stats row for "{leader}" exists', r is not None, True)
        if r:
            for wi in range(N_WEEKS):
                check(f'{sheet_prefix} leader "{leader}" {WEEK_HEADERS[wi]}',
                      ws.cell(r, 5 + wi).value, exp['leader_stats'][leader][wi])

    gs = wb[f'GUIDE STATE {sheet_prefix}']
    for leader, glist in exp['guides_by_leader'].items():
        for guide in glist:
            r = find_guide_state_row(gs, leader, guide)
            check(f'GUIDE STATE {sheet_prefix}: row for "{leader}"/"{guide}" exists', r is not None, True)
            if r:
                expected_counts = exp['guide_state'][(leader, guide)]
                for wi in range(N_WEEKS):
                    check(f'GUIDE STATE {sheet_prefix} "{leader}"/"{guide}" {WEEK_HEADERS[wi]}',
                          gs.cell(r, 3 + wi).value, expected_counts[wi])

    gsf = wb[f'GUIDE STATE FOR {sheet_prefix}']
    for leader in exp['leaders12']:
        rows_found = find_all_rows(gsf, 1, leader)
        check(f'GUIDE STATE FOR {sheet_prefix}: exactly 2 blocks for "{leader}"', len(rows_found), 2)
        if len(rows_found) != 2:
            continue
        block1_row, block2_row = sorted(rows_found)
        exp_leader = exp['guide_state_for'][leader]
        for pos, wi in enumerate([0, 1, 2, 3, 4]):
            col = 2 + pos * 3
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} Guides',
                  gsf.cell(block1_row, col).value, exp_leader['Guide'][wi])
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} First Time',
                  gsf.cell(block1_row, col + 1).value, exp_leader['Delegate: First Time'][wi])
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} Second Time',
                  gsf.cell(block1_row, col + 2).value, exp_leader['Delegate: Second Time'][wi])
        for pos, wi in enumerate([5, 6, 7, 8]):
            col = 2 + pos * 3
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} Guides',
                  gsf.cell(block2_row, col).value, exp_leader['Guide'][wi])
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} First Time',
                  gsf.cell(block2_row, col + 1).value, exp_leader['Delegate: First Time'][wi])
            check(f'GUIDE STATE FOR {sheet_prefix} "{leader}" {WEEK_HEADERS[wi]} Second Time',
                  gsf.cell(block2_row, col + 2).value, exp_leader['Delegate: Second Time'][wi])


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------
def run():
    OUT = 'LifeClass_Supabase_Test_Report.xlsx'
    manifest = None
    global wb
    try:
        manifest, men_names, women_names = populate.main()

        client = get_client()
        men_rows, women_rows, warnings = fetch_lifeclass_rows(
            client, cohort_event_names=[manifest['men_cohort_name'], manifest['women_cohort_name']])

        exp_men = build_expectations(roster_to_rows(populate.men_roster(), men_names))
        exp_women = build_expectations(roster_to_rows(populate.women_roster(), women_names))

        check('fetched men row count', len(men_rows), len(exp_men['rows']))
        check('fetched women row count', len(women_rows), len(exp_women['rows']))

        report.build_report(men_rows, women_rows, OUT)

        recalc = subprocess.run(
            [SOFFICE, '--headless', '--convert-to', 'xlsx', '--outdir', 'recalced', OUT],
            capture_output=True, text=True, timeout=120)
        if recalc.returncode != 0:
            print('--- LibreOffice recalculation FAILED ---')
            print(recalc.stderr)
            sys.exit(1)

        wb = openpyxl.load_workbook(f'recalced/{OUT}', data_only=True)
        verify_group('MEN', exp_men)
        verify_group('WOMEN', exp_women)

        print(f'\n{checks_run} checks run.')
        if failures:
            print(f'{len(failures)} FAILED:\n')
            for f in failures:
                print(' -', f)
        else:
            print('All checks PASSED.')

    finally:
        if manifest is not None:
            print('\nCleaning up fixture data ...')
            cleanup.main()

    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    run()