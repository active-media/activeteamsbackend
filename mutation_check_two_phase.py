"""Mutation-checks the new guards in tests/test_events_two_phase_read.py.

Each mutation reverts one fix and asserts that the relevant test goes red. A
test that still passes after its own fix has been reverted is not actually
testing anything, so "MISSED" here is a hole in the suite, not a broken fix.

Works on a throwaway copy of the repository. It deliberately does NOT edit the
real main.py: an earlier in-place version left a mutated main.py on disk if the
process was killed between the write and the restore, and main.py is 16k lines
carrying a lot of unrelated work.

Run:
    venv/bin/python mutation_check_two_phase.py
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent
VENV_PYTHON = REPO / "venv/bin/python"
TEST_FILE = "test_events_two_phase_read.py"
IGNORED = shutil.ignore_patterns("venv", ".git", "node_modules", "dist",
                                 "__pycache__", ".pytest_cache", "*.pyc")

MUTATIONS = [
    (
        "phase 2 skips rows with no dated record (the persistent_attendees bug)",
        'if not payload:\n                    continue',
        'if not payload or not payload.get("has_date_record"):\n                    continue',
        "TestTwoPhaseMatchesFullRead::test_unfiltered_response_is_identical",
    ),
    (
        "recurring fallback keys off the emptied array instead of the count",
        "if not date_attendance and exact_date_str == original_date_str and event_level_attendee_count > 0:",
        "if not date_attendance and exact_date_str == original_date_str and root_attendees:",
        "TestPhase2EdgeCasesFoundByTheEquivalenceTest::test_recurring_root_attendee_fallback_still_fires",
    ),
    (
        "one-time branch keys off the emptied array instead of the count",
        "if event_level_attendee_count == 0:",
        "if not weekly_attendees:",
        "TestPhase2EdgeCasesFoundByTheEquivalenceTest::test_one_time_event_prefers_root_attendees",
    ),
    (
        "pipeline stops dropping the person arrays",
        '"attendees": 0,\n                "persistent_attendees": 0,',
        '"persistent_attendees": 0,',
        "TestCountsPipelineShape::test_drops_the_person_arrays_in_the_final_projection",
    ),
    (
        # The first occurrence is the recurring branch's copy at ~line 3410, so
        # the target has to be the recurring fixture, not the one-time one.
        "recurring status reads the count's array rather than the count",
        "has_weekly_attendees = weekly_attendees_count > 0",
        "has_weekly_attendees = len(weekly_attendees) > 0",
        "TestPhase2EdgeCasesFoundByTheEquivalenceTest::test_recurring_record_with_attendees_but_no_status_reads_as_complete",
    ),
    (
        # Anchored with a leading newline on purpose: this line's 20 leading
        # spaces are a *suffix* of the recurring branch's 28-space line, so an
        # unanchored replace would silently re-mutate the recurring copy instead.
        "one-time status reads the count's array rather than the count",
        "\n                    has_weekly_attendees = weekly_attendees_count > 0",
        "\n                    has_weekly_attendees = len(weekly_attendees) > 0",
        "TestPhase2EdgeCasesFoundByTheEquivalenceTest::test_record_with_attendees_but_no_status_reads_as_complete",
    ),
    (
        "page detail projection asks for the whole attendance map",
        "for key in wanted_keys:\n        projection[key] = 1",
        "projection['attendance'] = 1",
        "TestPageDetailProjection::test_requests_only_the_needed_dates",
    ),
    (
        "cache ignores its TTL",
        "if cached and cached[0] > time.monotonic():",
        "if cached:",
        "TestPhase1Cache::test_expired_entries_are_reread",
    ),
    (
        "projection asks for persistent_attendees unconditionally",
        '    if include_persistent_attendees:\n        projection["persistent_attendees"] = 1',
        '    projection["persistent_attendees"] = 1',
        "TestPageDetailProjection::test_persistent_attendees_is_opt_in",
    ),
    (
        # The opt-in is enforced in five places - the projection, the detail
        # payload, the recurring branch, the one-time branch and the phase 2
        # re-attach - and reverting any ONE of them is a silent no-op because
        # the others still hold. That layering is deliberate (defence in depth),
        # but it also means per-gate mutation testing is the wrong tool here:
        # each single revert would be reported as a MISSED hole that is not a
        # real one. So this reverts all five together, which is the regression
        # that would actually reach a user.
        "the persistent_attendees opt-in is ignored everywhere",
        [
            # 1. the projection
            '    for field in ("attendees", "new_people", "consolidations"):\n'
            '        projection[field] = 1\n'
            '    if include_persistent_attendees:\n'
            '        projection["persistent_attendees"] = 1',
            # 2. the detail payload
            '            if include_persistent_attendees:\n'
            '                detail[(str(event_id), date)]["persistent_attendees"] = \\\n'
            '                    event_level("persistent_attendees")',
            # 3. the recurring branch
            '                                "persistent_attendees": (\n'
            '                                    enrich_attendees_with_financials('
            'event.get("persistent_attendees", []))\n'
            '                                    if include_persistent_attendees else []\n'
            '                                ),',
            # 4. the one-time branch
            '                        "persistent_attendees": (\n'
            '                            enrich_attendees_with_financials('
            'event.get("persistent_attendees", []))\n'
            '                            if include_persistent_attendees else []\n'
            '                        ),',
            # 5. the phase 2 re-attach
            'if include_persistent_attendees and isinstance(\n'
            '                    payload.get("persistent_attendees"), list\n'
            '                ):',
        ],
        [
            '    for field in ("attendees", "persistent_attendees",\n'
            '                  "new_people", "consolidations"):\n'
            '        projection[field] = 1',
            '            detail[(str(event_id), date)]["persistent_attendees"] = \\\n'
            '                event_level("persistent_attendees")',
            '                                "persistent_attendees": '
            'enrich_attendees_with_financials(event.get("persistent_attendees", [])),',
            '                        "persistent_attendees": '
            'enrich_attendees_with_financials(event.get("persistent_attendees", [])),',
            'if isinstance(\n'
            '                    payload.get("persistent_attendees"), list\n'
            '                ):',
        ],
        "TestPhase2EdgeCasesFoundByTheEquivalenceTest::test_persistent_attendees_are_empty_by_default",
    ),
]


def run_test(sandbox, node_id):
    """Run one node id, e.g. tests/test_x.py::Class::method, inside the sandbox."""
    return subprocess.run(
        [str(VENV_PYTHON), "-m", "pytest", node_id, "-q",
         "--no-header", "-p", "no:cacheprovider"],
        cwd=sandbox, capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(sandbox),
             "PYTHONDONTWRITEBYTECODE": "1"},
        timeout=900,
    )


def _apply(source: str, old, new) -> str:
    """Apply one replacement, or a sequence of them, to a source string.

    A mutation may carry a list of (old, new) pairs when one regression spans
    several guards.
    """
    if isinstance(old, str):
        return source.replace(old, new, 1)
    for one_old, one_new in zip(old, new):
        if one_old not in source:
            raise KeyError(one_old)
        source = source.replace(one_old, one_new, 1)
    return source


def main():
    sandbox = Path(tempfile.mkdtemp(prefix="mutcheck-"))
    failures = []

    try:
        shutil.copytree(REPO, sandbox / "repo", ignore=IGNORED)
        work = sandbox / "repo"
        main_py = work / "main.py"
        original = main_py.read_text()

        node = lambda test: f"tests/{TEST_FILE}::{test}"  # noqa: E731

        print(f"sandbox: {work}\n")
        print("baseline (no mutation) - every target test must be green\n")
        for _label, _old, _new, test in MUTATIONS:
            r = run_test(work, node(test))
            if r.returncode != 0:
                print(f"  UNEXPECTED FAIL {test}")
                print("   " + r.stdout.strip().splitlines()[-1])
                failures.append(("baseline", test))

        for label, old, new, test in MUTATIONS:
            if isinstance(old, str):
                if old not in original:
                    print(f"  SKIP (anchor not found) {label}")
                    failures.append((label, "anchor missing"))
                    continue
            else:
                missing = [one for one in old if one not in original]
                if missing:
                    print(f"  SKIP (anchor not found) {label}: {missing[0]!r}")
                    failures.append((label, "anchor missing"))
                    continue

            main_py.write_text(_apply(original, old, new))
            r = run_test(work, node(test))
            caught = r.returncode != 0
            print(f"  {'CAUGHT ' if caught else 'MISSED '} {label}")
            print(f"           test: {test}")
            if not caught:
                failures.append((label, test))
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)

    print()
    if failures:
        print(f"{len(failures)} mutation(s) not caught:")
        for label, test in failures:
            print(f"  - {label} ({test})")
        return 1
    print("all mutations caught - the guards are load-bearing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
