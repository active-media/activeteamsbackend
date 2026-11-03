# report_exports

Pure builders (`payload -> .xlsx bytes`) replicating the admin team's workbooks, plus `training_exports.py`,
the Supabase layer for Life Class, School of Leaders and Plan 40.

Wiring: `reporting_core/router.py` (patched copy provided) routes `GET /reporting/reports/{life_class|school_of_leaders|plan_40}/export?format=xlsx&period=...`
to `build_training_xlsx`.

Tests (run from the parent dir): `python -m report_exports.tests.roundtrip` (layout vs originals),
`python -m report_exports.tests.test_training` (DB layer vs an in-memory fake of the schema).

Assumptions to confirm (marked ASSUMPTION in training_exports.py)
- Events are found by `event_type_name` / `event_name` containing "life class", "school of leaders", "plan 40".
- Sessions = `event_sessions` for those events inside the chosen period, ordered by date; Life Class slot 5 = Encounter.
- Presence = `event_session_attendees.is_checked_in` OR `event_training_attendance.is_present`.
- Gender comes from People.Gender via `mongo_person_id`; people without a gender are left off the gendered sheets.
- School of Leaders: one event per cohort (event_name = "Destiny 1 & 2" ...); names containing "yoml" go on the YOML sheet.
- Plan 40 top leaders default to Gavin Enslin / Vicky Enslin (env TOP_LEADER_MEN / TOP_LEADER_WOMEN).
