# Reporting Dashboard Investigation

This note describes the reporting dashboard implementation on
`fix/reporting-dashboard-investigation`.

## Routes and data flow

- The dashboard is `/reporting/dashboard`; its API request is
  `GET /reporting/dashboard` through `Active-teams-frontend/src/api/reportingApi.js`.
- The React route is registered in `Active-teams-frontend/src/App.jsx`. Unknown
  `/reporting/*` paths redirect to `/reporting/dashboard`.
- The backend reporting router resolves the period and organization scope, then
  calls `sb_get_reporting_dashboard` and builds the response with
  `dashboard_envelope`.
- Report pages are served at `/reports/{report_type}`. Every dashboard KPI card
  links to a corresponding report; there is no disabled or “Coming soon” state.

## KPI-to-report mapping

| Dashboard KPI | Report opened |
|---|---|
| Total Attendance | `/reports/cells_graph` — attendance trend and cell attendance |
| Active Cells | `/reports/cells_graph` — active-cell trends and cell list |
| Life Class | `/reports/life_class` |
| School of Leaders | `/reports/school_of_leaders` |
| Plan 40 | `/reports/plan_40` |
| Service Target | `/reports/service_target` |
| Twelve Tasks | `/reports/twelve_tasks` |
| Staff/Interns/Youth | `/reports/staff_interns_youth` |

These report types are registered in the backend catalog and are handled by
their report builders. Unsupported KPI keys are not rendered as cards. The
report API honors the `organization` query parameter for super-admin requests;
regular users remain scoped to their authenticated organization.

## KPI values and hints

The response contains `kpis`, `tables`, and `series`. Total Attendance is the
sum of `checked_in_count` for complete `event_sessions` inside the selected
period; it is not a distinct-person count. Its hint is “Sum of check-ins across
completed services.” The growth percentage remains visible in the card header,
and KPI hints are rendered below their values without removing the other cards’
hints. Cards use link semantics, visible keyboard focus, and respond to Enter
and Space.

Links retain the dashboard period (including custom date ranges), campus, and
any `organization`/`org` query parameter. Regular users remain restricted to
their authenticated organization. For super-admins, the dashboard and report
API apply the selected `organization` query parameter. The dashboard has no
separate organization selector.

For the in-progress `thisMonth`, `thisWeek`, and `today` periods, attendance
growth compares the elapsed portion with the corresponding elapsed portion of
the preceding period. Completed periods use the preceding equal-length span.

The dashboard reports no-data states explicitly:

- An empty/unusable dashboard response displays the informational alert
  “No dashboard data is available.”
- API failures display an error alert with a Retry action; connection failures
  also identify the backend URL setting.
- Empty charts and tables display “No data for this period.”
- Service Target and Twelve Tasks data are optional dashboard sections. A
  failure is logged by the backend and the corresponding KPI is returned as
  unavailable rather than failing the core dashboard.

## Attendance Trend labels

`supabase_helpers/weekly_buckets.py` creates Monday-to-Sunday buckets clipped
to the selected period. Each bucket has one canonical Monday key; attendance,
cell counts, and leader trends aggregate and look up values with that same key.
The Sunday label is separate from the key and is clipped so it never falls
outside the selected dates.

A first partial bucket containing a Sunday remains a point. If the last
partial bucket would end after the period and a preceding bucket exists, its
sessions are folded into that preceding bucket, avoiding adjacent labels such
as November 29 and November 30. The chart formats date labels as short month
and day (for example, `Nov 1`) and reduces tick density to fit the chart width.
The database-free pytest covers June, November, and December 2026, plus
`thisWeek` and `previousWeek`; it checks label bounds, uniqueness, spacing, and
attendance preservation.

## Target write behavior

The Twelve Tasks target endpoint is `POST /targets/twelve-tasks`. It requires an
admin role, attempts the existing upsert and insert paths, and returns success
only when Supabase returns a target row. A failed Supabase write is logged on
the server with its operation, error message, and HTTP status; the endpoint
returns the Supabase message with a suitable error status. If neither write
returns a row, the endpoint responds with an error instead of a null target.

The Service Target write endpoint is `POST /targets/service`; it uses
`sb_upsert_service_target`, which raises on write errors or when Supabase
returns no row.

## Twelve Tasks report source

The Admin page does not define twelve named task categories. It defines
per-person target rows for a selected week and organization in
`twelve_tasks_targets`; the relevant columns are `person_id`, `target`,
`week_identifier`, `organization`, and `is_default`. The Admin UI displays
“Custom” when `is_default` is false; there is no separate target-type column.
The number of people/rows is variable. The Twelve Tasks report uses those
Admin rows as its source of truth, scoped to the report organization and ISO
weeks overlapping the selected period, and displays each person's current
name, target, weeks, and completed task actuals. It no longer uses the separate
`twelve_tasks_tracked_people` roster to create report rows or defaults missing
targets. An Admin-defined organization default remains one “Leader default”
row; it is not expanded into synthetic person rows.
`migration_twelve_tasks_weekly_targets.sql` replaces the old person-only
primary key with a unique person/week/organization index, so weekly definitions
can coexist; the default-target writer uses the same conflict key. The
migration must be applied to the database before weekly targets can persist
independently. No live database migration was run.

For bucket resolution, the report uses `build_week_buckets` from
`supabase_helpers/weekly_buckets.py`. Every source ISO week intersecting the
period is queried (for November 2026: `2026-W44` through `2026-W49`); when the
attendance bucketing rule folds a trailing partial week, its canonical display
bucket is mapped to the preceding bucket. Targets are aggregated to one row
per person: targets are summed across that person's in-period weekly rows, and
actuals are summed over completed tasks whose completion timestamps fall in
the selected period. The current implementation also sums the preceding-week
actual for each target row into the “Prior-week actuals” column.

Rows are not padded or truncated to twelve. If a person has since been deleted
from `People`, their target row remains visible under the person ID and the
report does not fail. Actuals are matched by the current person's ID/email, so
unmatched historical task assignments do not get attributed to someone else.
A period without Admin target rows in any overlapping week has no zero-filled
KPIs or report rows; the report page shows “No tasks set for this period” (or
the matching week label for week periods) with a link to Admin. Super-admin
report requests require an explicit organization; the report page's
organization selector and dashboard KPI links preserve that context.

Admin's target-list and available-week routes accept organization selection
for super-admins while enforcing the authenticated organization for regular
users. The KPI link carries its current week/period and organization context
to the report.

## Read-only KPI audit

`scripts/kpi-data-audit.py` uses the backend reporting and period helpers with
Supabase credentials from the environment. It accepts `--period` (`thisMonth`,
`thisWeek`, or `previousWeek`) and `--org`. Provide `SUPABASE_URL` and
`SUPABASE_SERVICE_ROLE_KEY`; if only an anon key is available, the script warns
that RLS may hide rows. It performs reads only and prints aggregate counts, not
personal data.

The audit includes an Active Cells funnel and compares calculated values with
the backend dashboard envelope. An optional API comparison can be enabled with
`AUDIT_BEARER_TOKEN` and `AUDIT_BACKEND_URL` (or `VITE_BACKEND_URL`); when
`--org` is used, the API comparison uses the bearer account’s scope while the
direct Supabase reads use the requested organization.
