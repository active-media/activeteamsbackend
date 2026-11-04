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
- Report pages are served at `/reports/{report_type}`. The Active Cells,
  Service Target, and Twelve Tasks cards link to `/reports/cells_graph`,
  `/reports/service_target`, and `/reports/twelve_tasks`, respectively.
- Total Attendance, Life Class, School of Leaders, and Plan 40 have no linked
  detail page in the current dashboard; their cards are disabled and show
  “Coming soon.” Total Attendance still displays its value and growth caption.

## KPI values and hints

The response contains `kpis`, `tables`, and `series`. Total Attendance is the
sum of `checked_in_count` for complete `event_sessions` inside the selected
period; it is not a distinct-person count. Its hint is “Sum of check-ins across
completed services.” The growth percentage remains visible in the card header,
and KPI hints are rendered below their values without removing the other cards’
hints.

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
