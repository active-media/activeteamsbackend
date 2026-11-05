# Staff & Intern Statistics

Feature that classifies people as **Staff** or **Intern**, captures their
weekly **Friday/Sunday Service Targets** and the **actual** recorded, marks each
target **met / not met**, **locks** (closes) targets once the service has
passed, and renders a **Target vs Actual** chart on the Stats page — next to the
existing **Calendar Events** section. The chart data can be **downloaded as
XLSX**.

## Access / permissions

| Area | Rule |
|---|---|
| UI (Stats page tab + section) | `admin` or `leader@12`. Gated by `canAccessStaffIntern(user)` in `src/utils/staffIntern.js` (accepts `admin`, `leaderAt12`, `leader12`, `leader@12`, `leader at 12`). |
| Classification API (`PUT /people/{person_id}/staff-intern`) | `admin` or `leader@12` (plus supreme admin). Other roles receive **403 Forbidden**. |
| Targets API (`/staff-intern/*`) | `admin` or `leader@12` (plus supreme admin). Enforced server-side in `routers/staff_intern.py`. |

The frontend gate is not trusted on its own — every mutating endpoint performs
its own role check.

## Data model

**Classification** stays on the MongoDB `People` collection (additive fields):

| Field | Type | Description |
|---|---|---|
| `staffStatus` | `"none" \| "staff" \| "intern"` | Classification. `none`/missing = unmarked. |

**Weekly targets** live in the Supabase `service_targets` table, one row per
`(person, week, service)`. Rows created by this feature are identified by
`service IN ('friday','sunday')`; the legacy leader-target rows leave `service`
`NULL` so the two usages coexist. See
`migrations/001_service_targets_staff_intern.sql`:

| Column | Type | Description |
|---|---|---|
| `person_id` | text | The `People._id`. |
| `service` | text | `friday` or `sunday`. |
| `week_identifier` | varchar | ISO week, `YYYY-Www` (SAST). |
| `target_count` | integer | The target. |
| `actual` | integer | The recorded actual. |
| `met` | boolean | `actual >= target` (finalised on close). |
| `closed` | boolean | Locked after the service day has passed. |
| `closed_at` / `closed_by` | timestamptz / text | Who/when closed (or `system:auto`). |
| `leader_name` / `email` / `organization` | text | Snapshot from the person (required columns). |

### Lifecycle

- **met = actual >= target**, computed on every write.
- A service counts as **passed** at the end of its calendar day (SAST):
  - Friday = ISO-week Monday + 4 days
  - Sunday = ISO-week Monday + 6 days
- When a target's service has passed it is **auto-closed** (locked) lazily on
  the next list request; its `met` state is finalised.
- `admin`/`leader@12` may **close** early or **reopen** a closed target.

## API

All routes live in `routers/staff_intern.py` (prefix `/staff-intern`) and
require JWT auth via `get_current_user`.

### `GET /staff-intern/targets?week=YYYY-Www&service=friday|sunday`

Lists the week's targets plus upcoming service metadata. Defaults to the
current SAST week. The optional `service` filter limits results to Friday or
Sunday. Auto-closes any passed-and-open targets.

```json
{
  "week_identifier": "2026-W41",
  "services": {
    "friday": {"label": "Friday Service", "date": "2026-10-09", "passed": false, "event": {"event_id": "...", "event_name": "Friday Service"}},
    "sunday": {"label": "Sunday Service", "date": "2026-10-11", "passed": false, "event": null}
  },
  "targets": [
    {"id": 1, "person_id": "...", "service": "friday", "week_identifier": "2026-W41",
     "target": 5, "actual": 4, "met": false, "closed": false,
     "closed_at": null, "closed_by": null, "leader_name": "...", "email": "...", "organization": "..."}
  ]
}
```

`services[*].event` is a best-effort match against the `Events` collection; the
computed weekday date is always authoritative for the close logic.

### `GET /staff-intern/targets?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&service=friday|sunday`

Returns historical targets whose service dates fall inside the inclusive date
range. `service` is optional; without it, Friday and Sunday targets are both
returned. The Stats page lets admins and leader@12 choose a date range and
service, inspect the target and actual values, and download the filtered rows
as an XLSX workbook with Summary and Detail sheets.

### `PUT /staff-intern/targets/{person_id}`

Create/update one person's target for a service in a week.

```json
{ "service": "friday", "target": 5, "actual": 4, "week_identifier": "2026-W41" }
```

- `service` must be `friday`/`sunday`; `target`/`actual` must be non-negative
  integers (booleans rejected).
- `met` is computed as `actual >= target`.
- **409** if the target is closed (reopen first).
- **400** invalid service / value / week / person id.

### `POST /staff-intern/targets/{person_id}/close`

Body `{ "service": "friday", "week_identifier": "2026-W41" }`. Sets
`closed=true`, finalises `met`, records `closed_at`/`closed_by`. **404** if the
target does not exist.

### `POST /staff-intern/targets/{person_id}/reopen`

Body as above. Clears `closed`, `closed_at`, `closed_by` so the target is
editable again.

## Frontend

- `src/components/StaffInternStatistics.jsx` — the section UI.
  - Loads all people (paged, `perPage=200`) for classification, and
    `GET /staff-intern/targets` for the current week in parallel.
  - Shows the **week** chip and the **upcoming Friday/Sunday services** (date,
    upcoming/closed, matched event name when available).
  - Marking/unmarking persists `staffStatus` via `PUT /people/{id}/staff-intern`.
  - Target/Actual inputs are **disabled when closed**; closed targets show a
    **Met / Not met** chip and a **Reopen** button, otherwise **Save** + **Close**.
  - **Download XLSX** writes a two-sheet workbook (Summary + per-person Targets)
    using the existing `xlsx` dependency.
  - **Past Targets** supports an inclusive date range and All/Friday/Sunday
    service filter; XLSX downloads include the applied historical results.
- `src/utils/staffIntern.js` — framework-free helpers:
  `canAccessStaffIntern`, `toNonNegativeInt`, `validateTargetPayload`,
  `buildTargetsByPerson`, `combineClassificationAndTargets`,
  `splitPeopleByClassification`, `buildTargetVsActualChart`,
  `computeTargetVsActual`, `buildTargetExport`.
- `src/Pages/Stats.jsx` — renders the section under the **Staff & Intern
  Statistics** tab (visible only to admins / leader@12) next to Calendar Events.
- Charting reuses the project's existing `chart.js` + `react-chartjs-2` `Line`
  component. **X-axis = service** ("Friday Service", "Sunday Service"),
  **Y-axis = number**. Four dotted trend lines: Staff Target, Staff Actual
  (blue shades) and Intern Target, Intern Actual (red shades).
- The unmarked list has a search box and 25-row pagination for large orgs.

## Tests

- `test_staff_intern_targets.py` (pytest, targets `main.app`, mocks Supabase +
  Mongo) — week/date helpers, role gating, history date/service filters, list + auto-close, upsert create/
  update, closed → 409, validation (bad service/week/value/bool), close/reopen.
- `test_staff_intern.py` (pytest) — the classification endpoint.
- `tests/staffIntern.test.js` (node:test) — role gate, target validation,
  target indexing, merge, separation, chart, totals, Excel export rows.
- `tests/components/StaffInternStatistics.test.jsx` (vitest) — role
  restriction, empty state, paging, classification PUT, target PUT payload,
  met/not-met + locked inputs, historical service filtering and XLSX download.

## Running locally

1. Run the Supabase migration **once** (PostgREST/service key cannot `ALTER`):

   ```sql
   -- activeteamsbackend/migrations/001_service_targets_staff_intern.sql
   ```

   Paste it into the Supabase SQL editor.

2. Start the backend (the `venv` lacks uvicorn; use the global interpreter):

   ```bash
   python3 -m uvicorn main:app --reload --port 8000
   ```

   The frontend `.env` `VITE_BACKEND_URL` points at the backend you are testing
   (e.g. `http://localhost:8000`).

The scheduled auto-reactivation job is started from a Startup event (not at
import time), because `AsyncIOScheduler.start()` needs a running event loop.

## Implementation notes

- Validation is enforced both client-side (immediate feedback) and server-side
  (authoritative).
- Once the `service_targets` columns exist, the feature works end-to-end; until
  then the targets endpoints return a Supabase error (the people/classification
  flow is unaffected).
- Empty states: no people → "No people found."; no classification → guidance
  and no chart until someone is marked.
- Errors are surfaced via toast + inline alert (with a Retry action on load
  failure).
