# Backend M4 Requirements — Safe removal of legacy flat leader fields

Branch: `OrgSetUp` (backend repo) · Move this file into the backend repo root before work starts.

## STATUS: DONE — all exit criteria green (last verified 2026-09-09)

- Live scope test `/tmp/opencode/m4_live_test.py`: **21/21 PASS** (people/events/admin-users canonical
  + dual-write, org guards, positional leaders round-trip).
- Frontend M4 smoke test `/tmp/opencode/smoke_m4.py`: **23/23 PASS** against this backend — custom
  hierarchy (shepherd/deacon) round-trips `/org-config`; people/events return canonical
  `leaders: {key}` with zero populated flat keys; canonical edits persist via
  `leaders`/`hierarchy_leaders`; `/check-leader-status` reports custom-hierarchy levels (no
  leader1/12/144 leaks); cross-org guards (403/401); `/admin/users` stores + echoes `leaders`;
  create-consolidation accepts positional `leaders`.
- Exit-8 back-compat sweep: hardcoded legacy flat-field refs reduced 337 → 76. Remaining matches are
  intentional: canonical hierarchy-key tokens (`leader12`, …), legacy maintenance/repair utilities
  (bulk-assign, verify, gender backfill, admin missing-leaders aggregation, `_resolve_leader_field`
  compat helpers), helper definitions (`legacy_flat_keys/read/echo`, `leader_match_conditions`,
  `mongo_flat_ifnull`, `LEGACY_FLAT_PROJECTION`), and comments.
- Cleanup: 17 leftover scratch orgs (`smoketest-*`, `diag-*`) removed.
- Note: `/signup` default for optional `invited_by`/`leader` fixed (`main.py:1411`) — no longer 500s
  when omitted. `toggle-status` guards cross-org with 401 vs `real-time-data`'s 403 (both safe;
  left as-is). API cannot delete an org once it has users (known limitation; cleanup via DB script
  `/tmp/opencode/cleanup_smoketest_orgs.py`).

---

## 1. Purpose

The frontend (same branch, separate repo) can already send and read **canonical per-org leader data**
(`leaders.{key}` / `hierarchy_leaders` keyed by org hierarchy keys), but it still writes and reads the
legacy flat fields (`leader1`, `leader12`, `leader144`, `leader1728`, `"Leader @1"`, `"Leader @12"`,
`leaderAt12`, `leader_at_12`, …) for **events** and **`/admin/users`**, and still keeps read-fallbacks
for people/events because the API can return docs that only carry flats.

The milestone ordering rule is: the backend stops depending on flats for a surface **first**, then the
frontend removes the corresponding flat code. Removing frontend flats before the backend is canonical
on that surface = broken data (blank leader chains). This document defines the backend changes and the
**exit criteria** (verifiable checks) that mark each surface safe.

## 2. Current backend state (already done, keep as-is)

- People CRUD is canonical (`POST /people`, `PATCH /people/{id}` accept `leaders` dict; returns resolve org hierarchy). (M1)
- Org config: `GET`/`PUT /org-config`, signup org auto-create, per-org hierarchy + roles. (M0–M3)
- Capabilities: `resolve_capabilities`, `require_capability` (admin = wildcard; orgless/legacy fallback). (M3)
- Enforcement: `PATCH /people` → `manage_people`, `POST /events` → `create_events`, service check-in → `checkin`, `toggle-status` → `close_events`. (M3)
- `GET /me/capabilities`. (M3)
- §3.4 roles wiring uses each org's `OrgConfig.roles`; legacy Active-Church behavior preserved when no config. (M3)
- All service check-in endpoints `scoped(event)`: `real-time-data`, `validate-removal`, `checkin`, `remove`, `update`, `create-consolidation`. (M2)
- Tasks org-filtered (`/tasks/all`, `/tasks/leader/{email}`). (M2)
- Consolidations org-scoped + positional `leaders` list mapped onto `leaders.{key}`; flats dual-written only for `leader\d+` keys. (M2)

Files touched: `auth/utils.py`, `auth/models.py`, `main.py`, `dynamic_config.py`, `seed_orgs.py`. Nothing committed yet.

## 3. What still needs to change

### 3.1 Lazy backfill on read (§9) — the no-break guarantee

Whenever a person **or event** doc is returned by the API and it has no `leaders`/`hierarchy_leaders`
but has flat leader fields, populate `leaders` from the flats using the org's hierarchy (keys matching
`^leader\d+$` map directly; keys from a configured hierarchy otherwise matched by `level`).
Do this on **all read paths**: people list/search/cache, event list/detail/cells, `/check-leader-status`,
service-check-in real-time data, attendance, tasks, consolidations, exports.

This is what lets the frontend drop `person["Leader @…"]`-style read fallbacks without old
Active-Church/flat-only records rendering blank.

### 3.2 Events canonical — largest gap

The event document still stores its lead chain in flats (`leader1`, `leader12`, `leader144`) and the
server reads them back. Required:

- `POST /events` and `PUT /events/{id}` must accept the lead chain from `leaders` (keyed dict or positional list)
  **or** `hierarchy_leaders` (keyed by org hierarchy key) as source of truth, and store `leaders.{key}` on the event.
- Dual-write the legacy flats for `leader\d+` keys only (same rule as people §9/§2) **until** §3.6, so nothing currently
  reading flats breaks mid-transition.
- **All event reads return the chain under `leaders`** (and keep flats during transition): event list, event detail,
  `/events/cells`, `/events/cells/{identifier}`, `/events/{event_id}/persistent-attendees`, `toggle-status` responses,
  `/check-leader-status`, service check-in real-time, attendance, exports.
- Event-leader resolution in check-in / consolidations / tickets should walk the org `hierarchy` ascending —
  never `leader1…leader1728` by name.

### 3.3 `/admin/users` canonical

`POST /admin/users` currently persists the flat fields. Required:

- Accept a `leaders` dict (like people CRUD does) and persist `leaders.{key}` on the created user.
- Echo the created user back with canonical `leaders` populated in the response.
- Dual-write flats for `leader\d+` keys during transition.

### 3.4 Unscoped legacy endpoints — apply org guards

Two known routes still bypass org scoping (cross-org access possible):

- `DELETE /events/{event_id}`
- `PUT /events/person/{person_identifier}/event/{event_name}/day/{day}` (legacy check-in write path, used by EditEventModal)

Apply the same `scoped()`/org-guard treatment as the M2 sweep. Cross-org → 403, own-org → 200.

### 3.5 Out of scope (deliberate — do not "fix")

- **Per-org cache** (§3.7) is intentionally filtered-global (shares `leaders`-independent cached people; no API leak today). Keep as-is.
- **Orgless/legacy fallback** in `resolve_capabilities` and untagged docs (treated as legacy) — keep during M4 transition.
- **Role-name strings** like `leaderAt12` as a *role* (not a data field) — these come from `Organization`/`role`, not the hierarchy. Frontend keeps them.

### 3.6 End state after everything is green

Stop dual-writing flats entirely for people/events/users/consolidations. Untagged/flat-only docs are no
longer produced going forward (backfill still covers historical reads).

## 4. Exit criteria — run these against the live server; each green number unlocks a frontend removal

Placeholders: swap `$TOKEN`, `$ORG`, or whatever the scoping header is. Default Active-Church org has no
config → hierarchy keys `leader1/leader12/leader144/leader1728`.

1. **People round-trip (regression, already green at M1)** — checks §3.1 backfill for people.
   `POST /people` with body `{ name, surname, email, leaders: { leader12: "Gavin Enslin" } }` (no flats) →
   `GET /people?email=…` returns `leaders.leader12` populated.
   **Unlocks:** nothing new (already done).

2. **Flat-only people backfill** — checks §3.1 read backfill. Insert a person with only `leader12` flat (or use a real old doc) →
   `GET /people?email=…` returns `leaders` populated (key from hierarchy, e.g. `leader12`).
   **Unlocks:** frontend removes `person["Leader @…"]`/`leaderAt…`/`leader_at_…` read fallbacks on people paths.

3. **Event create canonical** — `POST /events` with `hierarchy_leaders: { leader12: "Gavin Enslin" }` and **no** `leader1/leader12` fields →
   `GET /events` (and `/events/{id}`, `/events/cells`) return the event with `leaders` populated; leader chain survives a page reload.
   **Unlocks:** `CreateEvents.jsx` stops sending `leader1/leader12/leader144` in the event payload.

4. **Event edit canonical** — `PUT /events/{id}` with only `hierarchy_leaders`→ re-GET shows the chain; also verify the frontend edit path
   (it PUTs raw form state today — must be switched to canonical **only after this passes**).
   **Unlocks:** edit flow flat payload removal.

5. **Admin user create canonical** — `POST /admin/users` with `leaders: { leader12: "…" }` and no flats →
   created user persists with the chain; `GET /admin/users?organization=…` returns it via `leaders`.
   **Unlocks:** `Admin.jsx`/`NewUserModal.jsx` drop `leader12/leader144/leader1728` from the create body.

6. **Event backfill on read** — a previously-created event that only has `leader12` flat returns with `leaders` populated.
   **Unlocks:** the remaining `event[lv.key]`/`"Leader @…"` read fallbacks in Events list/cards/export.

7. **Org guards on legacy endpoints** — cross-org `DELETE /events/{id}` → 403; cross-org `PUT /events/person/…/day/…` → 403; own-org → 200.
   **Unlocks:** nothing frontend-side; closes the remaining leak.

8. **Global sweep** — backend has zero remaining hardcoded `leader1/leader12/leader144/leader1728`/`"Leader @1"`-style *data-field* references
   (search `rg -n "leader1|leader12|leader144|leader1728|Leader @1|Leader @12|Leader @144|Leader @1728" main.py auth/utils.py` → matches only in
   pure dual-write/backfill helpers keyed by regex `^leader(\d+)$`).

## 5. Frontend removals that follow (so you know the payoff)

After checks 2–6 are green, in order:
- Remove people read-fallbacks (AddPersonDialog map/echo, People.jsx, AttendanceModal, ServiceCheckIn, ConsolidationModal, EventHistoryModal clients).
- Remove event payload flats (`CreateEvents.jsx` lines ~726-727, ~764-766) and event read fallbacks (`Events.jsx` cards/export/search).
- Remove admin-user flats (`Admin.jsx` handleCreateUser, `NewUserModal.jsx`) — swap to the already-built `leaders` dict.
- Final `rg` sweep for the field literals across `src` → zero matches.

## 6. Test hygiene

Keep the live-DB verification pattern used for M0–M3 (scratch data cleaned up, `git status` still shows only the 5 files + this spec).
Copy this file into the backend repo root as `BACKEND-M4-REQUIREMENTS.md` (or rename), and commit it alongside the M4 backend work when you're ready to start committing.