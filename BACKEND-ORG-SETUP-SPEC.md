# Backend Spec: Multi-Org Setup + Dynamic Hierarchy ("Org Setup")

> Target: backend repo (FastAPI/Python, per the existing endpoints). Implement in phases. The
> frontend (React) will be built against this contract on the other side. Read this whole doc first —
> several "existing" endpoints only need to be **scoped/loosened**, not rewritten.

---

## 1. Goal

Let any church/organisation use the system:

1. A pastor/admin signs up (they can type a new org name OR pick an existing one — already works today).
2. If the org has never been configured, they are taken to an **org setup wizard** where they define
   their own hierarchy — **any levels, any labels, any backing field keys** (e.g. G12's
   "Leader @1 / @12 / @144 / @1728", or "Shepherd / Deacon / Zone Leader", etc.).
3. They import their Excel of people **mapped to that hierarchy** and assign people to levels.
4. Every screen (people, daily tasks, events, check-in, stats) works from their org's imported
   people and their own hierarchy labels.
5. Org config (hierarchy, roles, settings) is editable **only by org admins**.

Hard requirement: **no hardcoded "leader1 / leader12 / leader144 / leader1728" anywhere on the
backend.** That is currently the single biggest blocker — see Section 10.

---

## 2. Data model changes

### 2.1 Organization collection

Current: `{_id, name, ...}`. Add:

```json
{
  "_id": "…",
  "name": "Encounter Church",
  "slug": "encounter-church",
  "is_setup": false,

  "hierarchy": [
    { "key": "leader1",   "label": "Leader @1",   "level": 1 },
    { "key": "leader12",  "label": "Leader @12",  "level": 12 },
    { "key": "leader144", "label": "Leader @144", "level": 144 },
    { "key": "leader1728","label": "Leader @1728","level": 1728 }
  ],

  "roles": [
    { "key": "admin",      "label": "Admin",   "capabilities": ["admin"] },
    { "key": "leader",     "label": "Leader",  "capabilities": ["view_people","manage_people","create_events","close_events","view_stats","checkin"] },
    { "key": "user",       "label": "Member",  "capabilities": ["checkin"] }
  ],

  "settings": {
    "recurring_event_type": "Cells",
    "top_leaders": { "male": null, "female": null },
    "allows_create_event": true,
    "allows_create_event_type": true
  }
}
```

Example of a **completely different** church that shares no fields with the G12 example:

```json
{
  "_id": "…",
  "name": "Grace Network",
  "is_setup": true,
  "hierarchy": [
    { "key": "evangelist", "label": "Evangelist",    "level": 1 },
    { "key": "shepherd",   "label": "Shepherd",      "level": 7 },
    { "key": "deacon",     "label": "Deacon",        "level": 20 }
  ],
  "settings": {
    "recurring_event_type": "Small Groups",
    "top_leaders": { "male": "Emeka Obi", "female": "Chidinma Obi" }
  }
}
```

Note: not a single `leader1/leader12/…` key, not "Leader @1", not "Gavin Enslin". Every org defines
its own keys, labels, top-leader titles and top-leader people.

Notes:
- **`hierarchy` canonical** — ORDER of the array = display order. Every entry:
  - `key`: snake_case, non-empty, **unique within the org**, stable identifier used everywhere.
  - `label`: what users see (e.g. "Zone Leader").
  - `level`: positive int, **unique**, used ONLY for ascending order; values need NOT be
    consecutive. G12 uses the group multiplier (1, 12, 144, 1728, 20736, …) and is **open-ended** —
    every level's leaders each bring in ×12 more, so the org simply adds another level row in
    org-setup as it grows. @1 is the top-most leader; a person sits one row below whoever brought
    them in (brought by a @144 leader → you are @1728).
  - Minimum: **1 level.** No fixed max (cap at ~16).
- **`roles` optional (Phase 3)** — store + return them; capability enforcement is server-side per
  endpoint (Section 7). Frontend will gate pages with them later.
- **`settings.top_leaders` is always organ-specific.** "Gavin Enslin" / "Vicky Enslin" are the current
  single-org's values ONLY — they must never be a global default. New orgs start with
  `{male: null, female: null}` and set their own during org-setup. Top-leader label also comes from
  the org (their own `hierarchy[0].label` or their `top_leaders` config), never a hardcoded string.
- Org with `hierarchy: []` or `is_setup: false` → server returns the **default G12 hierarchy**
  (Section 5) so nothing breaks pre-setup.

### 2.2 Person collection

Current people have flat fields `leader1…leader1728` and sometimes an object `leaders: {…}`. Add +
standardize:

```json
{
  "_id": "…",
  "org_id": "…",
  "Name": "…", "Surname": "…", "Email": "…",
  "leaders": { "leader1": "Gavin Enslin", "shepherd": "" },
  "leader1": "Gavin Enslin",          // legacy flat fields kept during migration
  "leader12": "", "leader144": "", "leader1728": ""
}
```

- **`leaders` is the canonical source of truth** — keys must match the org's `hierarchy[].key`.
- Keep writing the legacy flat `leader1/leader12/leader144/leader1728` ONLY for keys that match the
  legacy pattern, so the current frontend keeps working during the transition (dual-write, Section 9).
- Unknown keys (not in org hierarchy) → ignore + log, do not reject the whole write.

### 2.3 User collection

Already has `organization`, `org_id`, `role`. Add on signup:
- First member of a **new** org → role `admin`, their org gets `is_setup: false`.
- Members joining an existing org stay `user` (admin must promote via /admin).
- Keep `is_supreme_admin` flag = platform-level, bypasses org scoping.

---

## 3. Endpoint contract changes

All endpoints below already exist in some form; this doc lists the required behaviour changes
(scope + hierarchy). Auth header stays `Authorization: Bearer <access_token>` everywhere.

### 3.1 Signup (POST /signup)

Body already includes `organization` (name string) + user fields.

- Resolve org: exact match against `Organization.name` (case-insensitive).
  - **Found** → set `user.org_id`.
  - **Not found** → **auto-create** the Organization (`is_setup:false`, `hierarchy: []`), set
    `user.org_id`, first member = `admin`.
- Response: include `org_id` and `org.is_setup`.

### 3.2 GET /org-config  (changed response) — FOR THE FRONTEND, THIS IS THE CONTRACT

Current frontend reads `/org-config` and expects exactly these keys — keep them all, add `key`,
`is_setup`, `roles`:

```json
{
  "org_id": "642f…",
  "org_name": "Encounter Church",
  "is_setup": false,
  "hierarchy": [
    { "key": "leader1",   "field": "leader1",   "label": "Leader @1",   "level": 1 },
    { "key": "leader12",  "field": "leader12",  "label": "Leader @12",  "level": 12 },
    { "key": "leader144", "field": "leader144", "label": "Leader @144", "level": 144 },
    { "key": "leader1728","field": "leader1728","label": "Leader @1728","level": 1728 }
  ],
  "roles": [
    { "key": "admin", "label": "Admin", "capabilities": ["admin"] },
    { "key": "leader", "label": "Leader", "capabilities": ["view_people","manage_people","create_events","close_events","view_stats","checkin"] },
    { "key": "user", "label": "Member", "capabilities": ["checkin"] }
  ],
  "recurring_event_type": "Cells",
  "top_leaders": { "male": null, "female": null },
  "allows_create_event": true,
  "allows_create_event_type": true
}
```

- `hierarchy[i].field` = `hierarchy[i].key` → **always include both** (frontend migrates off `field`
  but keeping both is harmless and avoids a breaking window).
- When the org has no hierarchy configured → return the default G12 block above, `is_setup:false`.
- 401 if no token; 404 if user has no org_id.

### 3.3 PUT /org-config  (NEW) — org setup wizard

**Permission: org admin only** (capability `admin` OR legacy role `admin`).

Body (all optional, this is a patch):

```json
{
  "org_name": "Encounter Church",
  "hierarchy": [
    { "key": "shepherd", "label": "Shepherd", "level": 1 },
    { "key": "deacon",   "label": "Deacon",   "level": 2 }
  ],
  "roles": [ { "key": "leader", "label": "Leader", "capabilities": ["view_people","checkin"] } ],
  "recurring_event_type": "Cells",
  "top_leaders": { "male": "…", "female": "…" },
  "allows_create_event": true,
  "allows_create_event_type": true,
  "is_setup": true
}
```

Validation (return 422 with a clear `detail` array on failure):
- `hierarchy`: >= 1 entry; keys unique + snake_case + regex `^[a-z][a-z0-9_]*$`; levels unique ints >= 1.
- Changing/removing a hierarchy key that people already use → **don't delete the people data**, but
  flag it: return a warning list `{ key, affected_people: N }` and proceed (people keep the orphan
  value; frontend shows it as "unassigned (old level removed)").
- Response = **the full updated org-config** (same shape as GET) with `is_setup:true`.
- After this call, old flat fields that don't exist in the new hierarchy are left alone (migration
  handles them, Section 9).

### 3.4 Organizations CRUD

- `GET /organizations` — already exists. Add to each org: `is_setup`. **Scope**: any authenticated
  user may list names (signup dropdown needs it).
- `POST /organizations` — already exists (admin page). Body may now include `hierarchy`, `roles`,
  `settings`, `is_setup`. Default `is_setup:false`.
- `PUT /organizations/:id` / `DELETE /organizations/:id` — keep; used by supreme-admin org table.
  `DELETE` must also cascade: users & people of that org → refuse if children exist (409), or soft-delete.

### 3.5 People endpoints — org scoping + dynamic leaders

- **`GET /people`, `GET /people?perPage=…`** — scope to `requester.org_id` (server-enforced).
  Include `leaders` object on every person.
- **`POST /people`** — body may include `leaders: { key: name }`. Validate keys against org
  hierarchy (ignore unknown keys). Resolve `leaderId` (first person in same org whose full name OR
  email matches) as today. Dual-write flat legacy fields (Section 9).
- **`PATCH /people/:id`** — same as POST. 403 if person.org_id != requester.org_id.
- **`GET /people/search?query=&limit=`** — scope by org_id.
- **`DELETE /people/:id`** — scope by org_id (403 if not same org).

### 3.6 People import endpoints — hierarchy-driven columns

- **`GET /people/import/preview-columns`** — currently returns hardcoded G12 leader column hints.
  Now: return leader columns from the org's `hierarchy` labels (fallback = default G12). Same for
  any reconciliation hints that mention "Leader @1/@12/@144/@1728".
- **`GET /people/import/spreadsheet?organization=…`** — keeps `organization` param. Column mapping:
  any header matching an org hierarchy label (case-insensitive; also tolerate prefixes "Leader @",
  "Leader at ", and "Leader@N" suffixes like the old file format) → store into `leaders[key]`.
- Deep-search / reconcile that matches people by `"Leader @…"` columns → derive the column list from
  the org hierarchy (level asc), not a fixed 4-level list.
- Unknown extra columns in the file → keep going (only mapped columns become hierarchy).

### 3.7 Cache / people status

- **`GET /cache/people`**, **`POST /cache/people/refresh`**, **`GET /cache/people/status`** — must
  scope/cache **per org_id** (a central cache shared across orgs is a data-leak risk). Include org_id
  in the cache key and in the returned payload.

### 3.8 Check-leader-status — dynamic semantics

- **`GET /check-leader-status`** — currently means "is this user a G12 cell leader (has a cell)".
  New meaning: **"is this user a leader at ANY level of the org's configured hierarchy?"** i.e.:
  - they appear as the value of `leaders.{key}` for another person in the same org, OR
  - their role maps to a capability that includes leader duties.
- Return: `{ "hasCell": bool, "isLeader": bool, "levels": [ {"key","label","level"} ] }`.
  Keep `hasCell` (frontend gates /events for role `user` on it). When org hierarchy is unconfigured,
  fall back to today's G12 behaviour so nothing breaks.

### 3.9 Events + persistent attendees — scope + reject cross-org

- **`GET /events/eventsdata`, `GET /events/:id`, `PATCH /events/:id`, `PATCH /events/:id/toggle-status`**
  → 404/403 if event.org_id != requester.org_id.
- **`PUT /events/:id/persistent-attendees`** body = `{ "persistent_attendees": [ … ] }` — validate
  every attendee belongs to the event's org (or is a fresh `new_people` entry with the org_id set).
- **`GET /events/:id/persistent-attendees`** response keeps `persistent_attendees`,
  `checked_in_attendees`, `attendance_status`.
- Events auto-created from people/hierarchy groups → derive the recipient of each ticket/Cell ticket
  from the person's `leaders` using the org's hierarchy order (frontend may pass the resolved fields
  explicitly — accept them).

### 3.10 Service check-in + consolidations + tasks

- **`GET /service-checkin/real-time-data?event_id=…`**, **`POST /service-checkin/checkin`**,
  **`POST /service-checkin/remove`** — scope by event + org; ensure a person checked in belongs to
  the event's org.
- **`GET/POST /consolidations`** — scope by requester org_id; `consolidation_of`/hierarchy values
  keyed by org hierarchy.
- **`GET/POST /tasks`, `GET /tasks/my-special-tasks`, `GET/POST /tasktypes`, `PATCH/DELETE /tasktypes/:id`**
  — scope by org_id; tasktypes per org.
- **`GET /profile/:userId`, `PUT /profile/:userId`, avatar/password endpoints** — keep; a user only
  ever edits their own (403 otherwise).

---

## 4. Auth / permission model

Add a small helper, e.g.:

```python
def get_org_id(user) -> str | None      # user.org_id or from their org
def require_org(user) -> Org            # 404 if no org
def require_admin(user, org) -> None    # 403 unless is_supreme_admin OR org.roles capability "admin" OR legacy role == "admin"
def scoped(request, doc) -> None        # 403 if doc.org_id != user.org_id unless is_supreme_admin
```

- `is_supreme_admin` bypasses org scoping (sees all orgs — existing admin pages keep working).
- All multi-tenant filtering MUST be server-side. The frontend sending `organization=` is a UX
  filter, **not** security.

---

## 5. Default hierarchy (compat)

When an org has no hierarchy configured, the server returns this everywhere (identical to today's
hardcoded G12), preserving the current single-org experience:

```json
[
  { "key": "leader1",   "label": "Leader @1",   "level": 1 },
  { "key": "leader12",  "label": "Leader @12",  "level": 12 },
  { "key": "leader144", "label": "Leader @144", "level": 144 },
  { "key": "leader1728","label": "Leader @1728","level": 1728 }
]
```

This chain is **open-ended** (×12 per level: 1 → 12 → 144 → 1728 → 20736 → …). It is only the
current org's default; the moment an org saves its own hierarchy via `PUT /org-config`, this default
no longer applies to them.

---

## 6. Hierarchy validation rules (shared with frontend)

- 1..16 levels.
- `key`: `^[a-z][a-z0-9_]*$`, unique.
- `label`: 1..48 chars, human-readable, unique.
- `level`: unique int >= 1, need NOT be consecutive — purely for ascending order
  (G12: 1 → 12 → 144 → 1728 → 20736 → …). Adding a new level later = just add a row in org-setup;
  no data migration.
- Changing labels (not keys) = free, no data impact.
- Adding a new level = free, people start empty on it.

---

## 7. Capability set (Phase 3, but design it now)

Fixed internal capability names (orgs just label them differently):

`admin`, `view_people`, `manage_people`, `create_events`, `close_events`, `view_stats`, `checkin`,
`reassign_people`, `manage_org`.

Server should enforce per-endpoint (e.g. PATCH /people → requires `manage_people`), even before the
frontend swaps over, so the API is already safe.

---

## 8. Endpoint matrix for the backend session

| # | Method & path | Change |
|---|---|---|
| 1 | POST /signup | auto-create org when name is new; return `org_id`, `is_setup` |
| 2 | GET /org-config | return full contract §3.2 incl. `key`, `is_setup`, `roles` |
| 3 | PUT /org-config | **NEW** — org setup wizard save (admin only) |
| 4 | GET /organizations | add `is_setup` |
| 5 | POST /organizations | accept hierarchy/roles/settings/is_setup |
| 6 | PUT/DELETE /organizations/:id | keep; DELETE guard children |
| 7 | GET/POST/PATCH/DELETE /people* | org-scope; accept+resolve `leaders` |
| 8 | GET /people/search | org-scope |
| 9 | GET /people/import/preview-columns | derive from org hierarchy |
| 10 | GET /people/import/spreadsheet | map columns from org hierarchy |
| 11 | GET /cache/people (+refresh/status) | per-org cache/scope |
| 12 | GET /check-leader-status | dynamic (§3.8) |
| 13 | GET /events*, PATCH /events/:id* | org-scope; persistent-attendees validate org |
| 14 | GET /service-checkin/*, POST checkin & remove | org-scope |
| 15 | GET/POST /consolidations | org-scope |
| 16 | GET/POST /tasks*, /tasktypes* | org-scope |

---

## 9. Migration / dual-write

For the existing single-org dataset:

- **Lazy backfill**: whenever a person is read/written and has `leaders` empty but flat
  `leader1/leader12/leader144/leader1728` set, populate `leaders` from those using the org's
  hierarchy keys that match the legacy `leader<group>` pattern.
- **Dual-write going forward**: on save, write `leaders[key]` AND (if key matches legacy pattern)
  the flat field — until this task list is done and you remove flat fields.
- No destructive migration needed; do not cascade-delete people on hierarchy edits (warn instead, §3.3).

---

## 10. Things to delete / stop hardcoding

Search the backend for: `leader1`, `leader12`, `leader144`, `leader1728`, `"Leader @1"`,
`"Leader @12"`, `"Leader @144"`, `"Leader @1728"`. Replace all of these with the org's hierarchy
iteration / `leaders` object. This is the core of the feature. Any place that builds
"cell" / "consolidation" / "ticket recipient" chains by hardcoded level names must instead walk
`hierarchy` in `level` ascending order.

---

## 11. Recommended order (backend)

1. **M0**: §2 model + §3.2/§3.3 (`GET`/`PUT /org-config`) + §3.1 signup org auto-create + §5 default
   hierarchy. → This unblocks the frontend org-setup wizard + People list.
2. **M1**: people + import endpoints (§3.5, §3.6) dynamic hierarchy + dual-write (§9).
3. **M2**: check-leader-status, cache scoping, events/persistent-attendees (§3.7–3.10).
4. **M3**: roles/capabilities (§4, §7) + org admin UI endpoints (§3.4).
5. **M4**: remove legacy flat fields + hardcoded references (§10) after both UIs switch over.

If you only do ONE endpoint this pass, make it **#3 `PUT /org-config`** — everything else derives
from the hierarchy it stores.