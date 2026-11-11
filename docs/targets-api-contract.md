# Targets API Contract

All endpoints are served under `/targets` and require the existing bearer token.
No application code is generated from this contract; it defines the database
schema, response shape, and permission rules for the custom_targets feature.

## Roles

| Role | Source |
|------|--------|
| `leader` | Users.role |
| `user` | Users.role |
| `admin` | Users.role |
| `registrant` | Users.role |
| `leaderAt12` | Users.role |

`supreme_admin` is **NOT** a role value. Supreme admins are identified by
`lower(Users.is_supreme_admin) = 'true'` (values: `'true'`, `'false'`, `NULL`)
and are treated as admin-level regardless of `Users.role` (e.g. one registrant
is a supreme admin).

## Permission matrix

| Action | leader | user | admin | registrant | leaderAt12 | supreme admin (is_supreme_admin='true') |
|--------|--------|------|-------|------------|------------|------------------------------------------|
| Read own targets | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Read org targets | — | — | ✓ | — | ✓ (own downline) | ✓ |
| Create target | — | — | ✓ | — | ✓ (own downline) | ✓ |
| Update target | — | — | ✓ | — | ✓ (own downline) | ✓ |
| Deactivate target | — | — | ✓ | — | ✓ (own downline) | ✓ |
| Hard-delete target | — | — | — | — | — | — (never hard-deleted) |

## Database tables

### custom_targets

| Column | Type | Notes |
|--------|------|-------|
| id | text PK | DEFAULT gen_random_uuid()::text |
| person_id | text NOT NULL | Canonical person key = People._id |
| organization | text NOT NULL | |
| monday..sunday | integer NOT NULL DEFAULT 0 | CHECK >= 0 each; CHECK sum > 0 |
| start_date | date NOT NULL | CHECK isodow = 1 (Monday) |
| end_date | date | CHECK isodow = 7 (Sunday); NULL = open-ended |
| is_active | boolean NOT NULL DEFAULT true | false = cancelled-before-effect, never evaluated |
| set_by | text NOT NULL | |
| created_at | timestamptz | DEFAULT now() |
| updated_at | timestamptz | DEFAULT now() |

Constraints:
- Idempotent exclusion on `(person_id, daterange(start_date, COALESCE(end_date,'infinity'),'[]')) WHERE is_active` — prevents overlapping active ranges for the same person.
- No `week_identifier` column.
- No unique on `(person_id, week_identifier, organization)` — that is the 12 Tasks per-week pattern and conflicts with versioning.

### custom_target_snapshots

| Column | Type | Notes |
|--------|------|-------|
| id | text PK | DEFAULT gen_random_uuid()::text |
| custom_target_id | text NOT NULL | FK → custom_targets(id) |
| person_id | text NOT NULL | |
| week_start | date NOT NULL | Monday of the snapshot week |
| organization | text NOT NULL | |
| mon_target .. sun_target | integer NOT NULL DEFAULT 0 | Per-day targets |
| mon_completed .. sun_completed | integer NOT NULL DEFAULT 0 | Per-day completed counts |
| weekly_target | integer NOT NULL DEFAULT 0 | Sum of mon..sun targets |
| weekly_completed | integer NOT NULL DEFAULT 0 | Sum of mon..sun completed |
| excluded_no_completedat | integer NOT NULL DEFAULT 0 | Completed tasks with NULL completedAt excluded from counts |
| status | text NOT NULL | CHECK in ('met', 'not_met') |
| evaluated_at | timestamptz | DEFAULT now() |

- UNIQUE (person_id, week_start)
- `provisional` is NOT stored — it is computed at read time.
- Unmapped context is NOT stored per person.

## Weekly-results response

One row per target:

| Field | Type | Description |
|-------|------|-------------|
| person_id | text | Canonical person key = People._id |
| week_start | date | Monday of the week |
| weekly_target | integer | Sum of mon..sun targets |
| weekly_completed | integer | Sum of mon..sun completed |
| excluded_no_completedat | integer | Completed tasks with NULL completedAt excluded from this person's count |
| status | string | `met`, `not_met`, or `in_progress` |
| week.provisional | boolean | Computed: true when completed but not yet frozen |

### Status rules

- `in_progress`: ONLY the current unfinished week.
- `met` / `not_met`: a completed week. If completed but not yet frozen (before the freeze time), `week.provisional = true`. The UI shows red/green plus a "Provisional" tag.

## Completed definition

A task counts as completed when:

    lower(status) = 'completed' AND completedAt IS NOT NULL

- `done`, `closed`, `finished` do **not** exist in the live data.
- ~518 "Completed" rows have NULL completedAt: exclude from counts but report `excluded_no_completedat` per person and in the summary.
- Do not fall back to `updated_at`.

## Deactivate semantics

Deactivating a target:
- Sets `end_date` = Sunday of the current week
- KEEPS `is_active = true`

`is_active = false` means "cancelled before it ever took effect" and is never
evaluated in active-target logic.

Targets are never hard-deleted. The snapshot foreign key enforces this.

## Identity

Canonical person key: `custom_targets.person_id = People._id` (text).

Reasons: People._id is unique and complete (17,750 rows); LeaderPath[1] matches People._id for 16,253 of 16,715 rows (0 match People.person_id); 32 of 40 existing twelve_tasks_targets match People._id (0 match uuid).

Known data issues (do not fix):
- 462 dangling LeaderPath ids
- 8 dangling twelve_tasks targets

## User-to-person mapping

Chain: `Users.person_id` (uuid) → `People.person_id` → `People._id`

Email fallback (lower/trim) only for users without a uuid link; every fallback
is logged. 10 of 237 users have no uuid link.

## Task attribution

Rule (FINAL): a completed task belongs to the user whose
`lower(trim(Users.email)) = lower(trim(Tasks.assignedfor))`; then map
`Users.person_id` → `People.person_id` → `People._id`.

Secondary matches (`assigned_to_email`, `assigned_to_user_id`) are used ONLY
when `assignedfor` does not match, and each secondary match is logged.

NEVER use `memberID` (it is the contact, not the assignee) or `Tasks.person_id`.

Unattributable completed tasks (~278 unmatched emails plus ~367 with a
non-email `assignedfor`) are reported as `unmapped_completed_tasks` and are
never silently dropped.

### Tracking eligibility

Only people with a linked `Users` account can be tracked — they are the only
ones who can log tasks. The person picker and `POST /custom-targets` and
`POST /custom-targets/bulk` must reject a person without a linked user with a
clear `400` message:

    "This person has no user account, so their tasks can't be tracked"

### Tests (specified here, implemented in the test suite)

- **Reject person without linked user:** `POST /custom-targets` with a
  `person_id` that has no matching `Users` row returns `400` with the message
  above.
- **Duplicate Users.email:** when two `Users` rows share the same `email`
  (after lower/trim), the match is counted once and logged as ambiguous; the
  task is not double-counted.

## Task types

`Tasks.taskType` mixes names ("Follow Up", "Visit", ...) and ObjectId-like ids.
Find (read-only) which table maps the ids to names. Count ALL completed tasks
for now, but isolate the counting in one function with a pluggable `taskType`
filter (empty by default).

## LeaderPath column quoting

The `people` table exposes columns literally named with brackets:
`LeaderPath[0]`, `LeaderPath[1]`, `LeaderPath[2]`, `LeaderPath[3]`
(root-first leader lineage). Because the brackets are part of the column name,
these columns must be **double-quoted** in every SQL query, e.g.:

```sql
SELECT "LeaderPath[0]", "LeaderPath[1]", "LeaderPath[2]", "LeaderPath[3]"
FROM public.people
WHERE "LeaderPath[0]" = 'some-uuid';
```

In the Supabase/PostgREST client and any ORM, pass the quoted name as a string
key (do not rely on automatic quoting), e.g. `profile['LeaderPath[0]']`. Never
interpolate an unquoted `LeaderPath[0]` — the bare token is a syntax error.

## Root cause (remaining candidates)

- `on_conflict` passed as a list (not a string / column set)
- The `.insert()` fallback after a failed upsert collides with the unique
  constraint when the row exists, masking the real error
- `"not target_count"` zero-rejection (a target of 0 is wrongly rejected)
- Role-based 403 (`supreme_admin` is not a role value; derive from
  `lower(Users.is_supreme_admin) = 'true'`)

**Eliminated candidate:** `updated_at` and `id` both have defaults — removed,
as defaults do not cause an upsert/insert collision.

## Migration notes

- Test the `daterange(...,'infinity','[]')` exclusion constraint on a scratch DB
  before applying to the real DB.
- `custom_target_snapshots` is created in the same migration; rollback drops
  snapshots first, then `custom_targets`. `btree_gist` is not dropped.
