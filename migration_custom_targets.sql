-- Migration: create custom_targets + custom_target_snapshots
--
-- NOTE (migration): before applying the daterange exclusion constraint
-- daterange(...,'infinity','[]') to the real database, test it first on a
-- scratch copy of the DB to confirm the constraint behaves as intended and
-- does not reject valid rows.
--
-- Deactivate semantics: deactivating a target sets end_date = Sunday of the
-- current week and KEEPS is_active = true. is_active = false means
-- "cancelled before it ever took effect" and is never evaluated in active
-- target logic. Targets are never hard-deleted — the snapshot foreign key
-- enforces this.

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS public.custom_targets (
    id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    person_id text NOT NULL,
    organization text NOT NULL,
    monday integer NOT NULL DEFAULT 0 CHECK (monday >= 0),
    tuesday integer NOT NULL DEFAULT 0 CHECK (tuesday >= 0),
    wednesday integer NOT NULL DEFAULT 0 CHECK (wednesday >= 0),
    thursday integer NOT NULL DEFAULT 0 CHECK (thursday >= 0),
    friday integer NOT NULL DEFAULT 0 CHECK (friday >= 0),
    saturday integer NOT NULL DEFAULT 0 CHECK (saturday >= 0),
    sunday integer NOT NULL DEFAULT 0 CHECK (sunday >= 0),
    start_date date NOT NULL CHECK (extract(isodow FROM start_date) = 1),
    end_date date CHECK (extract(isodow FROM end_date) = 7),
    CHECK (end_date IS NULL OR end_date >= start_date),
    CHECK (monday + tuesday + wednesday + thursday + friday + saturday + sunday > 0),
    is_active boolean NOT NULL DEFAULT true,
    set_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT custom_targets_no_overlap_active
        EXCLUDE USING gist (
            person_id WITH =,
            daterange(start_date, COALESCE(end_date, 'infinity'), '[]') WITH &&
        ) WHERE (is_active)
);

CREATE TABLE IF NOT EXISTS public.custom_target_snapshots (
    id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
    custom_target_id text NOT NULL REFERENCES public.custom_targets(id),
    person_id text NOT NULL,
    week_start date NOT NULL,
    organization text NOT NULL,
    mon_target integer NOT NULL DEFAULT 0,
    tue_target integer NOT NULL DEFAULT 0,
    wed_target integer NOT NULL DEFAULT 0,
    thu_target integer NOT NULL DEFAULT 0,
    fri_target integer NOT NULL DEFAULT 0,
    sat_target integer NOT NULL DEFAULT 0,
    sun_target integer NOT NULL DEFAULT 0,
    mon_completed integer NOT NULL DEFAULT 0,
    tue_completed integer NOT NULL DEFAULT 0,
    wed_completed integer NOT NULL DEFAULT 0,
    thu_completed integer NOT NULL DEFAULT 0,
    fri_completed integer NOT NULL DEFAULT 0,
    sat_completed integer NOT NULL DEFAULT 0,
    sun_completed integer NOT NULL DEFAULT 0,
    weekly_target integer NOT NULL DEFAULT 0,
    weekly_completed integer NOT NULL DEFAULT 0,
    excluded_no_completedat integer NOT NULL DEFAULT 0,
    status text NOT NULL CHECK (status IN ('met', 'not_met')),
    evaluated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (person_id, week_start)
);
