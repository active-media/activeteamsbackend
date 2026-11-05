-- Staff & Intern Statistics: weekly Friday/Sunday service targets.
--
-- Extends the existing public.service_targets table (already used for leader
-- weekly targets) with the columns needed to track one target per person, per
-- service (Friday/Sunday), per week, plus its met / closed state.
--
-- Rows created by the Staff & Intern feature are identified by
-- service IN ('friday','sunday'); the legacy leader-target rows leave
-- service NULL so the two usages can coexist.
--
-- Run this in the Supabase SQL editor (PostgREST/service key cannot ALTER).

ALTER TABLE public.service_targets
  ADD COLUMN IF NOT EXISTS person_id   text,
  ADD COLUMN IF NOT EXISTS service     text,
  ADD COLUMN IF NOT EXISTS actual      integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS met         boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS closed      boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS closed_at   timestamptz,
  ADD COLUMN IF NOT EXISTS closed_by   text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'service_targets_service_check'
  ) THEN
    ALTER TABLE public.service_targets
      ADD CONSTRAINT service_targets_service_check
      CHECK (service IS NULL OR service IN ('friday', 'sunday'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_service_targets_person_week_service
  ON public.service_targets (person_id, week_identifier, service);
