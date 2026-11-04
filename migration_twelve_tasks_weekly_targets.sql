-- Allow one Admin-managed target per person, week, and organization.
ALTER TABLE public.twelve_tasks_targets
    DROP CONSTRAINT IF EXISTS twelve_tasks_targets_pkey;

CREATE UNIQUE INDEX IF NOT EXISTS twelve_tasks_targets_person_week_org_idx
    ON public.twelve_tasks_targets (person_id, week_identifier, organization);
