-- Persist the admin-managed roster and targets used by the Twelve Tasks report.
-- Access is restricted to the backend service role; API routes enforce user scope.

CREATE TABLE IF NOT EXISTS public.twelve_tasks_tracked_people (
    person_id text NOT NULL,
    leader12_id text NOT NULL,
    org text NOT NULL,
    display_name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    added_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (person_id, leader12_id)
);

CREATE INDEX IF NOT EXISTS twelve_tasks_tracked_people_org_active_idx
    ON public.twelve_tasks_tracked_people (org, active);
CREATE INDEX IF NOT EXISTS twelve_tasks_tracked_people_leader_active_idx
    ON public.twelve_tasks_tracked_people (leader12_id, active);

CREATE TABLE IF NOT EXISTS public.twelve_tasks_targets (
    person_id text PRIMARY KEY,
    target integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text
);

ALTER TABLE public.twelve_tasks_tracked_people ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.twelve_tasks_targets ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.twelve_tasks_tracked_people FROM PUBLIC, anon, authenticated;
REVOKE ALL ON TABLE public.twelve_tasks_targets FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.twelve_tasks_tracked_people TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.twelve_tasks_targets TO service_role;
