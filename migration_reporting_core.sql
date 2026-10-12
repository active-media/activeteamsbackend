CREATE TABLE IF NOT EXISTS public.report_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    org text NOT NULL DEFAULT '',
    report_type text NOT NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    generated_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS report_runs_org_created_at_idx
    ON public.report_runs (org, created_at DESC);