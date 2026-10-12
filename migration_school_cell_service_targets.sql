--
-- Migration: Add is_school_cell to events table and create service_targets table
--

-- Add is_school_cell column to events table (Supabase/Postgres)
ALTER TABLE events ADD COLUMN IF NOT EXISTS is_school_cell BOOLEAN DEFAULT FALSE;

-- Create service_targets table
CREATE TABLE IF NOT EXISTS service_targets (
    id SERIAL PRIMARY KEY,
    leader_name VARCHAR(255) NOT NULL,
    target_count INTEGER NOT NULL DEFAULT 0,
    email VARCHAR(255) NOT NULL,
    organization VARCHAR(255) NOT NULL,
    week_identifier VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add index for common queries
CREATE INDEX IF NOT EXISTS idx_service_targets_leader_name ON service_targets(leader_name);
CREATE INDEX IF NOT EXISTS idx_service_targets_week ON service_targets(week_identifier);
CREATE INDEX IF NOT EXISTS idx_service_targets_org ON service_targets(organization);

-- Keep service-target data accessible only through the scoped backend API.
ALTER TABLE public.service_targets ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.service_targets FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.service_targets TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.service_targets_id_seq TO service_role;