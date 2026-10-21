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

-- Grant permissions if using Supabase auth
GRANT ALL ON service_targets TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON service_targets TO service_role;