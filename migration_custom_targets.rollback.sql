-- Rollback: drop custom_target_snapshots first, then custom_targets.
DROP TABLE IF EXISTS public.custom_target_snapshots;
DROP TABLE IF EXISTS public.custom_targets;
-- btree_gist extension is NOT dropped here (shared extension).
