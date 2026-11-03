-- Potential Leaders: no equivalent exists in the current schema.
create table if not exists public.potential_leaders (
  id            uuid primary key default gen_random_uuid(),
  person_id     text not null,                 -- People._id of the potential leader
  leader12_id   text not null,                 -- People._id of the Leader @12 who nominated them
  comment       text,
  "Organization" text,
  org_id        text,
  active        boolean not null default true,
  created_by    text,
  created_at    timestamptz not null default now(),
  unique (person_id, leader12_id)
);
create index if not exists potential_leaders_leader_idx on public.potential_leaders (leader12_id, active);
