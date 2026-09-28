-- Optional Supabase schema hardening for the cell report.
-- Run this in the Supabase SQL Editor after reviewing the existing columns.

alter table if exists public.events
    add column if not exists created_at timestamptz;

alter table if exists public.events
    alter column created_at set default now();

update public.events
set created_at = coalesce(created_at, event_date::timestamptz)
where created_at is null;

alter table if exists public.event_new_people
    add column if not exists created_at timestamptz;

alter table if exists public.event_new_people
    alter column created_at set default now();

update public.event_new_people
set created_at = coalesce(created_at, added_at::timestamptz)
where created_at is null;

-- Keep the canonical report timestamp populated if an older client sends NULL.
create or replace function public.set_cell_report_created_at()
returns trigger
language plpgsql
as $$
begin
    if new.created_at is null then
        new.created_at := now();
    end if;
    return new;
end;
$$;

drop trigger if exists events_set_cell_report_created_at on public.events;
create trigger events_set_cell_report_created_at
before insert or update on public.events
for each row execute function public.set_cell_report_created_at();

drop trigger if exists event_new_people_set_cell_report_created_at on public.event_new_people;
create trigger event_new_people_set_cell_report_created_at
before insert or update on public.event_new_people
for each row execute function public.set_cell_report_created_at();

create index if not exists events_created_at_idx
    on public.events (created_at);

create index if not exists event_new_people_created_at_idx
    on public.event_new_people (created_at);
