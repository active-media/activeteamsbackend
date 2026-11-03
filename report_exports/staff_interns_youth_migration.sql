-- Staff / Interns / Youth: who belongs to which group
alter table public.twelve_tasks_tracked_people
  add column if not exists category text;
alter table public.twelve_tasks_tracked_people
  drop constraint if exists twelve_tasks_tracked_people_category_check;
alter table public.twelve_tasks_tracked_people
  add constraint twelve_tasks_tracked_people_category_check
  check (category is null or category in ('staff','intern','youth'));
