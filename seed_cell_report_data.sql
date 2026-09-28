-- Cell report test data seed
-- Run in Supabase SQL Editor. Every row is standalone and linked by IDs.
-- Safe to remove with the cleanup block at the end.

begin;

-- Remove an earlier run without touching real data.
delete from public.event_consolidations
where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
delete from public.event_session_attendees
where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
delete from public.event_attendees
where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
delete from public.event_new_people
where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
delete from public.event_sessions
where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
delete from public.events
where event_name like 'REPORT_TEST_%';
delete from public."People"
where "Email" like 'report-test-%@example.com'
    or "_id"::text like 'report-test-person-%';
delete from public."Users"
where email like 'report-%@example.com'
    or "_id"::text in (
         '00000000-0000-4000-8000-000000000144',
         '00000000-0000-4000-8000-000000000012',
         '00000000-0000-4000-8000-000000001728'
    );

-- Test hierarchy rows. These are normal Users rows, not embedded leader objects.
insert into public."Users" (
    "_id", name, surname, email, role, "Organization", org_id,
    leader12, leader144, leader1728
)
values
('00000000-0000-4000-8000-000000000144', 'Report', 'Leader 144', 'report-leader144@example.com', 'leader', 'Active Church', 'active-church', null, null, null),
('00000000-0000-4000-8000-000000000012', 'Report', 'Leader 12', 'report-leader12@example.com', 'leader', 'Active Church', 'active-church', null, '00000000-0000-4000-8000-000000000144', null),
('00000000-0000-4000-8000-000000001728', 'Report', 'Cell Leader', 'report-cell-leader@example.com', 'leader', 'Active Church', 'active-church', '00000000-0000-4000-8000-000000000012', '00000000-0000-4000-8000-000000000144', null)
;

-- People are independent rows. Their IDs are used by attendee/consolidation rows.
insert into public."People" ("_id", "Name", "Surname", "Number", "Email", "Organization", "Stage", "Date Created")
values
('report-test-person-001', 'Report', 'Person 001', '+27000000001', 'report-test-001@example.com', 'Active Church', 'Win', now() - interval '25 days'),
('report-test-person-002', 'Report', 'Person 002', '+27000000002', 'report-test-002@example.com', 'Active Church', 'Win', now() - interval '20 days'),
('report-test-person-003', 'Report', 'Person 003', '+27000000003', 'report-test-003@example.com', 'Active Church', 'Win', now() - interval '12 days'),
('report-test-person-004', 'Report', 'Person 004', '+27000000004', 'report-test-004@example.com', 'Active Church', 'Win', now() - interval '8 days'),
('report-test-person-005', 'Report', 'Person 005', '+27000000005', 'report-test-005@example.com', 'Active Church', 'Win', now() - interval '3 days')
;

-- Three independent cell event rows across the current monthly range.
insert into public.events (
    event_name, event_type_name, event_leader, event_leader_email,
    organization, event_date, status, is_active, is_recurring, recurring_day
)
values
('REPORT_TEST_CELL_A', 'Cells', 'Report Leader 12', 'report-leader12@example.com', 'Active Church', current_date - 20, 'open', true, true, 'Wednesday'),
('REPORT_TEST_CELL_B', 'Cells', 'Report Leader 12', 'report-leader12@example.com', 'Active Church', current_date - 10, 'open', true, true, 'Thursday'),
('REPORT_TEST_CELL_C', 'Cells', 'Report Leader 144', 'report-leader144@example.com', 'Active Church', current_date - 3, 'open', true, true, 'Friday');

-- One completed session per cell. No attendee data is embedded in the session.
insert into public.event_sessions (
    event_id, session_date, week_identifier, status, is_did_not_meet,
    checked_in_count, total_headcounts, decisions_first_time,
    decisions_recommitment, decisions_total, total_associated
)
select event_id, current_date - days_back,
       to_char(current_date - days_back, 'IYYY-"W"IW'),
       'complete', false, 0, 0, 0, 0, 0, 0
from public.events
cross join (values (20), (10), (3)) as dates(days_back)
where event_name in ('REPORT_TEST_CELL_A', 'REPORT_TEST_CELL_B', 'REPORT_TEST_CELL_C')
  and event_name = case days_back
      when 20 then 'REPORT_TEST_CELL_A'
      when 10 then 'REPORT_TEST_CELL_B'
      when 3 then 'REPORT_TEST_CELL_C'
  end;

-- Existing people attending cells. Each row is an independent attendance fact.
insert into public.event_session_attendees (
    session_id, event_id, mongo_person_id, full_name, email, phone, is_checked_in, check_in_date
)
select s.session_id, s.event_id, p."_id", p."Name" || ' ' || p."Surname", p."Email", p."Number", true, now()::date
from public.event_sessions s
join public.events e on e.event_id::text = s.event_id::text
join public."People" p on p."_id" in ('report-test-person-001', 'report-test-person-002')
where e.event_name = 'REPORT_TEST_CELL_A';

insert into public.event_session_attendees (
    session_id, event_id, mongo_person_id, full_name, email, phone, is_checked_in, check_in_date
)
select s.session_id, s.event_id, p."_id", p."Name" || ' ' || p."Surname", p."Email", p."Number", true, now()::date
from public.event_sessions s
join public.events e on e.event_id::text = s.event_id::text
join public."People" p on p."_id" in ('report-test-person-002', 'report-test-person-003', 'report-test-person-004')
where e.event_name = 'REPORT_TEST_CELL_B';

insert into public.event_session_attendees (
    session_id, event_id, mongo_person_id, full_name, email, phone, is_checked_in, check_in_date
)
select s.session_id, s.event_id, p."_id", p."Name" || ' ' || p."Surname", p."Email", p."Number", true, now()::date
from public.event_sessions s
join public.events e on e.event_id::text = s.event_id::text
join public."People" p on p."_id" in ('report-test-person-004', 'report-test-person-005')
where e.event_name = 'REPORT_TEST_CELL_C';

-- Explicit new visitors are separate rows, not nested in events.
insert into public.event_new_people (
    id, event_id, mongo_id, name, surname, email, phone, is_checked_in, needs_db_entry, added_at
)
select gen_random_uuid()::text, e.event_id::text, 'report-test-new-001', 'Visitor', 'One', 'report-test-new-001@example.com', '+27000000101', true, true, now() - interval '10 days'
from public.events e where e.event_name = 'REPORT_TEST_CELL_A';

insert into public.event_new_people (
    id, event_id, mongo_id, name, surname, email, phone, is_checked_in, needs_db_entry, added_at
)
select gen_random_uuid()::text, e.event_id::text, 'report-test-new-002', 'Visitor', 'Two', 'report-test-new-002@example.com', '+27000000102', true, true, now() - interval '3 days'
from public.events e where e.event_name = 'REPORT_TEST_CELL_C';

-- Additional dated cells make every report period return useful data.
insert into public.events (
    event_name, event_type_name, event_leader, event_leader_email,
    organization, event_date, status, is_active, is_recurring, recurring_day
)
select
    'REPORT_TEST_CELL_' || cell_code,
    'Cells',
    case when cell_code in ('D', 'E', 'F', 'G', 'H') then 'Report Leader 12' else 'Report Leader 144' end,
    case when cell_code in ('D', 'E', 'F', 'G', 'H') then 'report-leader12@example.com' else 'report-leader144@example.com' end,
    'Active Church',
    current_date - days_back,
    'open', true, true, 'Wednesday'
from (values
    ('D', 40), ('E', 70), ('F', 100), ('G', 130), ('H', 160),
    ('I', 190), ('J', 220), ('K', 250), ('L', 280), ('M', 320)
) as cells(cell_code, days_back);

insert into public.event_sessions (
    event_id, session_date, week_identifier, status, is_did_not_meet,
    checked_in_count, total_headcounts, decisions_first_time,
    decisions_recommitment, decisions_total, total_associated
)
select event_id, event_date::date,
       to_char(event_date::date, 'IYYY-"W"IW'),
       'complete', false, 0, 0, 0, 0, 0, 0
from public.events
where event_name like 'REPORT_TEST_CELL_%'
  and event_name not in ('REPORT_TEST_CELL_A', 'REPORT_TEST_CELL_B', 'REPORT_TEST_CELL_C');

-- Three attendance visits per added cell, stored as separate rows.
insert into public.event_session_attendees (
    session_id, event_id, mongo_person_id, full_name, email, phone, is_checked_in, check_in_date
)
select s.session_id, s.event_id, p."_id", p."Name" || ' ' || p."Surname",
    p."Email", p."Number", true, s.session_date::date
from public.event_sessions s
join public.events e on e.event_id = s.event_id
join public."People" p on p."_id" in ('report-test-person-001', 'report-test-person-002', 'report-test-person-003')
where e.event_name like 'REPORT_TEST_CELL_%'
  and e.event_name not in ('REPORT_TEST_CELL_A', 'REPORT_TEST_CELL_B', 'REPORT_TEST_CELL_C');

-- One new visitor per added cell, with a standalone ID and added_at date.
insert into public.event_new_people (
    id, event_id, mongo_id, name, surname, email, phone,
    is_checked_in, needs_db_entry, added_at
)
select gen_random_uuid()::text, e.event_id::text,
       'report-test-' || lower(right(e.event_name, 1)),
       'Visitor', 'Period ' || right(e.event_name, 1),
       'report-test-' || lower(right(e.event_name, 1)) || '@example.com',
       '+27000000' || (200 + row_number() over (order by e.event_name))::text,
     true, true, e.event_date::timestamptz
from public.events e
where e.event_name like 'REPORT_TEST_CELL_%'
  and e.event_name not in ('REPORT_TEST_CELL_A', 'REPORT_TEST_CELL_B', 'REPORT_TEST_CELL_C');

-- One first-time decision per added cell.
insert into public.event_consolidations (
    event_id, mongo_person_id, person_name, person_surname, person_email,
    person_phone, decision_type, decision_display_name, assigned_to,
    status, created_at, session_id
)
select e.event_id, 'report-test-' || lower(right(e.event_name, 1)),
       'Visitor', 'Period ' || right(e.event_name, 1),
       'report-test-' || lower(right(e.event_name, 1)) || '@example.com',
       '+27000000' || (200 + row_number() over (order by e.event_name))::text,
       'first_time', 'First Time', e.event_leader_email, 'active',
    e.event_date::timestamptz, s.session_id::text
from public.events e
join public.event_sessions s on s.event_id = e.event_id
where e.event_name like 'REPORT_TEST_CELL_%'
  and e.event_name not in ('REPORT_TEST_CELL_A', 'REPORT_TEST_CELL_B', 'REPORT_TEST_CELL_C');

-- Keep session summary counters consistent with their standalone child rows.
update public.event_sessions s
set checked_in_count = counts.total_attendance,
    total_headcounts = counts.total_attendance,
    decisions_first_time = counts.total_decisions,
    decisions_total = counts.total_decisions
from (
    select s2.session_id,
           count(distinct a.mongo_person_id) as total_attendance,
           count(distinct c.id) as total_decisions
    from public.event_sessions s2
    left join public.event_session_attendees a on a.session_id = s2.session_id and a.is_checked_in = true
    left join public.event_consolidations c on c.session_id = s2.session_id::text and c.decision_type = 'first_time'
    group by s2.session_id
) counts
where s.session_id = counts.session_id;

-- First-time decisions are separate report facts linked to the event.
insert into public.event_consolidations (
    event_id, mongo_person_id, person_name, person_surname, person_email,
    person_phone, decision_type, decision_display_name, assigned_to,
    status, created_at
)
select e.event_id, 'report-test-new-001', 'Visitor', 'One', 'report-test-new-001@example.com',
       '+27000000101', 'first_time', 'First Time', 'report-leader12@example.com', 'active', now() - interval '10 days'
from public.events e where e.event_name = 'REPORT_TEST_CELL_A';

insert into public.event_consolidations (
    event_id, mongo_person_id, person_name, person_surname, person_email,
    person_phone, decision_type, decision_display_name, assigned_to,
    status, created_at
)
select e.event_id, 'report-test-new-002', 'Visitor', 'Two', 'report-test-new-002@example.com',
       '+27000000102', 'first_time', 'First Time', 'report-leader144@example.com', 'active', now() - interval '3 days'
from public.events e where e.event_name = 'REPORT_TEST_CELL_C';

update public.event_consolidations c
set session_id = s.session_id::text
from public.events e
join public.event_sessions s on s.event_id = e.event_id
where c.event_id = e.event_id
    and e.event_name like 'REPORT_TEST_CELL_%';

commit;

-- Verify the seeded standalone records.
select 'events' as table_name, count(*) as row_count
from public.events where event_name like 'REPORT_TEST_%'
union all
select 'sessions', count(*) from public.event_sessions s
join public.events e on e.event_id::text = s.event_id::text
where e.event_name like 'REPORT_TEST_%'
union all
select 'attendees', count(*) from public.event_session_attendees a
join public.events e on e.event_id::text = a.event_id::text
where e.event_name like 'REPORT_TEST_%'
union all
select 'new_people', count(*) from public.event_new_people n
join public.events e on e.event_id::text = n.event_id::text
where e.event_name like 'REPORT_TEST_%'
union all
select 'first_time_decisions', count(*) from public.event_consolidations c
join public.events e on e.event_id::text = c.event_id::text
where e.event_name like 'REPORT_TEST_%';

-- Cleanup later, if required:
-- delete from public.event_consolidations where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
-- delete from public.event_session_attendees where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
-- delete from public.event_new_people where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
-- delete from public.event_sessions where event_id::text in (select event_id::text from public.events where event_name like 'REPORT_TEST_%');
-- delete from public.events where event_name like 'REPORT_TEST_%';
-- delete from public."People" where "Email" like 'report-test-%@example.com';
