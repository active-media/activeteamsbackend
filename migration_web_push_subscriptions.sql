create table if not exists public.web_push_subscriptions (
    id bigint generated always as identity primary key,
    user_email text not null,
    endpoint text not null unique,
    subscription jsonb not null,
    created_at timestamptz not null default now()
);

create index if not exists web_push_subscriptions_user_email_idx
    on public.web_push_subscriptions (lower(user_email));

alter table public.web_push_subscriptions enable row level security;
