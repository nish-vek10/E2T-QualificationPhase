-- Q02: final usable accounts (three columns only)
create table if not exists public.e2t_active (
  account_id  text primary key,
  country     text,
  plan        numeric,
  updated_at  timestamptz default now()
);

create index if not exists idx_e2t_active_country on public.e2t_active(country);
