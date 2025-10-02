-- Q04: materialized totals for frontend (optional if you prefer the view)
create table if not exists public.e2t_country_allocation (
  country     text primary key,
  total_plan  numeric not null default 0,
  updated_at  timestamptz default now()
);
