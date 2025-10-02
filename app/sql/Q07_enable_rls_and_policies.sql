-- Q07_enable_rls_and_policies.sql

-- Enable RLS on raw tables
alter table public.e2t_active enable row level security;
alter table public.e2t_excluded enable row level security;
alter table public.lv_tpaccount_skim enable row level security;
alter table public.e2t_country_allocation enable row level security;

-- Keep raw tables private to anon (no select)
revoke all on public.lv_tpaccount_skim from anon;
revoke all on public.e2t_active from anon;
revoke all on public.e2t_excluded from anon;

-- Allow anon to read the public view (we’ll create it next)
grant usage on schema public to anon;
grant select on public.v_e2t_country_allocation to anon;

-- If you choose the materialized totals table instead of the view, additionally:
-- grant select on public.e2t_country_allocation to anon;
