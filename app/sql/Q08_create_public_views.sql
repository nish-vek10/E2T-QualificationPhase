-- Q08_create_public_views.sql

-- Public view for country totals
create or replace view public.v_e2t_country_allocation as
select
  coalesce(nullif(trim(country), ''), 'Unknown') as country,
  sum(coalesce(plan,0))::numeric as total_plan
from public.e2t_active
group by 1
order by total_plan desc;

-- Optional: Public, read-only subset of active accounts (for drilldown)
create or replace view public.v_e2t_active_public as
select
  account_id,
  coalesce(nullif(trim(country), ''), 'Unknown') as country,
  coalesce(plan, 0)::numeric as plan
from public.e2t_active;

-- Grant anon read on the public active view if needed on FE
grant select on public.v_e2t_active_public to anon;
