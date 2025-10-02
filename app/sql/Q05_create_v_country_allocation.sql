-- Q05: live view of totals (frontend can read this directly)
create or replace view public.v_e2t_country_allocation as
select
  coalesce(nullif(trim(country), ''), 'Unknown') as country,
  sum(coalesce(plan,0))::numeric as total_plan
from public.e2t_active
group by 1
order by total_plan desc;
