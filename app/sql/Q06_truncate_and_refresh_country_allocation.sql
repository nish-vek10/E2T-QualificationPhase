-- Q06: rebuild totals table from e2t_active (run after worker completes)
truncate table public.e2t_country_allocation;

insert into public.e2t_country_allocation (country, total_plan, updated_at)
select
  coalesce(nullif(trim(country), ''), 'Unknown') as country,
  sum(coalesce(plan,0)) as total_plan,
  now()
from public.e2t_active
group by 1;
