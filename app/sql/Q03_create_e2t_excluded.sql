-- Q03: accounts filtered out by TempName
create table if not exists public.e2t_excluded (
  account_id  text primary key,
  reason      text,           -- 'audition', 'free trial', or both (comma-separated)
  tempname    text,
  updated_at  timestamptz default now()
);
