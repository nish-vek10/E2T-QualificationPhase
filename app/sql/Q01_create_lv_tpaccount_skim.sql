-- Q01: mirror of minimal CRM columns (stored in public schema)
create extension if not exists pg_trgm;

create table if not exists public.lv_tpaccount_skim (
  lv_name           text primary key,       -- Sirix user id
  lv_tempname       text,
  lv_accountidname  text,
  src_loaded_at     timestamptz default now()
);

create index if not exists idx_lv_tpaccount_skim_tempname
  on public.lv_tpaccount_skim using gin (lv_tempname gin_trgm_ops);
