# app/worker.py
import os
import time
from typing import List, Dict, Any
from .config import (
    SUPABASE_URL, SUPABASE_KEY, SIRIX_TOKEN, RATE_DELAY_SEC, TZ_LABEL,
    TABLE_CRM_SKIM, TABLE_EXCLUDED, TABLE_ACTIVE,
    COL_LV_NAME, COL_LV_TEMPNAME, COL_LV_ACCNAME
)
from .supa import pg_select_all, pg_upsert, pg_delete
from .classify import split_excluded
from .sirix import fetch_country_and_plan
from .aggregate import recompute_country_totals

def assert_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not SIRIX_TOKEN:  missing.append("SIRIX_TOKEN")
    if missing:
        raise SystemExit(f"[FATAL] Missing env vars: {', '.join(missing)}")

def load_crm_minimal() -> List[Dict[str, Any]]:
    cols = f"{COL_LV_NAME},{COL_LV_TEMPNAME},{COL_LV_ACCNAME}"
    rows = pg_select_all(TABLE_CRM_SKIM, cols)
    return rows

def upsert_excluded(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        pg_upsert(TABLE_EXCLUDED, r, on_conflict="account_id")

def upsert_active_row(payload: Dict[str, Any]) -> None:
    # payload must have account_id, country, plan
    pg_upsert(TABLE_ACTIVE, payload, on_conflict="account_id")

def run_once():
    print(f"[SERVICE] Starting run (TZ={TZ_LABEL})")
    assert_env()

    # 1) Read CRM minimal list (from Supabase mirror)
    crm_rows = load_crm_minimal()
    if not crm_rows:
        print(f"[WARN] No rows in {TABLE_CRM_SKIM}. Populate this table first.")
        return

    # 2) Filter out audition/free trial
    excluded, to_process = split_excluded(crm_rows)
    print(f"[INFO] Excluded: {len(excluded)} | To process: {len(to_process)}")
    upsert_excluded(excluded)

    # 3) Sirix loop for the rest
    processed = 0
    for r in to_process:
        aid = r.get(COL_LV_NAME)
        res = fetch_country_and_plan(aid)
        if res is None:
            continue
        upsert_active_row(res)
        processed += 1
        if RATE_DELAY_SEC > 0:
            time.sleep(RATE_DELAY_SEC)
    print(f"[INFO] Active upserts: {processed}")

    # 4) Totals
    recompute_country_totals()
    print("[DONE] Country allocation recomputed.")

if __name__ == "__main__":
    try:
        run_once()
    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
