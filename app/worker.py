# app/worker.py  (FAST + VERBOSE)
import os
import time
import math
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

from .config import (
    SUPABASE_URL, SUPABASE_KEY, SIRIX_TOKEN, RATE_DELAY_SEC, TZ_LABEL,
    TABLE_CRM_SKIM, TABLE_EXCLUDED, TABLE_ACTIVE,
    COL_LV_NAME, COL_LV_TEMPNAME, COL_LV_ACCNAME
)
from .classify import split_excluded
from .aggregate import recompute_country_totals

# ---------- Tunables via env ----------
MAX_WORKERS      = int(os.environ.get("E2T_MAX_WORKERS", "8"))         # threads for Sirix calls
LOG_EVERY        = int(os.environ.get("E2T_LOG_EVERY", "500"))         # heartbeat interval
UPSERT_BATCH     = int(os.environ.get("E2T_UPSERT_BATCH", "1000"))     # Supabase batch size
SKIP_EXISTING    = os.environ.get("E2T_SKIP_EXISTING", "true").lower() in ("1","true","yes","y")
# --------------------------------------

# Supabase REST base and headers
BASE_REST = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
HEADERS_BASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ---------------- Sirix call (inlined; similar to app/sirix.py but no import overhead per-call) -------------
SIRIX_API_URL = os.environ.get("SIRIX_API_URL", "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions").strip()
def _norm_id(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and math.isnan(v)): return None
    s = str(v).strip()
    try: return str(int(float(s)))
    except Exception: return s

def fetch_country_and_plan(uid: Any) -> Optional[Dict[str, Any]]:
    aid = _norm_id(uid)
    if not aid: return None
    try:
        headers = {"Authorization": f"Bearer {SIRIX_TOKEN}", "Content-Type": "application/json", "Accept": "application/json"}
        payload = {
            "UserID": aid,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True,
        }
        r = requests.post(SIRIX_API_URL, headers=headers, json=payload, timeout=25)
        if r.status_code != 200:
            return {"__error__": f"{r.status_code}", "account_id": aid}
        data = r.json() or {}
        country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")
        plan = None
        for t in (data.get("MonetaryTransactions") or []):
            if str(t.get("Comment", "")).lower().startswith("initial balance"):
                plan = t.get("Amount")
                break
        try:
            plan = float(plan) if plan is not None else None
        except Exception:
            plan = None
        return {"account_id": aid, "country": country, "plan": plan}
    except Exception as e:
        return {"__error__": str(e)[:160], "account_id": aid}
# -----------------------------------------------------------------------------------------------------------

# --------------- Supabase helpers (batch upsert + select existing keys) -----------------
def make_supa_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS_BASE)
    return s

def supa_select_all(session: requests.Session, table: str, cols: str, page_size: int = 1000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"select": cols, "limit": page_size, "offset": offset}
        r = session.get(f"{BASE_REST}/{table}", params=params, timeout=60)
        if r.status_code not in (200,206): r.raise_for_status()
        chunk = r.json() or []
        if not chunk: break
        out.extend(chunk)
        if len(chunk) < page_size: break
        offset += page_size
    return out

def supa_fetch_existing_active_keys(session: requests.Session) -> set:
    keys = set()
    offset = 0
    page_size = 2000
    while True:
        params = {"select": "account_id", "limit": page_size, "offset": offset}
        r = session.get(f"{BASE_REST}/{TABLE_ACTIVE}", params=params, timeout=60)
        if r.status_code not in (200,206): break
        rows = r.json() or []
        if not rows: break
        keys.update((str(x.get("account_id") or "").strip() for x in rows))
        if len(rows) < page_size: break
        offset += page_size
    return keys

def supa_upsert_batch(session: requests.Session, table: str, rows: List[Dict[str, Any]], on_conflict: str) -> bool:
    if not rows: return True
    params = {"on_conflict": on_conflict}
    headers = {**HEADERS_BASE, "Prefer": "resolution=merge-duplicates"}
    backoff = 0.5
    for attempt in range(1, 6):
        try:
            r = session.post(f"{BASE_REST}/{table}", params=params, json=rows, headers=headers, timeout=90)
            if r.status_code in (200,201,204): return True
            if r.status_code == 400:
                print(f"[UPSERT 400] table={table} sample={str(rows[0])[:180]} resp={r.text[:180]}")
                return False
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 5 or "timeout" not in msg.lower():
                print(f"[UPSERT ERR] table={table} {msg[:180]}")
                return False
            time.sleep(backoff * (1.0 + random.random() * 0.35))
            backoff = min(backoff * 1.8, 8.0)
    return False

def chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf: yield buf
# ---------------------------------------------------------------------------------------

def assert_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not SIRIX_TOKEN:  missing.append("SIRIX_TOKEN")
    if missing:
        raise SystemExit(f"[FATAL] Missing env vars: {', '.join(missing)}")

def run_once():
    started = time.time()
    print(f"[SERVICE] Starting run (TZ={TZ_LABEL})")
    assert_env()
    session = make_supa_session()

    # 1) Load CRM skim
    cols = f"{COL_LV_NAME},{COL_LV_TEMPNAME},{COL_LV_ACCNAME}"
    crm_rows = supa_select_all(session, TABLE_CRM_SKIM, cols)
    total_crm = len(crm_rows)
    if total_crm == 0:
        print(f"[WARN] No rows in {TABLE_CRM_SKIM}. Populate this table first.")
        return

    # 2) Filter out audition/free trial
    excluded, to_process_rows = split_excluded(crm_rows)
    print(f"[INFO] Read {total_crm:,} CRM rows → Excluded: {len(excluded):,} | To process: {len(to_process_rows):,}")

    # 2a) Upsert excluded in batch
    excl_payload = [{"account_id": r["account_id"], "reason": r["reason"], "tempname": r["tempname"]} for r in excluded]
    up_ok = 0
    for batch in chunked(excl_payload, UPSERT_BATCH):
        if supa_upsert_batch(session, TABLE_EXCLUDED, batch, on_conflict="account_id"):
            up_ok += len(batch)
    if excl_payload:
        print(f"[INFO] Excluded upserts: {up_ok:,}")

    # 3) Build account_id list to process (dedupe)
    todo = []
    seen = set()
    for r in to_process_rows:
        aid = _norm_id(r.get(COL_LV_NAME))
        if not aid: continue
        if aid in seen: continue
        seen.add(aid)
        todo.append(aid)
    print(f"[INFO] Unique accounts to fetch from Sirix: {len(todo):,}")

    # 3a) Optionally skip accounts already present in e2t_active
    skipped_existing = 0
    if SKIP_EXISTING:
        existing = supa_fetch_existing_active_keys(session)
        if existing:
            before = len(todo)
            todo = [x for x in todo if x not in existing]
            skipped_existing = before - len(todo)
            print(f"[INFO] SKIP_EXISTING enabled → skipped {skipped_existing:,} already in {TABLE_ACTIVE}; to fetch: {len(todo):,}")

    if not todo:
        print("[INFO] Nothing to fetch from Sirix.")
        recompute_country_totals()
        print("[DONE] Country allocation recomputed.")
        return

    # 4) Parallel Sirix fetch
    ok_results: List[Dict[str, Any]] = []
    fails = 0
    null_plan = 0
    processed = 0

    def task(uid):
        res = fetch_country_and_plan(uid)
        return res

    print(f"[SIRIX] Fetching with {MAX_WORKERS} workers …")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(task, uid) for uid in todo]
        for i, fut in enumerate(as_completed(futures), start=1):
            res = fut.result()
            processed += 1
            if res is None or "__error__" in res:
                fails += 1
            else:
                if res.get("plan") is None: null_plan += 1
                ok_results.append(res)

            if LOG_EVERY and (processed % LOG_EVERY == 0):
                pct = processed * 100.0 / max(len(todo), 1)
                print(f"[SIRIX] Progress: {processed:,}/{len(todo):,} ({pct:0.1f}%) | ok≈{len(ok_results):,} fail≈{fails:,} nullPlan≈{null_plan:,}")

            if RATE_DELAY_SEC > 0:
                # very light throttle to be nice; still concurrent
                time.sleep(RATE_DELAY_SEC / max(MAX_WORKERS,1))

    print(f"[SIRIX] Done. ok={len(ok_results):,} fail={fails:,} nullPlan={null_plan:,}")

    # 5) Batch upsert to e2t_active
    up_ok = up_fail = 0
    for batch in chunked(ok_results, UPSERT_BATCH):
        if supa_upsert_batch(session, TABLE_ACTIVE, batch, on_conflict="account_id"):
            up_ok += len(batch)
        else:
            up_fail += len(batch)
    print(f"[INFO] Active upserts ~ ok={up_ok:,}, fail={up_fail:,}")

    # 6) Totals
    recompute_country_totals()
    print("[DONE] Country allocation recomputed.")

    # 7) Summary
    elapsed = time.time() - started
    mm, ss = divmod(int(elapsed), 60)
    print("\n===== WORKER SUMMARY =====")
    print(f"CRM rows read        : {total_crm:,}")
    print(f"Excluded             : {len(excluded):,}")
    print(f"To process (unique)  : {len(todo) + skipped_existing:,}")
    if SKIP_EXISTING:
        print(f"Skipped existing     : {skipped_existing:,}")
    print(f"Sirix ok             : {len(ok_results):,}")
    print(f"Sirix failed         : {fails:,}")
    print(f"Plan missing (null)  : {null_plan:,}")
    print(f"Upserted active ok   : {up_ok:,}")
    print(f"Upserted active fail : {up_fail:,}")
    print(f"Duration             : {mm:02d}:{ss:02d} (mm:ss)")
    print("===== END =====")

if __name__ == "__main__":
    try:
        run_once()
    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
