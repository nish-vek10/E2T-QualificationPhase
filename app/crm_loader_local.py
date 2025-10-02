# app/crm_loader_local.py  (FAST BATCH UPSERT)
import os, time, math, random, urllib, requests, pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# --- env/config ---
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BASE_REST = f"{SUPABASE_URL}/rest/v1"
TABLE = "lv_tpaccount_skim"
MSSQL_ODBC_DSN = os.environ["MSSQL_ODBC_DSN"]

# knobs (env overrides)
BATCH_SIZE   = int(os.environ.get("CRM_BATCH_SIZE", "1000"))        # rows per POST
LOG_EVERY    = int(os.environ.get("CRM_LOG_EVERY", "2000"))         # heartbeat
ONLY_NEW     = os.environ.get("CRM_ONLY_NEW", "true").lower() in ("1","true","yes","y")
BATCH_SLEEP  = float(os.environ.get("CRM_BATCH_SLEEP", "0.0"))      # throttle

# --- HTTP session with keep-alive ---
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",   # allow upsert
    })
    return s

def _retryable(msg: str) -> bool:
    msg = (msg or "").lower()
    return any(k in msg for k in ("timeout", "temporarily unavailable", "remote", "reset", "eof", "disconnect"))

def supa_count_rows(session: requests.Session, table: str) -> int:
    url = f"{BASE_REST}/{table}"
    params = {"select": "lv_name"}
    headers = {"Prefer": "count=exact", "Range": "0-0"}
    try:
        r = session.get(url, params=params, headers=headers, timeout=30)
        if r.status_code not in (200, 206): return -1
        cr = r.headers.get("Content-Range", "")
        if "/" in cr: return int(cr.split("/")[-1])
        return -1
    except Exception:
        return -1

def supa_fetch_existing_keys(session: requests.Session) -> set:
    """
    Pull all existing lv_name keys once, paginated, to allow ONLY_NEW filtering.
    """
    keys = set()
    url = f"{BASE_REST}/{TABLE}"
    page = 0
    page_size = 1000
    while True:
        params = {
            "select": "lv_name",
            "limit": page_size,
            "offset": page * page_size
        }
        r = session.get(url, params=params, timeout=60)
        if r.status_code not in (200,206):
            print(f"[WARN] existing-keys fetch status={r.status_code} at page {page}")
            break
        rows = r.json() or []
        if not rows:
            break
        keys.update((str(x.get("lv_name") or "").strip() for x in rows))
        if len(rows) < page_size:
            break
        page += 1
    return keys

def supa_upsert_batch(session: requests.Session, rows: list) -> bool:
    """
    Upsert a list[dict] in one POST (fast). Returns True if 2xx.
    """
    if not rows:
        return True
    url = f"{BASE_REST}/{TABLE}"
    params = {"on_conflict": "lv_name"}
    backoff = 0.5
    for attempt in range(1, 6):
        try:
            r = session.post(url, params=params, json=rows, timeout=90)
            if r.status_code in (200,201,204):
                return True
            # show one sample error if bad request
            if r.status_code == 400:
                print(f"[UPsert 400] resp: {r.text[:220]}")
                return False
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 5 or not _retryable(msg):
                print(f"[UPsert ERR] {msg[:200]}")
                return False
            time.sleep(backoff * (1.0 + random.random() * 0.35))
            backoff = min(backoff * 1.8, 8.0)
    return False

def chunked(iterable, n):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def main():
    started = time.time()
    ts_start = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[CRM] Sync started @ {ts_start}")
    session = make_session()

    before_count = supa_count_rows(session, TABLE)
    if before_count >= 0:
        print(f"[CRM] Supabase rows BEFORE: {before_count:,}")
    else:
        print("[CRM] Could not read BEFORE count")

    # --- read SQL Server ---
    print("[CRM] Extracting minimal columns …")
    params = urllib.parse.quote_plus(MSSQL_ODBC_DSN)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    sql = """
    SELECT
        CAST(Lv_name AS NVARCHAR(255))              AS lv_name,
        CAST(Lv_TempName AS NVARCHAR(255))          AS lv_tempname,
        CAST(lv_accountidName AS NVARCHAR(255))     AS lv_accountidname
    FROM dbo.Lv_tpaccount
    """
    df = pd.read_sql(sql, engine)
    total = len(df)
    print(f"[CRM] Fetched {total:,} rows")

    # --- pre-filter: drop empty lv_name + dedupe
    df["lv_name"] = df["lv_name"].astype(str).str.strip()
    df = df[df["lv_name"] != ""]
    df = df.drop_duplicates(subset=["lv_name"], keep="last").reset_index(drop=True)
    total = len(df)
    print(f"[CRM] After dedupe/non-empty: {total:,} rows")

    # --- OPTIONAL: skip already existing keys ---
    skipped_existing = 0
    if ONLY_NEW:
        existing = supa_fetch_existing_keys(session)
        if existing:
            df = df[~df["lv_name"].isin(existing)].reset_index(drop=True)
            skipped_existing = len(existing)
            print(f"[CRM] ONLY_NEW enabled → will skip {skipped_existing:,} existing keys; to upsert: {len(df):,}")

    attempted = ok = fail = 0
    # --- batch upserts ---
    for batch_idx, batch_df in enumerate(chunked(df.to_dict(orient="records"), BATCH_SIZE), start=1):
        # massage None/NaN
        rows = []
        for r in batch_df:
            rows.append({
                "lv_name": r.get("lv_name"),
                "lv_tempname": (None if pd.isna(r.get("lv_tempname")) else r.get("lv_tempname")),
                "lv_accountidname": (None if pd.isna(r.get("lv_accountidname")) else r.get("lv_accountidname")),
            })
        attempted += len(rows)
        ok_batch = supa_upsert_batch(session, rows)
        if ok_batch:
            ok += len(rows)
        else:
            fail += len(rows)

        if (attempted % max(LOG_EVERY, 1)) == 0 or ok_batch is False:
            pct = (attempted / max(total,1)) * 100
            print(f"[CRM] Progress: {attempted:,}/{total:,} ({pct:0.1f}%) | ok≈{ok:,} fail≈{fail:,}")

        if BATCH_SLEEP > 0:
            time.sleep(BATCH_SLEEP)

    after_count = supa_count_rows(session, TABLE)
    if after_count >= 0:
        print(f"[CRM] Supabase rows AFTER: {after_count:,}")
    else:
        print("[CRM] Could not read AFTER count")

    # --- summary ---
    elapsed = time.time() - started
    mm, ss = divmod(int(elapsed), 60)
    print("\n===== CRM SYNC SUMMARY =====")
    print(f"Rows fetched         : {total:,}")
    if ONLY_NEW:
        print(f"Skipped existing keys: {skipped_existing:,} (prefetch set size; not all were in SQL snapshot)")
    print(f"Attempted upserts    : {attempted:,}")
    print(f"Successful (approx)  : {ok:,}")
    print(f"Failed (approx)      : {fail:,}")
    if before_count >= 0 and after_count >= 0:
        delta = after_count - before_count
        print(f"Supabase delta       : {delta:+,} (from {before_count:,} to {after_count:,})")
    print(f"Batch size           : {BATCH_SIZE}")
    print(f"ONLY_NEW             : {ONLY_NEW}")
    print(f"Duration             : {mm:02d}:{ss:02d} (mm:ss)")
    print("===== END =====")

if __name__ == "__main__":
    main()
