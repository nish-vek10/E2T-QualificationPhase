# app/scheduler.py
import os, time, requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .worker import run_once
from .config import TZ_LABEL, E2T_NOTIFY_NETLIFY, NETLIFY_BUILD_HOOK_URL

# Optional: import CRM loader
try:
    from .crm_loader_local import main as crm_sync
except Exception:
    crm_sync = None

RUN_CRM = os.environ.get("RUN_CRM", "false").lower() in ("1","true","yes","y")
LONDON = ZoneInfo("Europe/London")

def next_midnight_london(now_utc: datetime) -> datetime:
    now_ldn = now_utc.astimezone(LONDON)
    next_day = now_ldn.date() + timedelta(days=1)
    target_ldn = datetime.combine(next_day, datetime.min.time(), tzinfo=LONDON)
    return target_ldn.astimezone(timezone.utc)

def trigger_netlify():
    if E2T_NOTIFY_NETLIFY and NETLIFY_BUILD_HOOK_URL:
        try:
            requests.post(NETLIFY_BUILD_HOOK_URL, timeout=10)
            print("[SCHED] Triggered Netlify build hook.")
        except Exception as e:
            print(f"[SCHED] Netlify hook failed: {e}")

def main():
    print(f"[SCHED] Starting daily scheduler. Local TZ={TZ_LABEL} (Europe/London used for timing).")

    # Boot run
    if RUN_CRM and crm_sync:
        print("[SCHED] CRM → lv_tpaccount_skim refresh starting…")
        try:
            crm_sync()
        except Exception as e:
            print(f"[SCHED] CRM refresh FAILED: {e}")
    else:
        print("[SCHED] RUN_CRM is false or CRM loader unavailable → skipping CRM refresh.")

    try:
        print("[SCHED] Boot run starting now.")
        run_once()
        trigger_netlify()
    except Exception as e:
        print(f"[SCHED] Boot run error: {e}")

    while True:
        now_utc = datetime.now(timezone.utc)
        target_utc = next_midnight_london(now_utc)
        secs = max(5, int((target_utc - now_utc).total_seconds()))
        hh = secs // 3600; mm = (secs % 3600) // 60; ss = secs % 60
        print(f"[SCHED] Sleeping until next London midnight: {target_utc.isoformat()} (in {hh:02d}:{mm:02d}:{ss:02d}).")
        time.sleep(secs)

        if RUN_CRM and crm_sync:
            print("[SCHED] CRM → lv_tpaccount_skim refresh starting…")
            try:
                crm_sync()
            except Exception as e:
                print(f"[SCHED] CRM refresh FAILED: {e}")
        else:
            print("[SCHED] RUN_CRM is false or CRM loader unavailable → skipping CRM refresh.")

        try:
            print("[SCHED] Midnight run starting.")
            run_once()
            trigger_netlify()
        except Exception as e:
            print(f"[SCHED] Midnight run error: {e}")

if __name__ == "__main__":
    main()
