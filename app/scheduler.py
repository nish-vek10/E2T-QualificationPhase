# app/scheduler.py
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .worker import run_once
from .config import TZ_LABEL

LONDON = ZoneInfo("Europe/London")

def next_midnight_london(now_utc: datetime) -> datetime:
    # Convert now to London time, jump to next day 00:00, then back to UTC
    now_ldn = now_utc.astimezone(LONDON)
    next_day = now_ldn.date() + timedelta(days=1)
    target_ldn = datetime.combine(next_day, datetime.min.time(), tzinfo=LONDON)
    return target_ldn.astimezone(timezone.utc)

def main():
    print(f"[SCHED] Starting daily scheduler. Local TZ={TZ_LABEL} (Europe/London used for timing).")
    # Run immediately once on boot so you see progress
    try:
        print("[SCHED] Boot run starting now.")
        run_once()
    except Exception as e:
        print(f"[SCHED] Boot run error: {e}")

    while True:
        now_utc = datetime.now(timezone.utc)
        target_utc = next_midnight_london(now_utc)
        secs = max(5, int((target_utc - now_utc).total_seconds()))
        hh = secs // 3600
        mm = (secs % 3600) // 60
        ss = secs % 60
        print(f"[SCHED] Sleeping until next London midnight: {target_utc.isoformat()} (in {hh:02d}:{mm:02d}:{ss:02d}).")
        time.sleep(secs)

        # wake → run
        try:
            print("[SCHED] Midnight run starting.")
            run_once()
        except Exception as e:
            print(f"[SCHED] Midnight run error: {e}")

if __name__ == "__main__":
    main()
