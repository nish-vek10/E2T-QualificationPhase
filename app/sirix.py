# app/sirix.py
import math
import requests
from typing import Optional, Dict, Any
from .config import SIRIX_API_URL, SIRIX_TOKEN

def _norm_id(v: Any) -> Optional[str]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    try:
        return str(int(float(s)))
    except Exception:
        return s

def fetch_country_and_plan(user_id: Any) -> Optional[Dict[str, Any]]:
    uid = _norm_id(user_id)
    if not uid:
        return None
    try:
        headers = {
            "Authorization": f"Bearer {SIRIX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": uid,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True,
        }
        resp = requests.post(SIRIX_API_URL, headers=headers, json=payload, timeout=25)
        if resp.status_code != 200:
            print(f"[SIRIX] {resp.status_code} for {uid}")
            return None
        data = resp.json() or {}

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

        return {"account_id": uid, "country": country, "plan": plan}
    except Exception as e:
        print(f"[SIRIX] exception for {uid}: {e}")
        return None
