# app/classify.py
from typing import List, Dict, Tuple
from .config import COL_LV_TEMPNAME, COL_LV_NAME

def split_excluded(rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Returns (excluded_list, to_process_list)
    excluded has fields: account_id, reason, tempname
    to_process has lv_name (as provided)
    """
    excluded, ok = [], []
    for r in rows:
        temp = str(r.get(COL_LV_TEMPNAME, "") or "")
        name = str(r.get(COL_LV_NAME, "") or "").strip()
        low = temp.lower()
        reason = []
        if "audition" in low:   reason.append("audition")
        if "free trial" in low: reason.append("free trial")
        if reason:
            excluded.append({
                "account_id": name,
                "reason": ",".join(reason),
                "tempname": temp
            })
        else:
            if name:
                ok.append(r)
    return (excluded, ok)
