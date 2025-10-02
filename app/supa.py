# app/supa.py
import time
import random
import requests
from typing import Any, Dict, List, Optional
from .config import SUPABASE_URL, SUPABASE_KEY

BASE_REST = f"{SUPABASE_URL}/rest/v1".rstrip("/")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Accept-Profile": "public",
    "Content-Profile": "public",
}

def _retryable(msg: str) -> bool:
    for k in ("RemoteProtocolError", "ConnectionReset", "ServerDisconnected", "ReadTimeout",
              "EOF", "temporarily unavailable", "timeout", "pool"):
        if k.lower() in (msg or "").lower():
            return True
    return False

def _backoff_sleep(backoff: float) -> float:
    time.sleep(backoff * (1.0 + random.random() * 0.3))
    return min(backoff * 2.0, 10.0)

def pg_select(table: str, select: str, *, filters: Optional[Dict[str,str]] = None,
              order: Optional[str] = None, desc: bool=False,
              limit: Optional[int]=None, offset: Optional[int]=None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"select": select}
    if filters: params.update(filters)
    if order:   params["order"] = f"{order}.{'desc' if desc else 'asc'}"
    if limit is not None:  params["limit"] = limit
    if offset is not None: params["offset"] = offset

    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.get(f"{BASE_REST}/{table}", headers=HEADERS, params=params, timeout=30)
            if r.status_code in (200, 206):
                return r.json() or []
            if r.status_code == 406:
                return []
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_select {table}: {msg[:200]}")
                raise
            backoff = _backoff_sleep(backoff)
    return []

def pg_select_all(table: str, select: str, *, filters: Optional[Dict[str, str]]=None,
                  order: Optional[str]=None, desc: bool=False, page_size: int=1000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        chunk = pg_select(table, select, filters=filters, order=order, desc=desc, limit=page_size, offset=offset)
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return out

def pg_upsert(table: str, row: dict, on_conflict: str) -> None:
    params = {"on_conflict": on_conflict}
    headers = {**HEADERS, "Prefer": "resolution=merge-duplicates"}
    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.post(f"{BASE_REST}/{table}", headers=headers, params=params, json=row, timeout=30)
            if r.status_code in (200, 201, 204):
                return
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_upsert {table}: {msg[:200]} | row={str(row)[:160]}")
                return
            backoff = _backoff_sleep(backoff)

def pg_delete(table: str, filters: Dict[str, str]) -> None:
    params = dict(filters)
    backoff = 0.5
    for attempt in range(1, 7):
        try:
            r = requests.delete(f"{BASE_REST}/{table}", headers=HEADERS, params=params, timeout=30)
            if r.status_code in (200, 204):
                return
            r.raise_for_status()
        except Exception as e:
            msg = str(e)
            if attempt == 6 or not _retryable(msg):
                print(f"[ERROR] pg_delete {table}: {msg[:200]} | filters={filters}")
                return
            backoff = _backoff_sleep(backoff)

def pg_truncate(table: str) -> None:
    """No direct TRUNCATE via PostgREST; emulate by deleting all."""
    pg_delete(table, {})  # dangerous only if RLS is open; we use service role
