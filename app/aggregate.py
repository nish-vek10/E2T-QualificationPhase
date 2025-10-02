# app/aggregate.py
from typing import Dict, Any, List, Set
from collections import defaultdict
from .supa import pg_select_all, pg_upsert, pg_delete
from .config import TABLE_ACTIVE, TABLE_ALLOC

def recompute_country_totals() -> None:
    # 1) Build new totals from e2t_active
    rows = pg_select_all(TABLE_ACTIVE, "country,plan")
    buckets: Dict[str, float] = defaultdict(float)
    for r in rows:
        c = (r.get("country") or "").strip() or "Unknown"
        try:
            p = float(r.get("plan") or 0)
        except Exception:
            p = 0.0
        buckets[c] += p

    # 2) Upsert all new totals
    new_countries: Set[str] = set(buckets.keys())
    for country, total in buckets.items():
        pg_upsert(TABLE_ALLOC, {"country": country, "total_plan": total}, on_conflict="country")

    # 3) Delete stale rows (countries that existed before but are not in the new set)
    existing = pg_select_all(TABLE_ALLOC, "country")
    for r in existing:
        c = (r.get("country") or "").strip() or "Unknown"
        if c not in new_countries:
            pg_delete(TABLE_ALLOC, {"country": f"eq.{c}"})
