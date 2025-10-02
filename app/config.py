# app/config.py
from dotenv import load_dotenv
load_dotenv()

import os


def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "")
    if not v:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", os.environ.get("SUPABASE_ANON_KEY", "")).strip()

SIRIX_API_URL = os.environ.get("SIRIX_API_URL", "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions").strip()
SIRIX_TOKEN   = os.environ.get("SIRIX_TOKEN", "").strip()

TZ_LABEL = os.environ.get("TZ_LABEL", "Europe/London")
RATE_DELAY_SEC = float(os.environ.get("RATE_DELAY_SEC", "0.2"))

E2T_NOTIFY_NETLIFY = getenv_bool("E2T_NOTIFY_NETLIFY", False)
NETLIFY_BUILD_HOOK_URL = os.environ.get("NETLIFY_BUILD_HOOK_URL", "").strip()

# Table names (public schema)
TABLE_CRM_SKIM   = "lv_tpaccount_skim"     # lv_name, lv_tempname, lv_accountidname (mirror)
TABLE_ACTIVE     = "e2t_active"            # account_id, country, plan
TABLE_EXCLUDED   = "e2t_excluded"          # account_id, reason, tempname
TABLE_ALLOC      = "e2t_country_allocation"  # country, total_plan
VIEW_ALLOC       = "v_e2t_country_allocation" # optional view

# Columns used from CRM mirror
COL_LV_NAME      = "lv_name"
COL_LV_TEMPNAME  = "lv_tempname"
COL_LV_ACCNAME   = "lv_accountidname"
