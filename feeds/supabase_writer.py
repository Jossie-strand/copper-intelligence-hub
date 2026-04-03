"""
feeds/supabase_writer.py

Shared Supabase writer for all three exchange inventory feed scripts.
Analogous to dashboard.py (the shared Google Sheets writer).

Tables:
  exchange_inventory_daily  — daily totals per exchange (COMEX, LME, SHFE)
  comex_warehouse_daily     — per-warehouse registered/eligible detail
  shfe_region_daily         — SHFE regional breakdown
  lme_detail_daily          — LME cancelled warrants detail

All writes are upserts keyed on the unique constraints defined in the schema.
"""

import os
import datetime
from supabase import create_client

_client = None


def get_client():
    global _client
    if _client is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_KEY"]
        _client = create_client(url, key)
    return _client


def write_inventory(data_date, exchange, total_mt, change_mt, source_url=None):
    """Upsert one row into exchange_inventory_daily."""
    row = {
        "data_date": data_date,
        "exchange": exchange,
        "total_mt": total_mt,
        "change_mt": change_mt,
        "source_url": source_url,
        "fetched_at": datetime.datetime.utcnow().isoformat(),
    }
    get_client().table("exchange_inventory_daily").upsert(
        row, on_conflict="data_date,exchange"
    ).execute()
    print(f"  ✅ Supabase exchange_inventory_daily: {exchange} {data_date} → {total_mt} mt (Δ {change_mt})")


def write_comex_warehouses(data_date, warehouses, report_date=None, activity_date=None):
    """
    Upsert COMEX per-warehouse rows into comex_warehouse_daily.

    warehouses: dict keyed by warehouse name, each value has:
      reg_prev, reg_today, elig_prev, elig_today, total_prev, total_today
      (all in SHORT TONS — converted to metric tons here)
    """
    ST_TO_MT = 0.907185
    rows = []
    for wh_name, wh in warehouses.items():
        reg_today  = wh.get("reg_today")
        elig_today = wh.get("elig_today")
        total_today = wh.get("total_today")
        reg_prev   = wh.get("reg_prev")
        elig_prev  = wh.get("elig_prev")

        rows.append({
            "data_date": data_date,
            "warehouse": wh_name,
            "registered_mt":      round(reg_today * ST_TO_MT)  if reg_today  is not None else None,
            "eligible_mt":        round(elig_today * ST_TO_MT) if elig_today is not None else None,
            "total_mt":           round(total_today * ST_TO_MT) if total_today is not None else None,
            "registered_prev_mt": round(reg_prev * ST_TO_MT)   if reg_prev   is not None else None,
            "eligible_prev_mt":   round(elig_prev * ST_TO_MT)  if elig_prev  is not None else None,
            "report_date":   report_date,
            "activity_date": activity_date,
        })

    get_client().table("comex_warehouse_daily").upsert(
        rows, on_conflict="data_date,warehouse"
    ).execute()
    print(f"  ✅ Supabase comex_warehouse_daily: {data_date} → {len(rows)} warehouses")


def write_shfe_regions(data_date, regions, total_mt=None):
    """
    Upsert SHFE regional rows into shfe_region_daily.

    regions: dict keyed by region name, each value has: {"mt": float, "change": float}
    total_mt: overall total, used to compute "Other" region
    """
    rows = []
    known_total = 0.0
    for region_name, vals in regions.items():
        mt  = vals.get("mt")
        chg = vals.get("change")
        if mt is not None:
            known_total += mt
        rows.append({
            "data_date": data_date,
            "region": region_name,
            "total_mt": mt,
            "change_mt": chg,
        })

    # Compute "Other" if total_mt is available and exceeds known regions
    if total_mt is not None and known_total > 0:
        other_mt = round(total_mt - known_total, 0)
        if other_mt != 0:
            rows.append({
                "data_date": data_date,
                "region": "Other",
                "total_mt": other_mt,
                "change_mt": None,
            })

    get_client().table("shfe_region_daily").upsert(
        rows, on_conflict="data_date,region"
    ).execute()
    print(f"  ✅ Supabase shfe_region_daily: {data_date} → {len(rows)} regions")


def write_lme_detail(data_date, cancelled_warrants_mt=None, cancelled_pct=None,
                     live_warrants_mt=None, source=None):
    """Upsert one row into lme_detail_daily."""
    row = {
        "data_date": data_date,
        "cancelled_warrants_mt": cancelled_warrants_mt,
        "cancelled_pct": cancelled_pct,
        "live_warrants_mt": live_warrants_mt,
        "source": source,
    }
    get_client().table("lme_detail_daily").upsert(
        row, on_conflict="data_date"
    ).execute()
    print(f"  ✅ Supabase lme_detail_daily: {data_date}")
