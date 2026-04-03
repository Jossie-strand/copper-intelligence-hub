"""
feeds/backfill_to_supabase.py

One-time script to backfill historical inventory data from Google Sheets
into Supabase. Safe to re-run — checks for existing dates before inserting.

Usage:
  GOOGLE_SERVICE_ACCOUNT_JSON='...' SUPABASE_URL='...' SUPABASE_SERVICE_KEY='...' \
    python feeds/backfill_to_supabase.py

Tables populated:
  exchange_inventory_daily  ← from Dashboard tab (COMEX/LME/SHFE totals)
  comex_warehouse_daily     ← from COMEX tab (per-warehouse registered/eligible)
  shfe_region_daily         ← from SHFE tab (per-region breakdown)
  lme_detail_daily          ← from Dashboard tab (cancelled warrants column)
"""

import os
import json
import datetime
import gspread
from google.oauth2.service_account import Credentials
from supabase import create_client

# ── Config ────────────────────────────────────────────────────────────────
SHEET_NAME = "3-exchange-inventory-tracker"
ST_TO_MT   = 0.907185

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

WAREHOUSES = [
    "BALTIMORE", "DETROIT", "EL PASO",
    "NEW ORLEANS", "OWENSBORO", "SALT LAKE CITY", "TUCSON",
]

# Dashboard tab column indices (0-based, from dashboard.py)
D_DATE      = 0
D_COMEX_T   = 1
D_COMEX_C   = 2
D_COMEX_REG = 3
D_COMEX_ELG = 4
D_LME_T     = 6
D_LME_C     = 7
D_LME_CW    = 8
D_SHFE_T    = 10
D_SHFE_C    = 11

# COMEX tab column indices
C_RUN_DATE  = 0
C_REPORT    = 1
C_ACTIVITY  = 2   # activity date in m/d/YYYY — this is the true data date

# SHFE tab column indices
S_RUN_DATE  = 0
S_REPORT    = 1   # report date in YYYY-MM-DD
S_TOTAL     = 2
S_CHANGE    = 3
S_SHANGHAI  = 4
S_GUANGDONG = 5
S_JIANGSU   = 6
S_ZHEJIANG  = 7
S_OTHER     = 8

# Running totals
stats = {"inserted": 0, "skipped_dup": 0, "skipped_missing": 0}


# ── Helpers ───────────────────────────────────────────────────────────────
def safe_float(val):
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(str(val).replace(",", "").replace("%", ""))
    except (ValueError, TypeError):
        return None


def parse_date(val):
    if not val or not isinstance(val, str):
        return None
    val = val.strip()
    # ISO: 2026-03-14
    try:
        datetime.date.fromisoformat(val)
        return val
    except ValueError:
        pass
    # US: 3/14/2026
    try:
        return datetime.datetime.strptime(val, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def cell(row, idx):
    return row[idx] if idx < len(row) else None


def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def fetch_existing(sb):
    """Return sets of existing keys from each Supabase table."""
    inv = sb.table("exchange_inventory_daily").select("data_date, exchange").execute()
    inv_keys = {(r["data_date"], r["exchange"]) for r in (inv.data or [])}

    cw = sb.table("comex_warehouse_daily").select("data_date, warehouse").execute()
    cw_keys = {(r["data_date"], r["warehouse"]) for r in (cw.data or [])}

    sr = sb.table("shfe_region_daily").select("data_date, region").execute()
    sr_keys = {(r["data_date"], r["region"]) for r in (sr.data or [])}

    ld = sb.table("lme_detail_daily").select("data_date").execute()
    ld_keys = {r["data_date"] for r in (ld.data or [])}

    return inv_keys, cw_keys, sr_keys, ld_keys


def upsert_chunked(sb, table, rows, conflict, chunk_size=500):
    for i in range(0, len(rows), chunk_size):
        sb.table(table).upsert(rows[i:i+chunk_size], on_conflict=conflict).execute()


# ── Dashboard tab → exchange_inventory_daily + lme_detail_daily ───────────
def backfill_dashboard(sb, book, inv_keys, lme_keys):
    print("\n" + "=" * 60)
    print("DASHBOARD TAB → exchange_inventory_daily + lme_detail_daily")
    print("=" * 60)

    dash = book.worksheet("Dashboard")
    rows = dash.get_all_values()
    if len(rows) < 2:
        print("  No data rows")
        return

    print(f"  Header: {rows[0][:12]}")
    print(f"  Data rows: {len(rows) - 1}")

    inv_batch = []
    lme_batch = []
    now = datetime.datetime.utcnow().isoformat()

    for i, row in enumerate(rows[1:], start=2):
        data_date = parse_date(cell(row, D_DATE))
        if not data_date:
            stats["skipped_missing"] += 1
            print(f"  Row {i}: SKIP — invalid date '{cell(row, D_DATE)}'")
            continue

        # ── COMEX ──
        comex_t = safe_float(cell(row, D_COMEX_T))
        if comex_t is not None:
            if (data_date, "COMEX") in inv_keys:
                stats["skipped_dup"] += 1
            else:
                inv_batch.append({
                    "data_date": data_date, "exchange": "COMEX",
                    "total_mt": comex_t,
                    "change_mt": safe_float(cell(row, D_COMEX_C)),
                    "source_url": "backfill:google-sheets/Dashboard",
                    "fetched_at": now,
                })
                inv_keys.add((data_date, "COMEX"))

        # ── LME ──
        lme_t = safe_float(cell(row, D_LME_T))
        if lme_t is not None:
            if (data_date, "LME") in inv_keys:
                stats["skipped_dup"] += 1
            else:
                inv_batch.append({
                    "data_date": data_date, "exchange": "LME",
                    "total_mt": lme_t,
                    "change_mt": safe_float(cell(row, D_LME_C)),
                    "source_url": "backfill:google-sheets/Dashboard",
                    "fetched_at": now,
                })
                inv_keys.add((data_date, "LME"))

            # LME cancelled warrants (manual entry column)
            lme_cw = safe_float(cell(row, D_LME_CW))
            if lme_cw is not None and data_date not in lme_keys:
                lme_batch.append({
                    "data_date": data_date,
                    "cancelled_warrants_mt": lme_cw,
                    "cancelled_pct": round(lme_cw / lme_t * 100, 2) if lme_t else None,
                    "live_warrants_mt": round(lme_t - lme_cw, 2) if lme_t else None,
                    "source": "backfill:google-sheets/Dashboard",
                })
                lme_keys.add(data_date)

        # ── SHFE ──
        shfe_t = safe_float(cell(row, D_SHFE_T))
        if shfe_t is not None:
            if (data_date, "SHFE") in inv_keys:
                stats["skipped_dup"] += 1
            else:
                inv_batch.append({
                    "data_date": data_date, "exchange": "SHFE",
                    "total_mt": shfe_t,
                    "change_mt": safe_float(cell(row, D_SHFE_C)),
                    "source_url": "backfill:google-sheets/Dashboard",
                    "fetched_at": now,
                })
                inv_keys.add((data_date, "SHFE"))

    if inv_batch:
        upsert_chunked(sb, "exchange_inventory_daily", inv_batch, "data_date,exchange")
        stats["inserted"] += len(inv_batch)
        print(f"  ✅ Inserted {len(inv_batch)} rows into exchange_inventory_daily")
    else:
        print("  No new exchange_inventory_daily rows")

    if lme_batch:
        upsert_chunked(sb, "lme_detail_daily", lme_batch, "data_date")
        stats["inserted"] += len(lme_batch)
        print(f"  ✅ Inserted {len(lme_batch)} rows into lme_detail_daily")


# ── COMEX tab → comex_warehouse_daily ─────────────────────────────────────
def backfill_comex(sb, book, cw_keys):
    print("\n" + "=" * 60)
    print("COMEX TAB → comex_warehouse_daily")
    print("=" * 60)

    try:
        tab = book.worksheet("COMEX")
    except gspread.exceptions.WorksheetNotFound:
        print("  COMEX tab not found, skipping")
        return

    rows = tab.get_all_values()
    if len(rows) < 2:
        print("  No data rows")
        return

    print(f"  Header: {rows[0][:7]}…")
    print(f"  Data rows: {len(rows) - 1}")

    batch = []

    for i, row in enumerate(rows[1:], start=2):
        # Activity date (col 2) is the canonical data date
        data_date = parse_date(cell(row, C_ACTIVITY))
        if not data_date:
            data_date = parse_date(cell(row, C_RUN_DATE))
        if not data_date:
            stats["skipped_missing"] += 1
            print(f"  Row {i}: SKIP — no valid date")
            continue

        report_date   = cell(row, C_REPORT)
        activity_date = cell(row, C_ACTIVITY)

        for j, wh in enumerate(WAREHOUSES):
            base = 7 + j * 6
            if len(row) <= base + 5:
                continue

            reg_prev    = safe_float(cell(row, base + 0))
            reg_today   = safe_float(cell(row, base + 1))
            elig_prev   = safe_float(cell(row, base + 2))
            elig_today  = safe_float(cell(row, base + 3))
            total_prev  = safe_float(cell(row, base + 4))
            total_today = safe_float(cell(row, base + 5))

            # Skip if no usable data
            if all(v is None for v in [reg_today, elig_today, total_today]):
                stats["skipped_missing"] += 1
                continue

            if (data_date, wh) in cw_keys:
                stats["skipped_dup"] += 1
                continue

            batch.append({
                "data_date":          data_date,
                "warehouse":          wh,
                "registered_mt":      round(reg_today * ST_TO_MT) if reg_today is not None else None,
                "eligible_mt":        round(elig_today * ST_TO_MT) if elig_today is not None else None,
                "total_mt":           round(total_today * ST_TO_MT) if total_today is not None else None,
                "registered_prev_mt": round(reg_prev * ST_TO_MT) if reg_prev is not None else None,
                "eligible_prev_mt":   round(elig_prev * ST_TO_MT) if elig_prev is not None else None,
                "report_date":        report_date,
                "activity_date":      activity_date,
            })
            cw_keys.add((data_date, wh))

    if batch:
        upsert_chunked(sb, "comex_warehouse_daily", batch, "data_date,warehouse")
        stats["inserted"] += len(batch)
        print(f"  ✅ Inserted {len(batch)} rows into comex_warehouse_daily")
    else:
        print("  No new comex_warehouse_daily rows")


# ── SHFE tab → shfe_region_daily ──────────────────────────────────────────
def backfill_shfe(sb, book, sr_keys):
    print("\n" + "=" * 60)
    print("SHFE TAB → shfe_region_daily")
    print("=" * 60)

    try:
        tab = book.worksheet("SHFE")
    except gspread.exceptions.WorksheetNotFound:
        print("  SHFE tab not found, skipping")
        return

    rows = tab.get_all_values()
    if len(rows) < 2:
        print("  No data rows")
        return

    print(f"  Header: {rows[0]}")
    print(f"  Data rows: {len(rows) - 1}")

    REGION_COLS = [
        (S_SHANGHAI,  "Shanghai"),
        (S_GUANGDONG, "Guangdong"),
        (S_JIANGSU,   "Jiangsu"),
        (S_ZHEJIANG,  "Zhejiang"),
    ]

    batch = []

    for i, row in enumerate(rows[1:], start=2):
        data_date = parse_date(cell(row, S_REPORT))
        if not data_date:
            data_date = parse_date(cell(row, S_RUN_DATE))
        if not data_date:
            stats["skipped_missing"] += 1
            print(f"  Row {i}: SKIP — no valid date")
            continue

        total = safe_float(cell(row, S_TOTAL))
        known_sum = 0.0

        for col_idx, region in REGION_COLS:
            val = safe_float(cell(row, col_idx))
            if val is None:
                continue
            known_sum += val

            if (data_date, region) in sr_keys:
                stats["skipped_dup"] += 1
                continue

            batch.append({
                "data_date": data_date,
                "region":    region,
                "total_mt":  val,
                "change_mt": None,
            })
            sr_keys.add((data_date, region))

        # "Other" — from sheet column or computed
        other = safe_float(cell(row, S_OTHER))
        if other is None and total is not None and known_sum > 0:
            other = round(total - known_sum, 0)
        if other is not None and other != 0 and (data_date, "Other") not in sr_keys:
            batch.append({
                "data_date": data_date,
                "region":    "Other",
                "total_mt":  other,
                "change_mt": None,
            })
            sr_keys.add((data_date, "Other"))

    if batch:
        upsert_chunked(sb, "shfe_region_daily", batch, "data_date,region")
        stats["inserted"] += len(batch)
        print(f"  ✅ Inserted {len(batch)} rows into shfe_region_daily")
    else:
        print("  No new shfe_region_daily rows")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("BACKFILL: Google Sheets → Supabase")
    print(f"  {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    gs = get_sheet_client()
    sb = get_supabase()
    book = gs.open(SHEET_NAME)

    print(f"\n  Sheet: {SHEET_NAME}")
    print(f"  Tabs:  {[ws.title for ws in book.worksheets()]}")

    print("\n  Fetching existing Supabase data...")
    inv_keys, cw_keys, sr_keys, lme_keys = fetch_existing(sb)
    print(f"  Existing rows: {len(inv_keys)} inventory, {len(cw_keys)} comex warehouse, "
          f"{len(sr_keys)} shfe region, {len(lme_keys)} lme detail")

    backfill_dashboard(sb, book, inv_keys, lme_keys)
    backfill_comex(sb, book, cw_keys)
    backfill_shfe(sb, book, sr_keys)

    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    print(f"  Inserted:            {stats['inserted']}")
    print(f"  Skipped (duplicate): {stats['skipped_dup']}")
    print(f"  Skipped (missing):   {stats['skipped_missing']}")
    print("\n✅ Backfill complete.")


if __name__ == "__main__":
    main()
