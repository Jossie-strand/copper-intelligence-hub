"""
feeds/comex_inventory.py

Fetches COMEX Copper Vault Inventory XLS from CME Group.
CME serves old-format binary .xls — parsed with xlrd.

COMEX tab columns:
  Date, Report Date, Activity Date,
  Registered (st), Eligible (st), Total (st), Total (mt),
  Per warehouse (7x): Reg Prev, Reg Today, Elig Prev, Elig Today, Total Prev, Total Today,
  Source

XLS structure (0-based cols):
  0: Label  2: Prev Total  3: Received  4: Withdrawn  5: Net Change  6: Adjustment  7: Total Today
"""

import os
import io
import json
import datetime
import requests
import xlrd
import gspread
from google.oauth2.service_account import Credentials
from dashboard import write_exchange, ensure_headers

# ── CONFIG ────────────────────────────────────────────────────
CME_URL       = "https://www.cmegroup.com/delivery_reports/Copper_Stocks.xls"
SHEET_NAME    = "3-exchange-inventory-tracker"
TAB_COMEX     = "COMEX"
TAB_DASHBOARD = "Dashboard"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

ST_TO_MT = 0.907185

WAREHOUSES = [
    "BALTIMORE", "DETROIT", "EL PASO",
    "NEW ORLEANS", "OWENSBORO", "SALT LAKE CITY", "TUCSON"
]

COMEX_HEADERS = [
    "Date", "Report Date", "Activity Date",
    "Registered (st)", "Eligible (st)", "Total (st)", "Total (mt)",
    # Per warehouse: Reg Prev, Reg Today, Elig Prev, Elig Today, Total Prev, Total Today
    "BALTIMORE Reg Prev", "BALTIMORE Reg Today",
    "BALTIMORE Elig Prev", "BALTIMORE Elig Today",
    "BALTIMORE Total Prev", "BALTIMORE Total Today",
    "DETROIT Reg Prev", "DETROIT Reg Today",
    "DETROIT Elig Prev", "DETROIT Elig Today",
    "DETROIT Total Prev", "DETROIT Total Today",
    "EL PASO Reg Prev", "EL PASO Reg Today",
    "EL PASO Elig Prev", "EL PASO Elig Today",
    "EL PASO Total Prev", "EL PASO Total Today",
    "NEW ORLEANS Reg Prev", "NEW ORLEANS Reg Today",
    "NEW ORLEANS Elig Prev", "NEW ORLEANS Elig Today",
    "NEW ORLEANS Total Prev", "NEW ORLEANS Total Today",
    "OWENSBORO Reg Prev", "OWENSBORO Reg Today",
    "OWENSBORO Elig Prev", "OWENSBORO Elig Today",
    "OWENSBORO Total Prev", "OWENSBORO Total Today",
    "SALT LAKE CITY Reg Prev", "SALT LAKE CITY Reg Today",
    "SALT LAKE CITY Elig Prev", "SALT LAKE CITY Elig Today",
    "SALT LAKE CITY Total Prev", "SALT LAKE CITY Total Today",
    "TUCSON Reg Prev", "TUCSON Reg Today",
    "TUCSON Elig Prev", "TUCSON Elig Today",
    "TUCSON Total Prev", "TUCSON Total Today",
    "Source"
]


# ── FETCH ─────────────────────────────────────────────────────
def fetch_xls():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Accept": "application/vnd.ms-excel,*/*",
        "Referer": "https://www.cmegroup.com/"
    }
    resp = requests.get(CME_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    print(f"Fetched XLS: HTTP {resp.status_code}, {len(resp.content)} bytes")
    return resp.content


# ── PARSE ─────────────────────────────────────────────────────
def _num(val):
    """Convert xlrd cell value to float, None if blank/text."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
        return f if f != 0.0 or val == 0 else f
    except (ValueError, TypeError):
        return None


def parse_xls(content):
    wb = xlrd.open_workbook(file_contents=content)
    ws = wb.sheet_by_index(0)

    result = {
        "report_date":   "",
        "activity_date": "",
        "registered_st": None,
        "eligible_st":   None,
        "total_st":      None,
        "total_mt":      None,
        "registered_mt": None,
        "eligible_mt":   None,
        "warehouses": {
            wh: {
                "reg_prev": None,   "reg_today": None,
                "elig_prev": None,  "elig_today": None,
                "total_prev": None, "total_today": None,
            }
            for wh in WAREHOUSES
        }
    }

    # Read all rows as lists of string values
    rows = []
    for i in range(ws.nrows):
        row = [str(ws.cell_value(i, c)).strip() for c in range(ws.ncols)]
        rows.append(row)
        if i < 20:
            print(f"Row {i}: {row}")

    current_wh = None

    for row in rows:
        label = row[0].upper()

        # ── Dates ────────────────────────────────────────────
        # They appear in col 6: "Report Date: 2/24/2026"
        for cell in row:
            if "REPORT DATE" in cell.upper() and "/" in cell:
                result["report_date"] = cell.split(":")[-1].strip()
            if "ACTIVITY DATE" in cell.upper() and "/" in cell:
                result["activity_date"] = cell.split(":")[-1].strip()

        # ── Warehouse header ─────────────────────────────────
        if label in WAREHOUSES:
            current_wh = label
            continue

        # ── Per-warehouse rows ───────────────────────────────
        if current_wh:
            prev_val  = _num(row[2]) if len(row) > 2 else None
            today_val = _num(row[7]) if len(row) > 7 else None

            if "REGISTERED" in label and "TOTAL" not in label:
                result["warehouses"][current_wh]["reg_prev"]  = prev_val
                result["warehouses"][current_wh]["reg_today"] = today_val

            elif "ELIGIBLE" in label and "TOTAL" not in label:
                result["warehouses"][current_wh]["elig_prev"]  = prev_val
                result["warehouses"][current_wh]["elig_today"] = today_val

            elif label == "TOTAL" and prev_val is not None:
                result["warehouses"][current_wh]["total_prev"]  = prev_val
                result["warehouses"][current_wh]["total_today"] = today_val

        # ── Grand totals ─────────────────────────────────────
        if "TOTAL REGISTERED" in label:
            result["registered_st"] = _num(row[7])
            print(f"  → registered_st = {result['registered_st']}")

        if "TOTAL ELIGIBLE" in label:
            result["eligible_st"] = _num(row[7])
            print(f"  → eligible_st = {result['eligible_st']}")

        if label == "TOTAL COPPER":
            result["total_st"] = _num(row[7])
            print(f"  → total_st = {result['total_st']}")

    # Fallbacks
    if result["total_st"] is None and result["registered_st"] and result["eligible_st"]:
        result["total_st"] = result["registered_st"] + result["eligible_st"]

    if result["total_st"]:
        result["total_mt"] = round(result["total_st"] * ST_TO_MT)
    if result["registered_st"]:
        result["registered_mt"] = round(result["registered_st"] * ST_TO_MT)
    if result["eligible_st"]:
        result["eligible_mt"] = round(result["eligible_st"] * ST_TO_MT)

    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_comex_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Date":
        tab.insert_row(COMEX_HEADERS, 1)
        print("✅ COMEX headers written")
    elif len(first) < len(COMEX_HEADERS):
        # Schema expanded — update headers in place without touching data
        tab.update("A1:AW1", [COMEX_HEADERS], value_input_option="USER_ENTERED")
        print("✅ COMEX headers updated to expanded schema")


def calc_comex_change(tab, total_mt):
    """Delta from most recent prior row's Total (mt) — col G (col 7)."""
    if total_mt is None:
        return None
    all_vals = tab.col_values(7)
    for v in reversed(all_vals[1:]):
        try:
            prev = float(str(v).replace(",", ""))
            return round(total_mt - prev, 0)
        except (ValueError, TypeError):
            continue
    return None


def write_to_sheet(data):
    client    = get_sheet_client()
    book      = client.open(SHEET_NAME)
    comex_tab = book.worksheet(TAB_COMEX)
    dash_tab  = book.worksheet(TAB_DASHBOARD)

    today = datetime.date.today().isoformat()

    ensure_comex_headers(comex_tab)

    # Duplicate check on activity_date (col C = col 3)
    existing = comex_tab.col_values(3)
    already_logged = data["activity_date"] and data["activity_date"] in existing

    change_mt = calc_comex_change(comex_tab, data["total_mt"])

    if already_logged:
        print(f"Duplicate: {data['activity_date']} already in COMEX tab. Skipping tab write.")
    else:
        wh_cells = []
        for wh in WAREHOUSES:
            w = data["warehouses"][wh]
            wh_cells += [
                w["reg_prev"]   if w["reg_prev"]   is not None else "",
                w["reg_today"]  if w["reg_today"]  is not None else "",
                w["elig_prev"]  if w["elig_prev"]  is not None else "",
                w["elig_today"] if w["elig_today"] is not None else "",
                w["total_prev"] if w["total_prev"] is not None else "",
                w["total_today"]if w["total_today"]is not None else "",
            ]

        comex_row = [
            today,
            data["report_date"],
            data["activity_date"],
            data["registered_st"] or "",
            data["eligible_st"]   or "",
            data["total_st"]      or "",
            data["total_mt"]      or "",
            *wh_cells,
            CME_URL
        ]
        comex_tab.append_row(comex_row, value_input_option="USER_ENTERED")
        print(f"✅ COMEX tab: {data['total_mt']} mt | reg {data['registered_mt']} mt | elig {data['eligible_mt']} mt | change {change_mt} mt")
        for wh in WAREHOUSES:
            w = data["warehouses"][wh]
            print(f"   {wh}: reg={w['reg_today']} elig={w['elig_today']} total={w['total_today']}")

    # Dashboard always runs — keyed on activity_date
    data_date = today
    if data["activity_date"]:
        try:
            data_date = datetime.datetime.strptime(
                data["activity_date"], "%m/%d/%Y"
            ).strftime("%Y-%m-%d")
        except ValueError:
            pass

    ensure_headers(dash_tab)
    write_exchange(
        dash_tab, data_date, "COMEX",
        total_mt=data["total_mt"],
        change_mt=change_mt,
        extras={
            "registered": data["registered_mt"],
            "eligible":   data["eligible_mt"],
        }
    )


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"COMEX Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        content = fetch_xls()
        data    = parse_xls(content)
        print(f"\nParsed: total_st={data['total_st']}, total_mt={data['total_mt']}, "
              f"registered_mt={data['registered_mt']}, eligible_mt={data['eligible_mt']}")

        if not data["total_st"]:
            raise ValueError("Parse failed — total_st is None. Check row logs above.")

        write_to_sheet(data)
        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
