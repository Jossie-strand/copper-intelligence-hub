"""
feeds/comex_inventory.py

Fetches COMEX Copper Vault Inventory XLS from CME Group.
Captures aggregate totals AND per-warehouse breakdown for all 7 locations:
Baltimore, Detroit, El Paso, New Orleans, Owensboro, Salt Lake City, Tucson

Sheet columns (in order):
Date | Report Date | Activity Date |
Registered (st) | Eligible (st) | Total (st) | Total (mt) |
BALTIMORE Prev Total | BALTIMORE Total Today |
DETROIT Prev Total | DETROIT Total Today |
EL PASO Prev Total | EL PASO Total Today |
NEW ORLEANS Prev Total | NEW ORLEANS Total Today |
OWENSBORO Prev Total | OWENSBORO Total Today |
SALT LAKE CITY Prev Total | SALT LAKE CITY Total Today |
TUCSON Prev Total | TUCSON Total Today |
Source
"""

import os
import json
import datetime
import requests
import xlrd
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
CME_URL    = "https://www.cmegroup.com/delivery_reports/Copper_Stocks.xls"
SHEET_NAME = "3-exchange-inventory-tracker"
TAB_COMEX  = "COMEX"
TAB_DASH   = "Dashboard"

WAREHOUSES = [
    "BALTIMORE",
    "DETROIT",
    "EL PASO",
    "NEW ORLEANS",
    "OWENSBORO",
    "SALT LAKE CITY",
    "TUCSON"
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ── FETCH ─────────────────────────────────────────────────────
def fetch_xls():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Accept":     "application/vnd.ms-excel,*/*",
        "Referer":    "https://www.cmegroup.com/"
    }
    resp = requests.get(CME_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    print(f"Fetched XLS: HTTP {resp.status_code}, {len(resp.content)} bytes")
    return resp.content

# ── PARSE ─────────────────────────────────────────────────────
def parse_xls(content):
    """
    XLS structure (confirmed from file inspection 2026-02-24):
      Row 8:  col6 = 'Report Date: M/DD/YYYY'
      Row 9:  col6 = 'Activity Date: M/DD/YYYY'
      Row 11: Headers — col0=DELIVERY POINT, col2=PREV TOTAL, col7=TOTAL TODAY
      Each warehouse block:
        Row N:    warehouse name in col0 (no numbers)
        Row N+1:  Registered (warranted)
        Row N+2:  Eligible (non-warranted)
        Row N+3:  Total  ← col2=prev, col7=today
      Row 48: Total Registered (warranted)  col7=today aggregate
      Row 49: Total Eligible (non-warranted) col7=today aggregate
      Row 50: TOTAL COPPER                  col7=today aggregate
    """
    wb = xlrd.open_workbook(file_contents=content)
    ws = wb.sheet_by_index(0)

    all_rows = []
    for r in range(ws.nrows):
        row = [str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)]
        all_rows.append(row)
        print(f"Row {r+1:02d}: {row}")

    result = {
        "report_date":   "",
        "activity_date": "",
        "registered_st": None,
        "eligible_st":   None,
        "total_st":      None,
        "total_mt":      None,
        "warehouses":    {w: {"prev": None, "today": None} for w in WAREHOUSES}
    }

    current_warehouse = None

    for row in all_rows:
        label = row[0].upper().strip()

        # Dates
        if "REPORT DATE:" in row[6].upper():
            result["report_date"] = row[6].split(":")[-1].strip()
        if "ACTIVITY DATE:" in row[6].upper():
            result["activity_date"] = row[6].split(":")[-1].strip()

        # Warehouse section header
        for wh in WAREHOUSES:
            if label == wh:
                current_warehouse = wh
                break

        # Per-warehouse Total row — reset current_warehouse after capture
        if current_warehouse and label == "TOTAL":
            result["warehouses"][current_warehouse]["prev"]  = safe_float(row[2])
            result["warehouses"][current_warehouse]["today"] = safe_float(row[7])
            print(f"  → {current_warehouse}: prev={result['warehouses'][current_warehouse]['prev']}, today={result['warehouses'][current_warehouse]['today']}")
            current_warehouse = None

        # Aggregate totals (col 7 = TOTAL TODAY)
        if "TOTAL REGISTERED" in label:
            result["registered_st"] = safe_float(row[7])
            print(f"  → registered_st = {result['registered_st']}")

        if "TOTAL ELIGIBLE" in label:
            result["eligible_st"] = safe_float(row[7])
            print(f"  → eligible_st = {result['eligible_st']}")

        if label == "TOTAL COPPER":
            result["total_st"] = safe_float(row[7])
            print(f"  → total_st = {result['total_st']}")

    # Fallback
    if result["total_st"] is None and result["registered_st"] and result["eligible_st"]:
        result["total_st"] = result["registered_st"] + result["eligible_st"]

    if result["total_st"]:
        result["total_mt"] = round(result["total_st"] * 0.907185)

    return result


def safe_float(val):
    try:
        v = float(str(val).replace(",", ""))
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None

# ── GOOGLE SHEETS AUTH ────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── ENSURE HEADERS ────────────────────────────────────────────
def ensure_headers(comex_tab):
    first = comex_tab.row_values(1)
    if not first or first[0] != "Date":
        headers = [
            "Date", "Report Date", "Activity Date",
            "Registered (st)", "Eligible (st)", "Total (st)", "Total (mt)",
            "BALTIMORE Prev Total",      "BALTIMORE Total Today",
            "DETROIT Prev Total",        "DETROIT Total Today",
            "EL PASO Prev Total",        "EL PASO Total Today",
            "NEW ORLEANS Prev Total",    "NEW ORLEANS Total Today",
            "OWENSBORO Prev Total",      "OWENSBORO Total Today",
            "SALT LAKE CITY Prev Total", "SALT LAKE CITY Total Today",
            "TUCSON Prev Total",         "TUCSON Total Today",
            "Source"
        ]
        comex_tab.insert_row(headers, 1)
        print("✅ Headers written to COMEX tab")

# ── WRITE TO SHEET ────────────────────────────────────────────
def write_to_sheet(data, comex_tab, dash_tab):
    today = datetime.date.today().isoformat()

    # Duplicate check on activity date
    existing = comex_tab.col_values(3)
    if data["activity_date"] and data["activity_date"] in existing:
        print(f"Duplicate: {data['activity_date']} already logged. Skipping.")
        return

    wh = data["warehouses"]

    comex_row = [
        today,
        data["report_date"],
        data["activity_date"],
        data["registered_st"]            or "",
        data["eligible_st"]              or "",
        data["total_st"]                 or "",
        data["total_mt"]                 or "",
        wh["BALTIMORE"]["prev"]          or "", wh["BALTIMORE"]["today"]       or "",
        wh["DETROIT"]["prev"]            or "", wh["DETROIT"]["today"]         or "",
        wh["EL PASO"]["prev"]            or "", wh["EL PASO"]["today"]         or "",
        wh["NEW ORLEANS"]["prev"]        or "", wh["NEW ORLEANS"]["today"]     or "",
        wh["OWENSBORO"]["prev"]          or "", wh["OWENSBORO"]["today"]       or "",
        wh["SALT LAKE CITY"]["prev"]     or "", wh["SALT LAKE CITY"]["today"]  or "",
        wh["TUCSON"]["prev"]             or "", wh["TUCSON"]["today"]          or "",
        CME_URL
    ]

    comex_tab.append_row(comex_row, value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(comex_row)} columns to COMEX tab")

    dash_tab.append_row([
        today, data["report_date"],
        data["registered_st"] or "", data["eligible_st"] or "",
        data["total_st"] or "", data["total_mt"] or "", ""
    ], value_input_option="USER_ENTERED")
    print("✅ Wrote to Dashboard tab")

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"COMEX Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        content = fetch_xls()
        data    = parse_xls(content)

        print(f"\nParsed summary:")
        print(f"  Report Date:   {data['report_date']}")
        print(f"  Activity Date: {data['activity_date']}")
        print(f"  Registered:    {data['registered_st']} st")
        print(f"  Eligible:      {data['eligible_st']} st")
        print(f"  Total:         {data['total_st']} st / {data['total_mt']} mt")
        for wh in WAREHOUSES:
            print(f"  {wh}: prev={data['warehouses'][wh]['prev']} today={data['warehouses'][wh]['today']}")

        if not data["total_st"]:
            raise ValueError("Parse failed — total_st is None")

        client    = get_sheet_client()
        book      = client.open(SHEET_NAME)
        comex_tab = book.worksheet(TAB_COMEX)
        dash_tab  = book.worksheet(TAB_DASH)

        ensure_headers(comex_tab)
        write_to_sheet(data, comex_tab, dash_tab)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
