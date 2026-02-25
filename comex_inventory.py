"""
feeds/comex_inventory.py
Fetches COMEX Copper Vault Inventory XLS from CME Group,
parses registered/eligible/total, converts to metric tonnes,
and appends a row to the Google Sheet tracker.
"""

import os
import io
import json
import datetime
import requests
import xlrd
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
CME_URL = "https://www.cmegroup.com/delivery_reports/Copper_Stocks.xls"

SHEET_NAME       = "3-exchange-inventory-tracker"
TAB_COMEX        = "COMEX"
TAB_DASHBOARD    = "Dashboard"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ── FETCH ─────────────────────────────────────────────────────
def fetch_xls():
    """
    Fetch the CME copper XLS. GitHub Actions IPs are not on CME's
    blocklist the way Google Cloud IPs are — this should return 200.
    """
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
def parse_xls(content):
    """
    Parse the CME copper stocks XLS using xlrd.
    Returns dict with keys: report_date, activity_date,
    registered_st, eligible_st, total_st, total_mt
    """
    wb = xlrd.open_workbook(file_contents=content)
    ws = wb.sheet_by_index(0)

    result = {
        "report_date":   "",
        "activity_date": "",
        "registered_st": None,
        "eligible_st":   None,
        "total_st":      None,
        "total_mt":      None,
    }

    # Dump all cell values for inspection
    all_rows = []
    for row_idx in range(ws.nrows):
        row_vals = [str(ws.cell_value(row_idx, col)).strip()
                    for col in range(ws.ncols)]
        all_rows.append(row_vals)
        # Log first 20 rows for debugging
        if row_idx < 20:
            print(f"Row {row_idx}: {row_vals}")

    # Find dates
    for row in all_rows:
        joined = " ".join(row)
        if "Report Date" in joined and not result["report_date"]:
            # Date usually in next cell or same row after label
            for cell in row:
                if "/" in cell and len(cell) >= 8:
                    result["report_date"] = cell
                    break
        if "Activity Date" in joined and not result["activity_date"]:
            for cell in row:
                if "/" in cell and len(cell) >= 8:
                    result["activity_date"] = cell
                    break

    # Find Total Registered, Total Eligible, Total Copper
    for row in all_rows:
        joined = " ".join(row).upper()
        nums = [float(c.replace(",", "")) for c in row
                if c.replace(",", "").replace(".", "").isdigit()
                and float(c.replace(",", "")) >= 100]

        if "TOTAL" in joined and "REGISTERED" in joined and nums:
            result["registered_st"] = nums[-1]
            print(f"  → registered_st = {result['registered_st']}")

        if "TOTAL" in joined and "ELIGIBLE" in joined and nums:
            result["eligible_st"] = nums[-1]
            print(f"  → eligible_st = {result['eligible_st']}")

        if "TOTAL" in joined and "COPPER" in joined and nums:
            result["total_st"] = nums[-1]
            print(f"  → total_st = {result['total_st']}")

    # Fallback: derive total if not found
    if result["total_st"] is None and result["registered_st"] and result["eligible_st"]:
        result["total_st"] = result["registered_st"] + result["eligible_st"]

    if result["total_st"]:
        result["total_mt"] = round(result["total_st"] * 0.907185)

    return result

# ── GOOGLE SHEETS AUTH ────────────────────────────────────────
def get_sheet_client():
    """
    Authenticate using service account JSON stored in
    GOOGLE_SERVICE_ACCOUNT_JSON GitHub secret.
    """
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# ── WRITE TO SHEET ────────────────────────────────────────────
def write_to_sheet(data):
    client    = get_sheet_client()
    wb        = client.open(SHEET_NAME)
    comex_tab = wb.worksheet(TAB_COMEX)
    dash_tab  = wb.worksheet(TAB_DASHBOARD)

    today = datetime.date.today().isoformat()

    # Duplicate check: skip if activity_date already logged
    existing = comex_tab.col_values(3)  # Activity Date column (col C)
    if data["activity_date"] and data["activity_date"] in existing:
        print(f"Duplicate: activity_date {data['activity_date']} already in sheet. Skipping.")
        return

    comex_row = [
        today,
        data["report_date"],
        data["activity_date"],
        data["registered_st"] or "",
        data["eligible_st"]   or "",
        data["total_st"]      or "",
        data["total_mt"]      or "",
        CME_URL
    ]
    comex_tab.append_row(comex_row, value_input_option="USER_ENTERED")
    print(f"Wrote to COMEX tab: {comex_row}")

    dash_row = [
        today,
        data["report_date"],
        data["registered_st"] or "",
        data["eligible_st"]   or "",
        data["total_st"]      or "",
        data["total_mt"]      or "",
        ""
    ]
    dash_tab.append_row(dash_row, value_input_option="USER_ENTERED")
    print(f"Wrote to Dashboard tab: {dash_row}")

# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print(f"COMEX Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 50)

    try:
        content = fetch_xls()
        data    = parse_xls(content)

        print(f"\nParsed result: {json.dumps(data, indent=2)}")

        if not data["total_st"]:
            raise ValueError("Parse failed — total_st is None. Check XLS structure above.")

        write_to_sheet(data)
        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise  # Re-raise so GitHub Actions marks the run as failed

if __name__ == "__main__":
    main()
