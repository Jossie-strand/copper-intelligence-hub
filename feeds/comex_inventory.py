"""
feeds/comex_inventory.py

Fetches COMEX Copper Vault Inventory XLS from CME Group,
parses registered/eligible/total, converts to metric tonnes,
writes to COMEX tab and updates the shared Dashboard.
"""

import os
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

COMEX_HEADERS = [
    "Date", "Report Date", "Activity Date",
    "Total Registered (st)", "Total Eligible (st)", "Total (st)", "Total (mt)",
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
    }

    all_rows = []
    for row_idx in range(ws.nrows):
        row_vals = [str(ws.cell_value(row_idx, col)).strip() for col in range(ws.ncols)]
        all_rows.append(row_vals)
        if row_idx < 20:
            print(f"Row {row_idx}: {row_vals}")

    # Dates
    for row in all_rows:
        joined = " ".join(row)
        if "Report Date" in joined and not result["report_date"]:
            for cell in row:
                if "/" in cell and len(cell) >= 8:
                    result["report_date"] = cell.strip()
                    break
        if "Activity Date" in joined and not result["activity_date"]:
            for cell in row:
                clean = cell.split(":")[-1].strip()
                if "/" in clean and len(clean) >= 8:
                    result["activity_date"] = clean
                    break

    # Totals
    for row in all_rows:
        joined = " ".join(row).upper()
        nums = []
        for c in row:
            clean = c.replace(",", "").replace(".", "")
            if clean.lstrip("-").isdigit():
                try:
                    v = float(c.replace(",", ""))
                    if abs(v) >= 100:
                        nums.append(v)
                except ValueError:
                    pass

        if "TOTAL" in joined and "REGISTERED" in joined and nums:
            result["registered_st"] = nums[-1]
            print(f"  → registered_st = {result['registered_st']}")

        if "TOTAL" in joined and "ELIGIBLE" in joined and nums:
            result["eligible_st"] = nums[-1]
            print(f"  → eligible_st = {result['eligible_st']}")

        if "TOTAL" in joined and "COPPER" in joined and nums:
            result["total_st"] = nums[-1]
            print(f"  → total_st = {result['total_st']}")

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


def calc_comex_change(tab, total_mt):
    """Delta from the most recent prior row's Total (mt) — col G (index 6, col 7)."""
    if total_mt is None:
        return None
    all_vals = tab.col_values(7)  # Total (mt) = col G
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

    # Duplicate check on activity_date (col C = col 3)
    existing = comex_tab.col_values(3)
    if data["activity_date"] and data["activity_date"] in existing:
        print(f"Duplicate: activity_date {data['activity_date']} already in sheet. Skipping.")
        return

    ensure_comex_headers(comex_tab)

    change_mt = calc_comex_change(comex_tab, data["total_mt"])

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
    print(f"✅ COMEX tab: {data['total_mt']} mt | registered {data['registered_mt']} mt | change {change_mt} mt")

    # Dashboard — pass registered + eligible as extras
    ensure_headers(dash_tab)
    write_exchange(
        dash_tab, today, "COMEX",
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
            raise ValueError("Parse failed — total_st is None")

        write_to_sheet(data)
        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
