"""
feeds/shfe_inventory.py

Fetches SHFE copper warehouse stocks directly from the Shanghai Futures Exchange.
URL: https://www.shfe.com.cn/data/tradedata/future/stockdata/dailystock_{YYYYMMDD}/EN/all.html

HTML structure (key insight):
  - Each metal has its own <table class="el-table_table">
  - Metal name is in a <tr class="special_row_type"> row
  - Region cells use rowspan — region name only appears on first warehouse row
  - Subtotal rows have class "isTotal" with 3 tds: label, value, change
  - Total rows use colspan="2" for label, then value, change

Columns written to SHFE tab:
  Date | Report Date | Total Stocks (mt) | Daily Change (mt) |
  Shanghai (mt) | Guangdong (mt) | Jiangsu (mt) | Zhejiang (mt) | Other (mt) | Source
"""

import os
import json
import datetime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
SHFE_URL_TEMPLATE = (
    "https://www.shfe.com.cn/data/tradedata/future/stockdata/"
    "dailystock_{date}/EN/all.html"
)
SHEET_NAME = "3-exchange-inventory-tracker"
TAB_SHFE   = "SHFE"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://www.shfe.com.cn/eng/reports/StatisticalData/DailyData/",
}


# ── FETCH ─────────────────────────────────────────────────────
def build_url(date_str):
    return SHFE_URL_TEMPLATE.format(date=date_str)


def find_latest_data():
    """Walk back up to 7 days to find the most recently published file."""
    today = datetime.date.today()
    for delta in range(7):
        d = today - datetime.timedelta(days=delta)
        date_str = d.strftime("%Y%m%d")
        url = build_url(date_str)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 500:
                print(f"  Found data for {date_str}")
                return date_str, resp.text
            else:
                print(f"  {date_str}: HTTP {resp.status_code}, skipping")
        except Exception as e:
            print(f"  {date_str}: {e}, skipping")
    raise ValueError("No SHFE data found in the last 7 days")


# ── PARSE ─────────────────────────────────────────────────────
def num(text):
    """Parse a number string to float, return None on failure."""
    try:
        return float(text.strip().replace(",", ""))
    except (ValueError, AttributeError):
        return None


def find_copper_table(soup):
    """
    Find the <table> containing COPPER data.
    Each metal is in its own table; the metal name is in a special_row_type tr.
    """
    for table in soup.find_all("table", class_="el-table_table"):
        special = table.find("tr", class_="special_row_type")
        if special:
            label = special.get_text(strip=True)
            # Match COPPER but not COPPER(BC)
            if label.upper().startswith("COPPER") and "BC" not in label.upper():
                return table
    return None


def parse_copper(html):
    """
    Parse the COPPER table from the all.html file.

    Row types in the copper table:
    1. special_row_type  — metal header (COPPER / Unit:Tonne)
    2. tdBorder (4 tds) — first warehouse in a region: [region, warehouse, value, change]
       region td has rowspan=N
    3. tdBorder (3 tds) — continuation warehouses: [warehouse, value, change]
    4. isTotal (3 tds)  — subtotal: [Subtotal, value, change]
    5. isTotal with colspan=2 — grand totals: [Total(Tax included), value, change]
    """
    soup = BeautifulSoup(html, "lxml")

    result = {
        "total_mt":  None,
        "change_mt": None,
        "regions":   {},
    }

    copper_table = find_copper_table(soup)
    if not copper_table:
        raise ValueError("Could not find COPPER table in HTML")

    current_region = None

    for row in copper_table.find_all("tr"):
        classes = row.get("class", [])

        # Skip the metal header row
        if "special_row_type" in classes:
            continue

        tds = row.find_all("td")
        if not tds:
            continue

        # ── Grand total rows (colspan=2 on first td) ──────────
        if tds[0].get("colspan") == "2":
            label = tds[0].get_text(strip=True)
            if label == "Total(Tax included)":
                result["total_mt"]  = num(tds[1].get_text(strip=True))
                result["change_mt"] = num(tds[2].get_text(strip=True))
                print(f"  Total(Tax included): {result['total_mt']} mt, change: {result['change_mt']}")
            # "Total" row is redundant with Tax included — skip
            continue

        # ── Subtotal rows (isTotal, 3 tds, no colspan) ────────
        if "isTotal" in classes and len(tds) == 3:
            label = tds[0].get_text(strip=True)
            if label == "Subtotal" and current_region:
                val = num(tds[1].get_text(strip=True))
                chg = num(tds[2].get_text(strip=True))
                result["regions"][current_region] = {"mt": val, "change": chg}
                print(f"  Subtotal {current_region}: {val} mt, change: {chg}")
            continue

        # ── First warehouse row in a region (4 tds) ───────────
        if len(tds) == 4:
            current_region = tds[0].get_text(strip=True)
            # tds[1]=warehouse, tds[2]=value, tds[3]=change — warehouse row, skip
            continue

        # ── Continuation warehouse rows (3 tds) ───────────────
        # warehouse, value, change — nothing to capture at warehouse level

    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Date":
        headers = [
            "Date", "Report Date",
            "Total Stocks (mt)", "Daily Change (mt)",
            "Shanghai (mt)", "Guangdong (mt)", "Jiangsu (mt)", "Zhejiang (mt)",
            "Other Regions (mt)", "Source"
        ]
        tab.insert_row(headers, 1)
        print("✅ Headers written")


def write_to_sheet(date_str, data, tab):
    today       = datetime.date.today().isoformat()
    report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    # Duplicate check
    existing = tab.col_values(2)
    if report_date in existing:
        print(f"Duplicate: {report_date} already logged. Skipping.")
        return False

    regions   = data.get("regions", {})
    shanghai  = regions.get("Shanghai",  {}).get("mt", "")
    guangdong = regions.get("Guangdong", {}).get("mt", "")
    jiangsu   = regions.get("Jiangsu",   {}).get("mt", "")
    zhejiang  = regions.get("Zhejiang",  {}).get("mt", "")

    known = sum(v for v in [shanghai, guangdong, jiangsu, zhejiang] if isinstance(v, float))
    other = round(data["total_mt"] - known, 0) if data["total_mt"] and known else ""

    row = [
        today,
        report_date,
        data["total_mt"]  or "",
        data["change_mt"] or "",
        shanghai, guangdong, jiangsu, zhejiang, other,
        build_url(date_str)
    ]

    tab.append_row(row, value_input_option="USER_ENTERED")
    print(f"✅ Wrote: {row}")
    return True


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"SHFE Copper Inventory — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        print("\n[1] Finding latest SHFE data file...")
        date_str, html = find_latest_data()

        print(f"\n[2] Parsing copper table for {date_str}...")
        data = parse_copper(html)

        print(f"\nParsed results:")
        print(f"  Total:      {data['total_mt']} mt")
        print(f"  Change:     {data['change_mt']} mt")
        print(f"  Regions:    {data['regions']}")

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None")

        print(f"\n[3] Writing to Google Sheets...")
        client = get_sheet_client()
        book   = client.open(SHEET_NAME)
        tab    = book.worksheet(TAB_SHFE)

        ensure_headers(tab)
        write_to_sheet(date_str, data, tab)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()


















