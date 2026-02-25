"""
feeds/shfe_inventory.py

Fetches SHFE copper warehouse stocks from Shanghai Futures Exchange.
URL pattern: dailystock_{YYYYMMDD}/EN/all.html

Writes to SHFE tab and updates shared Dashboard.
"""

import os
import json
import datetime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from dashboard import write_exchange, ensure_headers

# ── CONFIG ────────────────────────────────────────────────────
SHFE_URL_TEMPLATE = (
    "https://www.shfe.com.cn/data/tradedata/future/stockdata/"
    "dailystock_{date}/EN/all.html"
)
SHEET_NAME    = "3-exchange-inventory-tracker"
TAB_SHFE      = "SHFE"
TAB_DASHBOARD = "Dashboard"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHFE_HEADERS = [
    "Date", "Report Date",
    "Total Stocks (mt)", "Daily Change (mt)",
    "Shanghai (mt)", "Guangdong (mt)", "Jiangsu (mt)", "Zhejiang (mt)",
    "Other Regions (mt)", "Source"
]

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://www.shfe.com.cn/eng/reports/StatisticalData/DailyData/",
}


# ── FETCH ─────────────────────────────────────────────────────
def build_url(date_str):
    return SHFE_URL_TEMPLATE.format(date=date_str)


def find_latest_data():
    today = datetime.date.today()
    for delta in range(7):
        d = today - datetime.timedelta(days=delta)
        date_str = d.strftime("%Y%m%d")
        url = build_url(date_str)
        try:
            resp = requests.get(url, headers=FETCH_HEADERS, timeout=15)
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
    try:
        return float(text.strip().replace(",", ""))
    except (ValueError, AttributeError):
        return None


def find_copper_table(soup):
    for table in soup.find_all("table", class_="el-table_table"):
        special = table.find("tr", class_="special_row_type")
        if special:
            label = special.get_text(strip=True).upper()
            if label.startswith("COPPER") and "BC" not in label:
                return table
    return None


def parse_copper(html):
    soup = BeautifulSoup(html, "lxml")
    result = {"total_mt": None, "change_mt": None, "regions": {}}

    copper_table = find_copper_table(soup)
    if not copper_table:
        raise ValueError("Could not find COPPER table in HTML")

    current_region = None

    for row in copper_table.find_all("tr"):
        classes = row.get("class", [])
        if "special_row_type" in classes:
            continue

        tds = row.find_all("td")
        if not tds:
            continue

        # Grand total (colspan=2 on first td)
        if tds[0].get("colspan") == "2":
            label = tds[0].get_text(strip=True)
            if label == "Total(Tax included)":
                result["total_mt"]  = num(tds[1].get_text(strip=True))
                result["change_mt"] = num(tds[2].get_text(strip=True))
                print(f"  Total: {result['total_mt']} mt, change: {result['change_mt']}")
            continue

        # Subtotal rows
        if "isTotal" in classes and len(tds) == 3:
            label = tds[0].get_text(strip=True)
            if label == "Subtotal" and current_region:
                val = num(tds[1].get_text(strip=True))
                chg = num(tds[2].get_text(strip=True))
                result["regions"][current_region] = {"mt": val, "change": chg}
                print(f"  {current_region}: {val} mt")
            continue

        # First warehouse row in a new region (4 tds)
        if len(tds) == 4:
            current_region = tds[0].get_text(strip=True)
            continue

    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_shfe_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Date":
        tab.insert_row(SHFE_HEADERS, 1)
        print("✅ SHFE headers written")


def write_to_sheet(date_str, data):
    client   = get_sheet_client()
    book     = client.open(SHEET_NAME)
    shfe_tab = book.worksheet(TAB_SHFE)
    dash_tab = book.worksheet(TAB_DASHBOARD)

    today       = datetime.date.today().isoformat()
    report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    ensure_shfe_headers(shfe_tab)

    existing = shfe_tab.col_values(2)
    already_logged = report_date in existing

    regions   = data.get("regions", {})
    shanghai  = regions.get("Shanghai",  {}).get("mt", "")
    guangdong = regions.get("Guangdong", {}).get("mt", "")
    jiangsu   = regions.get("Jiangsu",   {}).get("mt", "")
    zhejiang  = regions.get("Zhejiang",  {}).get("mt", "")

    known = sum(v for v in [shanghai, guangdong, jiangsu, zhejiang] if isinstance(v, float))
    other = round(data["total_mt"] - known, 0) if data["total_mt"] and known else ""

    if already_logged:
        print(f"Duplicate: {report_date} already in SHFE tab. Skipping tab write.")
    else:
        shfe_row = [
            today, report_date,
            data["total_mt"]  or "",
            data["change_mt"] or "",
            shanghai, guangdong, jiangsu, zhejiang, other,
            build_url(date_str)
        ]
        shfe_tab.append_row(shfe_row, value_input_option="USER_ENTERED")
        print(f"✅ SHFE tab: {data['total_mt']} mt | change {data['change_mt']} mt")

    # Dashboard always runs — idempotent update
    ensure_headers(dash_tab)
    write_exchange(dash_tab, today, "SHFE", data["total_mt"], data["change_mt"])


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

        print(f"\nParsed: total={data['total_mt']} mt, change={data['change_mt']} mt")
        print(f"Regions: {data['regions']}")

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None")

        print(f"\n[3] Writing to Google Sheets...")
        write_to_sheet(date_str, data)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
