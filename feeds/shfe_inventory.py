"""
feeds/shfe_inventory.py

Fetches SHFE copper warehouse stocks directly from the Shanghai Futures Exchange.
Uses the native static HTML data file — no authentication, no JS rendering required.

URL pattern:
  https://www.shfe.com.cn/data/tradedata/future/stockdata/dailystock_{YYYYMMDD}/EN/all.html

Data: per-warehouse breakdown with regional subtotals and grand total.
Published daily after SHFE market close (~15:00 CST / 07:00 UTC).

Requires GitHub Secrets:
  GOOGLE_SERVICE_ACCOUNT_JSON

Sheet tab: SHFE
Columns:
  Date | Report Date | Total Stocks (mt) | Daily Change (mt) |
  Shanghai (mt) | Guangdong (mt) | Jiangsu (mt) | Zhejiang (mt) | Other (mt) | Source
"""

import os
import re
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

# Regions to capture as separate columns (matched against Subtotal rows)
REGIONS = ["Shanghai", "Guangdong", "Jiangsu", "Zhejiang"]


# ── FETCH ─────────────────────────────────────────────────────
def build_url(date_str):
    """date_str: YYYYMMDD"""
    return SHFE_URL_TEMPLATE.format(date=date_str)


def fetch_for_date(date_str):
    url = build_url(date_str)
    print(f"  Fetching: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def find_latest_date():
    """
    Try today and walk back up to 7 days to find the most recent published file.
    SHFE publishes after market close so today may not be available until ~07:00 UTC.
    """
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
def parse_copper(html):
    """
    Parse the all.html file for COPPER section.
    Returns dict with total, change, and regional subtotals.
    """
    soup = BeautifulSoup(html, "lxml")
    
    # The file contains all metals — find the COPPER section
    # Strategy: look for table rows, find COPPER header, parse until next metal
    result = {
        "total_mt":    None,
        "change_mt":   None,
        "regions":     {},
    }

    # Try to find tables
    tables = soup.find_all("table")
    print(f"  Found {len(tables)} tables in HTML")

    copper_table = None
    for table in tables:
        text = table.get_text()
        if "COPPER" in text.upper() and "Total" in text:
            copper_table = table
            break

    if not copper_table:
        # Fallback: parse raw text
        return parse_copper_from_text(soup.get_text("\n"))

    rows = copper_table.find_all("tr")
    in_copper = False
    current_region = None

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue

        first = cells[0].strip()
        
        # Detect COPPER section start
        if first.upper() == "COPPER" or (len(cells) > 1 and "COPPER" in first.upper()):
            in_copper = True
            continue

        # Detect next metal section (exit copper)
        if in_copper and first.upper() in [
            "ALUMINUM", "ZINC", "LEAD", "NICKEL", "TIN", "GOLD", "SILVER",
            "REBAR", "WIRE ROD", "HOT ROLLED", "STAINLESS", "NATURAL RUBBER",
            "FUEL OIL", "BITUMEN", "CRUDE OIL"
        ]:
            break

        if not in_copper:
            continue

        # Track current region
        if first and first not in ("", "Region", "Warehouse", "On Warrant", "Change"):
            if not first.startswith(",") and "Subtotal" not in first and "Total" not in first:
                # Check if this looks like a region name (not a warehouse)
                if len(cells) >= 3 and cells[1] and cells[1] != "0":
                    current_region = first
                elif len(cells) >= 1 and cells[1] == "":
                    current_region = first

        # Parse subtotals by region
        if "Subtotal" in first or (len(cells) > 1 and "Subtotal" in cells[1]):
            val = extract_number(cells)
            chg = extract_number(cells, idx=3)
            if current_region and val is not None:
                result["regions"][current_region] = {"mt": val, "change": chg}

        # Parse grand totals
        if first == "Total(Tax included)" or (len(cells) > 1 and cells[1] == "Total(Tax included)"):
            result["total_mt"]  = extract_number(cells)
            result["change_mt"] = extract_number(cells, idx=3)
        elif first == "Total" and result["total_mt"] is None:
            result["total_mt"]  = extract_number(cells)
            result["change_mt"] = extract_number(cells, idx=3)

    return result


def parse_copper_from_text(text):
    """Fallback text parser matching the TXT export format."""
    result = {"total_mt": None, "change_mt": None, "regions": {}}
    
    lines = text.split("\n")
    in_copper = False
    current_region = None

    for line in lines:
        line = line.strip()
        parts = [p.strip() for p in line.split(",")]

        if "COPPER" == parts[0].upper():
            in_copper = True
            continue

        if in_copper and parts[0].upper() in [
            "ALUMINUM", "ZINC", "LEAD", "NICKEL", "TIN", "GOLD", "SILVER"
        ]:
            break

        if not in_copper:
            continue

        # Region name (non-empty first cell, not a warehouse line)
        if parts[0] and parts[0] not in ("Region", "Warehouse"):
            if "Subtotal" not in parts[0] and "Total" not in parts[0]:
                current_region = parts[0]

        if len(parts) >= 3:
            if "Subtotal" in parts[0] or (len(parts) > 1 and "Subtotal" in parts[1]):
                try:
                    val = float(parts[2].replace(",", ""))
                    chg = float(parts[3].replace(",", "")) if len(parts) > 3 else None
                    if current_region:
                        result["regions"][current_region] = {"mt": val, "change": chg}
                except ValueError:
                    pass

            if parts[0] in ("Total(Tax included)", "Total"):
                try:
                    result["total_mt"]  = float(parts[2].replace(",", ""))
                    result["change_mt"] = float(parts[3].replace(",", "")) if len(parts) > 3 else None
                except ValueError:
                    pass

    return result


def extract_number(cells, idx=2):
    """Extract float from cells list at given index."""
    try:
        return float(cells[idx].replace(",", "").replace("+", ""))
    except (IndexError, ValueError, AttributeError):
        return None


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
    today = datetime.date.today().isoformat()
    report_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    # Duplicate check
    existing = tab.col_values(2)
    if report_date in existing:
        print(f"Duplicate: {report_date} already logged. Skipping.")
        return False

    # Regional breakdown
    regions = data.get("regions", {})
    shanghai   = regions.get("Shanghai",   {}).get("mt", "")
    guangdong  = regions.get("Guangdong",  {}).get("mt", "")
    jiangsu    = regions.get("Jiangsu",    {}).get("mt", "")
    zhejiang   = regions.get("Zhejiang",   {}).get("mt", "")

    # "Other" = total minus known regions
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
        date_str, html = find_latest_date()

        print(f"\n[2] Parsing copper section for {date_str}...")
        data = parse_copper(html)

        print(f"\nParsed results:")
        print(f"  Total:      {data['total_mt']} mt")
        print(f"  Change:     {data['change_mt']} mt")
        print(f"  Regions:    {data['regions']}")

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None. Check HTML structure.")

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
