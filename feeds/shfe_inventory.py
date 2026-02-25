"""
feeds/shfe_inventory.py

Scrapes SHFE copper warehouse stocks from MetalCharts.org.
No authentication required — MetalCharts aggregates SHFE data daily
sourced from the Shanghai Futures Exchange.

Published daily after SHFE market close (~15:00 CST / 07:00 UTC).
SHFE data is the key barometer for Chinese physical copper demand.

Requires GitHub Secrets:
  GOOGLE_SERVICE_ACCOUNT_JSON

Sheet tab: SHFE
Columns:
  Date | Report Date | Total Stocks (mt) | Daily Change (mt) | Source
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
METALCHARTS_URL = "https://metalcharts.org/shfe/copper"
METALCHARTS_BASE = "https://metalcharts.org/shfe"
SHEET_NAME      = "3-exchange-inventory-tracker"
TAB_SHFE        = "SHFE"
TAB_DASH        = "Dashboard"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── FETCH & PARSE ─────────────────────────────────────────────
def fetch_shfe_copper():
    """
    Scrape MetalCharts SHFE copper inventory page.
    Page shows: current stock (mt), last updated date, and daily change.
    Falls back to the main SHFE page if copper-specific page fails.
    """
    result = {
        "report_date": "",
        "total_mt":    None,
        "change_mt":   None,
    }

    # Try copper-specific page first
    for url in [METALCHARTS_URL, METALCHARTS_BASE]:
        print(f"Fetching {url}...")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            print(f"HTTP {resp.status_code}, {len(resp.content)} bytes")
        except Exception as e:
            print(f"  Failed: {e}, trying next URL...")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Log a snippet for debugging
        print(f"  Page text sample: {text[:500]}")

        # Strategy 1: look for structured data near "Copper" + "MT" patterns
        # MetalCharts shows "287.8K MT" style values
        # Try to find the copper inventory figure
        if url == METALCHARTS_BASE:
            # On the main SHFE page, copper entry reads:
            # "Copper  287.8K MT  Last updated: 2026-02-25"
            copper_match = re.search(
                r'Copper\s+([\d,\.]+[KkMm]?)\s*MT.*?Last updated:\s*(\d{4}-\d{2}-\d{2})',
                text, re.IGNORECASE
            )
            if copper_match:
                result["total_mt"]    = parse_kt(copper_match.group(1))
                result["report_date"] = copper_match.group(2)
                print(f"  ✅ Found via main page: {result['total_mt']} mt, {result['report_date']}")
                break
        else:
            # On the copper-specific page, look for the headline figure
            # and "Last updated" date
            date_match = re.search(r'Last updated:\s*(\d{4}-\d{2}-\d{2})', text)
            if date_match:
                result["report_date"] = date_match.group(1)

            # Look for large number near "MT" or "metric tons"
            # Patterns like "287,800" or "287.8K"
            stock_match = re.search(
                r'([\d,\.]+[KkMm]?)\s*(?:MT|metric\s*ton)',
                text, re.IGNORECASE
            )
            if stock_match:
                result["total_mt"] = parse_kt(stock_match.group(1))
                print(f"  ✅ Found via copper page: {result['total_mt']} mt, {result['report_date']}")

            # Look for daily change (could be +/- number)
            change_match = re.search(
                r'([+-][\d,\.]+[KkMm]?)\s*(?:MT|metric\s*ton|change|daily)',
                text, re.IGNORECASE
            )
            if change_match:
                result["change_mt"] = parse_kt(change_match.group(1))

            if result["total_mt"]:
                break

    # If we still don't have a date, use yesterday (SHFE publishes prior day)
    if not result["report_date"] and result["total_mt"]:
        yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        result["report_date"] = yesterday
        print(f"  Using yesterday as report date: {yesterday}")

    return result


def parse_kt(val_str):
    """
    Convert strings like '287.8K', '287,800', '287800' to float.
    K/k = thousands, M/m = millions.
    """
    val_str = str(val_str).strip().replace(",", "").replace("+", "")
    try:
        if val_str.upper().endswith("M"):
            return float(val_str[:-1]) * 1_000_000
        elif val_str.upper().endswith("K"):
            return float(val_str[:-1]) * 1_000
        else:
            return float(val_str)
    except (ValueError, TypeError):
        return None


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_headers(shfe_tab):
    first = shfe_tab.row_values(1)
    if not first or first[0] != "Date":
        headers = [
            "Date", "Report Date",
            "Total Stocks (mt)", "Daily Change (mt)",
            "Source"
        ]
        shfe_tab.insert_row(headers, 1)
        print("✅ SHFE headers written")


def write_to_sheet(data, shfe_tab, dash_tab):
    today = datetime.date.today().isoformat()

    # Duplicate check on report date
    existing = shfe_tab.col_values(2)
    if data["report_date"] and data["report_date"] in existing:
        print(f"Duplicate: {data['report_date']} already logged. Skipping.")
        return

    shfe_row = [
        today,
        data["report_date"],
        data["total_mt"]  or "",
        data["change_mt"] or "",
        METALCHARTS_URL
    ]
    shfe_tab.append_row(shfe_row, value_input_option="USER_ENTERED")
    print(f"✅ Wrote to SHFE tab: {shfe_row}")

    dash_tab.append_row([
        today, data["report_date"], "", "", "", "", "",
        data["total_mt"] or ""
    ], value_input_option="USER_ENTERED")
    print("✅ Wrote to Dashboard tab")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"SHFE Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        data = fetch_shfe_copper()

        print(f"\nParsed summary:")
        print(f"  Report Date:   {data['report_date']}")
        print(f"  Total Stocks:  {data['total_mt']} mt")
        print(f"  Daily Change:  {data['change_mt']} mt")

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None. Check page structure in logs above.")

        client    = get_sheet_client()
        book      = client.open(SHEET_NAME)
        shfe_tab  = book.worksheet(TAB_SHFE)
        dash_tab  = book.worksheet(TAB_DASH)

        ensure_headers(shfe_tab)
        write_to_sheet(data, shfe_tab, dash_tab)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
