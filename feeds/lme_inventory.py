"""
feeds/lme_inventory.py

Scrapes LME copper warehouse stocks from Westmetall.com.
No authentication required — Westmetall publishes LME data daily,
sourced directly from the London Metal Exchange (1-day lag, same as
LME's own free tier).

Requires GitHub Secrets:
  GOOGLE_SERVICE_ACCOUNT_JSON

Sheet tab: LME
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
WESTMETALL_URL = "https://www.westmetall.com/en/markdaten.php"
SHEET_NAME     = "3-exchange-inventory-tracker"
TAB_LME        = "LME"
TAB_DASH       = "Dashboard"

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
def fetch_lme_copper():
    """
    Scrape Westmetall main market data page.
    
    Page has a table headed "LME Stocks" with columns:
      metal | stock (mt) | daily change (mt)
    The date appears in the table header: e.g. "23. February 2026"
    """
    print(f"Fetching {WESTMETALL_URL}...")
    resp = requests.get(WESTMETALL_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    print(f"HTTP {resp.status_code}, {len(resp.content)} bytes")

    soup = BeautifulSoup(resp.text, "html.parser")

    result = {
        "report_date": "",
        "total_mt":    None,
        "change_mt":   None,
    }

    # Find all tables
    tables = soup.find_all("table")
    print(f"Found {len(tables)} tables")

    for table in tables:
        # Look for the LME Stocks table by checking header row text
        rows = table.find_all("tr")
        if not rows:
            continue

        header_text = rows[0].get_text(" ", strip=True)
        print(f"  Table header: {header_text[:80]}")

        if "LME Stocks" in header_text or "LME stocks" in header_text.lower():
            print("  ✅ Found LME Stocks table")

            # Extract date from header row
            date_match = re.search(
                r'(\d{1,2})\.\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})',
                header_text
            )
            if date_match:
                day   = date_match.group(1).zfill(2)
                month = date_match.group(2)
                year  = date_match.group(3)
                months = {
                    "January":"01","February":"02","March":"03","April":"04",
                    "May":"05","June":"06","July":"07","August":"08",
                    "September":"09","October":"10","November":"11","December":"12"
                }
                result["report_date"] = f"{year}-{months[month]}-{day}"
                print(f"  Report date: {result['report_date']}")

            # Find copper row
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                row_text = cells[0].get_text(strip=True)
                print(f"    Row: {[c.get_text(strip=True) for c in cells[:3]]}")

                if "copper" in row_text.lower():
                    if len(cells) >= 2:
                        result["total_mt"]  = safe_float(cells[1].get_text(strip=True))
                    if len(cells) >= 3:
                        result["change_mt"] = safe_float(cells[2].get_text(strip=True))
                    print(f"  ✅ Copper: total={result['total_mt']} change={result['change_mt']}")
                    break

            break  # done once we've found and processed the LME Stocks table

    return result


def safe_float(val):
    try:
        return float(str(val).replace(",", "").replace("+", "").strip())
    except (ValueError, TypeError):
        return None

# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_headers(lme_tab):
    first = lme_tab.row_values(1)
    if not first or first[0] != "Date":
        headers = [
            "Date", "Report Date",
            "Total Stocks (mt)", "Daily Change (mt)",
            "Source"
        ]
        lme_tab.insert_row(headers, 1)
        print("✅ LME headers written")


def write_to_sheet(data, lme_tab, dash_tab):
    today = datetime.date.today().isoformat()

    # Duplicate check on report date
    existing = lme_tab.col_values(2)
    if data["report_date"] and data["report_date"] in existing:
        print(f"Duplicate: {data['report_date']} already logged. Skipping.")
        return

    lme_row = [
        today,
        data["report_date"],
        data["total_mt"]  or "",
        data["change_mt"] or "",
        WESTMETALL_URL
    ]
    lme_tab.append_row(lme_row, value_input_option="USER_ENTERED")
    print(f"✅ Wrote to LME tab: {lme_row}")

    dash_tab.append_row([
        today, data["report_date"], "", "", "", "",
        data["total_mt"] or "", ""
    ], value_input_option="USER_ENTERED")
    print("✅ Wrote to Dashboard tab")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"LME Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        data = fetch_lme_copper()

        print(f"\nParsed summary:")
        print(f"  Report Date:   {data['report_date']}")
        print(f"  Total Stocks:  {data['total_mt']} mt")
        print(f"  Daily Change:  {data['change_mt']} mt")

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None. Check page structure in logs.")

        client   = get_sheet_client()
        book     = client.open(SHEET_NAME)
        lme_tab  = book.worksheet(TAB_LME)
        dash_tab = book.worksheet(TAB_DASH)

        ensure_headers(lme_tab)
        write_to_sheet(data, lme_tab, dash_tab)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()










