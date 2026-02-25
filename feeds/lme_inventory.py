"""
feeds/lme_inventory.py

Scrapes LME copper warehouse stocks from Westmetall.com.
Writes to LME tab and updates shared Dashboard.

NOTE: LME Cancelled Warrants (Dashboard col I) is a manual-entry column.
      It is preserved on update — this script never overwrites it.
      Future: wire in a source when one becomes reliably scrapable.
"""

import os
import re
import json
import datetime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from dashboard import write_exchange, ensure_headers

# ── CONFIG ────────────────────────────────────────────────────
WESTMETALL_URL = "https://www.westmetall.com/en/markdaten.php"
SHEET_NAME     = "3-exchange-inventory-tracker"
TAB_LME        = "LME"
TAB_DASHBOARD  = "Dashboard"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

LME_HEADERS = [
    "Date", "Report Date",
    "Total Stocks (mt)", "Daily Change (mt)",
    "Source"
]

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── FETCH & PARSE ─────────────────────────────────────────────
def fetch_lme_copper():
    print(f"Fetching {WESTMETALL_URL}...")
    resp = requests.get(WESTMETALL_URL, headers=FETCH_HEADERS, timeout=30)
    resp.raise_for_status()
    print(f"HTTP {resp.status_code}, {len(resp.content)} bytes")

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    result = {"report_date": "", "total_mt": None, "change_mt": None}

    # Date
    date_match = re.search(r'(\d{1,2})\.\s+(\w+)\s+(\d{4})', text)
    if date_match:
        try:
            dt = datetime.datetime.strptime(
                f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}",
                "%d %B %Y"
            )
            result["report_date"] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Copper row in stocks table
    for table in soup.find_all("table"):
        if "copper" not in table.get_text().lower():
            continue
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if "copper" in " ".join(cells).lower():
                nums = []
                for c in cells:
                    clean = c.replace(",", "").replace(".", "").lstrip("+-")
                    if clean.isdigit() and int(clean) > 1000:
                        try:
                            nums.append(float(c.replace(",", "")))
                        except ValueError:
                            pass
                if nums:
                    result["total_mt"]  = nums[0]
                    result["change_mt"] = nums[1] if len(nums) > 1 else None
                    break
        if result["total_mt"]:
            break

    # Fallback regex
    if result["total_mt"] is None:
        m = re.search(r'[Cc]opper[^\d]+([\d,]+)\s*(?:t|mt|tonnes?)?[^\d]*([\+\-][\d,]+)?', text)
        if m:
            try:
                result["total_mt"]  = float(m.group(1).replace(",", ""))
                result["change_mt"] = float(m.group(2).replace(",", "")) if m.group(2) else None
            except (ValueError, AttributeError):
                pass

    if not result["report_date"]:
        result["report_date"] = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

    print(f"  Report Date : {result['report_date']}")
    print(f"  Total       : {result['total_mt']} mt")
    print(f"  Change      : {result['change_mt']} mt")
    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_lme_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Date":
        tab.insert_row(LME_HEADERS, 1)
        print("✅ LME headers written")


def write_to_sheet(data):
    client   = get_sheet_client()
    book     = client.open(SHEET_NAME)
    lme_tab  = book.worksheet(TAB_LME)
    dash_tab = book.worksheet(TAB_DASHBOARD)

    today = datetime.date.today().isoformat()

    existing = lme_tab.col_values(2)
    if data["report_date"] and data["report_date"] in existing:
        print(f"Duplicate: {data['report_date']} already logged. Skipping.")
        return

    ensure_lme_headers(lme_tab)

    lme_row = [
        today,
        data["report_date"],
        data["total_mt"]  or "",
        data["change_mt"] or "",
        WESTMETALL_URL
    ]
    lme_tab.append_row(lme_row, value_input_option="USER_ENTERED")
    print(f"✅ LME tab: {data['total_mt']} mt | change {data['change_mt']} mt")

    # Dashboard — no extras; cancelled warrants col is preserved (manual)
    ensure_headers(dash_tab)
    write_exchange(dash_tab, today, "LME", data["total_mt"], data["change_mt"])


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"LME Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        data = fetch_lme_copper()

        if data["total_mt"] is None:
            raise ValueError("Parse failed — total_mt is None")

        write_to_sheet(data)
        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
