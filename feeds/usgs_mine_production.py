"""
feeds/usgs_mine_production.py

Fetches USGS copper mine production data from the USGS Minerals Information
National Minerals Information Center (NMIC) website.

Data available: https://www.usgs.gov/centers/national-minerals-information-center/copper
USGS publishes:
  - Monthly commodity summaries (PDF — headline kt only)
  - Annual Mineral Commodity Summaries (Jan each year)
  - Minerals Yearbook copper chapter (quarterly lag)

This script:
1. Checks for the latest USGS Mineral Commodity Summary page
2. Extracts global mine production estimates where available
3. Updates Mine Database tab: "Last Updated" column for USGS-sourced mines
4. Appends to Feed Log tab

Frequency: Quarterly (run monthly, skip if no update detected)

SHEET COLUMNS targeted in "Mine Database":
  F = Annual Output (kt)   — only updated when USGS revises estimates
  N = Last Updated
"""

import os
import json
import datetime
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

try:
    from mine_dashboard import ensure_feed_log_headers, log_update
except ImportError:
    from feeds.mine_dashboard import ensure_feed_log_headers, log_update

# ── CONFIG ────────────────────────────────────────────────────
USGS_URL    = "https://www.usgs.gov/centers/national-minerals-information-center/copper"
SHEET_NAME  = "mine-production-database"
TAB_MINES   = "Mine Database"
TAB_LOG     = "Feed Log"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# Known USGS-sourced mines (name must match Mine Database col A exactly)
USGS_MINES = [
    "Escondida", "Grasberg", "Collahuasi", "Cerro Verde",
    "Las Bambas", "Antamina", "Chuquicamata", "El Teniente",
    "Quellaveco", "Oyu Tolgoi", "Spence",
]


# ── FETCH ─────────────────────────────────────────────────────
def fetch_usgs_page():
    print(f"Fetching {USGS_URL}...")
    resp = requests.get(USGS_URL, headers=FETCH_HEADERS, timeout=30)
    resp.raise_for_status()
    print(f"HTTP {resp.status_code}, {len(resp.content)} bytes")
    return resp.text


def extract_summary_stats(html):
    """
    Parse USGS copper page for headline production stats.
    Returns dict with whatever can be found:
      { "world_production_kt": float, "year": int, "source_url": str }
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    result = {"world_production_kt": None, "year": None, "source_url": USGS_URL}

    # Look for patterns like "22 million tons" or "22,000,000 metric tons"
    import re
    # Pattern: number followed by million metric tons / thousand metric tons
    m = re.search(r'(\d[\d,\.]+)\s*million\s*(metric\s*)?tons?\s*(?:of\s*copper)?', text, re.IGNORECASE)
    if m:
        try:
            val = float(m.group(1).replace(",", "")) * 1_000  # Mt → kt
            result["world_production_kt"] = round(val, 0)
        except ValueError:
            pass

    # Year
    yr = re.search(r'(?:in|for)\s+(20\d{2})', text)
    if yr:
        result["year"] = int(yr.group(1))

    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def update_mine_last_updated(mines_tab, log_tab, year_updated):
    """
    Stamp 'Last Updated' (col N = col 14) for all USGS-sourced mines.
    Only updates if the year has advanced.
    """
    all_rows = mines_tab.get_all_values()
    updates = []

    for i, row in enumerate(all_rows):
        if i == 0:  # header
            continue
        mine_name = row[0].strip() if row else ""
        if mine_name not in USGS_MINES:
            continue
        current_year = str(row[13]).strip() if len(row) > 13 else ""
        if str(year_updated) != current_year:
            row_num = i + 1  # 1-indexed
            updates.append((row_num, mine_name, current_year, str(year_updated)))

    for row_num, mine_name, old_yr, new_yr in updates:
        mines_tab.update_cell(row_num, 14, new_yr)
        print(f"  ✅ Updated '{mine_name}' Last Updated: {old_yr} → {new_yr}")
        log_update(log_tab, datetime.date.today().isoformat(),
                   "USGS NMIC", mine_name,
                   "Last Updated", old_yr, new_yr,
                   "Auto-updated by usgs_mine_production.py")

    if not updates:
        print("  No Last Updated changes needed (all mines already current).")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"USGS Mine Production Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        html   = fetch_usgs_page()
        stats  = extract_summary_stats(html)
        year   = stats["year"] or datetime.date.today().year

        print(f"\nParsed: world_production_kt={stats['world_production_kt']}, year={year}")

        print("\nConnecting to Google Sheets...")
        client    = get_sheet_client()
        book      = client.open(SHEET_NAME)
        mines_tab = book.worksheet(TAB_MINES)
        log_tab   = book.worksheet(TAB_LOG)

        ensure_feed_log_headers(log_tab)

        print(f"\nUpdating Last Updated stamps for USGS-sourced mines (year={year})...")
        update_mine_last_updated(mines_tab, log_tab, year)

        # Log the world total if we got it
        if stats["world_production_kt"]:
            log_update(log_tab, datetime.date.today().isoformat(),
                       "USGS NMIC", "WORLD TOTAL",
                       "Global Mine Production (kt)", "",
                       str(stats["world_production_kt"]),
                       f"Year {year} — {USGS_URL}")
            print(f"\n✅ World copper mine production logged: {stats['world_production_kt']} kt ({year})")

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
