"""
feeds/icsg_supply_demand.py

Fetches ICSG (International Copper Study Group) supply/demand balance data.
ICSG publishes:
  - Monthly press releases (market balance kt, provisional)
  - Semi-annual detailed statistical reports (April & October)

Source: https://icsg.org/publications/

This script:
1. Fetches the ICSG publications page
2. Detects the most recent monthly press release
3. Extracts: world mine production, refined production, apparent usage, market balance (kt)
4. Logs to Feed Log tab
5. Optionally updates Production Summary tab notes with latest balance

Sheet targeted: "Production Summary" tab, notes column (G)
Feed Log: "Feed Log" tab
"""

import os
import re
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
ICSG_URL    = "https://icsg.org/publications/"
SHEET_NAME  = "mine-production-database"
TAB_SUMMARY = "Production Summary"
TAB_LOG     = "Feed Log"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
}


# ── FETCH ─────────────────────────────────────────────────────
def fetch_icsg_page():
    print(f"Fetching {ICSG_URL}...")
    resp = requests.get(ICSG_URL, headers=FETCH_HEADERS, timeout=30)
    resp.raise_for_status()
    print(f"HTTP {resp.status_code}, {len(resp.content)} bytes")
    return resp.text


def find_latest_press_release(html):
    """
    Return URL of most recent ICSG monthly press release PDF.
    ICSG press releases typically titled: 'Copper Market Forecast ...' or
    'World Copper Factbook ...' or 'Press Release ...'
    """
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=True)
    press_links = []
    for a in links:
        href = a["href"]
        text = a.get_text(strip=True).lower()
        if ("press" in text or "release" in text or "monthly" in text) and (
            href.endswith(".pdf") or "pdf" in href
        ):
            if not href.startswith("http"):
                href = "https://icsg.org" + href
            press_links.append(href)
    return press_links[0] if press_links else None


def parse_icsg_stats(html_or_text):
    """
    Try to extract headline numbers from the ICSG page text.
    Returns dict: mine_prod_kt, refined_kt, usage_kt, balance_kt, period
    """
    result = {
        "mine_prod_kt": None,
        "refined_kt": None,
        "usage_kt": None,
        "balance_kt": None,
        "period": None,
    }

    soup = BeautifulSoup(html_or_text, "lxml")
    text = soup.get_text(" ", strip=True)

    # Look for balance figure: "deficit of X,XXX tonnes" or "surplus of X,XXX tonnes"
    m = re.search(
        r'(deficit|surplus)\s+of\s+([\d,]+)\s*(thousand\s*metric\s*tonnes?|t\b|kt\b)',
        text, re.IGNORECASE
    )
    if m:
        sign = -1 if m.group(1).lower() == "deficit" else 1
        try:
            val = float(m.group(2).replace(",", ""))
            # Normalize to kt
            if "thousand" in m.group(3).lower():
                val = val  # already kt
            elif val > 10000:
                val = val / 1000  # likely in tonnes → convert to kt
            result["balance_kt"] = round(sign * val, 1)
        except ValueError:
            pass

    # Period
    yr = re.search(r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})', text)
    if yr:
        result["period"] = f"{yr.group(1)} {yr.group(2)}"

    return result


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def update_summary_note(summary_tab, log_tab, stats):
    """
    Write the ICSG balance note into Production Summary notes area.
    Targets a reserved cell (H2 area) below the summary table.
    """
    if stats["balance_kt"] is None:
        print("  No balance figure parsed — skipping summary update.")
        return

    sign = "surplus" if stats["balance_kt"] >= 0 else "deficit"
    abs_val = abs(stats["balance_kt"])
    note_text = (
        f"ICSG Market Balance ({stats.get('period','latest')}): "
        f"{sign} of {abs_val:,.0f} kt  |  Source: {ICSG_URL}"
    )
    print(f"  Writing note: {note_text[:80]}...")

    # Write to a note row — find first empty row after data
    all_vals = summary_tab.get_all_values()
    note_row = len(all_vals) + 2
    summary_tab.update(
        f"A{note_row}:G{note_row}",
        [[note_text, "", "", "", "", "", ""]],
        value_input_option="USER_ENTERED"
    )

    log_update(log_tab, datetime.date.today().isoformat(),
               "ICSG", "GLOBAL",
               "Market Balance (kt)", "",
               str(stats["balance_kt"]),
               f"Period: {stats.get('period','')} | {sign}")
    print(f"  ✅ ICSG balance written: {sign} {abs_val} kt")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"ICSG Supply/Demand Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        html  = fetch_icsg_page()
        stats = parse_icsg_stats(html)
        pr_url = find_latest_press_release(html)

        print(f"\nParsed: balance={stats['balance_kt']} kt, period={stats['period']}")
        if pr_url:
            print(f"Latest press release: {pr_url}")
        else:
            print("No press release PDF found on page.")

        print("\nConnecting to Google Sheets...")
        client      = get_sheet_client()
        book        = client.open(SHEET_NAME)
        summary_tab = book.worksheet(TAB_SUMMARY)
        log_tab     = book.worksheet(TAB_LOG)

        ensure_feed_log_headers(log_tab)
        update_summary_note(summary_tab, log_tab, stats)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
