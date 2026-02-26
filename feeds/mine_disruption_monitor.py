"""
feeds/mine_disruption_monitor.py

Monitors company investor relations pages for new operational updates
that may signal production disruptions at watchlist mines.

Targets:
  - BHP: https://www.bhp.com/investors/news-and-media
  - Freeport: https://www.fcx.com/news
  - MMG: https://www.mmg.com/en/investors/announcements
  - Anglo American: https://www.angloamerican.com/investors/news
  - Ivanhoe: https://www.ivanhoemines.com/news-media/press-releases
  - Rio Tinto: https://www.riotinto.com/media/press-releases
  - Codelco: https://www.codelco.com/prensa/noticias (Spanish — keyword scan only)

This script:
1. Fetches each IR page
2. Scans headlines for disruption keywords (strike, blockade, force majeure,
   suspension, outage, flood, accident, fatality, reduced guidance)
3. Flags any headline matching a watchlist mine + disruption keyword
4. Appends flags to Disruption Log tab (Status = "Monitoring")
5. Logs all scans to Feed Log tab

Frequency: Daily (Mon–Fri, same schedule as inventory feeds)
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
SHEET_NAME  = "mine-production-database"
TAB_DISRUPT = "Disruption Log"
TAB_LOG     = "Feed Log"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# Mine → aliases for keyword matching
MINE_ALIASES = {
    "Escondida":    ["escondida"],
    "Grasberg":     ["grasberg", "ptfi", "freeport indonesia"],
    "Collahuasi":   ["collahuasi"],
    "Kamoa-Kakula": ["kamoa", "kakula", "kamoa-kakula"],
    "Cerro Verde":  ["cerro verde"],
    "Las Bambas":   ["las bambas", "bambas"],
    "Antamina":     ["antamina"],
    "Chuquicamata": ["chuquicamata", "chuqui"],
    "El Teniente":  ["el teniente", "teniente"],
    "Quellaveco":   ["quellaveco"],
    "Oyu Tolgoi":   ["oyu tolgoi", "turquoise hill"],
    "Spence":       ["spence"],
}

DISRUPTION_KEYWORDS = [
    "strike", "labor action", "labour action", "blockade", "road block",
    "force majeure", "suspend", "suspension", "outage", "flood", "flooding",
    "accident", "fatality", "production cut", "reduced guidance", "lower guidance",
    "operational disruption", "power outage", "rainfall", "mud", "tailings",
    "reduced output", "lower production", "maintenance shutdown",
]

# IR pages to scan
IR_SOURCES = [
    {
        "company": "BHP",
        "url": "https://www.bhp.com/news",
        "mines": ["Escondida", "Antamina", "Spence"],
    },
    {
        "company": "Freeport-McMoRan",
        "url": "https://www.fcx.com/news",
        "mines": ["Grasberg", "Cerro Verde"],
    },
    {
        "company": "MMG",
        "url": "https://www.mmg.com/en/investors/announcements",
        "mines": ["Las Bambas"],
    },
    {
        "company": "Ivanhoe Mines",
        "url": "https://www.ivanhoemines.com/news-media/press-releases",
        "mines": ["Kamoa-Kakula"],
    },
    {
        "company": "Rio Tinto",
        "url": "https://www.riotinto.com/en/news",
        "mines": ["Oyu Tolgoi"],
    },
    {
        "company": "Anglo American",
        "url": "https://www.angloamerican.com/investors/news",
        "mines": ["Collahuasi", "Quellaveco"],
    },
    {
        "company": "Codelco",
        "url": "https://www.codelco.com/prensa/noticias",
        "mines": ["Chuquicamata", "El Teniente"],
    },
]


# ── FETCH ─────────────────────────────────────────────────────
def fetch_page(url, timeout=20):
    try:
        resp = requests.get(url, headers=FETCH_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  ⚠ Could not fetch {url}: {e}")
        return ""


def extract_headlines(html, source_url):
    soup = BeautifulSoup(html, "lxml")
    headlines = []
    # Collect text from common headline tags
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "a"]):
        text = tag.get_text(strip=True)
        if 10 < len(text) < 300:
            href = tag.get("href", "")
            if href and not href.startswith("http"):
                href = source_url.rstrip("/") + "/" + href.lstrip("/")
            headlines.append({"text": text, "href": href or source_url})
    return headlines


# ── MATCH ─────────────────────────────────────────────────────
def find_disruptions(headlines, relevant_mines):
    flags = []
    for h in headlines:
        text_lower = h["text"].lower()
        # Check if headline mentions a mine
        mine_hit = None
        for mine in relevant_mines:
            for alias in MINE_ALIASES.get(mine, [mine.lower()]):
                if alias in text_lower:
                    mine_hit = mine
                    break
            if mine_hit:
                break
        if not mine_hit:
            continue
        # Check for disruption keyword
        for kw in DISRUPTION_KEYWORDS:
            if kw in text_lower:
                flags.append({
                    "mine": mine_hit,
                    "headline": h["text"][:200],
                    "keyword": kw,
                    "url": h["href"],
                })
                break
    return flags


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheet_client():
    creds_dict = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_mine_country(mine_name):
    COUNTRIES = {
        "Escondida": "Chile", "Grasberg": "Indonesia", "Collahuasi": "Chile",
        "Kamoa-Kakula": "DRC", "Cerro Verde": "Peru", "Las Bambas": "Peru",
        "Antamina": "Peru", "Chuquicamata": "Chile", "El Teniente": "Chile",
        "Quellaveco": "Peru", "Oyu Tolgoi": "Mongolia", "Spence": "Chile",
    }
    return COUNTRIES.get(mine_name, "")


def already_logged(disrupt_tab, mine, headline_fragment):
    """Skip if same mine+headline already appears in the Disruption Log."""
    all_notes = disrupt_tab.col_values(12)  # Notes column
    all_mines = disrupt_tab.col_values(2)   # Mine Name column
    for i, (m, n) in enumerate(zip(all_mines, all_notes)):
        if m.strip() == mine and headline_fragment[:60] in n:
            return True
    return False


def write_disruption_flag(disrupt_tab, log_tab, flag, source_company, source_url):
    today = datetime.date.today().isoformat()
    mine  = flag["mine"]
    country = get_mine_country(mine)
    headline = flag["headline"]
    keyword  = flag["keyword"]

    if already_logged(disrupt_tab, mine, headline[:60]):
        print(f"  ⏩ Already logged: {mine} — {headline[:60]}")
        return

    row = [
        today, mine, country,
        f"Alert: {keyword}",   # Disruption Type
        today, "", "",          # Start Date, End Date, Duration
        "", "",                 # Daily Rate, kt Impact — TBD
        "Monitoring",           # Status
        f"{source_company} IR ({source_url})",
        f"Headline: {headline[:200]}",
    ]
    disrupt_tab.append_row(row, value_input_option="USER_ENTERED")
    print(f"  🚨 Flagged: [{mine}] '{keyword}' — {headline[:80]}")

    log_update(log_tab, today, source_company, mine,
               "Disruption Flag", "", "Monitoring",
               f"Keyword: {keyword} | {headline[:100]}")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"Mine Disruption Monitor — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    all_flags = []

    for source in IR_SOURCES:
        company = source["company"]
        url     = source["url"]
        mines   = source["mines"]
        print(f"\n[{company}] {url}")

        html      = fetch_page(url)
        if not html:
            continue
        headlines = extract_headlines(html, url)
        print(f"  {len(headlines)} headlines extracted")
        flags     = find_disruptions(headlines, mines)
        print(f"  {len(flags)} disruption flags found")
        for f in flags:
            f["company"] = company
            f["source_url"] = url
        all_flags.extend(flags)

    print(f"\nTotal flags: {len(all_flags)}")

    if all_flags or True:  # Always connect to log the scan run
        print("\nConnecting to Google Sheets...")
        client      = get_sheet_client()
        book        = client.open(SHEET_NAME)
        disrupt_tab = book.worksheet(TAB_DISRUPT)
        log_tab     = book.worksheet(TAB_LOG)

        ensure_feed_log_headers(log_tab)

        for flag in all_flags:
            write_disruption_flag(disrupt_tab, log_tab, flag,
                                  flag["company"], flag["source_url"])

        # Log scan run regardless
        log_update(log_tab, datetime.date.today().isoformat(),
                   "mine_disruption_monitor", "ALL MINES",
                   "Scan Run", "", str(len(all_flags)),
                   f"Scanned {len(IR_SOURCES)} IR pages; {len(all_flags)} new flags")

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
