"""
feeds/lme_inventory.py

Authenticates with LME.com using username/password, finds and downloads
the latest Stock Movement Report (XLSX), parses LME copper inventory,
and appends to the Google Sheet tracker.

Requires GitHub Secrets:
  LME_USERNAME              — LME account email
  LME_PASSWORD              — LME account password
  GOOGLE_SERVICE_ACCOUNT_JSON

Sheet tab: LME
Columns:
  Date | Report Date | On Warrant (mt) | Cancelled Warrants (mt) |
  Total Live Warrants (mt) | Delivered In (mt) | Delivered Out (mt) |
  Net Change (mt) | Source
"""

import os
import io
import re
import json
import datetime
import requests
from bs4 import BeautifulSoup
import openpyxl
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
LME_LOGIN_URL    = "https://www.lme.com/account/login"
LME_REPORT_PAGE  = "https://www.lme.com/market-data/reports-and-data/warehouse-and-stocks-reports/stocks-summary/stock-movement-report"
LME_DOWNLOAD_BASE = "https://www.lme.com/Lme-api/ReportsListingSearchApi/Download"

SHEET_NAME = "3-exchange-inventory-tracker"
TAB_LME    = "LME"
TAB_DASH   = "Dashboard"

COPPER_KEYWORDS = ["copper", "CA", "cu"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── AUTH ──────────────────────────────────────────────────────
def login(session):
    """
    Log in to LME.com.
    LME uses ASP.NET with a __RequestVerificationToken anti-forgery token.
    We fetch the login page first to get the token, then POST credentials.
    """
    print("Fetching login page...")
    resp = session.get(LME_LOGIN_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract anti-forgery token
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if not token_input:
        # Try meta tag
        token_meta = soup.find("meta", {"name": "RequestVerificationToken"})
        token = token_meta["content"] if token_meta else ""
    else:
        token = token_input.get("value", "")

    print(f"Got CSRF token: {token[:20]}..." if token else "Warning: no CSRF token found")

    username = os.environ["LME_USERNAME"]
    password = os.environ["LME_PASSWORD"]

    payload = {
        "__RequestVerificationToken": token,
        "Email":    username,
        "Password": password,
        "RememberMe": "false",
    }

    print("Posting credentials...")
    login_resp = session.post(
        LME_LOGIN_URL,
        data=payload,
        headers={**HEADERS, "Referer": LME_LOGIN_URL, "Content-Type": "application/x-www-form-urlencoded"},
        allow_redirects=True,
        timeout=30
    )
    login_resp.raise_for_status()

    # Check if login succeeded by looking for logged-in indicators
    if "logout" in login_resp.text.lower() or "sign out" in login_resp.text.lower() or "my account" in login_resp.text.lower():
        print("✅ Login successful")
    else:
        print("⚠️  Login may have failed — no logout link found. Proceeding anyway.")

    return session

# ── FIND LATEST REPORT ────────────────────────────────────────
def find_report_url(session):
    """
    Fetch the stock movement report listing page and extract the
    most recent report download URL.
    """
    print(f"Fetching report listing: {LME_REPORT_PAGE}")
    resp = session.get(LME_REPORT_PAGE, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Look for download links — LME uses /Lme-api/ReportsListingSearchApi/Download?id=<guid>
    download_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "Download" in href and ("id=" in href or "guid" in href.lower()):
            full_url = href if href.startswith("http") else f"https://www.lme.com{href}"
            download_links.append((a.get_text(strip=True), full_url))
            print(f"  Found link: {a.get_text(strip=True)[:60]} → {full_url[:80]}")

    # Also check for API endpoint that lists reports
    # LME may load report list via JS/XHR — try the search API directly
    if not download_links:
        print("No direct links found, trying API endpoint...")
        api_url = "https://www.lme.com/Lme-api/ReportsListingSearchApi/GetReports?reportType=StockMovementReport&page=1&pageSize=5"
        api_resp = session.get(api_url, headers={**HEADERS, "Accept": "application/json"}, timeout=30)
        if api_resp.status_code == 200:
            try:
                data = api_resp.json()
                print(f"API response: {json.dumps(data)[:500]}")
                # Extract first/latest report ID
                reports = data.get("reports") or data.get("items") or data.get("data") or []
                if reports:
                    latest = reports[0]
                    report_id = latest.get("id") or latest.get("guid") or latest.get("reportId")
                    if report_id:
                        url = f"{LME_DOWNLOAD_BASE}?id={report_id}"
                        download_links.append((latest.get("title", "Stock Movement Report"), url))
            except Exception as e:
                print(f"API parse error: {e}")
                print(f"Raw response: {api_resp.text[:500]}")

    if not download_links:
        raise ValueError("Could not find any report download links. Check login or page structure.")

    # Take the first (most recent) link
    title, url = download_links[0]
    print(f"Using report: {title} → {url}")
    return url

# ── DOWNLOAD REPORT ───────────────────────────────────────────
def download_report(session, url):
    """Download the XLSX report and return raw bytes."""
    print(f"Downloading: {url}")
    resp = session.get(
        url,
        headers={**HEADERS, "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*"},
        timeout=60
    )
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content)} bytes, Content-Type: {resp.headers.get('Content-Type','?')}")
    return resp.content

# ── PARSE XLSX ────────────────────────────────────────────────
def parse_xlsx(content):
    """
    Parse the LME Stock Movement Report XLSX for copper data.
    
    Expected structure: rows per metal with columns like:
    Metal | Opening Stock | Delivered In | Delivered Out | Net Change |
    On Warrant | Cancelled Warrants | Total Live Warrants | Report Date
    
    We look for the row where col0 contains 'copper' or 'CA'
    """
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)

    print(f"Sheets: {wb.sheetnames}")
    ws = wb.active

    result = {
        "report_date":         "",
        "on_warrant":          None,
        "cancelled_warrants":  None,
        "total_live_warrants": None,
        "delivered_in":        None,
        "delivered_out":       None,
        "net_change":          None,
    }

    rows = list(ws.iter_rows(values_only=True))

    print(f"Total rows: {len(rows)}")
    for i, row in enumerate(rows[:30]):
        print(f"  Row {i}: {[str(c)[:20] if c is not None else '' for c in row]}")

    # Find header row
    header_row = None
    header_idx = None
    for i, row in enumerate(rows):
        row_str = " ".join(str(c).lower() for c in row if c is not None)
        if ("on warrant" in row_str or "open" in row_str) and ("deliver" in row_str or "cancel" in row_str):
            header_row = row
            header_idx = i
            print(f"Header row at index {i}: {list(row)}")
            break

    # Find copper row
    for i, row in enumerate(rows):
        if row[0] is None:
            continue
        cell = str(row[0]).strip().lower()
        if any(kw.lower() in cell for kw in COPPER_KEYWORDS):
            print(f"Copper row at index {i}: {list(row)}")

            # Map columns by header if available, else use position
            if header_row:
                col_map = {str(h).strip().lower(): j for j, h in enumerate(header_row) if h}
                print(f"Column map: {col_map}")

                def get_col(keywords):
                    for kw in keywords:
                        for k, v in col_map.items():
                            if kw.lower() in k:
                                return safe_float(row[v])
                    return None

                result["on_warrant"]          = get_col(["on warrant", "open", "live warrant"])
                result["cancelled_warrants"]  = get_col(["cancel"])
                result["total_live_warrants"] = get_col(["total", "closing"])
                result["delivered_in"]        = get_col(["delivered in", "deliv in"])
                result["delivered_out"]       = get_col(["delivered out", "deliv out"])
                result["net_change"]          = get_col(["net change", "net"])
            else:
                # Fallback: positional (adjust based on actual structure)
                vals = [safe_float(c) for c in row[1:]]
                if len(vals) >= 5:
                    result["delivered_in"]        = vals[0]
                    result["delivered_out"]       = vals[1]
                    result["net_change"]          = vals[2]
                    result["on_warrant"]          = vals[3]
                    result["cancelled_warrants"]  = vals[4]
                    result["total_live_warrants"] = vals[5] if len(vals) > 5 else None
            break

    # Extract report date — look in top rows
    for row in rows[:10]:
        for cell in row:
            if cell is None:
                continue
            cell_str = str(cell)
            if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', cell_str) or isinstance(cell, datetime.datetime):
                if isinstance(cell, datetime.datetime):
                    result["report_date"] = cell.strftime("%Y-%m-%d")
                else:
                    result["report_date"] = cell_str
                break
        if result["report_date"]:
            break

    return result


def safe_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
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
            "On Warrant (mt)", "Cancelled Warrants (mt)", "Total Live Warrants (mt)",
            "Delivered In (mt)", "Delivered Out (mt)", "Net Change (mt)",
            "Source"
        ]
        lme_tab.insert_row(headers, 1)
        print("✅ LME headers written")


def write_to_sheet(data, lme_tab, dash_tab, source_url):
    today = datetime.date.today().isoformat()

    # Duplicate check on report date
    existing = lme_tab.col_values(2)
    if data["report_date"] and data["report_date"] in existing:
        print(f"Duplicate: {data['report_date']} already logged. Skipping.")
        return

    row = [
        today,
        data["report_date"],
        data["on_warrant"]          or "",
        data["cancelled_warrants"]  or "",
        data["total_live_warrants"] or "",
        data["delivered_in"]        or "",
        data["delivered_out"]       or "",
        data["net_change"]          or "",
        source_url
    ]

    lme_tab.append_row(row, value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(row)} columns to LME tab")

    # Dashboard: add LME total to existing row if same date, else new row
    # (simple append for now)
    dash_tab.append_row([
        today, data["report_date"], "", "", "", "",
        data["total_live_warrants"] or "", ""
    ], value_input_option="USER_ENTERED")
    print("✅ Wrote to Dashboard tab")


# ── MAIN ──────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"LME Copper Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    try:
        session = requests.Session()
        session = login(session)

        report_url = find_report_url(session)
        content    = download_report(session, report_url)
        data       = parse_xlsx(content)

        print(f"\nParsed summary:")
        for k, v in data.items():
            print(f"  {k}: {v}")

        if data["on_warrant"] is None and data["total_live_warrants"] is None:
            raise ValueError("Parse failed — no copper data found. Check XLSX structure in logs above.")

        client  = get_sheet_client()
        book    = client.open(SHEET_NAME)
        lme_tab = book.worksheet(TAB_LME)
        dash_tab = book.worksheet(TAB_DASH)

        ensure_headers(lme_tab)
        write_to_sheet(data, lme_tab, dash_tab, report_url)

        print("\n✅ Done.")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()










