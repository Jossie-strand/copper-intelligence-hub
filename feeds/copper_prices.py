"""
feeds/copper_prices.py

Copper pricing pipeline — five sources into one Supabase table:
  1. FRED Excel backfill   (IMF monthly, 1992–2026)
  2. FRED API latest       (IMF monthly, last 12 observations)
  3. Yahoo Finance daily   (COMEX HG=F front-month futures, daily OHLCV)
  4. Westmetall daily      (LME Cash Settlement + 3-Month, USD/mt)
  5. AKShare daily         (SHFE copper futures, CNY/mt)

Table: copper_prices (upsert on data_date + frequency + source + symbol)
"""

import os
import sys
import re
import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client

# ── Supabase client ──────────────────────────────────────────────────────────

_client = None

def get_client():
    global _client
    if _client is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_KEY"]
        _client = create_client(url, key)
    return _client


def upsert_prices(rows):
    """Upsert a list of row dicts into copper_prices, batched by 500."""
    if not rows:
        return 0
    client = get_client()
    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        client.table("copper_prices").upsert(
            batch, on_conflict="data_date,frequency,source,symbol"
        ).execute()
        total += len(batch)
    return total


# ── 1. FRED Excel backfill ───────────────────────────────────────────────────

def backfill_fred_excel():
    """Read the local FRED Excel file and upsert monthly copper prices."""
    excel_path = os.path.join(os.path.dirname(__file__), "data", "FRED Price History.xlsx")
    if not os.path.exists(excel_path):
        print(f"⚠️  Excel file not found: {excel_path}")
        return 0

    df = pd.read_excel(excel_path)
    print(f"📄 Read {len(df)} rows from FRED Excel file")

    # Expect columns: observation_date, PCOPPUSDM
    rows = []
    for _, r in df.iterrows():
        date_val = r.get("observation_date")
        price_val = r.get("PCOPPUSDM")

        if pd.isna(date_val) or pd.isna(price_val):
            continue

        # Normalize date
        if isinstance(date_val, str):
            date_str = date_val[:10]
        else:
            date_str = pd.Timestamp(date_val).strftime("%Y-%m-%d")

        rows.append({
            "data_date": date_str,
            "frequency": "monthly",
            "source": "FRED",
            "symbol": "PCOPPUSDM",
            "price": float(price_val),
            "price_unit": "USD/mt",
        })

    count = upsert_prices(rows)
    print(f"✅ FRED Excel backfill: {count} monthly rows upserted")
    return count


# ── 2. FRED API latest ──────────────────────────────────────────────────────

def fetch_fred_latest():
    """Fetch the latest 12 monthly observations from the FRED API."""
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("⚠️  FRED_API_KEY not set — skipping FRED API fetch")
        return 0

    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=PCOPPUSDM&api_key={api_key}&file_type=json"
        "&sort_order=desc&limit=12"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for obs in data.get("observations", []):
        if obs.get("value") in (None, "", "."):
            continue
        try:
            price = float(obs["value"])
        except (ValueError, TypeError):
            continue

        rows.append({
            "data_date": obs["date"],
            "frequency": "monthly",
            "source": "FRED",
            "symbol": "PCOPPUSDM",
            "price": price,
            "price_unit": "USD/mt",
        })

    count = upsert_prices(rows)
    print(f"✅ FRED API: {count} monthly rows upserted")
    return count


# ── 3. Yahoo Finance daily OHLCV ─────────────────────────────────────────────

def fetch_comex_daily():
    """Fetch COMEX copper futures (HG=F) daily data via Yahoo Finance raw API."""
    try:
        # Check how much data we already have
        client = get_client()
        result = client.table("copper_prices").select("data_date") \
            .eq("source", "Yahoo").eq("symbol", "HG=F") \
            .order("data_date", desc=True).limit(1).execute()

        today = datetime.date.today()
        if result.data:
            latest_date = result.data[0]["data_date"]
            start_dt = pd.Timestamp(latest_date) - pd.Timedelta(days=5)
            print(f"📊 COMEX HG=F: latest in DB is {latest_date}, fetching from {start_dt.date()}")
        else:
            start_dt = pd.Timestamp("2000-01-01")
            print("📊 COMEX HG=F: first run — fetching history from 2000")

        period1 = int(start_dt.timestamp())
        period2 = int(pd.Timestamp(today).timestamp())

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/HG%3DF"
            f"?period1={period1}&period2={period2}&interval=1d"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        chart = data.get("chart", {}).get("result", [])
        if not chart:
            print("⚠️  Yahoo Finance API returned no chart data for HG=F")
            return 0

        timestamps = chart[0].get("timestamp", [])
        quote = chart[0].get("indicators", {}).get("quote", [{}])[0]

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        print(f"📊 COMEX HG=F: received {len(timestamps)} rows")

        rows = []
        for i, ts in enumerate(timestamps):
            date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            close_val = closes[i] if i < len(closes) else None
            if close_val is None:
                continue

            row = {
                "data_date": date_str,
                "frequency": "daily",
                "source": "Yahoo",
                "symbol": "HG=F",
                "price": round(float(close_val), 4),
                "price_unit": "USD/lb",
            }
            if i < len(opens) and opens[i] is not None:
                row["open"] = round(float(opens[i]), 4)
            if i < len(highs) and highs[i] is not None:
                row["high"] = round(float(highs[i]), 4)
            if i < len(lows) and lows[i] is not None:
                row["low"] = round(float(lows[i]), 4)
            row["close"] = round(float(close_val), 4)
            if i < len(volumes) and volumes[i] is not None:
                row["volume"] = int(volumes[i])

            rows.append(row)

        count = upsert_prices(rows)
        print(f"✅ COMEX HG=F: {count} daily rows upserted")
        return count

    except Exception as e:
        print(f"⚠️  COMEX HG=F fetch failed: {e}")
        return 0


# ── 4. Westmetall LME prices ─────────────────────────────────────────────────

WESTMETALL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_lme_westmetall():
    """Scrape LME Cash Settlement and 3-Month copper prices from Westmetall."""
    url = "https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash"

    try:
        # Check what we already have to decide how far back to parse
        client = get_client()
        result = client.table("copper_prices").select("data_date") \
            .eq("source", "Westmetall").eq("symbol", "LME_Cu_cash") \
            .order("data_date", desc=True).limit(1).execute()

        latest_in_db = result.data[0]["data_date"] if result.data else None

        resp = requests.get(url, headers=WESTMETALL_HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Find the data table — it has columns: Date, Cash, 3-Month, Stock
        table = soup.find("table")
        if not table:
            print("⚠️  Westmetall: no table found on page")
            return 0

        trs = table.find_all("tr")
        cash_rows = []
        three_mo_rows = []
        parsed = 0

        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            # Parse date: "08. April 2026" format
            date_text = tds[0].get_text(strip=True)
            date_match = re.match(r"(\d{1,2})\.\s+(\w+)\s+(\d{4})", date_text)
            if not date_match:
                continue

            try:
                dt = datetime.datetime.strptime(
                    f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}",
                    "%d %B %Y"
                )
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

            # On incremental runs, stop once we reach data already in DB
            # (but always process at least 5 days for safety)
            if latest_in_db and date_str < latest_in_db and parsed > 5:
                break

            # Parse prices: "12,552.00" format (comma as thousands separator)
            def parse_price(td):
                text = td.get_text(strip=True).replace(",", "")
                try:
                    return float(text)
                except (ValueError, TypeError):
                    return None

            cash_price = parse_price(tds[1])
            three_mo_price = parse_price(tds[2])

            if cash_price is not None:
                cash_rows.append({
                    "data_date": date_str,
                    "frequency": "daily",
                    "source": "Westmetall",
                    "symbol": "LME_Cu_cash",
                    "price": cash_price,
                    "price_unit": "USD/mt",
                })

            if three_mo_price is not None:
                three_mo_rows.append({
                    "data_date": date_str,
                    "frequency": "daily",
                    "source": "Westmetall",
                    "symbol": "LME_Cu_3mo",
                    "price": three_mo_price,
                    "price_unit": "USD/mt",
                })

            parsed += 1

        count = upsert_prices(cash_rows) + upsert_prices(three_mo_rows)
        print(f"✅ Westmetall LME: {len(cash_rows)} cash + {len(three_mo_rows)} 3-month rows upserted")
        return count

    except Exception as e:
        print(f"⚠️  Westmetall LME fetch failed: {e}")
        return 0


# ── 5. AKShare SHFE copper futures ───────────────────────────────────────────

def fetch_shfe_akshare():
    """Fetch SHFE copper futures settlement prices via AKShare."""
    try:
        import akshare as ak
    except ImportError:
        print("⚠️  akshare not installed — skipping SHFE fetch")
        return 0

    try:
        # Check what we already have
        client = get_client()
        result = client.table("copper_prices").select("data_date") \
            .eq("source", "SHFE").eq("symbol", "CU") \
            .order("data_date", desc=True).limit(1).execute()

        if result.data:
            latest_date = result.data[0]["data_date"]
            start = (pd.Timestamp(latest_date) - pd.Timedelta(days=5)).strftime("%Y%m%d")
            end = pd.Timestamp.now().strftime("%Y%m%d")
            print(f"📊 AKShare SHFE: fetching {start} to {end} (latest in DB: {latest_date})")
        else:
            # First run — fetch last 14 days only to stay within timeout
            start = (pd.Timestamp.now() - pd.Timedelta(days=14)).strftime("%Y%m%d")
            end = pd.Timestamp.now().strftime("%Y%m%d")
            print(f"📊 AKShare SHFE: first run — fetching {start} to {end}")

        sys.stdout.flush()

        # Use signal-based timeout on Linux to prevent AKShare from hanging
        import signal

        def _timeout_handler(signum, frame):
            raise TimeoutError("AKShare call exceeded 120s timeout")

        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(120)  # 2 minute hard limit

        try:
            df = ak.get_futures_daily(start_date=start, end_date=end, market="SHFE")
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
        if df is None or df.empty:
            print("⚠️  AKShare returned no SHFE data")
            return 0

        # Debug: show columns and sample symbols to diagnose filtering
        print(f"📊 AKShare columns: {list(df.columns)}")
        sys.stdout.flush()

        # Find the symbol/variety column — akshare versions use different names
        sym_col = None
        for candidate in ["symbol", "variety", "品种", "product"]:
            if candidate in df.columns:
                sym_col = candidate
                break

        if sym_col is None:
            print(f"⚠️  Cannot find symbol column in: {list(df.columns)}")
            # Print first row for debugging
            print(f"📊 First row: {df.iloc[0].to_dict()}")
            return 0

        unique_syms = df[sym_col].unique()[:20]
        print(f"📊 AKShare unique symbols (first 20): {list(unique_syms)}")
        sys.stdout.flush()

        # Filter for copper — match "CU", "cu", or strings starting with "cu"
        cu_mask = df[sym_col].str.upper().str.startswith("CU")
        cu_df = df[cu_mask].copy()
        if cu_df.empty:
            print("⚠️  No CU (copper) rows in AKShare SHFE data")
            return 0

        print(f"📊 AKShare SHFE CU: {len(cu_df)} contracts across {cu_df['date'].nunique()} dates")
        sys.stdout.flush()

        rows = []
        for _, r in cu_df.iterrows():
            settle = r.get("settle")
            if pd.isna(settle) or settle == 0:
                continue

            date_val = str(r.get("date", ""))
            clean = date_val.replace("-", "").replace("/", "")
            date_str = f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"

            # Store each contract individually (CU2604, CU2605, etc.)
            symbol = str(r.get("symbol", "")).upper()

            row = {
                "data_date": date_str,
                "frequency": "daily",
                "source": "SHFE",
                "symbol": symbol,
                "price": float(settle),
                "price_unit": "CNY/mt",
            }
            if not pd.isna(r.get("open")):
                row["open"] = float(r["open"])
            if not pd.isna(r.get("high")):
                row["high"] = float(r["high"])
            if not pd.isna(r.get("low")):
                row["low"] = float(r["low"])
            if not pd.isna(r.get("close")):
                row["close"] = float(r["close"])
            vol = r.get("volume")
            if not pd.isna(vol) and vol > 0:
                row["volume"] = int(vol)

            rows.append(row)

        count = upsert_prices(rows)
        print(f"✅ AKShare SHFE CU: {count} daily rows upserted")
        return count

    except Exception as e:
        print(f"⚠️  AKShare SHFE fetch failed: {e}")
        return 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("COPPER PRICING PIPELINE")
    print(f"Run time: {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 60)
    sys.stdout.flush()

    # Check if table is empty — if so, run Excel backfill first
    client = get_client()
    result = client.table("copper_prices").select("id", count="exact").limit(1).execute()
    is_empty = result.count == 0

    monthly_count = 0
    daily_count = 0

    if is_empty:
        print("\n📦 Table is empty — running FRED Excel backfill first...")
        sys.stdout.flush()
        monthly_count += backfill_fred_excel()

    print("\n── FRED API (monthly) ──")
    sys.stdout.flush()
    monthly_count += fetch_fred_latest()

    print("\n── COMEX HG=F (daily OHLCV) ──")
    sys.stdout.flush()
    daily_count += fetch_comex_daily()

    print("\n── Westmetall LME (daily) ──")
    sys.stdout.flush()
    daily_count += fetch_lme_westmetall()

    print("\n── AKShare SHFE (daily) ──")
    sys.stdout.flush()
    daily_count += fetch_shfe_akshare()

    print("\n" + "=" * 60)
    print(f"SUMMARY: {monthly_count} monthly rows, {daily_count} daily rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
