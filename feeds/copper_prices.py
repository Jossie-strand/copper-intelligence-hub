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

def fetch_yfinance_daily():
    """Fetch COMEX copper futures (HG=F) daily data from Yahoo Finance."""
    try:
        import yfinance as yf
    except ImportError:
        print("⚠️  yfinance not installed — skipping daily fetch")
        return 0

    try:
        # Check how much data we already have
        client = get_client()
        result = client.table("copper_prices").select("data_date") \
            .eq("source", "Yahoo").eq("symbol", "HG=F") \
            .order("data_date", desc=True).limit(1).execute()

        # Use Ticker.history() with explicit start/end dates to avoid
        # the YFTzMissingError on headless Linux runners
        ticker = yf.Ticker("HG=F")
        today = datetime.date.today()

        if result.data:
            latest_date = result.data[0]["data_date"]
            start = (pd.Timestamp(latest_date) - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
            print(f"📊 Yahoo Finance: latest in DB is {latest_date}, fetching from {start}")
            df = ticker.history(start=start, end=today.strftime("%Y-%m-%d"))
        else:
            start = "2000-01-01"
            print("📊 Yahoo Finance: first run — fetching history from 2000")
            df = ticker.history(start=start, end=today.strftime("%Y-%m-%d"))

        if df.empty:
            print("⚠️  Yahoo Finance returned no data for HG=F")
            return 0

        # yfinance may return MultiIndex columns — flatten if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        print(f"📊 Yahoo Finance HG=F: received {len(df)} rows")

        rows = []
        for date_idx, r in df.iterrows():
            date_str = pd.Timestamp(date_idx).strftime("%Y-%m-%d")
            close_val = r.get("Close")
            if pd.isna(close_val):
                continue

            row = {
                "data_date": date_str,
                "frequency": "daily",
                "source": "Yahoo",
                "symbol": "HG=F",
                "price": round(float(close_val), 4),
                "price_unit": "USD/lb",
            }
            if not pd.isna(r.get("Open")):
                row["open"] = round(float(r["Open"]), 4)
            if not pd.isna(r.get("High")):
                row["high"] = round(float(r["High"]), 4)
            if not pd.isna(r.get("Low")):
                row["low"] = round(float(r["Low"]), 4)
            if not pd.isna(r.get("Close")):
                row["close"] = round(float(r["Close"]), 4)
            if not pd.isna(r.get("Volume")):
                row["volume"] = int(r["Volume"])

            rows.append(row)

        count = upsert_prices(rows)
        print(f"✅ Yahoo Finance HG=F: {count} daily rows upserted")
        return count

    except Exception as e:
        print(f"⚠️  Yahoo Finance fetch failed: {e}")
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
            # Fetch from a few days before latest to catch any gaps
            start = (pd.Timestamp(latest_date) - pd.Timedelta(days=5)).strftime("%Y%m%d")
            end = pd.Timestamp.now().strftime("%Y%m%d")
            print(f"📊 AKShare SHFE: fetching {start} to {end} (latest in DB: {latest_date})")
        else:
            # First run — fetch last 2 years
            start = (pd.Timestamp.now() - pd.Timedelta(days=730)).strftime("%Y%m%d")
            end = pd.Timestamp.now().strftime("%Y%m%d")
            print(f"📊 AKShare SHFE: first run — fetching {start} to {end}")

        # get_futures_daily returns all SHFE contracts for each date
        # We need to iterate through dates and filter for copper (CU)
        rows = []
        current = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)

        while current <= end_ts:
            date_str = current.strftime("%Y%m%d")
            try:
                df = ak.get_futures_daily(start_date=date_str, end_date=date_str, market="SHFE")
                if df is not None and not df.empty:
                    # Filter for copper — symbol starts with "cu" or "CU"
                    cu_df = df[df["symbol"].str.upper() == "CU"]
                    if not cu_df.empty:
                        for _, r in cu_df.iterrows():
                            settle = r.get("settle")
                            if pd.isna(settle) or settle == 0:
                                continue

                            row = {
                                "data_date": current.strftime("%Y-%m-%d"),
                                "frequency": "daily",
                                "source": "SHFE",
                                "symbol": "CU",
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
                            if not pd.isna(r.get("volume")):
                                row["volume"] = int(r["volume"])

                            rows.append(row)
                            break  # Take only the first CU row (aggregate/main)
            except Exception as e:
                # Skip individual dates that fail (weekends, holidays)
                pass

            current += pd.Timedelta(days=1)

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

    # Check if table is empty — if so, run Excel backfill first
    client = get_client()
    result = client.table("copper_prices").select("id", count="exact").limit(1).execute()
    is_empty = result.count == 0

    monthly_count = 0
    daily_count = 0

    if is_empty:
        print("\n📦 Table is empty — running FRED Excel backfill first...")
        monthly_count += backfill_fred_excel()

    print("\n── FRED API (monthly) ──")
    monthly_count += fetch_fred_latest()

    print("\n── Yahoo Finance COMEX (daily OHLCV) ──")
    daily_count += fetch_yfinance_daily()

    print("\n── Westmetall LME (daily) ──")
    daily_count += fetch_lme_westmetall()

    print("\n── AKShare SHFE (daily) ──")
    daily_count += fetch_shfe_akshare()

    print("\n" + "=" * 60)
    print(f"SUMMARY: {monthly_count} monthly rows, {daily_count} daily rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
