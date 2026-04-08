"""
feeds/copper_prices.py

Copper pricing pipeline — three sources into one Supabase table:
  1. FRED Excel backfill  (IMF monthly, 1992–2026)
  2. FRED API latest      (IMF monthly, last 12 observations)
  3. Stooq daily          (COMEX HG.F front-month futures, daily OHLCV)

Table: copper_prices (upsert on data_date + frequency + source + symbol)
"""

import os
import sys
import datetime
import requests
import pandas as pd
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


# ── 3. Stooq daily OHLCV ────────────────────────────────────────────────────

def fetch_stooq_daily():
    """Fetch COMEX copper futures (HG.F) daily data from Stooq."""
    try:
        import pandas_datareader.data as web
    except ImportError:
        print("⚠️  pandas_datareader not installed — skipping Stooq fetch")
        return 0

    try:
        # Check how much Stooq data we already have
        client = get_client()
        result = client.table("copper_prices").select("data_date") \
            .eq("source", "Stooq").eq("symbol", "HG.F") \
            .order("data_date", desc=True).limit(1).execute()

        if result.data:
            # Existing data — fetch last 30 days
            latest_date = result.data[0]["data_date"]
            start = pd.Timestamp(latest_date) - pd.Timedelta(days=5)
            end = pd.Timestamp.now()
            print(f"📊 Stooq: fetching from {start.date()} (latest in DB: {latest_date})")
            df = web.DataReader("HG.F", "stooq", start=start, end=end)
        else:
            # First run — fetch all available history
            print("📊 Stooq: first run — fetching full history")
            df = web.DataReader("HG.F", "stooq")

        if df.empty:
            print("⚠️  Stooq returned no data")
            return 0

        print(f"📊 Stooq: received {len(df)} rows")

        rows = []
        for date_idx, r in df.iterrows():
            date_str = pd.Timestamp(date_idx).strftime("%Y-%m-%d")
            close_val = r.get("Close")
            if pd.isna(close_val):
                continue

            row = {
                "data_date": date_str,
                "frequency": "daily",
                "source": "Stooq",
                "symbol": "HG.F",
                "price": float(close_val),
                "price_unit": "USD/lb",
            }
            if not pd.isna(r.get("Open")):
                row["open"] = float(r["Open"])
            if not pd.isna(r.get("High")):
                row["high"] = float(r["High"])
            if not pd.isna(r.get("Low")):
                row["low"] = float(r["Low"])
            if not pd.isna(r.get("Close")):
                row["close"] = float(r["Close"])
            if not pd.isna(r.get("Volume")):
                row["volume"] = int(r["Volume"])

            rows.append(row)

        count = upsert_prices(rows)
        print(f"✅ Stooq: {count} daily rows upserted")
        return count

    except Exception as e:
        print(f"⚠️  Stooq fetch failed: {e}")
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

    print("\n── Stooq (daily OHLCV) ──")
    daily_count += fetch_stooq_daily()

    print("\n" + "=" * 60)
    print(f"SUMMARY: {monthly_count} monthly rows, {daily_count} daily rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
