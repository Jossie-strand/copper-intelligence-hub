"""
feeds/wits_trade_data.py

Fetches bilateral copper trade data from the UN Comtrade API.
Products: HS 260300 (copper ores & concentrates), 740311 (refined copper cathodes)
Flow: Exports (X) and Imports (M)

Writes to Supabase table: wits_trade_annual

UN Comtrade API docs: https://comtradedeveloper.un.org/
Uses the comtradeapicall Python package.

Schedule: Quarterly via GitHub Actions (data is annual, updated with ~1yr lag)

SECRETS REQUIRED:
  SUPABASE_URL         — Your Supabase project URL
  SUPABASE_SERVICE_KEY — Service role key (not anon key) for write access
  COMTRADE_API_KEY     — UN Comtrade subscription key
"""

import os
import sys
import time
import datetime
import comtradeapicall
from supabase import create_client, Client

# ── CONFIG ────────────────────────────────────────────────────

# HS6 copper product codes
PRODUCTS = {
    "260300": "Copper ores & concentrates",
    "740311": "Refined copper cathodes",
}

# Trade flows
FLOWS = {
    "X": "Export",
    "M": "Import",
}

# Year range
YEAR_START = 1988
YEAR_END = 2024

# Rate limiting
REQUEST_DELAY = 2.0

# Max records per API call
MAX_RECORDS = 250000


# ── SUPABASE ──────────────────────────────────────────────────

def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# ── COMTRADE API ──────────────────────────────────────────────

def fetch_copper_trade(api_key, cmd_code, flow_code, period):
    """
    Fetch copper trade data from UN Comtrade.
    Returns list of dicts with trade data.
    """
    print(f"  Fetching {cmd_code} | flow={flow_code} | period={period}")

    try:
        df = comtradeapicall.getFinalData(
            api_key,
            typeCode='C',
            freqCode='A',
            clCode='HS',
            period=period,
            reporterCode=None,
            cmdCode=cmd_code,
            flowCode=flow_code,
            partnerCode=None,
            partner2Code=None,
            customsCode=None,
            motCode=None,
            maxRecords=MAX_RECORDS,
            format_output='JSON',
            aggregateBy=None,
            breakdownMode='classic',
            countOnly=None,
            includeDesc=True,
        )
    except Exception as e:
        print(f"    -> API error: {e}")
        return []

    if df is None or (hasattr(df, 'empty') and df.empty):
        print(f"    -> No data returned")
        return []

    records = []
    for _, row in df.iterrows():
        try:
            record = {
                "year": int(row.get("period", 0)),
                "reporter_code": str(row.get("reporterCodeIsoAlpha3", row.get("reporterCode", ""))),
                "reporter_name": str(row.get("reporterDesc", "")),
                "partner_code": str(row.get("partnerCodeIsoAlpha3", row.get("partnerCode", ""))),
                "partner_name": str(row.get("partnerDesc", "")),
                "product_code": str(row.get("cmdCode", cmd_code)),
                "product_label": PRODUCTS.get(cmd_code, ""),
                "indicator": "XPRT-TRD-VL" if flow_code == "X" else "MPRT-TRD-VL",
                "value_usd": float(row.get("primaryValue", 0)) if row.get("primaryValue") is not None else None,
                "datasource": "UN Comtrade",
            }

            if record["value_usd"] is None or record["value_usd"] == 0:
                continue

            if record["reporter_code"] in ("", "W00", "0"):
                continue

            records.append(record)
        except (ValueError, TypeError, KeyError):
            continue

    print(f"    -> {len(records)} records")
    return records


# ── DATA LOADING ──────────────────────────────────────────────

def determine_fetch_years(supabase):
    """
    First run: full backfill. Subsequent runs: last 3 years only.
    """
    result = supabase.table("wits_trade_annual") \
        .select("year") \
        .order("year", desc=True) \
        .limit(1) \
        .execute()

    if result.data:
        latest = result.data[0]["year"]
        start = max(YEAR_START, latest - 2)
        print(f"Incremental mode: existing data through {latest}, fetching {start}-{YEAR_END}")
    else:
        start = YEAR_START
        print(f"Backfill mode: fetching {YEAR_START}-{YEAR_END}")

    return list(range(start, YEAR_END + 1))


def chunk_years(years, chunk_size=5):
    """Split years into comma-separated chunks for API calls."""
    chunks = []
    for i in range(0, len(years), chunk_size):
        chunk = years[i:i + chunk_size]
        chunks.append(",".join(str(y) for y in chunk))
    return chunks


def upsert_trade_data(supabase, records):
    """Upsert trade data records into Supabase."""
    if not records:
        return

    now = datetime.datetime.utcnow().isoformat()

    rows = []
    for r in records:
        rows.append({
            "year": r["year"],
            "reporter_code": r["reporter_code"],
            "reporter_name": r.get("reporter_name", ""),
            "partner_code": r["partner_code"],
            "partner_name": r.get("partner_name", ""),
            "product_code": r["product_code"],
            "product_label": r.get("product_label", ""),
            "indicator": r["indicator"],
            "value_usd": r["value_usd"],
            "datasource": r.get("datasource", "UN Comtrade"),
            "fetched_at": now,
        })

    batch_size = 500
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table("wits_trade_annual").upsert(
                batch,
                on_conflict="year,reporter_code,partner_code,product_code,indicator"
            ).execute()
            total += len(batch)
        except Exception as e:
            print(f"    -> Upsert error on batch {i//batch_size}: {e}")
            for row in batch:
                try:
                    supabase.table("wits_trade_annual").upsert(
                        [row],
                        on_conflict="year,reporter_code,partner_code,product_code,indicator"
                    ).execute()
                    total += 1
                except Exception as e2:
                    print(f"    -> Skip: {row['reporter_code']}/{row['partner_code']}/{row['year']}: {e2}")

    print(f"  Upserted {total} trade records")


# ── MAIN ──────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"UN Comtrade Copper Trade Data Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 70)

    api_key = os.environ.get("COMTRADE_API_KEY", "")
    if not api_key:
        raise ValueError("COMTRADE_API_KEY environment variable is required")

    supabase = get_supabase()

    print("\n[1] Determining year range...")
    years = determine_fetch_years(supabase)
    year_chunks = chunk_years(years, chunk_size=5)
    print(f"    {len(years)} years in {len(year_chunks)} chunks")

    print(f"\n[2] Fetching copper trade data from UN Comtrade...")
    total_records = 0
    total_api_calls = 0

    for cmd_code, product_name in PRODUCTS.items():
        for flow_code, flow_name in FLOWS.items():
            print(f"\n  --- {product_name} | {flow_name}s ---")

            for period_chunk in year_chunks:
                time.sleep(REQUEST_DELAY)
                total_api_calls += 1

                records = fetch_copper_trade(
                    api_key=api_key,
                    cmd_code=cmd_code,
                    flow_code=flow_code,
                    period=period_chunk,
                )

                if records:
                    upsert_trade_data(supabase, records)
                    total_records += len(records)

    print(f"\n[3] Summary")
    print(f"    Total API calls: {total_api_calls}")
    print(f"    Total records upserted: {total_records}")
    print(f"\n Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n Fatal error: {e}")
        raise
