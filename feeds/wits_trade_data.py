"""
feeds/wits_trade_data.py

Fetches bilateral copper trade data from the World Bank WITS API.
Products: HS 260300 (copper ores & concentrates), 740311 (refined copper cathodes)
Indicators: XPRT-TRD-VL (export value USD), MPRT-TRD-VL (import value USD)

Writes to Supabase tables: wits_trade_annual, wits_country_ref

WITS API base: https://wits.worldbank.org/API/V1/SDMX/V21/
Data source: tradestats-trade

Schedule: Quarterly via GitHub Actions (data is annual, updated with ~1yr lag)

SECRETS REQUIRED:
  SUPABASE_URL         — Your Supabase project URL
  SUPABASE_SERVICE_KEY — Service role key (not anon key) for write access
"""

import os
import sys
import time
import datetime
import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client

# ── CONFIG ────────────────────────────────────────────────────

WITS_BASE = "https://wits.worldbank.org/API/V1/SDMX/V21"

# Products to fetch (HS 6-digit codes)
PRODUCTS = {
    "260300": "Copper ores & concentrates",
    "740311": "Refined copper cathodes",
}

# Indicators
INDICATORS = {
    "XPRT-TRD-VL": "Export trade value (USD)",
    "MPRT-TRD-VL": "Import trade value (USD)",
}

# Key partner countries for bilateral data (beyond World total).
# These are the most important bilateral flows for copper analysis.
# We fetch World total for ALL reporters, then bilateral detail for
# the top copper trading partners.
BILATERAL_PARTNERS = [
    "CHN",   # China — the demand anchor
    "JPN",   # Japan — major smelter/importer
    "KOR",   # South Korea — major smelter
    "DEU",   # Germany — European demand bellwether
    "IND",   # India — growing demand
    "USA",   # United States
    "ESP",   # Spain — European smelting
    "BGR",   # Bulgaria — European smelting
]

# We fetch ALL reporters to World (partner=WLD) to get total export/import
# values for every country. Then we fetch key reporters to key partners
# for bilateral detail.
KEY_REPORTERS_FOR_BILATERAL = [
    "CHL",   # Chile — #1 exporter (Escondida, Collahuasi, Chuquicamata, El Teniente)
    "PER",   # Peru — #2 exporter (Cerro Verde, Las Bambas, Antamina)
    "COD",   # DRC — fast-growing (Kamoa-Kakula)
    "IDN",   # Indonesia — (Grasberg)
    "AUS",   # Australia
    "ZMB",   # Zambia
    "MEX",   # Mexico
    "CHN",   # China — both importer and re-exporter
    "USA",   # United States
    "CAN",   # Canada
    "BRA",   # Brazil
    "RUS",   # Russia
    "KAZ",   # Kazakhstan
    "MNG",   # Mongolia (Oyu Tolgoi)
    "PHL",   # Philippines
    "PNG",   # Papua New Guinea
]

# Year range: WITS Trade Stats data starts at 1988.
# We backfill the full history on first run, then only fetch
# recent years on subsequent runs.
YEAR_START = 1988
YEAR_END = 2024   # Update annually; WITS data lags ~1 year

# Rate limiting: WITS is a free API; be polite
REQUEST_DELAY = 1.5  # seconds between API calls

FETCH_HEADERS = {
    "User-Agent": "CopperIntelligenceHub/1.0 (research)",
    "Accept": "application/xml",
}


# ── SUPABASE ──────────────────────────────────────────────────

def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# ── WITS API HELPERS ──────────────────────────────────────────

def fetch_wits_trade(reporter: str, partner: str, product: str,
                     indicator: str, year_start: int, year_end: int) -> list:
    """
    Fetch trade data from WITS Trade Stats API.

    URL pattern:
    /datasource/tradestats-trade/reporter/{r}/year/{y}/partner/{p}/
    product/{prod}/indicator/{ind}

    Returns list of dicts with parsed data.
    """
    # Build year range string (semicolon-delimited for URL-based queries)
    years = ";".join(str(y) for y in range(year_start, year_end + 1))

    url = (
        f"{WITS_BASE}/datasource/tradestats-trade"
        f"/reporter/{reporter}"
        f"/year/{years}"
        f"/partner/{partner}"
        f"/product/{product}"
        f"/indicator/{indicator}"
    )

    print(f"  GET {reporter} → {partner} | {product} | {indicator} | {year_start}-{year_end}")

    try:
        resp = requests.get(url, headers=FETCH_HEADERS, timeout=60)

        if resp.status_code == 400:
            # WITS returns 400 for "no data" combinations — not an error
            print(f"    → No data (HTTP 400)")
            return []

        if resp.status_code == 413:
            print(f"    → Response too large, need to split year range")
            return []

        resp.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"    → Request failed: {e}")
        return []

    return parse_wits_xml(resp.text, reporter, partner, product, indicator)


def parse_wits_xml(xml_text: str, reporter: str, partner: str,
                   product: str, indicator: str) -> list:
    """
    Parse WITS Trade Stats XML response (structure-specific schema).

    The response contains <Series> elements with attributes like:
      REPORTER="USA" PARTNER="WLD" PRODUCTCODE="260300" INDICATOR="XPRT-TRD-VL"
    Each <Series> contains <Obs> with:
      TIME_PERIOD="2020" OBS_VALUE="1234567.89" DATASOURCE="WITS-CMT"
    """
    results = []

    try:
        root = ET.fromstring(xml_text.lstrip('\ufeff'))
    except ET.ParseError as e:
        print(f"    → XML parse error: {e}")
        return []

    # Handle WITS XML namespaces — they vary by response format
    # Try to find Series elements regardless of namespace
    ns_map = {}
    for elem in root.iter():
        tag = elem.tag
        if "}" in tag:
            ns_uri = tag.split("}")[0] + "}"
            # Collect namespaces
            if "message" in ns_uri.lower():
                ns_map["msg"] = ns_uri.strip("{}")
            elif "structurespecific" in ns_uri.lower() or "data" in ns_uri.lower():
                ns_map["data"] = ns_uri.strip("{}")

    # Find all Series elements
    series_found = False
    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

        if tag == "Series":
            series_found = True
            attrs = elem.attrib
            series_reporter = attrs.get("REPORTER", reporter)
            series_partner = attrs.get("PARTNER", partner)
            series_product = attrs.get("PRODUCTCODE", product)
            series_indicator = attrs.get("INDICATOR", indicator)

            for obs in elem:
                obs_tag = obs.tag.split("}")[-1] if "}" in obs.tag else obs.tag
                if obs_tag == "Obs":
                    obs_attrs = obs.attrib
                    year = obs_attrs.get("TIME_PERIOD")
                    value = obs_attrs.get("OBS_VALUE")
                    datasource = obs_attrs.get("DATASOURCE", "")

                    if year and value:
                        try:
                            results.append({
                                "year": int(year),
                                "reporter_code": series_reporter,
                                "partner_code": series_partner,
                                "product_code": series_product,
                                "indicator": series_indicator,
                                "value_usd": float(value),
                                "datasource": datasource,
                            })
                        except (ValueError, TypeError):
                            pass

    if not series_found:
        # Try parsing as generic SDMX (different structure)
        results = parse_wits_generic_xml(root, reporter, partner, product, indicator)

    count = len(results)
    if count > 0:
        years_range = f"{min(r['year'] for r in results)}-{max(r['year'] for r in results)}"
        print(f"    → {count} observations ({years_range})")
    else:
        print(f"    → 0 observations")

    return results


def parse_wits_generic_xml(root, reporter, partner, product, indicator):
    """Fallback parser for SDMX generic schema format."""
    results = []

    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

        if tag == "Series":
            # Extract series key values
            s_reporter = reporter
            s_partner = partner
            s_product = product
            s_indicator = indicator

            for child in elem.iter():
                child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child_tag == "Value":
                    vid = child.attrib.get("id", "")
                    vval = child.attrib.get("value", "")
                    if vid == "REPORTER":
                        s_reporter = vval
                    elif vid == "PARTNER":
                        s_partner = vval
                    elif vid == "PRODUCTCODE":
                        s_product = vval
                    elif vid == "INDICATOR":
                        s_indicator = vval

                if child_tag == "Obs":
                    year = None
                    value = None
                    datasource = ""
                    for obs_child in child.iter():
                        oc_tag = obs_child.tag.split("}")[-1] if "}" in obs_child.tag else obs_child.tag
                        if oc_tag == "ObsDimension":
                            year = obs_child.attrib.get("value")
                        elif oc_tag == "ObsValue":
                            value = obs_child.attrib.get("value")
                        elif oc_tag == "Value" and obs_child.attrib.get("id") == "DATASOURCE":
                            datasource = obs_child.attrib.get("value", "")

                    if year and value:
                        try:
                            results.append({
                                "year": int(year),
                                "reporter_code": s_reporter,
                                "partner_code": s_partner,
                                "product_code": s_product,
                                "indicator": s_indicator,
                                "value_usd": float(value),
                                "datasource": datasource,
                            })
                        except (ValueError, TypeError):
                            pass

    return results


def fetch_country_metadata() -> list:
    """Fetch country reference data from WITS metadata API."""
    url = "https://wits.worldbank.org/API/V1/wits/datasource/tradestats-trade/country/ALL"
    print("Fetching WITS country metadata...")

    resp = requests.get(url, headers=FETCH_HEADERS, timeout=60)
    resp.raise_for_status()

    print(f"  Response status: {resp.status_code}")
    print(f"  Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
    print(f"  First 500 chars: {resp.text[:500]}")

    countries = []
    clean_text = resp.text.lstrip('\ufeff')

    if not clean_text.strip().startswith('<?xml') and not clean_text.strip().startswith('<'):
        raise ValueError(f"WITS returned non-XML response: {clean_text[:200]}")

    root = ET.fromstring(clean_text)

    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if tag == "country":
            attrs = elem.attrib
            country = {
                "country_code": attrs.get("countrycode", ""),
                "is_reporter": attrs.get("isreporter", "0") == "1",
                "is_partner": attrs.get("ispartner", "0") == "1",
                "is_group": attrs.get("isgroup", "No") == "Yes",
                "group_type": attrs.get("grouptype", ""),
            }
            # Extract child elements
            for child in elem:
                child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if child_tag == "iso3Code":
                    country["iso3"] = (child.text or "").strip()
                elif child_tag == "name":
                    country["name"] = (child.text or "").strip()

            if country.get("name"):
                countries.append(country)

    print(f"  → {len(countries)} countries/entities found")
    return countries


# ── DATA LOADING ──────────────────────────────────────────────

def determine_fetch_years(supabase: Client) -> tuple:
    """
    Determine which years to fetch.
    First run: full backfill (YEAR_START to YEAR_END).
    Subsequent runs: only last 3 years (to catch revisions).
    """
    result = supabase.table("wits_trade_annual") \
        .select("year") \
        .order("year", desc=True) \
        .limit(1) \
        .execute()

    if result.data:
        latest = result.data[0]["year"]
        start = max(YEAR_START, latest - 2)  # Re-fetch last 3 years for revisions
        print(f"Incremental mode: existing data through {latest}, fetching {start}-{YEAR_END}")
        return start, YEAR_END
    else:
        print(f"Backfill mode: fetching {YEAR_START}-{YEAR_END}")
        return YEAR_START, YEAR_END


def chunk_years(start: int, end: int, chunk_size: int = 10) -> list:
    """Split year range into chunks to avoid WITS 'too large' errors."""
    chunks = []
    y = start
    while y <= end:
        chunk_end = min(y + chunk_size - 1, end)
        chunks.append((y, chunk_end))
        y = chunk_end + 1
    return chunks


def upsert_trade_data(supabase: Client, records: list):
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
            "product_label": PRODUCTS.get(r["product_code"], ""),
            "indicator": r["indicator"],
            "value_usd": r["value_usd"],
            "datasource": r.get("datasource", ""),
            "fetched_at": now,
        })

    # Upsert in batches of 500
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        supabase.table("wits_trade_annual").upsert(
            batch,
            on_conflict="year,reporter_code,partner_code,product_code,indicator"
        ).execute()

    print(f"  ✅ Upserted {len(rows)} trade records")


def upsert_country_ref(supabase: Client, countries: list):
    """Upsert country reference data."""
    if not countries:
        return

    rows = []
    for c in countries:
        rows.append({
            "country_code": c["country_code"],
            "iso3": c.get("iso3", ""),
            "name": c["name"],
            "is_reporter": c.get("is_reporter", False),
            "is_partner": c.get("is_partner", False),
            "is_group": c.get("is_group", False),
            "group_type": c.get("group_type", ""),
        })

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        supabase.table("wits_country_ref").upsert(
            batch,
            on_conflict="country_code"
        ).execute()

    print(f"  ✅ Upserted {len(rows)} country references")


def enrich_names(records: list, country_lookup: dict):
    """Add human-readable country names to trade records."""
    for r in records:
        r["reporter_name"] = country_lookup.get(r["reporter_code"], "")
        r["partner_name"] = country_lookup.get(r["partner_code"], "")


# ── MAIN ──────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"WITS Copper Trade Data Feed — {datetime.datetime.utcnow().isoformat()}Z")
    print("=" * 70)

    supabase = get_supabase()

    # Step 1: Fetch and store country metadata
    print("\n[1] Country metadata...")
    countries = fetch_country_metadata()
    upsert_country_ref(supabase, countries)

    # Build ISO3 → name lookup
    country_lookup = {}
    for c in countries:
        iso3 = c.get("iso3", "")
        if iso3:
            country_lookup[iso3] = c["name"]
        country_lookup[c["country_code"]] = c["name"]
    # Add common codes
    country_lookup["WLD"] = "World"

    time.sleep(REQUEST_DELAY)

    # Step 2: Determine year range
    print("\n[2] Determining year range...")
    year_start, year_end = determine_fetch_years(supabase)
    year_chunks = chunk_years(year_start, year_end)

    # Step 3: Fetch ALL reporters → World totals
    # This gives us the global picture for every country.
    print(f"\n[3] Fetching all reporters → World totals...")
    all_records = []
    total_api_calls = 0

    for product_code in PRODUCTS:
        for indicator in INDICATORS:
            for y_start, y_end in year_chunks:
                time.sleep(REQUEST_DELAY)
                records = fetch_wits_trade(
                    reporter="all",
                    partner="WLD",
                    product=product_code,
                    indicator=indicator,
                    year_start=y_start,
                    year_end=y_end,
                )
                total_api_calls += 1

                if records:
                    enrich_names(records, country_lookup)
                    all_records.extend(records)

    print(f"\n  World totals: {len(all_records)} records from {total_api_calls} API calls")

    # Upsert world totals
    upsert_trade_data(supabase, all_records)
    all_records = []

    # Step 4: Fetch bilateral detail for key reporter-partner pairs
    print(f"\n[4] Fetching bilateral detail (key reporters → key partners)...")
    bilateral_calls = 0

    for reporter in KEY_REPORTERS_FOR_BILATERAL:
        for partner in BILATERAL_PARTNERS:
            if reporter == partner:
                continue  # Skip self-trade

            for product_code in PRODUCTS:
                for indicator in INDICATORS:
                    for y_start, y_end in year_chunks:
                        time.sleep(REQUEST_DELAY)
                        records = fetch_wits_trade(
                            reporter=reporter,
                            partner=partner,
                            product=product_code,
                            indicator=indicator,
                            year_start=y_start,
                            year_end=y_end,
                        )
                        bilateral_calls += 1

                        if records:
                            enrich_names(records, country_lookup)
                            all_records.extend(records)

                        # Batch upsert every 2000 records to avoid memory buildup
                        if len(all_records) >= 2000:
                            upsert_trade_data(supabase, all_records)
                            all_records = []

    # Final upsert
    if all_records:
        upsert_trade_data(supabase, all_records)

    print(f"\n  Bilateral detail: {bilateral_calls} API calls")
    print(f"  Total API calls this run: {total_api_calls + bilateral_calls}")
    print(f"\n✅ WITS trade data feed complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        raise
