# Copper Intelligence Hub — Supabase Database Schema

---

## Table 1: `exchange_inventory_daily`

The core table. One row per exchange per trading day. Replaces your Google Sheet Dashboard + exchange tabs.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | The trading day this data is for |
| exchange | text | 'COMEX', 'LME', or 'SHFE' |
| total_mt | numeric | Total stocks in metric tons |
| change_mt | numeric | Daily change in mt |
| source_url | text | URL of the source data |
| fetched_at | timestamptz | When the feed script ran |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, exchange) — one row per exchange per day.

---

## Table 2: `comex_warehouse_daily`

Per-warehouse COMEX detail. One row per warehouse per trading day.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Trading day |
| warehouse | text | Baltimore, Detroit, El Paso, New Orleans, Owensboro, Salt Lake City, Tucson |
| registered_mt | numeric | Registered stocks (mt) |
| eligible_mt | numeric | Eligible stocks (mt) |
| total_mt | numeric | Calculated: registered + eligible |
| registered_prev_mt | numeric | Previous day registered |
| eligible_prev_mt | numeric | Previous day eligible |
| report_date | text | CME report date string |
| activity_date | text | CME activity date string |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, warehouse)

---

## Table 3: `shfe_region_daily`

Per-region SHFE detail. One row per region per reporting day.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Reporting day |
| region | text | Shanghai, Guangdong, Jiangsu, Zhejiang, Other |
| total_mt | numeric | Regional stocks (mt) |
| change_mt | numeric | Daily change |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, region)

---

## Table 4: `lme_detail_daily`

LME-specific fields beyond what's in exchange_inventory_daily.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Trading day |
| cancelled_warrants_mt | numeric | Cancelled warrants in mt (manual or scraped) |
| cancelled_pct | numeric | Calculated: cancelled / total × 100 |
| live_warrants_mt | numeric | Total minus cancelled |
| source | text | Where the cancelled warrant data came from |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date)

---

## Table 5: `futures_prices_daily`

Daily futures prices from Barchart API. One row per contract per day.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Trading day |
| exchange | text | 'COMEX', 'LME', 'SHFE' |
| symbol | text | Barchart symbol: HG*1, LCQ*1, SCF*1 |
| price | numeric | Settlement/close price |
| price_unit | text | '$/lb', '$/t', 'CNY/t' |
| open | numeric | Open price |
| high | numeric | Day high |
| low | numeric | Day low |
| volume | integer | Trading volume |
| open_interest | integer | Open interest |
| change | numeric | Day change |
| change_pct | numeric | Day change % |
| source | text | 'barchart' |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, symbol)

---

## Table 6: `forward_curve`

LME forward curve snapshots. One row per tenor per snapshot date.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| snapshot_date | date | When this curve was captured |
| tenor | text | 'Spot', 'M+1', 'M+2', 'M+3', 'M+6', 'M+9', 'M+12', 'M+15' |
| price_usd_t | numeric | Price in $/t |
| source | text | Data source |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (snapshot_date, tenor)

---

## Table 7: `physical_premiums`

Regional physical premiums. Updated weekly or when new data arrives.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Effective date |
| region | text | 'Yangshan', 'US Midwest', 'Europe Rotterdam', 'Japan CIF', 'South Korea' |
| premium | numeric | Premium value |
| premium_unit | text | '$/t' or '$/lb' |
| trend | text | 'up', 'down', 'flat' |
| source | text | Fastmarkets, SMM, etc. |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, region)

---

## Table 8: `tcrc_rates`

TC/RC spot and benchmark rates. Updated weekly.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Effective date |
| rate_type | text | 'spot' or 'benchmark' |
| tc_usd_t | numeric | Treatment charge $/t |
| rc_usc_lb | numeric | Refining charge USc/lb (if available) |
| source | text | Fastmarkets, CSPT, etc. |
| notes | text | Any context (e.g., "2026 benchmark collapsed to $0") |
| created_at | timestamptz | Auto: now() |

---

## Table 9: `mines`

Master mine reference table. 20+ mines with static attributes updated quarterly.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| name | text | Mine name |
| country | text | Country |
| operator | text | Primary operator |
| ownership | text | Full ownership string (e.g., "BHP/Rio/JECO") |
| annual_capacity_kt | numeric | Nameplate annual capacity |
| status | text | 'operating', 'disrupted', 'recovering', 'constrained', 'closed' |
| lat | numeric | Latitude |
| lng | numeric | Longitude |
| key_risk | text | Primary risk factor |
| stage | text | 'production', 'development', 'exploration' |
| ore_type | text | 'copper', 'copper-gold', 'copper-moly', etc. |
| updated_at | timestamptz | Last time this record was updated |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (name)

---

## Table 10: `mine_production`

Actual production data per mine per period. Updated quarterly from USGS/Cochilco/company reports.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| mine_name | text | FK reference to mines.name |
| period | text | 'H1-2025', 'H2-2025', 'Q1-2026', '2025', etc. |
| production_kt | numeric | Actual production in kt |
| source | text | USGS, Cochilco, company report |
| notes | text | Any context |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (mine_name, period)

---

## Table 11: `disruptions`

Active and historical supply disruptions. This is where Claude Code manual entry shines.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| name | text | Event name (e.g., "Grasberg landslide") |
| mine_name | text | Mine affected (nullable — CSPT is not a mine) |
| country | text | Country |
| type | text | 'mine', 'smelter', 'logistics', 'labor', 'regulatory' |
| severity | text | 'critical', 'significant', 'moderate', 'minor' |
| kt_at_risk | numeric | Annualized production at risk |
| kt_lost_to_date | numeric | Cumulative kt lost so far |
| start_date | date | When disruption began |
| expected_resolution | text | 'Q4-2026', 'permanent', 'unknown', etc. |
| status | text | 'active', 'recovering', 'resolved' |
| description | text | Detailed description of the event |
| source | text | News source, company release, etc. |
| last_updated | timestamptz | When this record was last reviewed |
| created_at | timestamptz | Auto: now() |

---

## Table 12: `supply_demand_balance`

Global S&D data from ICSG. Updated bi-annually.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| year | text | '2022', '2023', '2024', '2025', '2026F', '2027F' |
| mine_supply_kt | numeric | Global mine production |
| refined_supply_kt | numeric | Total refined supply (mine + SX-EW + scrap) |
| demand_kt | numeric | Global refined demand |
| balance_kt | numeric | Supply minus demand |
| source | text | 'ICSG', 'CRU', etc. |
| notes | text | Any context |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (year)

---

## Table 13: `country_production`

Annual mine production by country. Updated quarterly from USGS/ICSG.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| year | text | '2024', '2025', '2026F' |
| country | text | Country name |
| production_kt | numeric | Mine production in kt |
| pct_global | numeric | % of global total |
| yoy_change_pct | numeric | Year-over-year growth % |
| source | text | USGS, ICSG |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (year, country)

---

## Table 14: `ai_market_signals`

AI-generated market intelligence from your Anthropic API signal engine.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| signal_date | date | Date of the signal |
| exchange | text | 'COMEX', 'LME', 'SHFE', 'OVERALL' |
| signal | text | 'strong_bull', 'bull', 'neutral', 'bear', 'strong_bear' |
| headline | text | One-line summary |
| details | text | Longer analysis paragraph |
| price_referenced | numeric | Price at time of signal |
| sources_used | text[] | Array of URLs searched |
| model_used | text | 'claude-sonnet-4-20250514' |
| created_at | timestamptz | Auto: now() |

---

## Table 15: `macro_indicators`

Macro backdrop data points. Mix of automated (FRED) and manual entry.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Effective date |
| indicator | text | 'DXY', 'US_10Y_REAL', 'CHINA_CREDIT_IMPULSE', 'CHINA_PMI_CONSTRUCTION', 'CHINA_GRID_INVEST_YOY', 'CHINA_EV_SALES_MOM' |
| value | numeric | The value |
| unit | text | 'index', '%', 'CNY bn', etc. |
| source | text | FRED, NBS, CAAM, etc. |
| notes | text | Any context |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, indicator)

---

## Table 16: `cot_positioning`

CFTC Commitments of Traders data. Updated weekly.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| report_date | date | CFTC report date (Tuesday) |
| net_speculative | integer | Net managed money position (contracts) |
| long_speculative | integer | Gross longs |
| short_speculative | integer | Gross shorts |
| net_commercial | integer | Net commercial/hedger position |
| open_interest | integer | Total open interest |
| source | text | 'CFTC' |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (report_date)

---

## Table 17: `equity_tickers`

Copper equity prices for the dashboard ticker bar. Updated daily.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid (PK) | Auto-generated |
| data_date | date | Trading day |
| ticker | text | COPX, FCX, SCCO, TECK, BHP, RIO, GLEN, etc. |
| price | numeric | Closing price |
| currency | text | 'USD', 'CAD', 'GBP' |
| change_pct | numeric | Daily % change |
| exchange | text | 'NYSE', 'TSX', 'LSE' |
| exchange_url | text | Link to quote page |
| created_at | timestamptz | Auto: now() |

**Unique constraint:** (data_date, ticker)

---

## Derived Views (created as Supabase views, not tables)

These replace the calculated columns from your Google Sheet Dashboard tab.

### `v_dashboard_summary`

```sql
SELECT
  d.data_date,
  MAX(CASE WHEN d.exchange = 'COMEX' THEN d.total_mt END) AS comex_total,
  MAX(CASE WHEN d.exchange = 'COMEX' THEN d.change_mt END) AS comex_change,
  MAX(CASE WHEN d.exchange = 'LME' THEN d.total_mt END) AS lme_total,
  MAX(CASE WHEN d.exchange = 'LME' THEN d.change_mt END) AS lme_change,
  MAX(CASE WHEN d.exchange = 'SHFE' THEN d.total_mt END) AS shfe_total,
  MAX(CASE WHEN d.exchange = 'SHFE' THEN d.change_mt END) AS shfe_change,
  COALESCE(MAX(CASE WHEN d.exchange='COMEX' THEN d.total_mt END),0)
    + COALESCE(MAX(CASE WHEN d.exchange='LME' THEN d.total_mt END),0)
    + COALESCE(MAX(CASE WHEN d.exchange='SHFE' THEN d.total_mt END),0)
    AS combined_total,
  l.cancelled_warrants_mt AS lme_cancelled,
  l.cancelled_pct AS lme_cancelled_pct,
  cr.registered_total AS comex_registered,
  cr.eligible_total AS comex_eligible
FROM exchange_inventory_daily d
LEFT JOIN lme_detail_daily l ON d.data_date = l.data_date
LEFT JOIN (
  SELECT data_date,
    SUM(registered_mt) AS registered_total,
    SUM(eligible_mt) AS eligible_total
  FROM comex_warehouse_daily
  GROUP BY data_date
) cr ON d.data_date = cr.data_date
GROUP BY d.data_date, l.cancelled_warrants_mt, l.cancelled_pct,
         cr.registered_total, cr.eligible_total
ORDER BY d.data_date DESC;
```

### `v_active_disruptions`

```sql
SELECT *, 
  CASE 
    WHEN severity = 'critical' THEN 1
    WHEN severity = 'significant' THEN 2
    WHEN severity = 'moderate' THEN 3
    ELSE 4
  END AS severity_rank
FROM disruptions
WHERE status IN ('active', 'recovering')
ORDER BY kt_at_risk DESC;
```

### `v_signal_thresholds`

```sql
SELECT
  data_date,
  combined_total,
  lme_cancelled_pct,
  CASE WHEN combined_total < 150000 THEN 'very_tight'
       WHEN combined_total < 200000 THEN 'tight'
       ELSE 'normal' END AS inventory_signal,
  CASE WHEN lme_cancelled_pct > 30 THEN 'physical_tightness'
       ELSE 'normal' END AS lme_signal
FROM v_dashboard_summary
ORDER BY data_date DESC
LIMIT 1;
```

---

## Data Flow Summary

```
AUTOMATED (GitHub Actions, daily):
  comex_inventory.py  → exchange_inventory_daily + comex_warehouse_daily
  lme_inventory.py    → exchange_inventory_daily + lme_detail_daily
  shfe_inventory.py   → exchange_inventory_daily + shfe_region_daily
  barchart_prices.py  → futures_prices_daily + forward_curve
  cot_feed.py         → cot_positioning
  equity_feed.py      → equity_tickers

MANUAL VIA CLAUDE CODE (as needed):
  "Add disruption..."    → disruptions
  "Update TC/RC..."      → tcrc_rates
  "Update mine status.." → mines
  "Add premium data..."  → physical_premiums
  "ICSG update..."       → supply_demand_balance, country_production
  "NBS data..."          → macro_indicators

AI-GENERATED (on demand):
  Cu Daily Brief engine  → ai_market_signals

REACT DASHBOARD READS FROM:
  All tables + derived views via Supabase JS client
```

---

## Migration Path from Google Sheets

1. Create all tables in Supabase
2. Export existing Google Sheet data as CSV
3. Import CSV into the relevant Supabase tables
4. Update Python feed scripts: replace gspread calls with supabase-py calls
5. Update React app: replace static data hooks with Supabase JS queries
6. Keep Google Sheet as read-only backup for 2 weeks, then decommission
