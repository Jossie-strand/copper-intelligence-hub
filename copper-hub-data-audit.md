# Copper Intelligence Hub — Data Model Audit & Build Plan

**Purpose:** Map every data point in the Perplexity reference app, assess accuracy, identify gaps against your analytical framework, and define the data sourcing strategy for the production build.

---

## 1. Dashboard KPI Cards

| Card | Perplexity Value | Source (Perplexity) | Accuracy Check | Your Feed Status |
|------|-----------------|--------------------|--------------------|-----------------|
| COMEX Spot | $5.78/lb, -0.82% | Hardcoded | Plausible for early 2026 | Barchart API (HG*1) — pending symbol confirm |
| LME 3-Month | $12,750/t, -1.2% | Hardcoded | Plausible | Barchart API (LCQ*1) — pending |
| Exchange Inventory | 1.2 Mt, +8.5% | Hardcoded | Suspicious — see inventory section | Your 3-exchange feeds are live |
| Market Balance | 150 Kt deficit | Hardcoded | Reasonable for 2026F | No feed — ICSG bi-annual source |
| TC/RC Benchmark | $0/t | Hardcoded | Correct — 2026 benchmark collapsed to $0 | No feed — Fastmarkets paywall |
| Days of Demand | ~4.5 days | Hardcoded | Plausible if ~1.2Mt / ~97kt/day | Calculated from inventory ÷ consumption |
| Mine Production | 23 Mt | Hardcoded | Reasonable 2026F | No feed — USGS quarterly |

### Gaps vs. Your Framework
- **Missing cards:** USD index, real rates, China credit impulse, COT net speculative position
- **Missing signal:** No bull/bear classification on any card (your Cu Daily Exchange Brief has this)
- **No live data:** Every value is static — your GitHub Actions feeds solve this for inventories

---

## 2. Inventories Page

### 2a. Warehouse-Level Data

| Exchange | Perplexity Locations | Perplexity Values | Accuracy Issues |
|----------|---------------------|-------------------|-----------------|
| LME | Rotterdam (85K), Singapore (52K), Busan (48K), New Orleans (35K), Johor (28K), Antwerp (22K), Hamburg (14.3K) | Total ~284K mt | Plausible LME total range |
| SHFE | Shanghai (125K), Guangdong (74K), Jiangsu (50K) | Total ~249K mt | Plausible |
| COMEX | New York (185K), Chicago (175K), Tucson (175K) | Total ~535K mt | **WRONG** — COMEX copper stocks are ~15-40K st total, not 535K. These look fabricated. Real COMEX warehouses are: Baltimore, Detroit, El Paso, New Orleans, Owensboro, Salt Lake City, Tucson |

**Critical issue:** The Perplexity app's COMEX warehouse data is completely wrong — both the locations (New York, Chicago don't exist for COMEX copper) and the magnitudes (off by 10x+). Your existing `comex_inventory.py` feed has the correct 7-warehouse schema.

### 2b. Inventory Time Series

| Data | Perplexity | Reality |
|------|-----------|---------|
| LME 12-month | [142K, 150K, 165K, 178K, 195K, 210K, 225K, 240K, 252K, 265K, 275K, 284K] × 1000 | Synthetic upward ramp — not real data |
| SHFE 12-month | [132K, 138K, 148K, 160K, 172K, 185K, 195K, 210K, 222K, 235K, 243K, 249K] × 1000 | Synthetic upward ramp |
| COMEX 12-month | [305K, 320K, 340K, 365K, 390K, 415K, 440K, 465K, 490K, 510K, 525K, 535K] × 1000 | Fabricated — COMEX never holds 300K+ mt |

**Your feeds solve this:** Your Dashboard tab already accumulates daily rows with real data from all 3 exchanges.

### 2c. Missing from Inventories

| Missing Data Point | Importance | Source |
|-------------------|------------|--------|
| COMEX Registered vs Eligible split | Critical — reg/total % is a deliverability signal | Your feed already captures this |
| LME Cancelled Warrants | Critical — >30% = physical tightness per your framework | Manual entry column exists in your Dashboard; no automated feed yet |
| LME Cancelled Warrant % | Critical | Calculated: cancelled / total |
| Combined 3-exchange total | High | Your Dashboard calculates this |
| WoW change | High | Your Dashboard calculates this |
| Bonded warehouse stocks (China) | Medium — your framework lists it under Demand | No source identified |

---

## 3. Pricing Page

### 3a. LME Forward Curve

| Tenor | Perplexity Price | Assessment |
|-------|-----------------|------------|
| Spot | $12,750 | Plausible |
| M+1 to M+15 | $12,785 → $13,210 | Contango shape is reasonable |

**Source issue:** Forward curves require a live feed. Barchart may provide this via different contract months, or LME's own data portal.

### 3b. Physical Premiums

| Region | Perplexity Value | Assessment |
|--------|-----------------|------------|
| Yangshan (China) | $48/t, trending up | Plausible range |
| US Midwest | $0.065/lb, flat | Plausible |
| Europe (Rotterdam) | $55/t, down | Plausible |
| Japan (CIF) | $330/t, up | **Suspicious** — Japan CIF premiums are typically $50-120/t, not $330 |
| South Korea | $85/t, flat | Plausible |

**Source:** Fastmarkets / Metal Bulletin (paywalled). Free alternatives: SMM for China premiums, some broker reports.

### 3c. TC/RC History

| Data | Perplexity Values |
|------|-------------------|
| TC/RC 18-month series | [80, 72, 60, 50, 40, 30, 21.25, 15, 8, 0, -10, -25, -40, -55, -67.6, -60, -50, -44] |

**Assessment:** The trajectory from $80/t down to negative values then partial recovery is directionally correct — TC/RC collapsed through 2025 into 2026. The $21.25 aligns with the 2025 benchmark. Negative TC/RC values reflect the spot market reality where smelters are paying miners. Exact numbers need verification.

**Your framework threshold:** TC/RC spot < $20/t = concentrate shortage = bullish refining bottleneck. This is clearly triggered.

---

## 4. Global Mines Page

### 4a. Top 20 Mines

The Perplexity app includes 20 mines with H1 production data, lat/lng, operator, and status. Cross-checking against your watchlist:

| Mine | Your Watchlist | Perplexity | Status Match |
|------|---------------|------------|--------------|
| Escondida | ~1,000 kt/yr | 680.5K (H1) → ~1,361K annualized | Plausible — H1 can be front-loaded |
| Grasberg | ~800 kt/yr | 297.1K (H1) → ~594K ann. | Low — reflects disruption |
| Kamoa-Kakula | ~500 kt (growing) | 210K (H1) → ~420K ann. | Reasonable for ramp-up |
| Collahuasi | ~600 kt/yr | 189.6K (H1) → ~379K ann. | Low — reflects "Constrained" status |
| Cerro Verde | ~470 kt/yr | 195.7K (H1) → ~391K ann. | Reasonable |
| Las Bambas | ~400 kt/yr | 210.6K (H1) → ~421K ann. | Reasonable |
| Antamina | ~430 kt/yr | 154.4K (H1) → ~309K ann. | Low |
| Chuquicamata | ~330 kt/yr | 170K (H1) → ~340K ann. | Reasonable |
| El Teniente | ~440 kt/yr | 175K (H1) → ~350K ann. | Low — reflects "Disrupted" |

**Additional mines not in your watchlist but in Perplexity:** QB2, Radomiro Tomic, Oyu Tolgoi, Sentinel, Spence, Los Pelambres, Toquepala, Olympic Dam, Kansanshi, Los Bronces, Sierra Gorda

**Assessment:** Production figures are in the right ballpark but use "H1 Production" which makes comparison to annual figures tricky. The status flags (Operating, Disrupted, Recovering, Constrained) are a good addition to your mine tracker schema.

### 4b. Missing Mine Data

| Missing Field | Your Framework Needs It | Source |
|--------------|------------------------|--------|
| Annual output forecast 2025/2026 | Yes — your mine tracker schema | Cochilco, ICSG, company reports |
| Key risk factors | Yes — your watchlist has these | Manual / news monitoring |
| Stage (exploration/development/production) | Yes — junior miner integration | Manual |
| Last disruption event | Yes — ties to disruptions page | News / your disruption log |

---

## 5. Supply & Demand Page

### 5a. Global Balance

| Year | Supply (Kt) | Demand (Kt) | Balance (Kt) |
|------|------------|------------|--------------|
| 2022 | 21,800 | 22,100 | -300 |
| 2023 | 22,200 | 22,400 | -200 |
| 2024 | 22,800 | 22,900 | -100 |
| 2025 | 23,000 | 22,970 | +30 |
| 2026F | 23,530 | 23,680 | -150 |
| 2027F | 24,100 | 24,400 | -300 |

**Assessment:** Numbers are in the right range for ICSG data. The 2025 near-balance and widening 2026-27 deficit narrative aligns with consensus. But these are static — ICSG updates bi-annually.

### 5b. Country Production Shares

Chile 23%, DRC 14%, Peru 12%, China 8%, Russia 6%, US 5%, Zambia 4%, Australia 3%, Indonesia 3%, Kazakhstan 3%

**Assessment:** Broadly correct. DRC at 14% reflects Kamoa-Kakula ramp. Source: USGS/ICSG.

### 5c. Demand Sectors

Electrical & Electronics 31%, Construction 28%, Consumer Products 16%, Transport 13%, Industrial Machinery 12%

**Assessment:** Standard Copper Alliance breakdown. Transport share may be understated given EV growth — your framework tracks this separately.

### 5d. Missing from Supply & Demand

| Missing | Importance | Source |
|---------|-----------|--------|
| China grid investment YoY | Critical — your bull signal threshold is >+15% | NBS China monthly |
| China EV sales MoM | Critical — demand proxy | CAAM monthly |
| Construction PMI sub-index | High | NBS China monthly |
| Chinese smelter utilization | High | SMM (partial free) |
| Scrap premiums | Medium — bear signal when falling | No free source identified |
| China credit impulse | High — macro backdrop | Bloomberg / calculated |

---

## 6. Disruptions Page

### 6a. Active Events

| Event | Kt at Risk | Severity | Assessment |
|-------|-----------|----------|------------|
| Grasberg landslide | 300 | Critical | Real event (Sept 2025) — numbers plausible |
| Cobre Panama closure | 300 | Critical | Real — permanent closure since Nov 2023 |
| CSPT Smelter Cuts | 1,200 | Significant | Real — CSPT announced capacity cuts for 2026 |
| El Teniente | 100 | Significant | Real — ongoing Codelco issues |
| Kamoa-Kakula | 80 | Significant | Plausible — seismic/flooding events |
| Collahuasi | 50 | Moderate | Real — grade constraints |

**Assessment:** Disruption data is the most current and accurate part of the Perplexity app. Total ~2,030 Kt at risk — well above your 100 Kt annualized bull signal threshold. However, CSPT is a smelter cut (refined output), not mine disruption — these are different supply-chain stages and should be categorized differently.

### 6b. Missing Disruption Fields

| Missing | Your Framework |
|---------|---------------|
| Disruption type (mine vs smelter vs logistics) | Needed to separate supply-chain stages |
| Expected resolution date | Needed for kt-impact duration calculation |
| Cumulative kt lost to date | Needed for supply impact assessment |
| Source/last updated date | Audit trail |

---

## 7. Completely Missing Pages/Modules

These are in your analytical framework but have no representation in the Perplexity app:

| Module | Framework Importance | Data Source | Feed Complexity |
|--------|---------------------|-------------|-----------------|
| **Macro Dashboard** | High — USD, real rates, China credit impulse | FRED API (free), Bloomberg | Medium |
| **COT Positioning** | Medium — speculative positioning | CFTC weekly report (free) | Low — weekly CSV download |
| **Weekly Brief Generator** | Core deliverable | AI synthesis of all feeds | High — your existing Cu Daily Exchange Brief is the prototype |
| **Signal Dashboard** | Core — bull/bear classification | Calculated from all feeds using your thresholds | Medium — logic exists in your React component |
| **China Trade Data** | Medium | GACC (free but awkward) | Medium |

---

## 8. Data Sourcing Strategy for Production Build

### Tier 1 — You Already Have Feeds (wire directly)

| Data | Feed Script | Status |
|------|------------|--------|
| COMEX inventory (7 warehouses, reg/elig) | `comex_inventory.py` | Running on GitHub Actions |
| LME inventory (total + change) | `lme_inventory.py` | Running |
| SHFE inventory (regions) | `shfe_inventory.py` | Running |
| Dashboard rollup (combined, WoW, derived) | `dashboard.py` | Running |

### Tier 2 — Ready to Build with Existing APIs

| Data | API/Source | Effort |
|------|-----------|--------|
| COMEX/LME/SHFE futures prices | Barchart OnDemand (you have premium) | Low — once symbols confirmed |
| COT positioning | CFTC Commitments of Traders (free CSV) | Low |
| USD index / real rates | FRED API (free) | Low |

### Tier 3 — Needs Web Scraping or AI Extraction

| Data | Source | Effort |
|------|--------|--------|
| LME cancelled warrants | LME website (login required) | Medium — you have LME creds |
| Mine disruptions | News + company releases | Medium — AI-powered (your Cu Brief approach) |
| TC/RC spot rates | Fastmarkets snippets / SMM | Hard — paywalled |
| Physical premiums | SMM / broker reports | Hard |

### Tier 4 — Manual or Semi-Annual Updates

| Data | Source | Cadence |
|------|--------|---------|
| Global S&D balance | ICSG | Bi-annual |
| Country production shares | USGS/ICSG | Quarterly |
| Mine production database | Cochilco + company reports | Quarterly |
| Demand sector breakdown | Copper Alliance / IEA | Annual |
| China grid/EV/PMI | NBS/CAAM | Monthly — manual download |

---

## 9. Recommended Build Order for Claude Code

**Phase 1 — Wire live inventory data to the UI**
- Connect Google Sheets API to read your Dashboard tab
- Replace Perplexity's fake COMEX warehouses with your real 7-warehouse data
- Display real LME/SHFE/COMEX totals and daily changes
- Apply your threshold signals (combined <200kt = tight, <150kt = very tight)

**Phase 2 — Add live pricing**
- Confirm Barchart symbols with Christopher Brisch
- Build price feed script → Google Sheets
- Wire COMEX spot, LME 3-month, and forward curve to the UI
- Build the basis spread calculations (COMEX-LME arb)

**Phase 3 — AI market intelligence layer**
- Port your Cu Daily Exchange Brief signal engine into the app
- Web search → structured extraction → bull/bear classification
- Cover all 4 panels: COMEX, LME, SHFE, Market Signal

**Phase 4 — Expand data model**
- Add COT positioning (CFTC feed)
- Add macro indicators (FRED API: USD, real rates)
- Build disruption tracker with AI news monitoring
- Add China demand proxies as data becomes available

**Phase 5 — Weekly brief automation**
- Synthesize all feeds into your weekly brief structure
- Generate the 6-section brief document automatically
- Apply your synthesis heuristics for signal classification
