# Copper Intelligence Hub

## What This Project Is
A copper market intelligence system that collects exchange inventory data
(COMEX, LME, SHFE), futures prices, and market signals to support copper
trading decisions. Built by a trader who understands the physical copper
market deeply but is new to coding — Claude Code is doing the development.

## Current State
The data collection layer is BUILT and RUNNING:
- GitHub Actions runs 3 Python feed scripts every weeknight at 10:30 PM ET
- Data flows into a Google Sheet called "3-exchange-inventory-tracker"
- COMEX, LME, and SHFE inventory data is accumulating daily

What's NOT built yet:
- A web dashboard to visualize the data
- Live price feeds (Barchart API — symbols pending confirmation)
- AI-powered market intelligence signals
- Weekly brief automation

## Architecture

### Feed Scripts (Python, running on GitHub Actions)
- feeds/comex_inventory.py — Scrapes CME XLS file, parses 7 warehouses
  (Baltimore, Detroit, El Paso, New Orleans, Owensboro, Salt Lake City, Tucson),
  extracts registered vs eligible tonnages, converts short tons → metric tons
- feeds/lme_inventory.py — Scrapes Westmetall for LME copper stocks
- feeds/shfe_inventory.py — Parses SHFE HTML tables for regional breakdowns
  (Shanghai, Guangdong, Jiangsu, Zhejiang)
- feeds/dashboard.py — Shared module that writes consolidated row to Dashboard tab

### Google Sheet Schema
Sheet name: "3-exchange-inventory-tracker"

**Dashboard tab** — one row per trading day, keyed on Data Date (col A):
  A: Data Date
  B: COMEX Total (mt)
  C: COMEX Change (mt)
  D: COMEX Registered (mt)
  E: COMEX Eligible (mt)
  F: COMEX Reg/Total %        ← calculated
  G: LME Total (mt)
  H: LME Change (mt)
  I: LME Cancelled Warrants   ← manual entry, no feed yet
  J: LME Cancelled %          ← calculated
  K: SHFE Total (mt)
  L: SHFE Change (mt)
  M: Combined Total (mt)      ← calculated
  N: Combined Change (mt)     ← calculated (only when all 3 present)
  O: WoW Change (mt)          ← same day last week comparison

**COMEX tab** — per-warehouse registered/eligible with prev/today pairs
**LME tab** — daily total + change
**SHFE tab** — regional breakdowns with subtotals

### GitHub Actions Workflow
- .github/workflows/copper_feeds.yml
- Schedule: Mon-Fri, 02:30 UTC (10:30 PM ET)
- Each feed is a separate job (one failure doesn't block others)
- Secrets: GOOGLE_SERVICE_ACCOUNT_JSON, LME_USERNAME, LME_PASSWORD

## Key Trading Signal Thresholds
- LME cancelled warrants > 30% = imminent physical tightness
- Combined 3-exchange stocks < 200 kt = historically tight
- Combined 3-exchange stocks < 150 kt = very tight
- TC/RC spot < $20/t = concentrate shortage = bullish refining bottleneck
- China grid investment YoY > +15% = strong demand tailwind
- Mine disruptions > 100 kt annualized = bullish supply shock

## Design System (for the web dashboard)
- Dark terminal aesthetic, NOT generic light mode
- Copper-toned palette:
  - Primary copper: #C87941
  - Copper light: #E8A76C
  - Copper dim: #8B5A2B
  - Background primary: #080D14
  - Background card: #0C1220
  - Border: #1A2332
  - Text primary: #E8E4DC
  - Text secondary: #9CA3AF
- Fonts:
  - JetBrains Mono — all data, numbers, tables
  - Syne — headings
  - Inter — body text
- Signal colors:
  - Bull/up: #22C55E (green)
  - Bear/down: #EF4444 (red)
  - Neutral: #F59E0B (amber)

## Reference Design
The reference/ folder contains a Perplexity-built copper dashboard app.
It has good UI structure (6 pages with sidebar nav) BUT its data is mostly
fake — especially the COMEX warehouse data (wrong locations, wrong magnitudes).
Use it as a VISUAL reference only, not a data reference.

The 6 pages are: Dashboard, Inventories, Pricing, Global Mines,
Supply & Demand, Disruptions.

## Tech Preferences
- React with Vite (not Next.js — keep it simple)
- Tailwind CSS for styling
- Recharts for charts
- Google Sheets API for reading live data
- Anthropic API for AI market intelligence (model: claude-sonnet-4-20250514)

## Important Technical Notes
- COMEX feeds binary .xls files → requires xlrd, NOT openpyxl
- COMEX data is in short tons, multiply by 0.907185 for metric tons
- SHFE HTML uses rowspan for regions + specific CSS classes for subtotals
- Dashboard write pattern: "write when any exchange reports" — each feed
  finds/creates the date row and updates only its columns
- The Google service account email has Editor access to the sheet
