# Copper Intelligence Hub — Google Drive Folder Architecture

## Recommended Structure

```
📁 Copper Intelligence Hub/
│
├── 📁 /Raw Feeds/
│   ├── 📁 /Futures & Prices/        ← Daily futures reports (structured)
│   ├── 📁 /Exchange Inventories/    ← LME, COMEX, SHFE CSV downloads
│   ├── 📁 /Mine & Production/       ← ICSG, Cochilco, USGS downloads
│   ├── 📁 /TC-RC Data/              ← Fastmarkets/Metal Bulletin spot & benchmark
│   ├── 📁 /Trade Data/              ← GACC Chinese customs exports/imports
│   └── 📁 /Demand Signals/          ← IEA, Copper Alliance, NBS PMI, EV data
│
├── 📁 /Weekly Briefs/
│   ├── 📁 /2025/
│   │   ├── 2025-W01 Copper Brief.docx
│   │   └── ...
│   └── 📁 /2026/
│       └── ...
│
├── 📁 /Mine Tracker/
│   ├── mine-production-database.xlsx
│   ├── disruption-log.xlsx
│   └── 📁 /Source Reports/          ← Original PDFs from ICSG, Cochilco, USGS
│
├── 📁 /Macro & Demand/
│   ├── demand-proxy-tracker.xlsx
│   └── 📁 /Source Reports/          ← IEA, BNEF/CRU public summaries
│
└── 📁 /Hub Files/                   ← Master tracking files
    ├── 3-exchange-inventory-tracker.xlsx
    ├── tcrc-tracker.xlsx
    ├── global-smelters.xlsx
    ├── china-trade-tracker.xlsx
    ├── port-disruption-log.xlsx
    └── source-log.docx
```

## Setup Steps (Manual — You Do These)

1. **Create the folder hierarchy above in Google Drive**
2. **Create a Claude.ai Project** named "Copper Intelligence Hub"
   - Upload `01-claude-context-doc.md` (in this folder) as context
   - This gives Claude persistent knowledge of your framework every session
3. **Bookmark key free sources:**
   - LME inventories: https://www.lme.com/en/Market-Data/Reports-and-data/Monthly-reports
   - COMEX inventories: https://www.cmegroup.com/trading/metals/base/copper_quotes_settlements_futures.html
   - SHFE inventories: http://www.shfe.com.cn/en/
   - ICSG: https://icsg.org/
   - Cochilco: https://www.cochilco.cl/en/
   - USGS Minerals: https://www.usgs.gov/centers/national-minerals-information-center/copper
   - GACC Chinese Customs: http://www.customs.gov.cn/
   - Kpler (free tier): https://www.kpler.com/
   - IEA Critical Minerals: https://www.iea.org/topics/critical-minerals
   - Copper Alliance: https://copperalliance.org/

## File Naming Convention

- Weekly briefs: `YYYY-WXX Copper Brief.docx`
- Raw data downloads: `YYYY-MM-DD_[source]_[type].csv`
- Example: `2026-02-24_LME_inventory.csv`
