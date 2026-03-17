# Copper Intelligence Hub — Claude.ai Project Context Document

> Upload this file to your Claude.ai Project so Claude has persistent context every session.

---

## Purpose

This is a systematic copper market intelligence hub. The goal is to synthesize supply, demand, and macro signals into a weekly brief for decision-making.

## Analytical Framework

**Copper price is primarily driven by:**
1. Exchange inventories (LME + COMEX + SHFE) — leading indicator
2. Mine supply disruptions — unexpected kt shortfalls move spot
3. TC/RC spot rates — concentrate tightness signal
4. Chinese demand proxy — grid investment + EV sales + construction PMI
5. Macro backdrop — USD, real rates, China credit impulse

**Bull signals:**
- Multi-exchange inventory drawdown (especially cancelled warrants at LME rising)
- TC/RC spot < annual benchmark (concentrate tightness)
- China grid capex surging + EV sales accelerating
- Mine disruptions > 100kt annualized

**Bear signals:**
- Inventory builds across all 3 exchanges simultaneously
- TC/RC spot rising (concentrate surplus)
- Chinese property sector stress + weak construction PMI
- Scrap premiums falling (weak fabricator demand)

## Key Mines Watchlist

| Mine | Owner | Country | Output (kt/yr) | Key Risk |
|------|-------|---------|----------------|----------|
| Escondida | BHP/Rio/JECO | Chile | ~1,000 | Labor (strikes every ~3 yrs) |
| Grasberg | Freeport/PTFI | Indonesia | ~800 | Regulatory, ore grade |
| Collahuasi | Anglo/Glencore | Chile | ~600 | Water, labor |
| Kamoa-Kakula | Ivanhoe/Zijin | DRC | ~500 (growing) | Logistics, power |
| Cerro Verde | Freeport | Peru | ~470 | Water, community |
| Las Bambas | MMG | Peru | ~400 | Road blockades |
| Antamina | BHP/Glencore/Teck | Peru | ~430 | Community, water |
| Chuquicamata | Codelco | Chile | ~330 | Underground transition |
| El Teniente | Codelco | Chile | ~440 | Underground, ore grade |

## Data Sources & Cadence

| Frequency | Source | What to Pull |
|-----------|--------|--------------|
| Daily | LME | Warrants + cancelled warrants |
| Daily | CME Group | COMEX eligible vs registered |
| Weekly | SHFE | Weekly inventory (Friday) |
| Weekly | Fastmarkets/MetalBulletin | TC/RC spot |
| Monthly | GACC (China Customs) | Cu imports/exports |
| Monthly | SMM | Chinese smelter utilization (partial free) |
| Monthly | NBS China | PMI construction sub-index |
| Monthly | CAAM | China EV sales |
| Quarterly | USGS | Global mine production |
| Bi-annual | ICSG | Global supply/demand balance |
| Annual | IEA | Critical minerals demand forecast |
| Annual | Cochilco | Chile production forecasts |

## Weekly Brief Structure

1. **Macro** — USD, real rates, China credit impulse, key events
2. **Supply Signals** — inventory levels (3 exchanges + WoW delta), mine disruptions (kt impact), TC/RC spot vs benchmark
3. **Demand** — China grid investment, EV sales (MoM), construction PMI, bonded warehouse stocks
4. **Inventory** — Combined exchange stocks chart, cancelled warrant ratio (LME)
5. **Technicals** — LME 3M price, key levels, positioning (COT if available)
6. **Positioning** — COT net speculative position, any notable flows

## Synthesis Heuristics

- Cancelled warrant ratio (LME) > 30% = imminent physical tightness
- SHFE pre-Golden Week build > 100kt = demand confidence
- TC/RC spot < $20/t = concentrate shortage = bullish refining bottleneck
- 3-exchange combined stocks < 200kt = historically tight; < 150kt = very tight
- China grid investment YoY > +15% = strong demand tailwind

## Junior Miner Report Integration

[ADD YOUR FIELDS HERE]
- Mine name, owner, country, stage (exploration/development/production)
- Annual output forecast 2025/2026 (kt)
- Risk flags (permitting, financing, labor, geology)
- Last updated date

---
*Context doc version: 1.0 — Update quarterly or when framework changes*
