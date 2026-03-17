// All prices in USD/mt unless noted
// NOTE: Static placeholder only — Barchart API symbols pending confirmation

// LME forward curve: Spot through M+15
export const staticForwardCurve = [
  { tenor: 'Cash',  price: 9412 },
  { tenor: 'M+1',   price: 9428 },
  { tenor: 'M+2',   price: 9445 },
  { tenor: 'M+3',   price: 9462 },
  { tenor: 'M+4',   price: 9478 },
  { tenor: 'M+5',   price: 9494 },
  { tenor: 'M+6',   price: 9510 },
  { tenor: 'M+7',   price: 9524 },
  { tenor: 'M+8',   price: 9536 },
  { tenor: 'M+9',   price: 9548 },
  { tenor: 'M+10',  price: 9560 },
  { tenor: 'M+11',  price: 9570 },
  { tenor: 'M+12',  price: 9580 },
  { tenor: 'M+13',  price: 9586 },
  { tenor: 'M+14',  price: 9590 },
  { tenor: 'M+15',  price: 9592 },
]

// Key benchmark prices
export const staticPriceBenchmarks = {
  lme3Month:       9462,   // USD/mt
  comexFront:      4.285,  // USD/lb (COMEX quotes in cents/lb)
  shfeFront:       76840,  // CNY/mt
  lmeComexSpread:  +18.2,  // USD/mt (LME premium / discount vs COMEX equivalent)
  asOf: '2026-03-14',
}

// Physical premiums by region (USD/mt over LME)
export const staticPhysicalPremiums = [
  { region: 'US Midwest',     premium: 148, weekAgo: 142, change: +6,  trend: 'up' },
  { region: 'US Southeast',   premium: 135, weekAgo: 130, change: +5,  trend: 'up' },
  { region: 'Rotterdam CIF',  premium:  92, weekAgo:  95, change: -3,  trend: 'down' },
  { region: 'Hamburg CIF',    premium:  88, weekAgo:  90, change: -2,  trend: 'down' },
  { region: 'Shanghai BonZo', premium:  62, weekAgo:  58, change: +4,  trend: 'up' },
  { region: 'Tokyo CIF',      premium: 118, weekAgo: 115, change: +3,  trend: 'up' },
  { region: 'Korea CIF',      premium: 105, weekAgo: 102, change: +3,  trend: 'up' },
]

// TC/RC 18-month history (USD/t for TC, cents/lb for RC)
export const staticTcrcHistory = [
  { month: '2024-10', tc: 28.0, rc: 2.80 },
  { month: '2024-11', tc: 26.5, rc: 2.65 },
  { month: '2024-12', tc: 24.2, rc: 2.42 },
  { month: '2025-01', tc: 22.8, rc: 2.28 },
  { month: '2025-02', tc: 21.5, rc: 2.15 },
  { month: '2025-03', tc: 20.4, rc: 2.04 },
  { month: '2025-04', tc: 19.8, rc: 1.98 },
  { month: '2025-05', tc: 18.6, rc: 1.86 },
  { month: '2025-06', tc: 17.4, rc: 1.74 },
  { month: '2025-07', tc: 16.2, rc: 1.62 },
  { month: '2025-08', tc: 15.8, rc: 1.58 },
  { month: '2025-09', tc: 15.2, rc: 1.52 },
  { month: '2025-10', tc: 14.8, rc: 1.48 },
  { month: '2025-11', tc: 14.4, rc: 1.44 },
  { month: '2025-12', tc: 14.0, rc: 1.40 },
  { month: '2026-01', tc: 13.6, rc: 1.36 },
  { month: '2026-02', tc: 13.2, rc: 1.32 },
  { month: '2026-03', tc: 12.8, rc: 1.28 },
]

// TC/RC alert threshold from CLAUDE.md
export const TC_BEAR_THRESHOLD = 20  // USD/t — below this = bullish (concentrate shortage)
