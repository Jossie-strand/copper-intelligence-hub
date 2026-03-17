// Supply & Demand balance data
// All values in kt (thousand metric tons) unless noted

// Annual S&D balance 2022–2027F
export const staticSdBalance = [
  { year: '2022', supply: 21800, demand: 22100, balance: -300 },
  { year: '2023', supply: 22400, demand: 22200, balance: +200 },
  { year: '2024', supply: 22900, demand: 23100, balance: -200 },
  { year: '2025', supply: 23200, demand: 23600, balance: -400 },
  { year: '2026F', supply: 23800, demand: 24200, balance: -400 },
  { year: '2027F', supply: 24400, demand: 24900, balance: -500 },
]

// Mine production by country (top 10, % of global total)
export const staticCountryProduction = [
  { country: 'Chile',     pct: 26.5, ktAnnual: 5600 },
  { country: 'Peru',      pct: 10.8, ktAnnual: 2280 },
  { country: 'DRC',       pct:  9.4, ktAnnual: 1990 },
  { country: 'China',     pct:  8.2, ktAnnual: 1730 },
  { country: 'USA',       pct:  6.1, ktAnnual: 1290 },
  { country: 'Indonesia', pct:  5.8, ktAnnual: 1225 },
  { country: 'Australia', pct:  4.9, ktAnnual: 1040 },
  { country: 'Russia',    pct:  4.2, ktAnnual:  890 },
  { country: 'Zambia',    pct:  3.8, ktAnnual:  800 },
  { country: 'Other',     pct: 20.3, ktAnnual: 4290 },
]

// Demand by end-use sector
export const staticDemandSectors = [
  { sector: 'Electrical & Electronics', pct: 31, color: '#C87941' },
  { sector: 'Construction',             pct: 28, color: '#E8A76C' },
  { sector: 'Industrial Machinery',     pct: 17, color: '#8B5A2B' },
  { sector: 'Transportation / EV',      pct: 14, color: '#22C55E' },
  { sector: 'Other',                    pct: 10, color: '#9CA3AF' },
]

// China demand proxy indicators (live data pending NBS integration)
export const staticChinaIndicators = {
  gridInvestmentYoY: null,        // awaiting NBS data
  evSalesMoM: null,               // awaiting NBS data
  smelterUtilizationPct: null,    // awaiting CNIA data
  note: 'China macro indicators will populate when NBS data feed is connected.',
}

// Current year market narrative
export const sdNarrative = {
  year: 2026,
  deficitKt: 400,
  daysOfConsumption: 6.1,
  trend: 'widening',
  summary: 'Global copper market in structural deficit. Energy transition demand (EV, grid build-out) absorbing supply growth from DRC ramp-up and Oyu Tolgoi underground.',
}
