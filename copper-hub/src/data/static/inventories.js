// Mirrors COMEX tab (7 warehouses), LME tab, and SHFE tab schemas
// All weights in metric tons (mt)

// ---- Data generator (trading days Mon-Fri only) ----
function generateDailyRows(startDate, endDate, startVal, endVal, extraFn) {
  const rows = []
  const start = new Date(startDate)
  const end = new Date(endDate)
  const totalMs = end - start
  let d = new Date(start)
  while (d <= end) {
    const dow = d.getDay()
    if (dow !== 0 && dow !== 6) {
      const t = totalMs === 0 ? 1 : (d - start) / totalMs
      const val = Math.round(startVal + (endVal - startVal) * t)
      const dateStr = d.toISOString().slice(0, 10)
      rows.push(extraFn ? extraFn(dateStr, val, t) : { date: dateStr, total: val })
    }
    d.setDate(d.getDate() + 1)
  }
  return rows
}

// COMEX history — starts ~22000 mt, declines to ~18420 mt
// registered ~65%, eligible ~35%
export const staticComexHistory = generateDailyRows(
  '2024-03-15', '2026-03-14', 22000, 18420,
  (date, total) => {
    const registered = Math.round(total * 0.65)
    const eligible = total - registered
    return { date, total, registered, eligible }
  }
)

// LME history — starts ~290000 mt, rises/dips to 273500 mt
// cancelledWarrants ~33% throughout
export const staticLmeHistory = generateDailyRows(
  '2024-03-15', '2026-03-14', 290000, 273500,
  (date, total, t) => {
    // Add a mid-period bump: peaks around t=0.5 then comes back down
    const bump = Math.round(Math.sin(t * Math.PI) * 8000)
    const adjTotal = total + bump
    const cancelledWarrants = Math.round(adjTotal * 0.333)
    const cancelledPct = parseFloat(((cancelledWarrants / adjTotal) * 100).toFixed(1))
    return { date, total: adjTotal, cancelledWarrants, cancelledPct }
  }
)

// SHFE history — starts ~140000, seasonal (higher winter/spring), down to 119800
export const staticShfeHistory = generateDailyRows(
  '2024-03-15', '2026-03-14', 140000, 119800,
  (date, total, t) => {
    // Seasonal: higher in Q1, lower in Q3 (northern hemisphere spring demand pattern)
    const month = parseInt(date.slice(5, 7))
    const seasonalBump = Math.round(Math.cos(((month - 1) / 12) * 2 * Math.PI) * 8000)
    return { date, total: total + seasonalBump }
  }
)

// COMEX — 7 correct warehouses (NOT New York/Chicago from reference app)
export const staticComexData = {
  date: '2026-03-14',
  warehouses: [
    {
      name: 'Baltimore',
      registered: 2340, eligible: 1120, total: 3460, regPct: 67.6,
      history: [
        { week: '2026-03-14', registered: 2340, eligible: 1120, total: 3460 },
        { week: '2026-03-07', registered: 2310, eligible: 1140, total: 3450 },
        { week: '2026-02-28', registered: 2290, eligible: 1150, total: 3440 },
        { week: '2026-02-21', registered: 2270, eligible: 1160, total: 3430 },
        { week: '2026-02-14', registered: 2250, eligible: 1170, total: 3420 },
        { week: '2026-02-07', registered: 2230, eligible: 1180, total: 3410 },
        { week: '2026-01-31', registered: 2210, eligible: 1190, total: 3400 },
        { week: '2026-01-24', registered: 2190, eligible: 1200, total: 3390 },
      ],
    },
    {
      name: 'Detroit',
      registered: 1890, eligible: 870, total: 2760, regPct: 68.5,
      history: [
        { week: '2026-03-14', registered: 1890, eligible: 870, total: 2760 },
        { week: '2026-03-07', registered: 1870, eligible: 880, total: 2750 },
        { week: '2026-02-28', registered: 1855, eligible: 885, total: 2740 },
        { week: '2026-02-21', registered: 1840, eligible: 890, total: 2730 },
        { week: '2026-02-14', registered: 1825, eligible: 895, total: 2720 },
        { week: '2026-02-07', registered: 1810, eligible: 900, total: 2710 },
        { week: '2026-01-31', registered: 1795, eligible: 905, total: 2700 },
        { week: '2026-01-24', registered: 1780, eligible: 910, total: 2690 },
      ],
    },
    {
      name: 'El Paso',
      registered: 3210, eligible: 1540, total: 4750, regPct: 67.6,
      history: [
        { week: '2026-03-14', registered: 3210, eligible: 1540, total: 4750 },
        { week: '2026-03-07', registered: 3190, eligible: 1550, total: 4740 },
        { week: '2026-02-28', registered: 3170, eligible: 1560, total: 4730 },
        { week: '2026-02-21', registered: 3150, eligible: 1570, total: 4720 },
        { week: '2026-02-14', registered: 3130, eligible: 1580, total: 4710 },
        { week: '2026-02-07', registered: 3110, eligible: 1590, total: 4700 },
        { week: '2026-01-31', registered: 3090, eligible: 1600, total: 4690 },
        { week: '2026-01-24', registered: 3070, eligible: 1610, total: 4680 },
      ],
    },
    {
      name: 'New Orleans',
      registered: 2100, eligible: 980, total: 3080, regPct: 68.2,
      history: [
        { week: '2026-03-14', registered: 2100, eligible: 980, total: 3080 },
        { week: '2026-03-07', registered: 2085, eligible: 985, total: 3070 },
        { week: '2026-02-28', registered: 2070, eligible: 990, total: 3060 },
        { week: '2026-02-21', registered: 2055, eligible: 995, total: 3050 },
        { week: '2026-02-14', registered: 2040, eligible: 1000, total: 3040 },
        { week: '2026-02-07', registered: 2025, eligible: 1005, total: 3030 },
        { week: '2026-01-31', registered: 2010, eligible: 1010, total: 3020 },
        { week: '2026-01-24', registered: 1995, eligible: 1015, total: 3010 },
      ],
    },
    {
      name: 'Owensboro',
      registered: 1450, eligible: 720, total: 2170, regPct: 66.8,
      history: [
        { week: '2026-03-14', registered: 1450, eligible: 720, total: 2170 },
        { week: '2026-03-07', registered: 1440, eligible: 725, total: 2165 },
        { week: '2026-02-28', registered: 1430, eligible: 730, total: 2160 },
        { week: '2026-02-21', registered: 1420, eligible: 735, total: 2155 },
        { week: '2026-02-14', registered: 1410, eligible: 740, total: 2150 },
        { week: '2026-02-07', registered: 1400, eligible: 745, total: 2145 },
        { week: '2026-01-31', registered: 1390, eligible: 750, total: 2140 },
        { week: '2026-01-24', registered: 1380, eligible: 755, total: 2135 },
      ],
    },
    {
      name: 'Salt Lake City',
      registered: 1210, eligible: 690, total: 1900, regPct: 63.7,
      history: [
        { week: '2026-03-14', registered: 1210, eligible: 690, total: 1900 },
        { week: '2026-03-07', registered: 1200, eligible: 695, total: 1895 },
        { week: '2026-02-28', registered: 1190, eligible: 700, total: 1890 },
        { week: '2026-02-21', registered: 1180, eligible: 705, total: 1885 },
        { week: '2026-02-14', registered: 1170, eligible: 710, total: 1880 },
        { week: '2026-02-07', registered: 1160, eligible: 715, total: 1875 },
        { week: '2026-01-31', registered: 1150, eligible: 720, total: 1870 },
        { week: '2026-01-24', registered: 1140, eligible: 725, total: 1865 },
      ],
    },
    {
      name: 'Tucson',
      registered: 900, eligible: 400, total: 1300, regPct: 69.2,
      history: [
        { week: '2026-03-14', registered: 900, eligible: 400, total: 1300 },
        { week: '2026-03-07', registered: 895, eligible: 402, total: 1297 },
        { week: '2026-02-28', registered: 890, eligible: 404, total: 1294 },
        { week: '2026-02-21', registered: 885, eligible: 406, total: 1291 },
        { week: '2026-02-14', registered: 880, eligible: 408, total: 1288 },
        { week: '2026-02-07', registered: 875, eligible: 410, total: 1285 },
        { week: '2026-01-31', registered: 870, eligible: 412, total: 1282 },
        { week: '2026-01-24', registered: 865, eligible: 414, total: 1279 },
      ],
    },
  ],
  // Derived totals
  totalRegistered: 13100,
  totalEligible:    6320,
  grandTotal:      19420,
  regPct:           67.5,
}

// LME — daily total and cancelled warrants
export const staticLmeData = {
  date: '2026-03-14',
  total: 273500,
  cancelledWarrants: 91200,
  cancelledPct: 33.3,   // >30% threshold = BULL signal
  onWarrant: 182300,
}

// SHFE — regional breakdown (Shanghai, Guangdong, Jiangsu, Zhejiang)
export const staticShfeData = {
  date: '2026-03-14',
  regions: [
    {
      name: 'Shanghai', total: 52400,
      history: [
        { week: '2026-03-14', total: 52400 },
        { week: '2026-03-07', total: 53100 },
        { week: '2026-02-28', total: 53800 },
        { week: '2026-02-21', total: 54500 },
        { week: '2026-02-14', total: 55200 },
        { week: '2026-02-07', total: 55900 },
        { week: '2026-01-31', total: 56600 },
        { week: '2026-01-24', total: 57300 },
      ],
    },
    {
      name: 'Guangdong', total: 38200,
      history: [
        { week: '2026-03-14', total: 38200 },
        { week: '2026-03-07', total: 38600 },
        { week: '2026-02-28', total: 39000 },
        { week: '2026-02-21', total: 39400 },
        { week: '2026-02-14', total: 39800 },
        { week: '2026-02-07', total: 40200 },
        { week: '2026-01-31', total: 40600 },
        { week: '2026-01-24', total: 41000 },
      ],
    },
    {
      name: 'Jiangsu', total: 18600,
      history: [
        { week: '2026-03-14', total: 18600 },
        { week: '2026-03-07', total: 18800 },
        { week: '2026-02-28', total: 19000 },
        { week: '2026-02-21', total: 19200 },
        { week: '2026-02-14', total: 19400 },
        { week: '2026-02-07', total: 19600 },
        { week: '2026-01-31', total: 19800 },
        { week: '2026-01-24', total: 20000 },
      ],
    },
    {
      name: 'Zhejiang', total: 10600,
      history: [
        { week: '2026-03-14', total: 10600 },
        { week: '2026-03-07', total: 10700 },
        { week: '2026-02-28', total: 10800 },
        { week: '2026-02-21', total: 10900 },
        { week: '2026-02-14', total: 11000 },
        { week: '2026-02-07', total: 11100 },
        { week: '2026-01-31', total: 11200 },
        { week: '2026-01-24', total: 11300 },
      ],
    },
  ],
  grandTotal: 119800,
}
