// Mirrors COMEX tab (7 warehouses), LME tab, and SHFE tab schemas
// All weights in metric tons (mt)

// COMEX — 7 correct warehouses (NOT New York/Chicago from reference app)
export const staticComexData = {
  date: '2026-03-14',
  warehouses: [
    { name: 'Baltimore',      registered: 2340, eligible: 1120, total: 3460, regPct: 67.6 },
    { name: 'Detroit',        registered: 1890, eligible:  870, total: 2760, regPct: 68.5 },
    { name: 'El Paso',        registered: 3210, eligible: 1540, total: 4750, regPct: 67.6 },
    { name: 'New Orleans',    registered: 2100, eligible:  980, total: 3080, regPct: 68.2 },
    { name: 'Owensboro',      registered: 1450, eligible:  720, total: 2170, regPct: 66.8 },
    { name: 'Salt Lake City', registered: 1210, eligible:  690, total: 1900, regPct: 63.7 },
    { name: 'Tucson',         registered:  900, eligible:  400, total: 1300, regPct: 69.2 },
  ],
  // Derived totals
  totalRegistered: 13100,
  totalEligible:    6320,
  grandTotal:      19420,
  regPct:           67.5,
}

// COMEX 30-day history for charts
export const staticComexHistory = [
  { date: '2026-02-03', total: 19840, registered: 13400, eligible: 6440 },
  { date: '2026-02-04', total: 19620, registered: 13280, eligible: 6340 },
  { date: '2026-02-05', total: 19410, registered: 13150, eligible: 6260 },
  { date: '2026-02-06', total: 19280, registered: 13080, eligible: 6200 },
  { date: '2026-02-07', total: 19100, registered: 12960, eligible: 6140 },
  { date: '2026-02-10', total: 18980, registered: 12880, eligible: 6100 },
  { date: '2026-02-11', total: 18860, registered: 12780, eligible: 6080 },
  { date: '2026-02-12', total: 18740, registered: 12680, eligible: 6060 },
  { date: '2026-02-13', total: 18680, registered: 12620, eligible: 6060 },
  { date: '2026-02-14', total: 18620, registered: 12560, eligible: 6060 },
  { date: '2026-02-17', total: 18560, registered: 12500, eligible: 6060 },
  { date: '2026-02-18', total: 18510, registered: 12460, eligible: 6050 },
  { date: '2026-02-19', total: 18480, registered: 12440, eligible: 6040 },
  { date: '2026-02-20', total: 18460, registered: 12430, eligible: 6030 },
  { date: '2026-02-21', total: 18450, registered: 12420, eligible: 6030 },
  { date: '2026-02-24', total: 18440, registered: 12415, eligible: 6025 },
  { date: '2026-02-25', total: 18435, registered: 12410, eligible: 6025 },
  { date: '2026-02-26', total: 18430, registered: 12408, eligible: 6022 },
  { date: '2026-02-27', total: 18428, registered: 12406, eligible: 6022 },
  { date: '2026-02-28', total: 18426, registered: 12404, eligible: 6022 },
  { date: '2026-03-03', total: 18424, registered: 12402, eligible: 6022 },
  { date: '2026-03-04', total: 18422, registered: 12402, eligible: 6020 },
  { date: '2026-03-05', total: 18420, registered: 12401, eligible: 6019 },
  { date: '2026-03-06', total: 18420, registered: 12401, eligible: 6019 },
  { date: '2026-03-07', total: 18420, registered: 12400, eligible: 6020 },
  { date: '2026-03-10', total: 18418, registered: 12398, eligible: 6020 },
  { date: '2026-03-11', total: 18415, registered: 12396, eligible: 6019 },
  { date: '2026-03-12', total: 18412, registered: 12394, eligible: 6018 },
  { date: '2026-03-13', total: 18410, registered: 12402, eligible: 6008 },
  { date: '2026-03-14', total: 18420, registered: 12400, eligible: 6020 },
]

// LME — daily total and cancelled warrants
export const staticLmeData = {
  date: '2026-03-14',
  total: 273500,
  cancelledWarrants: 91200,
  cancelledPct: 33.3,   // >30% threshold = BULL signal
  onWarrant: 182300,
}

export const staticLmeHistory = [
  { date: '2026-02-03', total: 268400, cancelledWarrants: 88500, cancelledPct: 33.0 },
  { date: '2026-02-04', total: 269100, cancelledWarrants: 89200, cancelledPct: 33.2 },
  { date: '2026-02-05', total: 270200, cancelledWarrants: 89800, cancelledPct: 33.2 },
  { date: '2026-02-06', total: 271000, cancelledWarrants: 90100, cancelledPct: 33.2 },
  { date: '2026-02-07', total: 271800, cancelledWarrants: 90400, cancelledPct: 33.3 },
  { date: '2026-02-10', total: 272200, cancelledWarrants: 90600, cancelledPct: 33.3 },
  { date: '2026-02-11', total: 272500, cancelledWarrants: 90800, cancelledPct: 33.3 },
  { date: '2026-02-12', total: 273000, cancelledWarrants: 91000, cancelledPct: 33.3 },
  { date: '2026-02-13', total: 273400, cancelledWarrants: 91100, cancelledPct: 33.3 },
  { date: '2026-02-14', total: 273800, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-17', total: 274100, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-18', total: 274300, cancelledWarrants: 91200, cancelledPct: 33.2 },
  { date: '2026-02-19', total: 274200, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-20', total: 274000, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-21', total: 273800, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-24', total: 273700, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-25', total: 273600, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-26', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-27', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-02-28', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-03', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-04', total: 273600, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-05', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-06', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-07', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-10', total: 273400, cancelledWarrants: 91100, cancelledPct: 33.3 },
  { date: '2026-03-11', total: 273350, cancelledWarrants: 91050, cancelledPct: 33.3 },
  { date: '2026-03-12', total: 273300, cancelledWarrants: 91000, cancelledPct: 33.3 },
  { date: '2026-03-13', total: 273350, cancelledWarrants: 91200, cancelledPct: 33.3 },
  { date: '2026-03-14', total: 273500, cancelledWarrants: 91200, cancelledPct: 33.3 },
]

// SHFE — regional breakdown (Shanghai, Guangdong, Jiangsu, Zhejiang)
export const staticShfeData = {
  date: '2026-03-14',
  regions: [
    { name: 'Shanghai',   total: 52400 },
    { name: 'Guangdong',  total: 38200 },
    { name: 'Jiangsu',    total: 18600 },
    { name: 'Zhejiang',   total: 10600 },
  ],
  grandTotal: 119800,
}

export const staticShfeHistory = [
  { date: '2026-02-03', total: 126200 },
  { date: '2026-02-04', total: 125600 },
  { date: '2026-02-05', total: 125000 },
  { date: '2026-02-06', total: 124200 },
  { date: '2026-02-07', total: 123500 },
  { date: '2026-02-10', total: 122800 },
  { date: '2026-02-11', total: 122200 },
  { date: '2026-02-12', total: 121600 },
  { date: '2026-02-13', total: 121000 },
  { date: '2026-02-14', total: 120400 },
  { date: '2026-02-17', total: 119900 },
  { date: '2026-02-18', total: 119500 },
  { date: '2026-02-19', total: 119200 },
  { date: '2026-02-20', total: 119000 },
  { date: '2026-02-21', total: 118800 },
  { date: '2026-02-24', total: 118600 },
  { date: '2026-02-25', total: 118400 },
  { date: '2026-02-26', total: 118200 },
  { date: '2026-02-27', total: 118100 },
  { date: '2026-02-28', total: 118000 },
  { date: '2026-03-03', total: 120500 },
  { date: '2026-03-04', total: 120200 },
  { date: '2026-03-05', total: 120000 },
  { date: '2026-03-06', total: 119900 },
  { date: '2026-03-07', total: 119800 },
  { date: '2026-03-10', total: 119700 },
  { date: '2026-03-11', total: 119650 },
  { date: '2026-03-12', total: 119600 },
  { date: '2026-03-13', total: 119550 },
  { date: '2026-03-14', total: 119800 },
]
