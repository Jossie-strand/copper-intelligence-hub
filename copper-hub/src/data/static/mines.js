// 20 major copper mines — correct field names, realistic magnitudes
// annualCapacity and h1Production in kt (thousand metric tons)

export const staticMinesData = [
  { name: 'Escondida',        country: 'Chile',    operator: 'BHP',                annualCapacityKt: 1180, h1ProductionKt: 568, status: 'operating',   note: '' },
  { name: 'Grasberg',         country: 'Indonesia',operator: 'Freeport-McMoRan',   annualCapacityKt:  680, h1ProductionKt: 312, status: 'operating',   note: '' },
  { name: 'Collahuasi',       country: 'Chile',    operator: 'Anglo American',     annualCapacityKt:  620, h1ProductionKt: 298, status: 'operating',   note: '' },
  { name: 'Morenci',          country: 'USA',      operator: 'Freeport-McMoRan',   annualCapacityKt:  490, h1ProductionKt: 235, status: 'operating',   note: '' },
  { name: 'Cerro Verde',      country: 'Peru',     operator: 'Freeport-McMoRan',   annualCapacityKt:  460, h1ProductionKt: 212, status: 'operating',   note: '' },
  { name: 'Antamina',         country: 'Peru',     operator: 'BHP / Glencore',     annualCapacityKt:  420, h1ProductionKt: 198, status: 'operating',   note: '' },
  { name: 'Las Bambas',       country: 'Peru',     operator: 'MMG',                annualCapacityKt:  380, h1ProductionKt: 162, status: 'constrained', note: 'Community access road disputes' },
  { name: 'El Teniente',      country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  470, h1ProductionKt: 210, status: 'operating',   note: '' },
  { name: 'Antofagasta Mins.',country: 'Chile',    operator: 'Antofagasta PLC',    annualCapacityKt:  710, h1ProductionKt: 338, status: 'operating',   note: '' },
  { name: 'Centinela',        country: 'Chile',    operator: 'Antofagasta PLC',    annualCapacityKt:  260, h1ProductionKt: 122, status: 'operating',   note: '' },
  { name: 'Chuquicamata',     country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  340, h1ProductionKt: 152, status: 'operating',   note: '' },
  { name: 'Buenavista (Cananea)', country: 'Mexico', operator: 'Grupo Mexico',    annualCapacityKt:  420, h1ProductionKt: 195, status: 'operating',   note: '' },
  { name: 'Oyu Tolgoi',       country: 'Mongolia', operator: 'Rio Tinto / TRQ',   annualCapacityKt:  500, h1ProductionKt: 228, status: 'operating',   note: '' },
  { name: 'Kamoa-Kakula',     country: 'DRC',      operator: 'Ivanhoe / Zijin',    annualCapacityKt:  620, h1ProductionKt: 295, status: 'operating',   note: '' },
  { name: 'Tenke Fungurume',  country: 'DRC',      operator: 'CMOC',               annualCapacityKt:  230, h1ProductionKt: 108, status: 'operating',   note: '' },
  { name: 'Quellaveco',       country: 'Peru',     operator: 'Anglo American',     annualCapacityKt:  300, h1ProductionKt: 140, status: 'operating',   note: '' },
  { name: 'Cobre Panama',     country: 'Panama',   operator: 'First Quantum',      annualCapacityKt:  350, h1ProductionKt:   0, status: 'disrupted',   note: 'Court-ordered shutdown Nov 2023 — idle' },
  { name: 'Highland Valley',  country: 'Canada',   operator: 'Teck Resources',     annualCapacityKt:  140, h1ProductionKt:  64, status: 'operating',   note: '' },
  { name: 'Radomiro Tomic',   country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  380, h1ProductionKt: 168, status: 'recovering',  note: 'Post-strike ramp-up' },
  { name: 'Cerro Colorado',   country: 'Chile',    operator: 'BHP',                annualCapacityKt:  110, h1ProductionKt:  48, status: 'constrained', note: 'Water usage restrictions' },
]

// Summary stats derived from above
export const minesSummary = {
  totalAnnualCapacityKt: staticMinesData.reduce((s, m) => s + m.annualCapacityKt, 0),
  minesDisrupted: staticMinesData.filter(m => m.status === 'disrupted').length,
  minesConstrained: staticMinesData.filter(m => m.status === 'constrained').length,
  ktAtRiskAnnualized: 700, // Cobre Panama (350) + Las Bambas partial + Cerro Colorado partial
}
