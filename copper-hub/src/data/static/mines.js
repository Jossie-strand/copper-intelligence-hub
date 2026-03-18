// 20 major copper mines — correct field names, realistic magnitudes
// annualCapacity and h1Production in kt (thousand metric tons)

export const staticMinesData = [
  { name: 'Escondida',        country: 'Chile',    operator: 'BHP',                annualCapacityKt: 1180, h1ProductionKt: 568, status: 'operating',   note: '',                                      lat: -24.269, lng: -69.074 },
  { name: 'Grasberg',         country: 'Indonesia',operator: 'Freeport-McMoRan',   annualCapacityKt:  680, h1ProductionKt: 312, status: 'operating',   note: '',                                      lat:  -4.055, lng: 137.116 },
  { name: 'Collahuasi',       country: 'Chile',    operator: 'Anglo American',     annualCapacityKt:  620, h1ProductionKt: 298, status: 'operating',   note: '',                                      lat: -20.983, lng: -68.697 },
  { name: 'Morenci',          country: 'USA',      operator: 'Freeport-McMoRan',   annualCapacityKt:  490, h1ProductionKt: 235, status: 'operating',   note: '',                                      lat:  33.091, lng: -109.353 },
  { name: 'Cerro Verde',      country: 'Peru',     operator: 'Freeport-McMoRan',   annualCapacityKt:  460, h1ProductionKt: 212, status: 'operating',   note: '',                                      lat: -16.553, lng: -71.671 },
  { name: 'Antamina',         country: 'Peru',     operator: 'BHP / Glencore',     annualCapacityKt:  420, h1ProductionKt: 198, status: 'operating',   note: '',                                      lat:  -9.521, lng: -77.052 },
  { name: 'Las Bambas',       country: 'Peru',     operator: 'MMG',                annualCapacityKt:  380, h1ProductionKt: 162, status: 'constrained', note: 'Community access road disputes',        lat: -14.148, lng: -72.733 },
  { name: 'El Teniente',      country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  470, h1ProductionKt: 210, status: 'operating',   note: '',                                      lat: -34.091, lng: -70.358 },
  { name: 'Antofagasta Mins.',country: 'Chile',    operator: 'Antofagasta PLC',    annualCapacityKt:  710, h1ProductionKt: 338, status: 'operating',   note: '',                                      lat: -31.823, lng: -70.752 },
  { name: 'Centinela',        country: 'Chile',    operator: 'Antofagasta PLC',    annualCapacityKt:  260, h1ProductionKt: 122, status: 'operating',   note: '',                                      lat: -22.694, lng: -69.138 },
  { name: 'Chuquicamata',     country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  340, h1ProductionKt: 152, status: 'operating',   note: '',                                      lat: -22.309, lng: -68.924 },
  { name: 'Buenavista (Cananea)', country: 'Mexico', operator: 'Grupo Mexico',    annualCapacityKt:  420, h1ProductionKt: 195, status: 'operating',   note: '',                                      lat:  30.967, lng: -110.298 },
  { name: 'Oyu Tolgoi',       country: 'Mongolia', operator: 'Rio Tinto / TRQ',   annualCapacityKt:  500, h1ProductionKt: 228, status: 'operating',   note: '',                                      lat:  43.000, lng: 106.847 },
  { name: 'Kamoa-Kakula',     country: 'DRC',      operator: 'Ivanhoe / Zijin',    annualCapacityKt:  620, h1ProductionKt: 295, status: 'operating',   note: '',                                      lat: -10.776, lng:  26.654 },
  { name: 'Tenke Fungurume',  country: 'DRC',      operator: 'CMOC',               annualCapacityKt:  230, h1ProductionKt: 108, status: 'operating',   note: '',                                      lat: -10.609, lng:  26.117 },
  { name: 'Quellaveco',       country: 'Peru',     operator: 'Anglo American',     annualCapacityKt:  300, h1ProductionKt: 140, status: 'operating',   note: '',                                      lat: -16.939, lng: -70.609 },
  { name: 'Cobre Panama',     country: 'Panama',   operator: 'First Quantum',      annualCapacityKt:  350, h1ProductionKt:   0, status: 'disrupted',   note: 'Court-ordered shutdown Nov 2023 — idle', lat:   8.614, lng: -80.121 },
  { name: 'Highland Valley',  country: 'Canada',   operator: 'Teck Resources',     annualCapacityKt:  140, h1ProductionKt:  64, status: 'operating',   note: '',                                      lat:  50.472, lng: -121.007 },
  { name: 'Radomiro Tomic',   country: 'Chile',    operator: 'Codelco',            annualCapacityKt:  380, h1ProductionKt: 168, status: 'recovering',  note: 'Post-strike ramp-up',                   lat: -22.444, lng: -68.970 },
  { name: 'Cerro Colorado',   country: 'Chile',    operator: 'BHP',                annualCapacityKt:  110, h1ProductionKt:  48, status: 'constrained', note: 'Water usage restrictions',              lat: -18.979, lng: -69.508 },
]

// Summary stats derived from above
export const minesSummary = {
  totalAnnualCapacityKt: staticMinesData.reduce((s, m) => s + m.annualCapacityKt, 0),
  minesDisrupted: staticMinesData.filter(m => m.status === 'disrupted').length,
  minesConstrained: staticMinesData.filter(m => m.status === 'constrained').length,
  ktAtRiskAnnualized: 700, // Cobre Panama (350) + Las Bambas partial + Cerro Colorado partial
}
